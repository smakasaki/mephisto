from fastapi import FastAPI, BackgroundTasks, HTTPException
from redis.asyncio import Redis as AsyncRedis
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import List
import uvicorn
import aio_pika
import uuid
import random
import json


class KMeansRequest(BaseModel):
    data_points: List[List[float]]  # Список точек для кластеризации
    num_clusters: int  # Количество кластеров
    # Максимальное количество итераций (с дефолтным значением)
    max_iterations: int = 100


def generate_initial_centroids(data_points: List[List[float]], num_clusters: int) -> List[List[float]]:
    """Генерирует случайные начальные центроиды из данных."""
    return random.sample(data_points, num_clusters)


def split_data(data_points: List[List[float]], num_parts: int) -> List[List[List[float]]]:
    """Разбивает список точек на num_parts частей."""
    split_data = []
    split_size = len(data_points) // num_parts
    remainder = len(data_points) % num_parts

    start = 0
    for i in range(num_parts):
        end = start + split_size + (1 if i < remainder else 0)
        split_data.append(data_points[start:end])
        start = end

    return split_data


async def create_rabbitmq_connection():
    connection = await aio_pika.connect_robust("amqp://user:password@localhost/")
    return connection


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.rabbitmq_connection = await create_rabbitmq_connection()
    app.state.redis = await AsyncRedis(host='localhost', port=6379, db=0, decode_responses=True)
    yield
    await app.state.rabbitmq_connection.close()
    await app.state.redis.aclose()

app = FastAPI(lifespan=lifespan)


@app.post("/cluster/")
async def cluster_data(kmeans_request: KMeansRequest, background_tasks: BackgroundTasks):
    correlation_id = str(uuid.uuid4())
    await app.state.redis.set(f"total_distance:{correlation_id}", 0)
    await app.state.redis.expire(f"total_distance:{correlation_id}", 900)
    # Сохранение исходных точек данных, количества кластеров и максимального количества итераций в Redis
    await app.state.redis.set(f"config:{correlation_id}:data_points", json.dumps(kmeans_request.data_points))
    await app.state.redis.set(f"config:{correlation_id}:num_clusters", kmeans_request.num_clusters)
    await app.state.redis.set(f"config:{correlation_id}:max_iterations", kmeans_request.max_iterations)
    # Установить время жизни для конфигурационных данных
    await app.state.redis.expire(f"config:{correlation_id}:data_points", 900)
    await app.state.redis.expire(f"config:{correlation_id}:num_clusters", 900)
    await app.state.redis.expire(f"config:{correlation_id}:max_iterations", 900)

    num_parts = 3  # Примерное количество частей, на которые вы хотите разделить данные
    data_parts = split_data(kmeans_request.data_points, num_parts)
    initial_centroids = generate_initial_centroids(
        kmeans_request.data_points, kmeans_request.num_clusters)

    # Установка счетчика задач в Redis
    tasks_key = f"tasks:{correlation_id}"
    await app.state.redis.set(tasks_key, len(data_parts))
    # Установить время жизни ключа
    await app.state.redis.expire(tasks_key, 900)

    for part in data_parts:
        task_data = {
            "data_points": part,
            "num_clusters": kmeans_request.num_clusters,
            "max_iterations": kmeans_request.max_iterations,
            "initial_centroids": initial_centroids
        }
        background_tasks.add_task(
            send_and_receive_rabbitmq_message, task_data, correlation_id, background_tasks)

    return {"message": "K-means clustering initiated, processing in background.", "correlation_id": correlation_id}


@app.get("/result/{correlation_id}")
async def get_result(correlation_id: str):
    results = await app.state.redis.hgetall(f"results:{correlation_id}")
    if not results:
        raise HTTPException(
            status_code=404, detail="Result not available yet or correlation_id is invalid."
        )
    results = {key: value for key, value in results.items()}
    return {"correlation_id": correlation_id, "results": results}


async def send_and_receive_rabbitmq_message(task_data: dict, correlation_id: str, background_tasks: BackgroundTasks):
    connection = app.state.rabbitmq_connection
    async with connection.channel() as channel:
        response_queue = await channel.declare_queue('', exclusive=True)
        task_data_json = json.dumps(task_data).encode()

        await channel.default_exchange.publish(
            aio_pika.Message(
                body=task_data_json,
                correlation_id=correlation_id,
                reply_to=response_queue.name,
            ),
            routing_key='request_queue',
        )

        async for message in response_queue:
            if message.correlation_id == correlation_id:
                key = f"results:{correlation_id}"
                field = str(uuid.uuid4())
                value = message.body.decode()

                await app.state.redis.hset(key, field, value)
                await app.state.redis.expire(key, 900)

                await message.ack()

                tasks_key = f"tasks:{correlation_id}"
                tasks_left = await app.state.redis.decr(tasks_key)
                if tasks_left == 0:
                    background_tasks.add_task(
                        aggregate_centroids, correlation_id, background_tasks)
                break


async def aggregate_centroids(correlation_id: str, background_tasks: BackgroundTasks):
    results_key = f"results:{correlation_id}"
    results = await app.state.redis.hgetall(results_key)

    # Инициализация агрегированных данных
    centroid_sums = {}
    centroid_counts = {}
    total_distance = 0.0

    # Агрегация результатов
    for result in results.values():
        data = json.loads(result)
        centroids = data.get('centroids', [])
        distance = data.get('total_distance', 0)
        total_distance += distance

        for centroid in centroids:
            centroid_id = centroid['id']
            coordinates = centroid['coordinates']
            if centroid_id not in centroid_sums:
                centroid_sums[centroid_id] = [0.0] * len(coordinates)
                centroid_counts[centroid_id] = 0
            centroid_sums[centroid_id] = [x + y for x,
                                          y in zip(centroid_sums[centroid_id], coordinates)]
            centroid_counts[centroid_id] += 1

    # Вычисление новых центроидов
    new_centroids = []
    for centroid_id, sums in centroid_sums.items():
        count = centroid_counts[centroid_id]
        new_centroids.append({
            "id": centroid_id,
            "coordinates": [x / count for x in sums]
        })

    # Проверка условия сходимости
    convergence_threshold = 0.01
    prev_distance = await app.state.redis.get(f"total_distance:{correlation_id}")
    delta = abs(float(prev_distance) - total_distance)

    print('\nPrev distance: ' + str(prev_distance))
    print('Total distance: ' + str(total_distance))
    print('Delta: ' + str(delta) + '\n')

    if delta > convergence_threshold:
        await app.state.redis.set(f"total_distance:{correlation_id}", total_distance)
        await app.state.redis.expire(f"total_distance:{correlation_id}", 900)
        # Извлечение исходных данных и параметров кластеризации из Redis
        original_data_points_json = await app.state.redis.get(f"config:{correlation_id}:data_points")
        original_data_points = json.loads(original_data_points_json)
        num_clusters = await app.state.redis.get(f"config:{correlation_id}:num_clusters")
        max_iterations = await app.state.redis.get(f"config:{correlation_id}:max_iterations")

        # Повторное использование функции разделения данных
        data_parts = split_data(original_data_points, 3)
        tasks_key = f"tasks:{correlation_id}"
        await app.state.redis.set(tasks_key, len(data_parts))

        for part in data_parts:
            task_data = {
                "data_points": part,
                "num_clusters": int(num_clusters),
                "max_iterations": int(max_iterations),
                "initial_centroids": [centroid['coordinates'] for centroid in new_centroids]
            }

            background_tasks.add_task(
                send_and_receive_rabbitmq_message, task_data, correlation_id, background_tasks)
    else:
        # Условие сходимости выполнено
        print("Convergence achieved.")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
