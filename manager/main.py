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
    await app.state.redis.close()

app = FastAPI(lifespan=lifespan)


@app.post("/cluster/")
async def cluster_data(kmeans_request: KMeansRequest, background_tasks: BackgroundTasks):
    correlation_id = str(uuid.uuid4())
    num_parts = 3  # Примерное количество частей, на которые вы хотите разделить данные
    data_parts = split_data(kmeans_request.data_points, num_parts)
    print('\nData parts:' + str(data_parts) + '\n')
    initial_centroids = generate_initial_centroids(
        kmeans_request.data_points, kmeans_request.num_clusters)

    for part in data_parts:
        task_data = {
            "data_points": part,
            "num_clusters": kmeans_request.num_clusters,
            "max_iterations": kmeans_request.max_iterations,
            "initial_centroids": initial_centroids
        }
        background_tasks.add_task(
            send_and_receive_rabbitmq_message, task_data, correlation_id)

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


async def send_and_receive_rabbitmq_message(task_data: dict, correlation_id: str):
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
                print("Received response:", message.body.decode())
                key = f"results:{correlation_id}"
                field = str(uuid.uuid4())
                value = message.body.decode()

                await app.state.redis.hset(key, field, value)
                await app.state.redis.expire(key, 900)

                await message.ack()
                break


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
