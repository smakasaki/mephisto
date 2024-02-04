import asyncio
import aio_pika
import json
import numpy as np
from sklearn.cluster import KMeans


async def process_data(data_points, num_clusters, max_iterations):
    if num_clusters <= 0:  # Проверка корректности количества кластеров
        return "Ошибка: количество кластеров должно быть больше 0"

    kmeans = KMeans(n_clusters=num_clusters,
                    max_iter=max_iterations, init='random', n_init=1)
    kmeans.fit(data_points)

    centroids = kmeans.cluster_centers_.tolist()  # Преобразование в список
    labels = kmeans.labels_.tolist()  # Преобразование в список
    cluster_sizes = [labels.count(i) for i in range(num_clusters)]
    total_distance = kmeans.inertia_

    # Убедитесь, что все числовые значения преобразованы в float, чтобы избежать ошибок сериализации
    response = {
        "centroids": [{"id": i, "coordinates": centroid} for i, centroid in enumerate(centroids)],
        "assignments": [{"point": point.tolist(), "cluster_id": cluster_id} for point, cluster_id in zip(data_points, labels)],
        "cluster_sizes": [{"id": i, "size": size} for i, size in enumerate(cluster_sizes)],
        "total_distance": total_distance
    }

    return json.dumps(response)


async def worker():
    connection = await aio_pika.connect_robust("amqp://user:password@localhost/")
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)
    request_queue = await channel.declare_queue('request_queue', durable=True)

    async for message in request_queue:
        async with message.process():
            print("Received request:", message.body.decode())
            data = json.loads(message.body.decode())

            if "data_points" in data and "num_clusters" in data and "max_iterations" in data:
                data_points = np.array(data["data_points"])
                num_clusters = data["num_clusters"]
                max_iterations = data["max_iterations"]
                response_message = await process_data(data_points, num_clusters, max_iterations)
            else:
                response_message = "Ошибка: неправильный формат данных"

            # Отправка ответа обратно
            if message.reply_to:
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=response_message.encode(),
                        correlation_id=message.correlation_id,
                    ),
                    routing_key=message.reply_to,
                )

if __name__ == "__main__":
    asyncio.run(worker())
