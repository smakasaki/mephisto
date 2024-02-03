import asyncio
import aio_pika


async def worker():
    connection = await aio_pika.connect_robust("amqp://user:password@localhost/")
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)
    request_queue = await channel.declare_queue('request_queue', durable=True)

    async for message in request_queue:
        async with message.process():
            print("Received request:", message.body.decode())

            # Здесь может быть логика обработки сообщения
            response_message = f"Processed: {message.body.decode()}"

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
