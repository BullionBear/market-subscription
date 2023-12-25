import aio_pika

class PikaHandler:
    def __init__(self, uri, username, password, port=5672):
        self.uri = uri
        self.username = username
        self.password = password
        self.port = port
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None

    async def connect(self, exchange_name, queue_name, topic):
        self.connection = await aio_pika.connect_robust(
            host=self.uri,
            port=self.port,
            login=self.username,
            password=self.password
        )

        # Create a channel
        self.channel = await self.connection.channel()

        # Declare the exchange
        self.exchange = await self.channel.declare_exchange(exchange_name, aio_pika.ExchangeType.DIRECT)

        # Declare a queue and bind it to the exchange
        self.queue = await self.channel.declare_queue(queue_name, durable=True)
        await self.queue.bind(self.exchange, routing_key=topic)

    async def subscribe(self, callback: callable):
        # Set up a consumer
        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    # Call the callback function with the message
                    await callback(message)

# Example usage
# Define a callback function to handle incoming messages
# async def handle_message(message: aio_pika.IncomingMessage):
#     with message.process():
#         print("Received message:", message.body.decode())

# pika_handler = PikaHandler('uri', 'username', 'password')
# await pika_handler.connect('exchange_name', 'queue_name', 'topic')
# await pika_handler.subscribe(handle_message)
