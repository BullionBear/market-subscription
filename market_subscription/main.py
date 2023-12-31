import asyncio
from message_handler import PikaHandler
import time


async def process_message(message):
    print("Received message:", message.body.decode())


async def main():
    pika_handler = PikaHandler('localhost', 'cryptostream', 'bullionbear')
    await pika_handler.connect('market', str(time.time()), "depth5.*")
    # Step 4: Subscribe to the Topic
    await pika_handler.subscribe(callback=process_message)


if __name__ == '__main__':
    asyncio.run(main())
