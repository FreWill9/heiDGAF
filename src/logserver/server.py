import asyncio
import datetime
import os
import sys
import uuid

import aiofiles
from pathlib import Path

sys.path.append(os.getcwd())    # noqa: E402
from src.base.kafka_handler import (
    SimpleKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    SimpleKafkaProduceHandler,
)
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.utils import setup_config
from src.base.log_config import get_logger

module_name = "log_storage.logserver"
logger = get_logger(module_name)

config = setup_config()
CONSUME_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]
PRODUCE_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "logserver_to_collector"
]
READ_FROM_FILES = config["pipeline"]["log_storage"]["logserver"]["input_files"]
KAFKA_BROKERS = ",".join(
    [
        f"{broker['hostname']}:{broker['port']}"
        for broker in config["environment"]["kafka_brokers"]
    ]
)


class LogServer:
    """
    Receives and sends single log lines. Listens for messages via Kafka and reads newly added lines from an input
    file.
    """

    def __init__(self) -> None:
        self.kafka_consume_handler = SimpleKafkaConsumeHandler(CONSUME_TOPIC)
        # self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()
        self.kafka_produce_handler = SimpleKafkaProduceHandler()

        # databases
        self.server_logs = ClickHouseKafkaSender("server_logs")
        self.server_logs_timestamps = ClickHouseKafkaSender("server_logs_timestamps")

    async def start(self) -> None:
        """
        Starts fetching messages from Kafka and from the input file.
        """
        logger.info("LogServer started")
        logger.debug(f"Global READ_FROM_FILES: {READ_FROM_FILES}")

        files = [file for file in READ_FROM_FILES if Path(file).is_file()]
        if len(files) == 0:
            logger.warning("None of the given input files found.")

        logger.info(
            "LogServer:\n"
            f"    ⤷  receiving on Kafka topic '{CONSUME_TOPIC}'\n"
            f"    ⤷  receiving from input files '{files}'\n"
            f"    ⤷  sending on Kafka topic '{PRODUCE_TOPIC}'"
        )

        task_fetch_kafka = asyncio.Task(self.fetch_from_kafka())
        tasks_fetch_files = [asyncio.create_task(self.fetch_from_file(file)) for file in files]

        try:
            task = asyncio.gather(
                task_fetch_kafka,
                *tasks_fetch_files
            )
            await task
        except KeyboardInterrupt:
            task_fetch_kafka.cancel()
            map(lambda x: x.lower(), tasks_fetch_files)

            logger.info("LogServer stopped.")

    def send(self, message_id: uuid.UUID, message: str) -> None:
        """
        Sends a received message using Kafka.

        Args:
            message_id (uuid.UUID): UUID of the message.
            message (str): Message to be sent.
        """
        self.kafka_produce_handler.produce(topic=PRODUCE_TOPIC, data=message)
        logger.debug(f"Sent: '{message}'")

        self.server_logs_timestamps.insert(
            dict(
                message_id=message_id,
                event="timestamp_out",
                event_timestamp=datetime.datetime.now(),
            )
        )

    async def fetch_from_kafka(self) -> None:
        """
        Starts a loop to continuously listen on the configured Kafka topic. If a message is consumed, it is sent.
        """
        loop = asyncio.get_running_loop()

        while True:
            key, value, topic = await loop.run_in_executor(
                None, self.kafka_consume_handler.consume
            )
            logger.debug(f"From Kafka: '{value}'")

            message_id = uuid.uuid4()
            self.server_logs.insert(
                dict(
                    message_id=message_id,
                    timestamp_in=datetime.datetime.now(),
                    message_text=value,
                )
            )

            self.send(message_id, value)

    async def get_inode(self, path):
        try:
            return os.stat(path).st_ino
        except FileNotFoundError:
            return None

    async def fetch_from_file(self, file_path: str) -> None:
        """
        Continuously checks for new lines at the end of the input file(s). If one or multiple new lines are found, any
        empty lines are removed and the remaining lines are sent individually.

        Args:
            file_path (str): Filename of the file to be read
        """
        last_inode = await self.get_inode(file_path)
        file = None

        async def open_and_seek(seek_end: bool = False):
            f = await aiofiles.open(file_path, mode="r")
            if seek_end:
                await f.seek(0, os.SEEK_END)
            return f

        if last_inode is not None:
            file = await open_and_seek(seek_end=False)

        try:
            while True:
                current_inode = await self.get_inode(file_path)

                if current_inode is None:
                    # File temporarily missing (probably during rotation), retry later
                    logger.debug(f"Log temporarily missing, probably due to rotation")
                    await asyncio.sleep(0.5)
                    continue

                if current_inode != last_inode:
                    # Detected file rotation
                    logger.info(f"Log rotation detected for {file_path}")
                    if file:
                        await file.close()
                    file = await open_and_seek(seek_end=False)
                    last_inode = current_inode

                if file:
                    lines = await file.readlines()
                else:
                    lines = None

                if not lines:
                    await asyncio.sleep(0.1)
                    continue

                for line in lines:
                    cleaned_line = line.strip()  # remove empty lines

                    if not cleaned_line:
                        continue

                    logger.debug(f"From {file.name}: '{cleaned_line}'")

                    message_id = uuid.uuid4()
                    self.server_logs.insert(
                        dict(
                            message_id=message_id,
                            timestamp_in=datetime.datetime.now(),
                            message_text=cleaned_line,
                        )
                    )

                    self.send(message_id, cleaned_line)
        finally:
            if file:
                await file.close()


def main() -> None:
    """
    Creates the :class:`LogServer` instance and starts it.
    """
    # Run Program
    server_instance = LogServer()
    asyncio.run(server_instance.start())


if __name__ == "__main__":  # pragma: no cover
    main()
