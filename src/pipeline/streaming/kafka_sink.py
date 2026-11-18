import asyncio
import logging
import json
from dataclasses import asdict
from typing import Any, Sequence
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

def on_delivery(err:Any, msg:Any) -> None:
    if err is not None:
        logger.error("Failed to deliver trade: %s", err)
    else:
        logger.debug(
            "Delivered to %s [partition=%d, offset=%d]",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )

class KafkaTradeSink:
    def __init__(self, bootstrap_servers:str = "localhost:9092", topic:str = "trades.raw", producer:Any | None = None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        if producer is None:
            self.producer = Producer({"bootstrap.servers":bootstrap_servers})
        else:
            self.producer = producer

    async def write(self, items: Sequence[Any]) -> None:
        if not items:
            return
        def _sync_write() -> None:
            for trade in items:
                obj = json.dumps(asdict(trade)).encode("utf-8")
                key = trade.symbol.encode('utf-8')
                while True:
                    try:
                        self.producer.produce(
                            topic=self.topic,
                            key=key,
                            value=obj,
                            on_delivery=on_delivery
                        )
                        self.producer.poll(0)
                        break
                    except BufferError:
                        logger.warning("Kafka's queue is full, wait...")
                        self.producer.poll(0.5)

            remaining = self.producer.flush(timeout=10.0)
            if remaining > 0:
                logger.warning("%d trades remaining un-flushed after timeout", remaining)
        await asyncio.to_thread(_sync_write)

class KafkaDlqSink:
    def __init__(self, topic:str="trades.dlq", bootstrap_servers:str="localhost:9092", producer:Any | None = None):
        self.topic=topic
        self.bootstrap_servers=bootstrap_servers
        if producer is None:
            self.producer = Producer({"bootstrap.servers":bootstrap_servers})
        else:
            self.producer = producer
    
    async def write(self, items: Sequence[Any]) -> None:
            if not items:
                return
            def _sync_write() -> None:
                for item in items:
                    obj = json.dumps(item, default=str).encode('utf-8')
                    if isinstance(item.get('raw'), dict) and isinstance(item['raw'].get('s'), str) and len(item['raw']['s']) > 0:
                        key = item['raw']['s'].encode('utf-8')
                    else:
                        key=None
                    while True:
                        try:
                            self.producer.produce(
                                key=key,
                                value=obj,
                                topic=self.topic,
                                on_delivery=on_delivery
                            )
                            self.producer.poll(0)
                            break
                        except BufferError:
                            logger.warning("Kafka's queue is full, wait 0.5 sec...")
                            self.producer.poll(0.5)
                remaining = self.producer.flush(timeout=10.0)
                if remaining > 0:
                    logger.warning("%d rejected-trades remaining un-flushed after timeout", remaining)
            await asyncio.to_thread(_sync_write)


