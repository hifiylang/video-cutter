import json
from kafka import KafkaProducer

from app.config import get_config
from app.utils.logging import get_logger


class Producer:
    def __init__(self):
        cfg = get_config()
        self.topic = cfg.kafka_output_topic
        self.log = get_logger("producer")
        self.producer = KafkaProducer(
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.log.info(f"producer ready topic={self.topic} servers={cfg.kafka_bootstrap_servers}")

    def send(self, payload: dict, topic: str):
        job_id = payload.get("job_id")
        chunks = payload.get("chunks", [])
        fut = self.producer.send(topic, value=payload)
        fut.get(timeout=60)
        self.log.info(f"send end job_id={job_id} chunks={len(chunks)} topic={topic} payload={payload}")

    def close(self):
        self.log.info("producer closing")
        self.producer.flush()
        self.producer.close()
