#!/usr/bin/env python3
"""
Standalone runner to start only the Kafka Consumer without FastAPI server.
This is suitable for process managers like Supervisor or systemd.
"""
import os
import signal
import sys
import time

# Ensure project root is on sys.path so `import app.*` works when running this script from scripts/
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.config import get_config
from app.utils.logging import setup_logging, get_logger
from app.services.kafka.flv_consumer import FlvStreamConsumer


log = get_logger("flv-consumer-runner")


def main():
    cfg = get_config()
    setup_logging(cfg.log_level)

    consumer = FlvStreamConsumer()
    consumer.start()
    log.info("consumer process started (supervised) env=%s", os.getenv("APP_ENV", "dev"))

    # Graceful shutdown handling
    def _shutdown(signum, frame):
        log.info("shutdown signal received: %s", signum)
        try:
            consumer.stop()
        except Exception as e:
            log.error("error during consumer stop: %s", e)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Keep the process alive under Supervisor
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()
