import multiprocessing
import os
import shutil
import uuid

from fastapi import FastAPI

from app.config import get_config
from app.models.schema import ChunkInfo, VideoJobInput, VideoJobOutput
from app.services.kafka.consumer import Consumer
from app.services.kafka.flv_consumer import FlvStreamConsumer
from app.services.kafka.producer import Producer
from app.services.tos.client import TOSClient
from app.services.video.chunker import split_file
from app.services.video.downloader import download_to_file
from app.services.video.flv_streamer import run_flv_process
from app.utils.logging import get_logger
from app.utils.logging import setup_logging

main_log = get_logger("main")


cfg = get_config()
setup_logging(cfg.log_level)
env = os.getenv("APP_ENV", "dev").lower()

# 新增：消费者启动开关（默认开启）
ENABLE_CONSUMER = os.getenv("ENABLE_CONSUMER", "true").lower() in ("1", "true", "yes")
ENABLE_FLV_CONSUMER = os.getenv("ENABLE_FLV_CONSUMER", "true").lower() in ("1", "true", "yes")

app = FastAPI()

consumer = None
flv_consumer = None

@app.on_event("startup")
def on_startup():
    global consumer, flv_consumer
    try:
        main_log.info("app starting up environment=%s", env)
        if ENABLE_CONSUMER:
            consumer = Consumer()
            consumer.start()
            main_log.info("consumer started (ENABLE_CONSUMER=%s)", ENABLE_CONSUMER)
        else:
            main_log.info("consumer disabled by env (ENABLE_CONSUMER=%s)", ENABLE_CONSUMER)

        if ENABLE_FLV_CONSUMER:
            flv_consumer = FlvStreamConsumer()
            flv_consumer.start()
            main_log.info("flv_consumer started (ENABLE_FLV_CONSUMER=%s)", ENABLE_FLV_CONSUMER)
        else:
            main_log.info("flv_consumer disabled by env (ENABLE_FLV_CONSUMER=%s)", ENABLE_FLV_CONSUMER)
    except Exception as e:
        main_log.error(f"consumer init failed: {e}")
        # Re-raise the exception to prevent the app from starting in a broken state
        raise


@app.on_event("shutdown")
def on_shutdown():
    if consumer:
        consumer.stop()
    if flv_consumer:
        flv_consumer.stop()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/test/cutter")
def test_split_url(job: VideoJobInput):
    job_id = job.job_id or uuid.uuid4().hex
    base_dir = os.path.join(cfg.data_dir, job_id)
    file_path, fname, total_size = download_to_file(job.source_url, base_dir)
    main_log.info("[%s] Downloaded file %s (%d bytes)", job_id, fname, total_size)
    parts = split_file(file_path, 30 * 1024 * 1024, os.path.join(base_dir, "chunks"))
    main_log.info("[%s] Split into %d parts", job_id, len(parts))
    prefix = job.prefix or "videoCutter"
    tos = TOSClient()
    out_chunks = []

    for part in parts:
        ext = os.path.splitext(part["path"])[1]
        object_key = f"{prefix}/{job_id}/part-{part['index']:02d}{ext}"
        tos.upload_file(object_key, part["path"])
        url = tos.build_public_url(object_key)
        out_chunks.append(ChunkInfo(index=part["index"], size=part["size"], url=url, start_time=part["start_time"],
                                    end_time=part["end_time"]))

    try:
        shutil.rmtree(base_dir)
    except Exception:
        pass

    return VideoJobOutput(job_id=job_id, total_size=total_size, chunks=out_chunks, ext=job.ext)


@app.post("/test/sendInput")
def test_send_input(job: VideoJobInput):
    job_id = job.job_id or uuid.uuid4().hex
    payload = job.dict()
    payload["job_id"] = job_id
    main_log.info("[%s] Sending message to Kafka topic=%s", job_id, cfg.kafka_input_topic)
    p = Producer()
    try:
        p.send(payload, topic=cfg.kafka_input_topic)
    finally:
        p.close()
    return {"status": "ok", "topic": cfg.kafka_input_topic, "job_id": job_id}


@app.post("/test/flvStream")
def test_flv_stream(job: VideoJobInput):
    """
    Start a standalone FLV stream monitor that segments every 5 minutes and uploads/sends Kafka messages.
    The call returns immediately with the spawned process id; process exits when the stream ends.
    """
    job_id = job.job_id or uuid.uuid4().hex
    payload = job.dict()
    payload["job_id"] = job_id
    if not payload.get("ext"):
        payload["ext"] = "flv"
    if not payload["source_url"].lower().endswith(".flv"):
        main_log.warning("[%s] source_url does not end with .flv; continuing anyway", job_id)
    ctx = multiprocessing.get_context("spawn")
    status_queue = ctx.Queue()
    proc = ctx.Process(target=run_flv_process, args=(payload, status_queue))
    proc.start()
    main_log.info("[%s] started flv stream monitor pid=%s url=%s", job_id, proc.pid, payload["source_url"])
    return {"status": "started", "job_id": job_id, "pid": proc.pid}
