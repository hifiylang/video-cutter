import json
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

from app.config import get_config
from app.models.schema import ChunkInfo, VideoJobInput, VideoJobOutput
from app.services.kafka.producer import Producer
from app.services.tos.client import TOSClient
from app.services.video.chunker import split_file
from app.services.video.downloader import download_to_file
from app.utils.logging import get_logger


class Consumer:
    def __init__(self):
        self.cfg = get_config()
        self.log = get_logger("consumer")
        self.consumer = KafkaConsumer(
            self.cfg.kafka_input_topic,
            bootstrap_servers=self.cfg.kafka_bootstrap_servers,
            group_id=self.cfg.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            max_poll_interval_ms=3600000,  # 1小时，避免长处理导致再均衡
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        self.producer = Producer()
        self.tos = TOSClient()
        self.thread = None
        self.running = False
        self.shutdown_event = threading.Event()
        # 单线程执行器用于每条消息的超时控制（一次只处理一条）
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.timeout_sec = 60 * 60  # 单条消息超时1小时

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.log.info(
            "consumer starting topic=%s group_id=%s servers=%s",
            self.cfg.kafka_input_topic,
            self.cfg.kafka_group_id,
            self.cfg.kafka_bootstrap_servers,
        )
        self.thread.start()

    def stop(self):
        # signal stop and wait for loop to finish gracefully
        self.running = False
        self.shutdown_event.set()
        self.log.info("consumer stopping")
        if self.thread:
            self.thread.join(timeout=30)
        # Ensure resources closed
        try:
            self.consumer.close()
        except Exception:
            pass
        try:
            self.producer.close()
        except Exception:
            pass
        try:
            self.executor.shutdown(wait=True)
        except Exception:
            pass

    def _process(self, payload: dict) -> VideoJobOutput:
        """业务处理，返回结果；总是清理临时目录。"""
        job = VideoJobInput(**payload)
        job_id = job.job_id
        base_dir = os.path.join(self.cfg.data_dir, job_id)
        os.makedirs(base_dir, exist_ok=True)
        self.log.info("job started job_id=%s source_url=%s", job_id, job.source_url)
        try:
            file_path, fname, total_size = download_to_file(job.source_url, base_dir)
            size_mb = round(total_size / (1024 * 1024), 2) if isinstance(total_size, (int, float)) else total_size
            self.log.info("downloaded file job_id=%s path=%s name=%s size=%s MB", job_id, file_path, fname, size_mb)

            parts = split_file(file_path, 30 * 1024 * 1024, os.path.join(base_dir, "chunks"))
            self.log.info("split done job_id=%s parts=%s chunk_size=%s", job_id, len(parts), 30 * 1024 * 1024)

            prefix = job.prefix or "videoCutter"
            out_chunks = []
            for part in parts:
                ext = os.path.splitext(part["path"])[1]
                object_key = f"{prefix}/{job_id}/part-{part['index']:02d}{ext}"
                try:
                    res = self.tos.upload_file(object_key, part["path"])
                except Exception as e:
                    self.log.error(
                        "upload failed job_id=%s index=%s error=%s",
                        job_id,
                        part["index"],
                        e,
                        exc_info=True,
                    )
                    # 失败分片不追加占位记录，直接跳过
                    continue

                self.log.info(
                    "uploaded chunk job_id=%s index=%s size=%s key=%s status=%s request_id=%s",
                    job_id,
                    part["index"],
                    part["size"],
                    object_key,
                    res.get("status_code"),
                    res.get("request_id"),
                )
                url = self.tos.build_public_url(object_key)
                out_chunks.append(
                    ChunkInfo(
                        index=part["index"],
                        size=part["size"],
                        url=url,
                        start_time=part["start_time"],
                        end_time=part["end_time"],
                    )
                )

            out = VideoJobOutput(
                job_id=job_id,
                total_size=total_size,
                chunks=out_chunks,
                ext=job.ext,
            )
            self.log.info("produced result (not yet sent) job_id=%s chunks=%s", job_id, len(out_chunks))
            return out
        except Exception as e:
            self.log.error("Error processing job_id=%s: %s", job_id, e, exc_info=True)
            # 构造一个包含错误信息的输出对象
            return VideoJobOutput(
                job_id=job_id,
                total_size=0,
                chunks=[],
                ext=job.ext,
                error=str(e),
            )
        finally:
            # cleanup
            try:
                shutil.rmtree(base_dir)
                self.log.info("cleanup done job_id=%s path=%s", job_id, base_dir)
            except Exception as e:
                self.log.error("cleanup failed job_id=%s path=%s error=%s", job_id, base_dir, e)

    def _loop(self):
        # 串行：每次只处理一条消息；每条消息使用单线程执行器实现超时控制
        try:
            while self.running and not self.shutdown_event.is_set():
                try:
                    records = self.consumer.poll(timeout_ms=1000, max_records=1)
                except Exception as e:
                    self.log.error("consumer poll error: %s", e)
                    time.sleep(0.2)
                    continue

                # 逐条处理
                for tp, msgs in records.items():
                    for msg in msgs:
                        try:
                            part_tp = TopicPartition(msg.topic, msg.partition)
                            self.log.info(
                                "message received topic=%s partition=%s offset=%s",
                                msg.topic,
                                msg.partition,
                                msg.offset,
                            )
                            future = self.executor.submit(self._process, msg.value)
                            try:
                                out: VideoJobOutput = future.result(timeout=self.timeout_sec)
                                # 成功：发送结果后提交
                                self.producer.send(out.model_dump(), topic=self.cfg.kafka_output_topic)
                                try:
                                    commit_offset = msg.offset + 1
                                    self.consumer.commit({part_tp: OffsetAndMetadata(commit_offset, "", -1)})
                                    self.log.info(
                                        "commit succeeded topic=%s partition=%s offset=%s",
                                        part_tp.topic,
                                        part_tp.partition,
                                        msg.offset,
                                    )
                                except Exception as e:
                                    self.log.error(
                                        "commit failed topic=%s partition=%s offset=%s error=%s",
                                        part_tp.topic,
                                        part_tp.partition,
                                        msg.offset,
                                        e,
                                    )
                            except TimeoutError:
                                # 超时：构造错误并发送后提交
                                self.log.error(
                                    "Processing timed out for message topic=%s partition=%s offset=%s",
                                    msg.topic,
                                    msg.partition,
                                    msg.offset,
                                )
                                try:
                                    payload = msg.value
                                    job = VideoJobInput(**payload)
                                    out = VideoJobOutput(
                                        job_id=job.job_id,
                                        total_size=0,
                                        chunks=[],
                                        ext=job.ext,
                                        error="processing timeout (1 hour)",
                                    )
                                    self.producer.send(out.model_dump(), topic=self.cfg.kafka_output_topic)
                                except Exception as e:
                                    self.log.error(
                                        "Failed to build/send timeout error for message topic=%s partition=%s offset=%s: %s",
                                        msg.topic,
                                        msg.partition,
                                        msg.offset,
                                        e,
                                        exc_info=True,
                                    )
                                try:
                                    commit_offset = msg.offset + 1
                                    self.consumer.commit({part_tp: OffsetAndMetadata(commit_offset, "", -1)})
                                    self.log.info(
                                        "commit succeeded after timeout topic=%s partition=%s offset=%s",
                                        part_tp.topic,
                                        part_tp.partition,
                                        msg.offset,
                                    )
                                except Exception as commit_error:
                                    self.log.error(
                                        "commit failed after timeout topic=%s partition=%s offset=%s error=%s",
                                        msg.topic,
                                        msg.partition,
                                        msg.offset,
                                        commit_error,
                                    )
                            except Exception as e:
                                # 处理异常：构造错误并发送后提交
                                self.log.error(
                                    "Error processing message topic=%s partition=%s offset=%s: %s",
                                    msg.topic,
                                    msg.partition,
                                    msg.offset,
                                    e,
                                    exc_info=True,
                                )
                                try:
                                    payload = msg.value
                                    job = VideoJobInput(**payload)
                                    out = VideoJobOutput(
                                        job_id=job.job_id,
                                        total_size=0,
                                        chunks=[],
                                        ext=job.ext,
                                        error=str(e),
                                    )
                                    self.producer.send(out.model_dump(), topic=self.cfg.kafka_output_topic)
                                except Exception as ee:
                                    self.log.error(
                                        "Failed to build/send error for message topic=%s partition=%s offset=%s: %s",
                                        msg.topic,
                                        msg.partition,
                                        msg.offset,
                                        ee,
                                        exc_info=True,
                                    )
                                try:
                                    commit_offset = msg.offset + 1
                                    self.consumer.commit({part_tp: OffsetAndMetadata(commit_offset, "", -1)})
                                    self.log.info(
                                        "commit succeeded after error topic=%s partition=%s offset=%s",
                                        part_tp.topic,
                                        part_tp.partition,
                                        msg.offset,
                                    )
                                except Exception as commit_error:
                                    self.log.error(
                                        "commit failed after error topic=%s partition=%s offset=%s error=%s",
                                        msg.topic,
                                        msg.partition,
                                        msg.offset,
                                        commit_error,
                                    )
                        except Exception as e:
                            # 防御性：单条消息处理未捕获的异常
                            self.log.error(
                                "Unhandled error in message processing topic=%s partition=%s offset=%s: %s",
                                msg.topic,
                                msg.partition,
                                msg.offset,
                                e,
                                exc_info=True,
                            )
        except Exception as e:
            self.log.error("Unexpected error in consumer loop: %s", e, exc_info=True)
        finally:
            self.log.info("consumer loop exiting")
