import json
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

from app.config import get_config
from app.services.video.flv_streamer import run_flv_process
from app.utils.logging import get_logger


@dataclass
class _Task:
    tp: TopicPartition
    offset: int
    job_id: str | None
    future: Future
    started_at: float


class FlvStreamConsumer:
    def __init__(self):
        self.cfg = get_config()
        self.log = get_logger("flv-consumer")
        self.consumer = KafkaConsumer(
            self.cfg.kafka_flv_topic,
            bootstrap_servers=self.cfg.kafka_bootstrap_servers,
            group_id=self.cfg.kafka_flv_group_id or self.cfg.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            # Long-running processing (live capture) requires a longer poll interval.
            max_poll_interval_ms=3600000,  # 1 hour
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            # make fetches reasonably sized (we'll do our own backpressure)
            max_poll_records=50,
        )
        self.thread = None
        self.running = False

        # Concurrency: handle many live rooms in parallel.
        # NOTE: threads are fine here because the heavy work is done by ffmpeg subprocess.
        self.max_workers = int(getattr(self.cfg, "flv_max_workers", 8) or 8)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

        # Backpressure per partition.
        # IMPORTANT: If your FLV topic has few partitions (even 1), setting this to 1 will
        # serialize all tasks on that partition. For streaming jobs, it's usually better to
        # allow multiple inflight per partition and rely on job_id idempotency.
        self.max_inflight_per_partition = int(getattr(self.cfg, "flv_max_inflight_per_partition", 0) or 0)

        # Global inflight cap (should be <= max_workers)
        self.max_inflight_total = int(getattr(self.cfg, "flv_max_inflight_total", self.max_workers) or self.max_workers)

        # Commit strategy:
        # - "start": commit offset as soon as the task is accepted, so a single partition can
        #            dispatch many long-running streams without being blocked for hours.
        # - "finish": commit only when task finishes successfully (classic at-least-once).
        self.commit_mode = str(getattr(self.cfg, "flv_commit_mode", "start") or "start")

        # Inflight tracking
        self._inflight_by_tp: dict[TopicPartition, dict[int, _Task]] = {}

        # Commit tracking (watermark per partition)
        self._next_commit_offset: dict[TopicPartition, int] = {}
        self._done_offsets: dict[TopicPartition, set[int]] = {}

        self._paused: set[TopicPartition] = set()

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.log.info(
            "flv consumer starting topic=%s group_id=%s servers=%s workers=%s",
            self.cfg.kafka_flv_topic,
            self.cfg.kafka_flv_group_id,
            self.cfg.kafka_bootstrap_servers,
            self.max_workers,
        )
        self.thread.start()

    def stop(self):
        self.running = False
        self.log.info("flv consumer stopping")
        if self.thread:
            self.thread.join(timeout=30)
        try:
            self.consumer.close()
        except Exception:
            pass
        try:
            self.executor.shutdown(wait=False, cancel_futures=False)
        except Exception:
            pass

    def _mark_done(self, tp: TopicPartition, offset: int):
        # In commit-on-start mode we don't advance commit here.
        if self.commit_mode == "start":
            return

        done = self._done_offsets.setdefault(tp, set())
        done.add(offset)

        # Initialize watermark if needed (first offset seen)
        if tp not in self._next_commit_offset:
            self._next_commit_offset[tp] = offset

        # Advance contiguous watermark
        wm = self._next_commit_offset[tp]
        advanced = False
        while wm in done:
            done.remove(wm)
            wm += 1
            advanced = True
        if advanced:
            self._next_commit_offset[tp] = wm
            try:
                self.consumer.commit({tp: OffsetAndMetadata(wm, "", -1)})
                self.log.info("flv offset committed tp=%s offset=%s", tp, wm - 1)
            except Exception as e:
                self.log.error("commit failed tp=%s offset=%s error=%s", tp, wm - 1, e, exc_info=True)

    def _maybe_pause_or_resume(self, tp: TopicPartition):
        # If disabled, never pause/resume.
        if self.max_inflight_per_partition <= 0:
            return

        inflight = self._inflight_by_tp.get(tp, {})
        if len(inflight) >= self.max_inflight_per_partition:
            if tp not in self._paused:
                try:
                    self.consumer.pause((tp,))  # type: ignore[arg-type]
                    self._paused.add(tp)
                    self.log.info("paused tp=%s inflight=%s", tp, len(inflight))
                except Exception:
                    pass
        else:
            if tp in self._paused:
                try:
                    self.consumer.resume((tp,))  # type: ignore[arg-type]
                    self._paused.remove(tp)
                    self.log.info("resumed tp=%s inflight=%s", tp, len(inflight))
                except Exception:
                    pass

    def _loop(self):
        """Continuously poll and dispatch long tasks; never block the poll loop."""
        while self.running:
            # 1) Reap completed tasks
            for tp, tasks in list(self._inflight_by_tp.items()):
                for offset, task in list(tasks.items()):
                    if not task.future.done():
                        continue

                    # remove from inflight
                    tasks.pop(offset, None)

                    try:
                        task.future.result()
                        self.log.info(
                            "flv task finished ok job_id=%s tp=%s offset=%s elapsed=%.1fs",
                            task.job_id,
                            task.tp,
                            task.offset,
                            time.time() - task.started_at,
                        )
                        # OK => mark done and commit when possible
                        self._mark_done(task.tp, task.offset)
                    except Exception as e:
                        # Failed: do NOT commit. Message will be retried after rebalance/restart.
                        self.log.error(
                            "flv task failed job_id=%s tp=%s offset=%s error=%s",
                            task.job_id,
                            task.tp,
                            task.offset,
                            e,
                            exc_info=True,
                        )

                    # after task completion, maybe resume tp
                    self._maybe_pause_or_resume(tp)

                # cleanup empty dicts
                if not tasks:
                    self._inflight_by_tp.pop(tp, None)

            # 2) Poll new records (fast)
            try:
                records = self.consumer.poll(timeout_ms=500, max_records=50)
            except Exception as e:
                self.log.error("flv consumer poll error: %s", e)
                continue

            for tp, msgs in records.items():
                # init watermark baseline for this tp (first time we see it)
                if tp not in self._next_commit_offset and msgs:
                    self._next_commit_offset[tp] = msgs[0].offset

                inflight = self._inflight_by_tp.setdefault(tp, {})

                for msg in msgs:
                    # Global inflight cap
                    total_inflight = sum(len(v) for v in self._inflight_by_tp.values())
                    if total_inflight >= self.max_inflight_total:
                        break

                    # Per-partition cap (only if enabled)
                    if self.max_inflight_per_partition > 0 and len(inflight) >= self.max_inflight_per_partition:
                        break

                    job_id = None
                    if isinstance(msg.value, dict):
                        job_id = msg.value.get("job_id")

                    self.log.info(
                        "flv message received job_id=%s tp=%s offset=%s",
                        job_id,
                        tp,
                        msg.offset,
                    )

                    fut = self.executor.submit(run_flv_process, msg.value)
                    inflight[msg.offset] = _Task(
                        tp=tp,
                        offset=msg.offset,
                        job_id=job_id,
                        future=fut,
                        started_at=time.time(),
                    )

                    # Commit on start (recommended for long-running streaming tasks)
                    if self.commit_mode == "start":
                        try:
                            self.consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "", -1)})
                            self.log.info("flv offset committed on start tp=%s offset=%s", tp, msg.offset)
                        except Exception as e:
                            self.log.error("commit on start failed tp=%s offset=%s error=%s", tp, msg.offset, e, exc_info=True)

                self._maybe_pause_or_resume(tp)

            # 3) avoid busy loop
            time.sleep(0.05)
