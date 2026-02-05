#!/usr/bin/env python3
"""
Azure Cosmos DB (NoSQL API) Advanced Load Generator
===================================================

Features
--------
✓ Multithread + Multiprocess loading
✓ RU-aware throttling + retries
✓ Real-time monitoring
✓ Post-load validation (count + estimated size)

Designed for Ubuntu 22.04

Requirements:
  pip install azure-cosmos tqdm psutil

Usage:
  python cosmos_nosql_db_data_generator.py --mode small_test
  python cosmos_nosql_db_data_generator.py --mode 100gb
  python cosmos_nosql_db_data_generator.py --mode 500gb

Optional:
  python cosmos_nosql_db_data_generator.py --mode 500gb --workers 4 --threads 8

Environment:
  COSMOS_ENDPOINT (e.g., https://<account>.documents.azure.com:443/)
  COSMOS_KEY
  COSMOS_DATABASE (default: test_100gb)
  COSMOS_THROUGHPUT (optional, per-container)
"""

import os
import sys
import time
import argparse
import random
import string
import psutil
import logging
import multiprocessing as mp
import math
import json
import re
from datetime import datetime, UTC
from concurrent.futures import ThreadPoolExecutor
from azure.cosmos import CosmosClient, PartitionKey, exceptions

# ================= CONFIG =================

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT", "https://account.documents.azure.com:443/")
COSMOS_KEY = os.getenv("COSMOS_KEY", "key")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE", "test_100gb")
COSMOS_THROUGHPUT = os.getenv("COSMOS_THROUGHPUT")

BATCH_SIZE = 2000
MAX_RETRIES = 5
RETRY_SLEEP = 2
COSMOS_RU_LIMIT = int(os.getenv("COSMOS_RU_LIMIT", "10000"))
COSMOS_THROTTLE_SLEEP = float(os.getenv("COSMOS_THROTTLE_SLEEP", "0.05"))
COSMOS_BATCH_MAX_BYTES = int(os.getenv("COSMOS_BATCH_MAX_BYTES", "131072"))  # 128 KB
LOG_EVERY_N_BATCHES = int(os.getenv("LOG_EVERY_N_BATCHES", "50"))

DEFAULT_PROCESSES = 4
DEFAULT_THREADS = 8

# ==========================================

# ================= LOGGING =================


def setup_logging(log_file, clear_file=True):
    """Configure logging to write to both file and console"""
    if clear_file:
        with open(log_file, "w") as f:
            f.write("")

    logger = logging.getLogger("cosmos_nosql_loader")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    # Filter out WARNING messages from file handler
    file_handler.addFilter(lambda record: record.levelno != logging.WARNING)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    return logger


# ==========================================


def random_string(size: int):
    return "".join(random.choices(string.ascii_letters + string.digits, k=size))


def generate_document(target_kb):
    session_id = random_string(32)
    return {
        "id": session_id,
        "created_at": datetime.now(UTC).isoformat(),
        "user_id": random.randint(1, 10_000_000),
        "session_id": session_id,
        "status": random.choice(["A", "B", "C", "D"]),
        "payload": random_string(target_kb * 1024),
    }


# ================= DATABASE =================


def get_client():
    return CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY, consistency_level="Session")


def ensure_database_and_container(client, db_name, container_name, throughput=None):
    db = client.create_database_if_not_exists(id=db_name)
    kwargs = {
        "id": container_name,
        "partition_key": PartitionKey(path="/id"),
    }
    if throughput:
        kwargs["offer_throughput"] = int(throughput)
    container = db.create_container_if_not_exists(**kwargs)
    return db, container


def close_client(client):
    if hasattr(client, "close"):
        client.close()


# ================= RU AWARE INSERT =================


def _get_retry_after_ms(error) -> int:
    retry_after = getattr(error, "retry_after_ms", None)
    if retry_after is not None:
        return int(retry_after)
    return 0


def insert_one_with_retry(container, doc, logger):
    retries = 0
    while True:
        try:
            container.upsert_item(doc)
            return
        except exceptions.CosmosHttpResponseError as e:
            if e.status_code == 429:
                if retries >= MAX_RETRIES:
                    logger.debug(f"[Insert] Throttled after {MAX_RETRIES} retries: {str(e)}")
                    return
                retries += 1
                retry_after_ms = _get_retry_after_ms(e)
                base_wait = max(RETRY_SLEEP, (retry_after_ms / 1000.0) if retry_after_ms else 0)
                sleep_time = base_wait * (2 ** (retries - 1))
                logger.debug(f"[Insert] 429 retry {retries}/{MAX_RETRIES}, waiting {sleep_time}s")
                time.sleep(sleep_time)
                continue
            logger.debug("[Insert] Failed")
            return
        except Exception as e:
            logger.debug("[Insert] Unexpected error")
            return


def insert_with_retry(container, batch, logger):
    for doc in batch:
        insert_one_with_retry(container, doc, logger)


# ================= COLLECTION WORKER =================


def load_collection_worker(args):
    (
        container_name,
        docs_target,
        doc_kb,
        avg_doc_bytes,
        threads,
        db_name,
        log_file,
        log_every_batches,
        throughput,
    ) = args

    logger = setup_logging(log_file, clear_file=False)
    logger.info(f"[Worker] Starting work on {container_name} - Target: {docs_target:,} docs")

    client = get_client()
    db, container = ensure_database_and_container(client, db_name, container_name, throughput)
    logger.info(f"[Worker] Connected to database '{db_name}', accessing container '{container_name}'")

    max_batch_bytes = COSMOS_BATCH_MAX_BYTES
    dynamic_batch_size = max(1, min(BATCH_SIZE, max_batch_bytes // max(1, avg_doc_bytes)))

    def thread_loader(start_idx, end_idx):
        inserted = start_idx
        batch = []
        batch_count = 0

        while inserted < end_idx:
            batch.append(generate_document(doc_kb))

            if len(batch) >= dynamic_batch_size:
                batch_count += 1
                if (
                    batch_count == 1
                    or batch_count % log_every_batches == 0
                    or inserted + len(batch) >= end_idx
                ):
                    logger.info(
                        f"[{container_name}] Inserting batch #{batch_count} ({len(batch)} docs) - "
                        f"Progress: {inserted:,}/{docs_target:,} ({(inserted/docs_target)*100:.1f}%)"
                    )

                insert_with_retry(container, batch, logger)
                if COSMOS_THROTTLE_SLEEP > 0:
                    time.sleep(COSMOS_THROTTLE_SLEEP)

                inserted += len(batch)
                batch.clear()

        if batch:
            batch_count += 1
            logger.info(
                f"[{container_name}] Inserting final batch #{batch_count} ({len(batch)} docs) - Completing {container_name}"
            )
            insert_with_retry(container, batch, logger)
            if COSMOS_THROTTLE_SLEEP > 0:
                time.sleep(COSMOS_THROTTLE_SLEEP)

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        remaining = docs_target
        chunk = (remaining // threads) + (1 if remaining % threads else 0)

        logger.info(f"[{container_name}] Spawning {threads} threads, each handling ~{chunk:,} docs")

        for i in range(threads):
            start = i * chunk
            end = min(start + chunk, docs_target)
            if start < end:
                futures.append(executor.submit(thread_loader, start, end))

        for f in futures:
            f.result()

    logger.info(f"[Worker] Completed {container_name} - Total docs: {docs_target:,}")
    close_client(client)

    return container_name


# ================= MONITOR =================


def system_monitor():
    while True:
        cpu = psutil.cpu_percent(interval=2)
        mem = psutil.virtual_memory().percent
        net = psutil.net_io_counters()

        logger.info(
            f"[MONITOR] CPU={cpu}% MEM={mem}% "
            f"NET(sent={net.bytes_sent//1024//1024}MB recv={net.bytes_recv//1024//1024}MB)"
        )

        time.sleep(5)


# ================= VALIDATION =================


def query_container_count(container):
    query = "SELECT VALUE COUNT(1) FROM c"
    return next(container.query_items(query=query, enable_cross_partition_query=True))


def validate(db, containers, docs_target, total_gb, name_prefix=None, expected_total_docs=None, avg_doc_bytes=0):
    logger.info("\n" + "=" * 60)
    logger.info("Starting validation...")
    logger.info("Connecting to database and counting documents...")

    total_docs = 0

    if name_prefix:
        names = [n["id"] for n in db.list_containers() if n["id"].startswith(name_prefix)]
        for i, name in enumerate(names, start=1):
            container = db.get_container_client(name)
            cnt = query_container_count(container)
            total_docs += cnt
            if i % 10 == 0 or i == len(names):
                logger.info(f"Validated {i}/{len(names)} containers... Total docs so far: {total_docs:,}")
    else:
        for i in range(1, containers + 1):
            name = f"collection_{i:04d}"
            container = db.get_container_client(name)
            cnt = query_container_count(container)
            total_docs += cnt
            if i % 10 == 0 or i == containers:
                logger.info(f"Validated {i}/{containers} containers... Total docs so far: {total_docs:,}")

    size_gb = 0
    if avg_doc_bytes:
        size_gb = (total_docs * avg_doc_bytes) / 1024**3

    logger.info("=" * 50)
    logger.info(f"Total Docs   : {total_docs:,}")
    logger.info(f"Data Size    : ~{size_gb:.2f} GB (estimated)")
    logger.info(f"Target Size  : {total_gb} GB")

    if size_gb >= total_gb * 0.9:
        logger.info("[✓] Size validation passed (estimated)")
    else:
        logger.warning("[✗] Size validation FAILED (estimated)")

    expected = expected_total_docs if expected_total_docs is not None else (docs_target * containers)

    if total_docs >= expected * 0.9:
        logger.info("[✓] Count validation passed")
    else:
        logger.warning("[✗] Count validation FAILED")

    logger.info("=" * 50)


# ================= MAIN =================


def estimate_avg_doc_bytes(doc_kb, samples=50):
    sample_count = max(1, samples)
    total_bytes = 0
    for _ in range(sample_count):
        doc = generate_document(doc_kb)
        total_bytes += len(json.dumps(doc).encode("utf-8"))
    return max(1, total_bytes // sample_count)


def main():
    parser = argparse.ArgumentParser("Cosmos NoSQL Advanced Loader")

    parser.add_argument("--mode", choices=["small_test", "100gb", "500gb"], required=True)
    parser.add_argument("--workers", type=int, default=DEFAULT_PROCESSES)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--monitor", action="store_true")
    parser.add_argument("--no-resume", action="store_true", help="Disable resume; always load full target")

    args = parser.parse_args()

    endpoint_prefix = COSMOS_ENDPOINT.split("//")[-1].split(".")[0].split(":")[0]
    endpoint_prefix = re.sub(r"[^A-Za-z0-9_-]+", "_", endpoint_prefix) or "endpoint"
    log_file = f"cosmos_nosql_loader_{args.mode}_{endpoint_prefix}.log"
    global logger
    logger = setup_logging(log_file)

    start_time = time.perf_counter()

    db_name = COSMOS_DATABASE

    if args.mode == "small_test":
        total_gb = 1
        containers = 10
        name_prefix = "small_"
        if args.workers == DEFAULT_PROCESSES:
            args.workers = 1
        if args.threads == DEFAULT_THREADS:
            args.threads = 2

    elif args.mode == "100gb":
        total_gb = 100
        containers = 100
        name_prefix = "collection_"
        if args.threads == DEFAULT_THREADS:
            args.threads = 8

    else:
        total_gb = 500
        containers = 1500
        name_prefix = "collection_"
        if args.threads == DEFAULT_THREADS:
            args.threads = 12

    if COSMOS_RU_LIMIT <= 10000:
        if args.mode == "small_test":
            args.workers = min(args.workers, 1)
            args.threads = min(args.threads, 2)
        else:
            args.workers = min(args.workers, 2)
            args.threads = min(args.threads, 4)

    log_every_batches = LOG_EVERY_N_BATCHES
    if args.mode == "100gb":
        log_every_batches = max(LOG_EVERY_N_BATCHES, 200)
    elif args.mode == "500gb":
        log_every_batches = max(LOG_EVERY_N_BATCHES, 500)

    logger.info("=" * 60)
    logger.info("Cosmos NoSQL Parallel Loader")
    logger.info(f"Mode        : {args.mode}")
    logger.info(f"Database    : {db_name}")
    logger.info(f"Processes   : {args.workers}")
    logger.info(f"Threads     : {args.threads}")
    logger.info(f"Containers  : {containers}")
    logger.info(f"Target Size : {total_gb} GB")
    logger.info(f"Endpoint    : {COSMOS_ENDPOINT}")
    logger.info("=" * 60)

    doc_kb = max(50, min(200, (total_gb * 1024 * 1024) // containers // 1000))
    avg_doc_bytes = estimate_avg_doc_bytes(doc_kb)
    target_bytes = (total_gb / containers) * 1024**3
    docs_target = max(1, int(target_bytes / avg_doc_bytes))
    avg_doc_kb = avg_doc_bytes / 1024.0

    logger.info(f"Doc Size     : ~{doc_kb} KB (payload target)")
    logger.info(f"Doc Size     : ~{avg_doc_kb:.2f} KB (avg generated)")
    logger.info(f"Docs/Cont    : ~{docs_target:,}")

    if args.monitor:
        mp.Process(target=system_monitor, daemon=True).start()

    jobs = []
    expected_total_docs = None

    if not args.no_resume:
        logger.info("Resume enabled; calculating remaining size per container")
        client = get_client()
        db = client.get_database_client(db_name)
        existing_names = {c["id"] for c in db.list_containers()}
        expected_total_docs = 0
        total_count_so_far = 0

        for i in range(1, containers + 1):
            name = f"{name_prefix}{i:04d}"
            existing_count = 0

            if name in existing_names:
                container = db.get_container_client(name)
                existing_count = query_container_count(container)

            remaining_docs = max(0, docs_target - existing_count)
            total_count_so_far += existing_count
            expected_total_docs += existing_count + remaining_docs

            if remaining_docs > 0:
                jobs.append(
                    (
                        name,
                        remaining_docs,
                        doc_kb,
                        avg_doc_bytes,
                        args.threads,
                        db_name,
                        log_file,
                        log_every_batches,
                        COSMOS_THROUGHPUT,
                    )
                )

        close_client(client)
    else:
        for i in range(1, containers + 1):
            name = f"{name_prefix}{i:04d}"
            jobs.append(
                (
                    name,
                    docs_target,
                    doc_kb,
                    avg_doc_bytes,
                    args.threads,
                    db_name,
                    log_file,
                    log_every_batches,
                    COSMOS_THROUGHPUT,
                )
            )

    logger.info(f"\nPrepared {len(jobs)} containers for parallel processing")
    logger.info(f"Starting {args.workers} worker processes...")
    logger.info("\nStarting parallel load...")

    if jobs:
        count_client = get_client()
        count_db = count_client.get_database_client(db_name)
        total_docs_logged = 0
        try:
            with mp.Pool(args.workers) as pool:
                for res in pool.imap_unordered(load_collection_worker, jobs):
                    logger.info(f"[✓] {res} completed")
                    if args.mode in ("100gb", "500gb"):
                        try:
                            container = count_db.get_container_client(res)
                            cnt = query_container_count(container)
                            total_docs_logged += cnt
                            est_gb = (total_docs_logged * avg_doc_bytes) / 1024**3
                            logger.info(f"[DB] Current data size: ~{est_gb:.2f} GB (estimated)")
                        except Exception as e:
                            logger.warning(f"[DB] Failed to fetch count for {res}: {str(e)}")
        finally:
            close_client(count_client)
    else:
        logger.info("No remaining work to load.")

    logger.info("\nLoad completed. Running validation...")

    client = get_client()
    db = client.get_database_client(db_name)
    validate(
        db,
        containers,
        docs_target,
        total_gb,
        name_prefix=name_prefix,
        expected_total_docs=expected_total_docs,
        avg_doc_bytes=avg_doc_bytes,
    )
    close_client(client)

    elapsed_seconds = time.perf_counter() - start_time
    elapsed_hhmmss = time.strftime("%H:%M:%S", time.gmtime(elapsed_seconds))
    logger.info("\nAll tasks finished successfully.")
    logger.info(f"Total execution time: {elapsed_hhmmss} ({elapsed_seconds:.2f} seconds)")


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
