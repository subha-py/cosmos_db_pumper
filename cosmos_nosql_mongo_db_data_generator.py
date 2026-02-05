#!/usr/bin/env python3
"""
Azure Cosmos DB (MongoDB API) Advanced Load Generator
====================================================

Features
--------
✓ Multithread + Multiprocess loading
✓ RU-aware throttling + retries
✓ Real-time monitoring
✓ Post-load validation (count + size)

Designed for Ubuntu 22.04

Requirements:
  pip install pymongo tqdm psutil

Usage:
  python generate_data.py --mode small_test
  python generate_data.py --mode 100gb
  python generate_data.py --mode 500gb

Optional:
  python generate_data.py --mode 500gb --workers 4 --threads 8

Before running, update MONGO_URI.
"""

import os
import sys
import time
import re
import argparse
import random
import string
import psutil
import logging
import multiprocessing as mp
import math
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient, errors
from bson import BSON
from tqdm import tqdm

# ================= CONFIG =================

MONGO_ENDPOINT = os.getenv("MONGO_ENDPOINT", "host:10255")
MONGO_USERNAME = os.getenv("MONGO_USERNAME", "username")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password")
MONGO_URI = (
    f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_ENDPOINT}/"
    "?ssl=true&replicaSet=globaldb&retrywrites=false"
)

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
    # Clear/reset the log file before starting (only in main process)
    if clear_file:
        with open(log_file, 'w') as f:
            f.write('')
    
    # Get or create logger
    logger = logging.getLogger('cosmos_loader')
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Add handlers
    logger.addHandler(logging.FileHandler(log_file))
    logger.addHandler(logging.StreamHandler(sys.stdout))
    
    # Set format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    
    return logger

# ==========================================


def random_string(size: int):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))


def generate_document(target_kb):
    session_id = random_string(32)
    return {
        "id": session_id,
        "created_at": datetime.utcnow(),
        "user_id": random.randint(1, 10_000_000),
        "session_id": session_id,
        "status": random.choice(["A", "B", "C", "D"]),
        "payload": random_string(target_kb * 1024)
    }


# ================= DATABASE =================


def get_client():
    return MongoClient(
        MONGO_URI,
        maxPoolSize=50,
        minPoolSize=10,
        maxIdleTimeMS=30000,

        # Timeouts
        socketTimeoutMS=60000,
        connectTimeoutMS=30000,
        serverSelectionTimeoutMS=30000,

        # Reliability
        retryWrites=False,
        retryReads=True,

        # TLS Stability
        tls=True,
        tlsAllowInvalidCertificates=False
    )


# ================= RU AWARE INSERT =================


def _get_retry_after_ms(error) -> int:
    details = getattr(error, "details", None) or {}
    for key in ("errmsg", "message"):
        if key in details and details[key]:
            match = re.search(r"RetryAfterMs=(\d+)", str(details[key]))
            if match:
                return int(match.group(1))

    for err in details.get("writeErrors", []) or []:
        match = re.search(r"RetryAfterMs=(\d+)", str(err.get("errmsg", "")))
        if match:
            return int(match.group(1))

    match = re.search(r"RetryAfterMs=(\d+)", str(error))
    if match:
        return int(match.group(1))

    return 0


def insert_one_with_retry(collection, doc, logger):
    retries = 0

    while True:
        try:
            collection.insert_one(doc)
            return

        except errors.DuplicateKeyError:
            logger.debug("[Insert] Duplicate key error on single doc; skipping")
            return

        except errors.PyMongoError as e:
            if retries >= MAX_RETRIES:
                logger.error(f"[Insert] Single insert failed after {MAX_RETRIES} retries: {str(e)}")
                return

            retries += 1
            retry_after_ms = _get_retry_after_ms(e)
            base_wait = max(RETRY_SLEEP, (retry_after_ms / 1000.0) if retry_after_ms else 0)
            sleep_time = base_wait * (2 ** (retries - 1))
            if isinstance(e, errors.BulkWriteError) or _get_retry_after_ms(e):
                logger.warning(
                    f"[Insert] Retry {retries}/{MAX_RETRIES} after throttling, waiting {sleep_time}s"
                )
            else:
                logger.warning(
                    f"[Insert] Retry {retries}/{MAX_RETRIES} after error, waiting {sleep_time}s"
                )
            time.sleep(sleep_time)


def insert_with_retry(collection, batch, logger):

    retries = 0

    while True:
        try:
            collection.insert_many(batch, ordered=False)
            return

        except errors.BulkWriteError as e:
            details = e.details or {}
            write_errors = details.get("writeErrors", [])
            failed_indices = [err.get("index") for err in write_errors if err.get("index") is not None]
            failed_docs = [batch[i] for i in failed_indices if 0 <= i < len(batch)]

            retry_after_ms = _get_retry_after_ms(e)
            if retry_after_ms:
                time.sleep(retry_after_ms / 1000.0)

            logger.warning(
                f"[Insert] BulkWriteError encountered; retrying {len(failed_docs)} failed docs individually"
            )

            for doc in failed_docs:
                insert_one_with_retry(collection, doc, logger)

            return

        except errors.PyMongoError as e:

            if retries >= MAX_RETRIES:
                logger.error(f"[Insert] Failed after {MAX_RETRIES} retries: {str(e)}")
                raise e

            retries += 1
            retry_after_ms = _get_retry_after_ms(e)
            base_wait = max(RETRY_SLEEP, (retry_after_ms / 1000.0) if retry_after_ms else 0)
            sleep_time = base_wait * (2 ** (retries - 1))
            if _get_retry_after_ms(e):
                logger.warning(
                    f"[Insert] Retry {retries}/{MAX_RETRIES} after throttling, waiting {sleep_time}s"
                )
            else:
                logger.warning(
                    f"[Insert] Retry {retries}/{MAX_RETRIES} after error, waiting {sleep_time}s"
                )
            time.sleep(sleep_time)


# ================= COLLECTION WORKER =================


def load_collection_worker(args):

    col_name, docs_target, doc_kb, avg_doc_bytes, threads, db_name, log_file, log_every_batches = args
    
    # Initialize logger in worker process
    logger = setup_logging(log_file, clear_file=False)

    logger.info(f"[Worker] Starting work on {col_name} - Target: {docs_target:,} docs")

    client = get_client()
    db = client[db_name]
    col = db[col_name]
    
    logger.info(f"[Worker] Connected to database '{db_name}', accessing collection '{col_name}'")

    logger.info(f"[Worker] Starting fresh insertion for {col_name}")

    # Keep batch size within safe request limits (Cosmos Mongo has small request cap)
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
                if batch_count == 1 or batch_count % log_every_batches == 0 or inserted + len(batch) >= end_idx:
                    logger.info(
                        f"[{col_name}] Inserting batch #{batch_count} ({len(batch)} docs) - "
                        f"Progress: {inserted:,}/{docs_target:,} ({(inserted/docs_target)*100:.1f}%)"
                    )
                
                insert_with_retry(col, batch, logger)
                if COSMOS_THROTTLE_SLEEP > 0:
                    time.sleep(COSMOS_THROTTLE_SLEEP)

                inserted += len(batch)
                batch.clear()

        if batch:
            batch_count += 1
            logger.info(f"[{col_name}] Inserting final batch #{batch_count} ({len(batch)} docs) - Completing {col_name}")
            insert_with_retry(col, batch, logger)
            if COSMOS_THROTTLE_SLEEP > 0:
                time.sleep(COSMOS_THROTTLE_SLEEP)

    with ThreadPoolExecutor(max_workers=threads) as executor:

        futures = []
        remaining = docs_target
        chunk = (remaining // threads) + (1 if remaining % threads else 0)
        
        logger.info(f"[{col_name}] Spawning {threads} threads, each handling ~{chunk:,} docs")

        for i in range(threads):
            start = i * chunk
            end = min(start + chunk, docs_target)

            if start < end:
                logger.debug(f"[{col_name}] Thread {i+1} starting at doc #{start:,} ending at #{end:,}")
                futures.append(executor.submit(thread_loader, start, end))

        for f in futures:
            f.result()
    
    logger.info(f"[Worker] Completed {col_name} - Total docs: {docs_target:,}")

    client.close()

    return col_name


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


def validate(db, collections, docs_target, total_gb, name_prefix=None, expected_total_docs=None):

    logger.info("\n" + "=" * 60)
    logger.info("Starting validation...")
    logger.info("Connecting to database and counting documents...")

    total_docs = 0

    if name_prefix:
        names = [n for n in db.list_collection_names() if n.startswith(name_prefix)]
        size_bytes = 0
        for i, name in enumerate(names, start=1):
            col = db[name]
            cnt = col.estimated_document_count()
            total_docs += cnt
            try:
                stats = db.command({"collStats": name})
                size_bytes += stats.get("size", 0)
            except errors.OperationFailure as e:
                logger.warning(f"[Validate] collStats unsupported for {name}: {str(e)}")
            if i % 10 == 0 or i == len(names):
                logger.info(f"Validated {i}/{len(names)} collections... Total docs so far: {total_docs:,}")
        size_gb = size_bytes / 1024**3
        logger.info(f"Collection stats retrieved - Size: {size_gb:.2f} GB")
    else:
        stats = db.command("dbstats")
        size_gb = stats.get("dataSize", 0) / 1024**3
        logger.info(f"Database stats retrieved - Size: {size_gb:.2f} GB")

        for i in range(1, collections + 1):
            col = db[f"collection_{i:04d}"]
            cnt = col.estimated_document_count()
            total_docs += cnt
            if i % 10 == 0 or i == collections:
                logger.info(f"Validated {i}/{collections} collections... Total docs so far: {total_docs:,}")

    logger.info("=" * 50)
    logger.info(f"Total Docs   : {total_docs:,}")
    logger.info(f"Data Size    : {size_gb:.2f} GB")
    logger.info(f"Target Size  : {total_gb} GB")

    if size_gb >= total_gb * 0.9:
        logger.info("[✓] Size validation passed")
    else:
        logger.warning("[✗] Size validation FAILED")

    expected = expected_total_docs if expected_total_docs is not None else (docs_target * collections)

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
        total_bytes += len(BSON.encode(doc))
    return max(1, total_bytes // sample_count)


def estimate_effective_doc_bytes(db, doc_kb, logger, samples=50, temp_name="_size_probe"):
    sample_count = max(1, samples)
    avg_bson_bytes = estimate_avg_doc_bytes(doc_kb, samples=sample_count)

    try:
        if temp_name in db.list_collection_names():
            db[temp_name].drop()

        docs = [generate_document(doc_kb) for _ in range(sample_count)]
        db[temp_name].insert_many(docs, ordered=False)

        stats = db.command({"collStats": temp_name})
        size_bytes = stats.get("size", 0) or stats.get("storageSize", 0)
        if size_bytes and sample_count:
            effective = max(1, int(size_bytes / sample_count))
            logger.info(
                f"Effective doc size estimated from collStats: {effective} bytes "
                f"(avg BSON: {avg_bson_bytes} bytes)"
            )
            return effective
        logger.warning("collStats returned size 0; falling back to BSON size estimation")
    except errors.PyMongoError as e:
        logger.warning(f"Failed to estimate effective doc size via collStats: {str(e)}")
    finally:
        try:
            if temp_name in db.list_collection_names():
                db[temp_name].drop()
        except errors.PyMongoError:
            pass

    return avg_bson_bytes


def main():

    parser = argparse.ArgumentParser("Cosmos Advanced Loader")

    parser.add_argument("--mode", choices=["small_test", "100gb", "500gb"], required=True)
    parser.add_argument("--workers", type=int, default=DEFAULT_PROCESSES)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--monitor", action="store_true")
    parser.add_argument("--no-drop-small", action="store_true", help="Do not drop small_* collections before small_test")
    parser.add_argument("--no-resume", action="store_true", help="Disable resume; always load full target")

    args = parser.parse_args()

    # Set log file based on mode and endpoint prefix
    endpoint_prefix = MONGO_ENDPOINT.split(".")[0].split(":")[0]
    endpoint_prefix = re.sub(r"[^A-Za-z0-9_-]+", "_", endpoint_prefix) or "endpoint"
    log_file = f"cosmos_loader_{args.mode}_{endpoint_prefix}.log"
    global logger
    logger = setup_logging(log_file)

    start_time = time.perf_counter()

    db_name = "test_100gb"

    if args.mode == "small_test":
        total_gb = 5
        collections = 10
        name_prefix = "small_"
        # Use fewer resources for small test
        if args.workers == DEFAULT_PROCESSES:
            args.workers = 1
        if args.threads == DEFAULT_THREADS:
            args.threads = 2

    elif args.mode == "100gb":
        total_gb = 100
        collections = 100
        name_prefix = "collection_"
        # Use default resources for 100gb
        if args.threads == DEFAULT_THREADS:
            args.threads = 8

    else:
        total_gb = 500
        collections = 1500
        name_prefix = "collection_"
        # Use more resources for 500gb
        if args.threads == DEFAULT_THREADS:
            args.threads = 12

    # RU-aware caps (default assumes 10k RU)
    if COSMOS_RU_LIMIT <= 10000:
        if args.mode == "small_test":
            args.workers = min(args.workers, 1)
            args.threads = min(args.threads, 2)
        else:
            args.workers = min(args.workers, 2)
            args.threads = min(args.threads, 4)

    # Reduce per-batch logging for large runs
    log_every_batches = LOG_EVERY_N_BATCHES
    if args.mode == "100gb":
        log_every_batches = max(LOG_EVERY_N_BATCHES, 200)
    elif args.mode == "500gb":
        log_every_batches = max(LOG_EVERY_N_BATCHES, 500)

    logger.info("=" * 60)
    logger.info("Cosmos Advanced Parallel Loader")
    logger.info(f"Mode        : {args.mode}")
    logger.info(f"Database    : {db_name}")
    logger.info(f"Processes   : {args.workers}")
    logger.info(f"Threads     : {args.threads}")
    logger.info(f"Collections : {collections}")
    logger.info(f"Target Size : {total_gb} GB")
    logger.info(f"Endpoint    : {MONGO_ENDPOINT}")
    logger.info(f"Username    : {MONGO_USERNAME}")
    logger.info("=" * 60)

    # Estimate document size
    doc_kb = max(50, min(200, (total_gb * 1024 * 1024) // collections // 1000))

    client = get_client()
    db = client[db_name]
    avg_doc_bytes = estimate_effective_doc_bytes(db, doc_kb, logger)
    client.close()
    target_bytes = (total_gb / collections) * 1024**3

    docs_target = max(1, int(target_bytes / avg_doc_bytes))

    avg_doc_kb = avg_doc_bytes / 1024.0

    logger.info(f"Doc Size     : ~{doc_kb} KB (payload target)")
    logger.info(f"Doc Size     : ~{avg_doc_kb:.2f} KB (avg generated)")
    logger.info(f"Docs/Col     : ~{docs_target:,}")

    # Start monitor
    if args.monitor:
        mp.Process(target=system_monitor, daemon=True).start()

    # Prepare jobs
    jobs = []
    expected_total_docs = None

    if args.mode == "small_test" and not args.no_drop_small and args.no_resume:
        logger.info("Dropping existing small_* collections before small_test run")
        client = get_client()
        db = client[db_name]
        for name in db.list_collection_names():
            if name.startswith("small_"):
                db[name].drop()
        if "_size_probe" in db.list_collection_names():
            db["_size_probe"].drop()
        client.close()

    if not args.no_resume:
        logger.info("Resume enabled; calculating remaining size per collection")
        client = get_client()
        db = client[db_name]
        existing_names = set(db.list_collection_names())
        expected_total_docs = 0
        remaining_total_bytes = 0

        for i in range(1, collections + 1):
            name = f"{name_prefix}{i:04d}"
            existing_size = 0
            existing_count = 0

            if name in existing_names:
                col = db[name]
                existing_count = col.estimated_document_count()
                try:
                    stats = db.command({"collStats": name})
                    existing_size = stats.get("size", 0) or stats.get("storageSize", 0)
                except errors.OperationFailure as e:
                    logger.warning(f"[Resume] collStats unsupported for {name}: {str(e)}")

            remaining_bytes = max(0, target_bytes - existing_size)
            remaining_total_bytes += remaining_bytes
            remaining_docs = max(0, math.ceil(remaining_bytes / avg_doc_bytes))

            expected_total_docs += existing_count + remaining_docs

            if remaining_docs > 0:
                jobs.append((name, remaining_docs, doc_kb, avg_doc_bytes, args.threads, db_name, log_file, log_every_batches))

        client.close()

        if remaining_total_bytes <= 0:
            logger.info("Resume check: target size already reached; skipping load phase")

    else:
        for i in range(1, collections + 1):
            name = f"{name_prefix}{i:04d}"
            jobs.append((name, docs_target, doc_kb, avg_doc_bytes, args.threads, db_name, log_file, log_every_batches))
    
    logger.info(f"\nPrepared {len(jobs)} collections for parallel processing")
    logger.info(f"Starting {args.workers} worker processes...")
    logger.info("\nStarting parallel load...")

    if jobs:
        db_size_client = get_client()
        db_size = db_size_client[db_name]
        try:
            with mp.Pool(args.workers) as pool:

                for res in pool.imap_unordered(load_collection_worker, jobs):
                    logger.info(f"[✓] {res} completed")
                    if args.mode in ("100gb", "500gb"):
                        try:
                            stats = db_size.command("dbstats")
                            size_gb = stats.get("dataSize", 0) / 1024**3
                            logger.info(f"[DB] Current data size: {size_gb:.2f} GB")
                        except errors.PyMongoError as e:
                            logger.warning(f"[DB] Failed to fetch dbstats: {str(e)}")
        finally:
            db_size_client.close()
    else:
        logger.info("No remaining work to load.")

    logger.info("\nLoad completed. Running validation...")

    client = get_client()
    db = client[db_name]

    validate(db, collections, docs_target, total_gb, name_prefix=name_prefix, expected_total_docs=expected_total_docs)

    client.close()

    elapsed_seconds = time.perf_counter() - start_time
    elapsed_hhmmss = time.strftime("%H:%M:%S", time.gmtime(elapsed_seconds))
    logger.info(f"\nAll tasks finished successfully.")
    logger.info(f"Total execution time: {elapsed_hhmmss} ({elapsed_seconds:.2f} seconds)")


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
