from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime
from bson import ObjectId
import threading
import requests
import time


@dag(
    dag_id="books_batch_processor_dag",
    start_date=datetime(2025, 10, 27),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mongo", "batch", "rest"],
)
def books_batch_processor():

    @task
    def books_batch_process():
        """
        1. Read batch size from MongoDB (batch_config.entries_per_batch)
        2. Read unread books from MongoDB
        3. Chunk them by batch size
        4. For each chunk:
           - Fire async PUT to REST endpoint (no wait)
           - Watch MongoDB until ≥90 % 'read'
        """

        # --- Mongo connection ---
        mongo_hook = MongoHook(conn_id="mongo_default")
        client = mongo_hook.get_conn()
        db = client["testdb"]

        # --- Step 1: configuration ---
        config_doc = db["batch_config"].find_one()
        if not config_doc or "entries_per_batch" not in config_doc:
            raise ValueError("Missing 'entries_per_batch' config in batch_config")
        entries_per_batch = int(config_doc["entries_per_batch"])
        print(f"Batch size configured: {entries_per_batch}")

        # --- Step 2: source data ---
        unread_books = list(db["books"].find({"status": "unread"}, {"_id": 1}))
        total_books = len(unread_books)
        if total_books == 0:
            print("No unread books found.")
            return
        print(f"Total unread books: {total_books}")

        # --- Step 3: chunking ---
        def chunk_list(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i : i + size]

        chunks = list(chunk_list(unread_books, entries_per_batch))
        print(f"Total chunks created: {len(chunks)}")

        # --- Step 4: HTTP setup ---
        conn_id = "rest-book-status-updater"
        base_conn = BaseHook.get_connection(conn_id)
        full_url = f"{base_conn.schema}://{base_conn.host}:{base_conn.port}/api/books/update-status"
        print(f"[INFO] Testing HTTP connection to: {full_url}")
        headers = {"Content-Type": "application/json"}

        # --- Async HTTP sender ---
        def send_update_request(chunk_ids):
            payload = {"ids": chunk_ids}    
            try:
                requests.put(full_url, json=payload, headers=headers, timeout=1)
            except Exception:
                pass  # ignore connection/timeout for fire-and-forget

        # --- Mongo watcher ---
        def wait_for_status_update(chunk_ids, threshold=0.9, delay=5):
            total = len(chunk_ids)
            object_ids = []
            for i in chunk_ids:
                if ObjectId.is_valid(i):
                    object_ids.append(ObjectId(i))
                else:
                    object_ids.append(i)

            while True:
                read_count = db["books"].count_documents(
                    {"_id": {"$in": object_ids}, "status": "read"}
                )
                ratio = read_count / total
                print(f"Chunk progress: {read_count}/{total} read ({ratio*100:.1f}%)")
                if ratio >= threshold:
                    return
                time.sleep(delay)

        # --- Step 5: process chunks sequentially ---
        for idx, chunk in enumerate(chunks, start=1):
            chunk_ids = [str(b["_id"]) for b in chunk]
            print(f"Sending update request for chunk {idx}/{len(chunks)}: {chunk_ids}")
            print(f"\nProcessing chunk {idx}/{len(chunks)} with {len(chunk_ids)} items")

            thread = threading.Thread(target=send_update_request, args=(chunk_ids,), daemon=True)
            thread.start()

            wait_for_status_update(chunk_ids)

        print("\n✅ All chunks processed successfully.")

    # DAG tasks
    books_batch_process()

# Run the DAG
books_batch_processor()
