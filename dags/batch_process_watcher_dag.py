from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from bson import ObjectId
import ast


class MongoBooksSensor(BaseSensorOperator):
    """Sensor that waits until all specified book IDs are no longer 'unread'."""

    def poke(self, context):
        mongo_hook = MongoHook(conn_id="mongo_default")
        conf = context["dag_run"].conf or {}
        print(f"Raw conf from dag_run: {conf}")

        ids_conf = conf.get("ids", [])
        if isinstance(ids_conf, str):
            try:
                ids_conf = ast.literal_eval(ids_conf)
            except Exception:
                ids_conf = []

        object_ids = []
        for _id in ids_conf:
            try:
                object_ids.append(ObjectId(_id))
            except Exception:
                continue  # ignore invalid ObjectId strings

        books = mongo_hook.find(
            mongo_collection="books",
            query={"_id": {"$in": object_ids}, "status": "unread"},
            projection={"_id": 1},
        )
        unread_books = list(books)
        return len(unread_books) == 0


with DAG(
    dag_id="batch_process_watcher_dag",
    start_date=datetime(2025, 10, 21),
    schedule=None,
    catchup=False,
    max_active_runs=5,
    tags=["mongo", "sensor"],
) as dag:

    watch_books = MongoBooksSensor(
        task_id="watch_books_sensor",
        poke_interval=10,
        mode="reschedule",
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_batch_data_server",
        trigger_dag_id="batch_data_server_dag",
    )

    watch_books >> trigger_next
