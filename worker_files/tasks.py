from time import sleep
from celery import Celery
import redis
from .models import ModelwithMethod


redis_client = redis.Redis(host="localhost", port=6379, db=1)


app = Celery("app")


@app.task(bind=True, max_retries=None)
def load_async(self, pk):
    print(f"Hello from object {pk}")
    sleep(1)
    print(f"Bye from object {pk}")
    obj = ModelwithMethod(pk)
    obj.load_success()
