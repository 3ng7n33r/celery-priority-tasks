from time import sleep
from .worker_entrypoint import computation_worker
from .tasks import load_async


class ModelwithMethod:
    def __init__(self, pk):
        self.pk = pk

    def load(self):
        computation_worker.start_concurrent_task(self, load_async)

    def load_success(self):
        computation_worker.end_concurrent_task(self)
