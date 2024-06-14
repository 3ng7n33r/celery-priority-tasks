# yourapp/queues.py

import importlib
import logging
import redis

from .models import ModelwithMethod


redis_client = redis.Redis(host="localhost", port=6379, db=1)

logger = logging.getLogger(__name__)

MAXIMUM_CONCURRENT_TASKS = 3
PRIORITY_CHOICES = [(0, "Low"), (1, "Normal"), (2, "High")]


class WorkerEntryPoint:
    def __init__(self, celery_worker: str = "computation"):
        self.task_queue = f"{celery_worker}_tasklist"
        self.waiting_queues = {}
        self.priority_levels = [x[0] for x in PRIORITY_CHOICES]
        self.priority_levels.sort(reverse=True)
        for priority_level in PRIORITY_CHOICES:
            # create waiting queues according to priority level
            # e.g. waiting_queues = {'0': 'computation_waitlist_Low', '1': 'computation_waitlist_Normal', ...}
            self.waiting_queues[priority_level[0]] = (
                f"{celery_worker}_waitlist_{priority_level[1]}"
            )

    def start_concurrent_task(self, obj, task):
        """
        func needs to be a celery task
        Checks queue length and starts a concurrent task if below max limit by writing
        object info to Redis and calling a specified method (load() for data and files)
        """
        if self.task_length() < MAXIMUM_CONCURRENT_TASKS:
            logger.info(f"{obj.__class__.__name__} written to control queue")
            # Write object information to Redis
            redis_client.rpush(self.task_queue, self.task_key(obj))

            # start task
            task.delay(obj.pk)

        else:
            chosen_waitlist = self.choose_waitlist(obj)
            redis_client.rpush(
                chosen_waitlist,
                self.waitlist_key(obj, task),
            )
            logger.info(f"{obj.__class__.__name__}:{obj.pk} put into {chosen_waitlist}")

    def choose_waitlist(self, obj):
        return self.waiting_queues[obj.priority]

    def task_key(self, obj) -> bytes:
        return f"{obj.pk}".encode("utf-8")

    def waitlist_key(self, obj, task) -> bytes:
        return f"{obj.pk}:{task.name}".encode("utf-8")

    def task_length(self):
        return redis_client.llen(self.task_queue)

    def end_concurrent_task(self, obj):
        redis_client.lrem(self.task_queue, 1, self.task_key(obj))
        logger.info(f"{obj.__class__.__name__}:{obj.pk} removed from control queue")
        # check waiting queue
        self.run_next_task()

    def run_next_task(self) -> bool:
        for priority_level in self.priority_levels:
            waiting_queue = self.waiting_queues[priority_level]
            waitlist_key: bytes = redis_client.lpop(waiting_queue)
            if waitlist_key:
                # recover object from waitlist_key
                pk, task_key = waitlist_key.decode("utf-8").rsplit(":", 3)
                obj = ModelwithMethod(int(pk))

                # recover task from waitlist_key
                module_name, task_name = task_key.rsplit(".", 1)
                module = importlib.import_module(module_name)
                task = getattr(module, task_name)

                # add task to control queue
                logger.info(
                    f"{obj.__class__.__name__}:{obj.pk} added to control queue from {waiting_queue}"
                )
                redis_client.rpush(self.task_queue, self.task_key(obj))

                # start task
                task.delay(obj.pk)
                return True

        logger.info("All queues are empty")
        return False

    def show_list_status(self):
        logger.info("Task queue status:")
        logger.info(redis_client.lrange(self.task_queue, 0, MAXIMUM_CONCURRENT_TASKS))
        for waiting_queue in self.waiting_queues.values():
            logger.info(f"{waiting_queue}: {redis_client.lrange(waiting_queue, 0, 5)}")

    def status_waitlist(self):
        # return a dictionary containing all wait queues and their items
        status = {}
        for waiting_queue in self.waiting_queues.values():
            status[waiting_queue.replace("computation_waitlist_", "")] = [
                s.decode("utf-8") for s in redis_client.lrange(waiting_queue, 0, 5)
            ]
        return status

    def status_tasklist(self):
        # return a dictionary containing the task queue and its items
        tasklist = redis_client.lrange(self.task_queue, 0, MAXIMUM_CONCURRENT_TASKS)
        decoded_strings = [s.decode("utf-8") for s in tasklist]
        return decoded_strings

    def flush_task_queue(self):
        # delete stalled tasks and fill with tasks from waiting queues
        redis_client.delete(self.task_queue)
        while redis_client.llen(self.task_queue) < MAXIMUM_CONCURRENT_TASKS:
            if not self.run_next_task():
                break

    def flush_all(self):
        redis_client.delete(self.task_queue)
        for waiting_queue in self.waiting_queues.values():
            redis_client.delete(waiting_queue)


computation_worker = WorkerEntryPoint()
