import argparse
import json
import threading
import time
import dill
import codecs
import redis
import zmq
from uuid import UUID
from multiprocessing import Pool
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


# Task Dispatcher for MPCSFaaS

# Responsible for:
# - Routing tasks from Redis to workers
# - Detecting failures
# - Updating Redis when results are available

# Modes:
# - local: Uses MultiProcessingPool for execution
# - pull: ZMQ REQ/REP pattern - workers request tasks
# - push: ZMQ DEALER/ROUTER pattern - dispatcher pushes tasks to workers

# Usage:
#     python3 task_dispatcher.py -m [local/pull/push] -p <port> -w <num_worker_processors>


# ============== Configurations ==============
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_TASKS_CHANNEL = "tasks"
DEFAULT_ZMQ_PORT = 5555
DEFAULT_NUM_WORKERS = 4

# fault tolerance settings
TASK_DEADLINE_SECONDS = 5  # for pull mode
HEARTBEAT_TIMEOUT_SECONDS = 2  # for push mode
HEARTBEAT_CHECK_INTERVAL = 1  # How often to check for dead workers


# task States
class TaskStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


# serialization
def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode().strip()


def deserialize(s: str):
    return dill.loads(codecs.decode(s.encode(), "base64"))


# ============== Data Classes ==============
@dataclass
# track task execution state
class TaskInfo:

    task_id: str
    worker_id: Optional[str] = None
    start_time: Optional[float] = None
    fn_payload: str = ""
    args_payload: str = ""


@dataclass
# track worker state for push mode
class WorkerInfo:

    worker_id: str
    last_heartbeat: float
    current_task: Optional[str] = None
    is_available: bool = True


# ============== Redis Helper ==============
# Helper class for Redis operations
class RedisClient:

    def __init__(self, host: str = REDIS_HOST, port: int = REDIS_PORT):
        self.r = redis.Redis(host=host, port=port, decode_responses=True)
        self.r.ping()  # Verify connection
        print(f"Connected to Redis at {host}:{port}")

    # gets task data from Redis
    def get_task(self, task_id: str) -> Optional[dict]:

        data = self.r.get(f"task:{task_id}")
        if data:
            return json.loads(data)
        return None

    # get function data from Redis
    def get_function(self, function_id: str) -> Optional[dict]:

        data = self.r.get(function_id)
        if data:
            return json.loads(data)
        return None

    # update task status in Redis
    def update_task_status(self, task_id: str, status: TaskStatus):

        task_data = self.get_task(task_id)
        if task_data:
            task_data["status"] = status.value
            self.r.set(f"task:{task_id}", json.dumps(task_data))

    # update task with result
    def update_task_result(self, task_id: str, status: TaskStatus, result: str):

        task_data = self.get_task(task_id)
        if task_data:
            task_data["status"] = status.value
            task_data["result"] = result
            self.r.set(f"task:{task_id}", json.dumps(task_data))

    # get all tasks with QUEUED status (for pull mode polling)
    def get_queued_tasks(self) -> list:

        queued = []
        # scan for task keys
        for key in self.r.scan_iter("task:*"):
            task_data = self.r.get(key)
            if task_data:
                task = json.loads(task_data)
                if task.get("status") == TaskStatus.QUEUED.value:
                    queued.append(task)
        return queued


# ============== Local Execution ==============
# Execute a task locally (for local mode and used by workers)
def execute_task(
    task_id: str, fn_payload: str, param_payload: str
) -> Tuple[str, str, str]:

    try:
        fn = deserialize(fn_payload)
        params = deserialize(param_payload)

        # Execute the function
        if isinstance(params, tuple):
            result = fn(*params)
        elif isinstance(params, dict):
            result = fn(**params)
        else:
            result = fn(params)

        return (task_id, TaskStatus.COMPLETE.value, serialize(result))

    except Exception as e:
        return (task_id, TaskStatus.FAILED.value, serialize(str(e)))


# ============== Base Dispatcher ==============
# base class for all dispatcher modes
class BaseDispatcher:

    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client
        self.running = True
        self.pending_tasks: Dict[str, TaskInfo] = {}  # task_id -> TaskInfo

    # get function and args payloads for a task
    def get_task_payloads(self, task_id: str) -> Optional[Tuple[str, str]]:

        task_data = self.redis.get_task(task_id)
        if not task_data:
            print(f"Task {task_id} not found in Redis")
            return None

        fn_id = task_data.get("function_id")
        fn_data = self.redis.get_function(fn_id)
        if not fn_data:
            print(f"Function {fn_id} not found in Redis")
            return None

        fn_payload = fn_data.get("payload")
        args_payload = task_data.get("payload")

        return (fn_payload, args_payload)

    def handle_result(self, task_id: str, status: str, result_payload: str):

        task_status = TaskStatus.COMPLETE if status == "COMPLETE" else TaskStatus.FAILED
        self.redis.update_task_result(task_id, task_status, result_payload)

        # remove from pending
        if task_id in self.pending_tasks:
            del self.pending_tasks[task_id]

        print(f"Task {task_id} completed with status: {status}")

    def start(self):
        raise NotImplementedError

    def stop(self):
        self.running = False


# ============== Local Dispatcher ==============
# Local mode using MultiProcessingPool
class LocalDispatcher(BaseDispatcher):

    def __init__(
        self, redis_client: RedisClient, num_workers: int = DEFAULT_NUM_WORKERS
    ):
        super().__init__(redis_client)
        self.num_workers = num_workers
        self.pool = Pool(processes=num_workers)
        print(f"LocalDispatcher initialized with {num_workers} worker processes")

    # callback on task completion
    def _on_task_complete(self, result: Tuple[str, str, str]):
        task_id, status, result_payload = result
        self.handle_result(task_id, status, result_payload)

    # callback on pool execution failure
    def _on_task_error(self, error):
        print(f"Pool execution error: {error}")

    # Dispatch task to process pool
    def dispatch_task(self, task_id: str):
        payloads = self.get_task_payloads(task_id)
        if not payloads:
            self.redis.update_task_result(
                task_id, TaskStatus.FAILED, serialize("Task or function not found")
            )
            return

        fn_payload, args_payload = payloads

        # update status to RUNNING
        self.redis.update_task_status(task_id, TaskStatus.RUNNING)

        # track the task
        self.pending_tasks[task_id] = TaskInfo(
            task_id=task_id,
            start_time=time.time(),
            fn_payload=fn_payload,
            args_payload=args_payload,
        )

        # submit to pool
        self.pool.apply_async(
            execute_task,
            args=(task_id, fn_payload, args_payload),
            callback=self._on_task_complete,
            error_callback=self._on_task_error,
        )

        print(f"Task {task_id} dispatched to local pool")

    # start listening for tasks on Redis pub/sub
    def start(self):
        pubsub = self.redis.r.pubsub()
        pubsub.subscribe(REDIS_TASKS_CHANNEL)

        print(f"LocalDispatcher listening on Redis channel '{REDIS_TASKS_CHANNEL}'")

        try:
            for message in pubsub.listen():
                if not self.running:
                    break

                if message["type"] == "message":
                    task_id = message["data"]
                    print(f"Received task: {task_id}")
                    self.dispatch_task(task_id)

        except KeyboardInterrupt:
            print("LocalDispatcher interrupted")
        finally:
            pubsub.unsubscribe()
            self.pool.close()
            self.pool.join()

    def stop(self):
        super().stop()
        self.pool.terminate()


# ============== Pull Dispatcher ==============
# Pull mode using ZMQ REQ/REP pattern
# Workers request tasks, dispatcher responds with tasks or NO_TASK
class PullDispatcher(BaseDispatcher):

    def __init__(
        self,
        redis_client: RedisClient,
        port: int = DEFAULT_ZMQ_PORT,
        deadline: float = TASK_DEADLINE_SECONDS,
    ):
        super().__init__(redis_client)
        self.port = port
        self.deadline = deadline
        self.task_queue: list = []  # tasks waiting to be assigned
        self.workers: Dict[str, float] = {}  # worker_id -> last_seen

        # ZMQ setup
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{port}")

        print(f"PullDispatcher bound to tcp://*:{port}")

    # background thread to check for deadline violations
    def _deadline_checker(self):
        while self.running:
            time.sleep(1)
            current_time = time.time()

            # check for timed out tasks
            timed_out = []
            for task_id, task_info in list(self.pending_tasks.items()):
                if task_info.start_time and (
                    current_time - task_info.start_time > self.deadline
                ):
                    timed_out.append(task_id)

            # Mark timed out tasks as failed
            for task_id in timed_out:
                print(f"Task {task_id} exceeded deadline of {self.deadline}s")
                self.redis.update_task_result(
                    task_id,
                    TaskStatus.FAILED,
                    serialize(
                        f"WorkerFailure: Task exceeded deadline of {self.deadline}s"
                    ),
                )
                del self.pending_tasks[task_id]

    # background thread to listen for new tasks from Redis
    def _redis_listener(self):
        pubsub = self.redis.r.pubsub()
        pubsub.subscribe(REDIS_TASKS_CHANNEL)

        print(f"PullDispatcher subscribed to Redis channel '{REDIS_TASKS_CHANNEL}'")

        for message in pubsub.listen():
            if not self.running:
                break

            if message["type"] == "message":
                task_id = message["data"]
                print(f"New task queued: {task_id}")
                self.task_queue.append(task_id)

        pubsub.unsubscribe()

    # start the pull dispatcher
    def start(self):

        # start background threads
        deadline_thread = threading.Thread(target=self._deadline_checker, daemon=True)
        deadline_thread.start()

        redis_thread = threading.Thread(target=self._redis_listener, daemon=True)
        redis_thread.start()

        print("PullDispatcher waiting for worker requests...")

        try:
            while self.running:
                # Wait for worker request with timeout
                if self.socket.poll(1000):  # 1 second timeout
                    msg = self.socket.recv_multipart()

                    msg_type = msg[0]

                    if msg_type == b"PULL_REQUEST":
                        worker_id = msg[1].decode()
                        self.workers[worker_id] = time.time()

                        # check if we have a task to assign
                        if self.task_queue:
                            task_id = self.task_queue.pop(0)
                            payloads = self.get_task_payloads(task_id)

                            if payloads:
                                fn_payload, args_payload = payloads

                                # update status
                                self.redis.update_task_status(
                                    task_id, TaskStatus.RUNNING
                                )

                                # track task
                                self.pending_tasks[task_id] = TaskInfo(
                                    task_id=task_id,
                                    worker_id=worker_id,
                                    start_time=time.time(),
                                    fn_payload=fn_payload,
                                    args_payload=args_payload,
                                )

                                # send task to worker
                                self.socket.send_multipart(
                                    [
                                        b"TASK",
                                        task_id.encode(),
                                        fn_payload.encode(),
                                        args_payload.encode(),
                                    ]
                                )
                                print(f"Sent task {task_id} to worker {worker_id}")
                            else:
                                # task/function not found
                                self.socket.send_multipart([b"NO_TASK"])
                        else:
                            # No tasks available
                            self.socket.send_multipart([b"NO_TASK"])

                    elif msg_type == b"PULL_RESULT":
                        task_id = msg[1].decode()
                        status = msg[2].decode()
                        result_payload = msg[3].decode()

                        self.handle_result(task_id, status, result_payload)

                        # send ACK
                        self.socket.send_multipart([b"ACK"])

        except KeyboardInterrupt:
            print("PullDispatcher interrupted")
        finally:
            self.socket.close()
            self.context.term()

    def stop(self):
        super().stop()


# ============== Push Dispatcher ==============
# Push mode using ZMQ DEALER/ROUTER pattern
# Dispatcher pushes tasks to available workers
class PushDispatcher(BaseDispatcher):

    def __init__(
        self,
        redis_client: RedisClient,
        port: int = DEFAULT_ZMQ_PORT,
        heartbeat_timeout: float = HEARTBEAT_TIMEOUT_SECONDS,
    ):
        super().__init__(redis_client)
        self.port = port
        self.heartbeat_timeout = heartbeat_timeout
        self.task_queue: list = []
        self.workers: Dict[str, WorkerInfo] = {}  # worker_id -> WorkerInfo
        self.worker_identities: Dict[str, bytes] = {}  # worker_id -> ZMQ identity

        # ZMQ setup
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind(f"tcp://*:{port}")

        # Lock for thread safety
        self.lock = threading.Lock()

        print(f"PushDispatcher bound to tcp://*:{port}")

    # get available worker ID
    def _get_available_worker(self) -> Optional[str]:
        with self.lock:
            for worker_id, info in self.workers.items():
                if info.is_available:
                    return worker_id
        return None

    def _heartbeat_checker(self):
        while self.running:
            time.sleep(HEARTBEAT_CHECK_INTERVAL)
            current_time = time.time()

            with self.lock:
                dead_workers = []
                for worker_id, info in self.workers.items():
                    if current_time - info.last_heartbeat > self.heartbeat_timeout:
                        dead_workers.append(worker_id)

                # Handle dead workers
                for worker_id in dead_workers:
                    print(f"Worker {worker_id} missed heartbeat, marking as dead")
                    info = self.workers[worker_id]

                    # If worker had a task, mark it as failed
                    if info.current_task:
                        task_id = info.current_task
                        self.redis.update_task_result(
                            task_id,
                            TaskStatus.FAILED,
                            serialize(
                                f"WorkerFailure: Worker {worker_id} failed during execution"
                            ),
                        )
                        if task_id in self.pending_tasks:
                            del self.pending_tasks[task_id]

                    # remove worker
                    del self.workers[worker_id]
                    if worker_id in self.worker_identities:
                        del self.worker_identities[worker_id]

    # background thread to listen for new tasks from Redis
    def _redis_listener(self):

        pubsub = self.redis.r.pubsub()
        pubsub.subscribe(REDIS_TASKS_CHANNEL)

        print(f"PushDispatcher subscribed to Redis channel '{REDIS_TASKS_CHANNEL}'")

        for message in pubsub.listen():
            if not self.running:
                break

            if message["type"] == "message":
                task_id = message["data"]
                print(f"New task received: {task_id}")

                with self.lock:
                    self.task_queue.append(task_id)

        pubsub.unsubscribe()

    # background thread to dispatch tasks to available workers
    def _task_dispatcher(self):

        while self.running:
            time.sleep(0.01)  # small delay to avoid busy loop

            # try to dispatch queued tasks
            with self.lock:
                if not self.task_queue:
                    continue

                # find available worker
                available_worker = None
                for worker_id, info in self.workers.items():
                    if info.is_available:
                        available_worker = worker_id
                        break

                if not available_worker:
                    continue

                # get next task
                task_id = self.task_queue.pop(0)

            # dispatch task (outside lock)
            self._dispatch_to_worker(task_id, available_worker)

    def _dispatch_to_worker(self, task_id: str, worker_id: str):

        payloads = self.get_task_payloads(task_id)

        if not payloads:
            self.redis.update_task_result(
                task_id, TaskStatus.FAILED, serialize("Task or function not found")
            )
            return

        fn_payload, args_payload = payloads

        with self.lock:
            if worker_id not in self.worker_identities:
                # Worker no longer available, requeue task
                self.task_queue.insert(0, task_id)
                return

            identity = self.worker_identities[worker_id]

            # mark worker as busy
            self.workers[worker_id].is_available = False
            self.workers[worker_id].current_task = task_id

            # track task
            self.pending_tasks[task_id] = TaskInfo(
                task_id=task_id,
                worker_id=worker_id,
                start_time=time.time(),
                fn_payload=fn_payload,
                args_payload=args_payload,
            )

        # update status
        self.redis.update_task_status(task_id, TaskStatus.RUNNING)

        # send task to worker
        self.socket.send_multipart(
            [
                identity,
                b"TASK",
                task_id.encode(),
                fn_payload.encode(),
                args_payload.encode(),
            ]
        )

        print(f"Pushed task {task_id} to worker {worker_id}")

    # start the push dispatcher
    def start(self):

        # start background threads
        heartbeat_thread = threading.Thread(target=self._heartbeat_checker, daemon=True)
        heartbeat_thread.start()

        redis_thread = threading.Thread(target=self._redis_listener, daemon=True)
        redis_thread.start()

        dispatch_thread = threading.Thread(target=self._task_dispatcher, daemon=True)
        dispatch_thread.start()

        print("PushDispatcher waiting for worker connections...")

        try:
            while self.running:
                # Wait for messages from workers
                if self.socket.poll(1000):  # 1 second timeout
                    msg = self.socket.recv_multipart()
                    identity = msg[0]
                    msg_type = msg[1]

                    if msg_type == b"REGISTER":
                        worker_id = msg[2].decode()

                        with self.lock:
                            self.workers[worker_id] = WorkerInfo(
                                worker_id=worker_id, last_heartbeat=time.time()
                            )
                            self.worker_identities[worker_id] = identity

                        print(f"Worker {worker_id} registered")

                    elif msg_type == b"HEARTBEAT":
                        worker_id = msg[2].decode()

                        with self.lock:
                            if worker_id in self.workers:
                                self.workers[worker_id].last_heartbeat = time.time()

                    elif msg_type == b"PUSH_RESULT":
                        task_id = msg[2].decode()
                        status = msg[3].decode()
                        result_payload = msg[4].decode()

                        # find which worker sent this
                        with self.lock:
                            for worker_id, wid in self.worker_identities.items():
                                if wid == identity:
                                    self.workers[worker_id].is_available = True
                                    self.workers[worker_id].current_task = None
                                    break

                        self.handle_result(task_id, status, result_payload)

        except KeyboardInterrupt:
            print("PushDispatcher interrupted")
        finally:
            self.socket.close()
            self.context.term()

    def stop(self):
        super().stop()


# ============== Main Entry Point ==============
def main():
    parser = argparse.ArgumentParser(description="MPCSFaaS Task Dispatcher")
    parser.add_argument(
        "-m",
        "--mode",
        choices=["local", "pull", "push"],
        required=True,
        help="Dispatcher mode: local, pull, or push",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=DEFAULT_ZMQ_PORT,
        help=f"ZMQ port for pull/push modes (default: {DEFAULT_ZMQ_PORT})",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=DEFAULT_NUM_WORKERS,
        help=f"Number of worker processes for local mode (default: {DEFAULT_NUM_WORKERS})",
    )
    parser.add_argument(
        "--deadline",
        type=float,
        default=TASK_DEADLINE_SECONDS,
        help=f"Task deadline in seconds for pull mode (default: {TASK_DEADLINE_SECONDS})",
    )
    parser.add_argument(
        "--heartbeat-timeout",
        type=float,
        default=HEARTBEAT_TIMEOUT_SECONDS,
        help=f"Heartbeat timeout in seconds for push mode (default: {HEARTBEAT_TIMEOUT_SECONDS})",
    )

    args = parser.parse_args()

    # connect to Redis
    try:
        redis_client = RedisClient()
    except redis.exceptions.ConnectionError:
        print(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        print("Please ensure Redis is running: redis-server")
        return 1

    # create appropriate dispatcher
    if args.mode == "local":
        dispatcher = LocalDispatcher(redis_client, num_workers=args.workers)
    elif args.mode == "pull":
        dispatcher = PullDispatcher(
            redis_client, port=args.port, deadline=args.deadline
        )
    elif args.mode == "push":
        dispatcher = PushDispatcher(
            redis_client, port=args.port, heartbeat_timeout=args.heartbeat_timeout
        )
    else:
        print(f"Unknown mode: {args.mode}")
        return 1

    print(f"Starting {args.mode} dispatcher...")

    try:
        dispatcher.start()
    except KeyboardInterrupt:
        print("\nShutting down dispatcher...")
        dispatcher.stop()

    return 0


if __name__ == "__main__":
    exit(main())
