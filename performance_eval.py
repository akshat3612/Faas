"""
MPCSFaaS Performance Evaluation Client

Evaluates performance of local, pull, and push dispatcher modes.

Metrics:
- Latency: Time from task submission to result retrieval
- Throughput: Tasks completed per second
- Overhead: Comparison against local baseline

Experiments:
1. Single task latency (no-op and sleep tasks)
2. Throughput under load (concurrent tasks)
3. Weak scaling study (tasks scale with workers)

Usage:
    python3 performance_eval.py --mode [local|pull|push|all] --experiment [latency|throughput|scaling|all]

Examples:
    python3 performance_eval.py --mode all --experiment all
    python3 performance_eval.py --mode pull --experiment latency --tasks 10
    python3 performance_eval.py --mode push --experiment scaling --max-workers 8
"""

import requests
import dill
import codecs
import time
import sys
import argparse
import json
import statistics
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Callable, Any

# ============== Configuration ==============
SERVER_URL = "http://127.0.0.1:8000"
RESULTS_DIR = "performance_results"

# Default experiment parameters
DEFAULT_TASK_COUNT = 20
DEFAULT_TASKS_PER_WORKER = 5
DEFAULT_MAX_WORKERS = 8
DEFAULT_SLEEP_DURATION = 0.1  # seconds
POLL_INTERVAL = 0.05  # seconds
TASK_TIMEOUT = 60  # seconds


def sleep_function(duration):
    """Sleep function - simulates work"""
    import time  # Move import inside function

    time.sleep(duration)
    return f"slept for {duration}s"


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode().strip()


def deserialize(s: str):
    return dill.loads(codecs.decode(s.encode(), "base64"))


# ============== Test Functions ==============
def noop_function(x):
    """No-op function - returns immediately"""
    return x


def cpu_function(n):
    """CPU-bound function - computes sum of squares"""
    total = 0
    for i in range(n):
        total += i * i
    return total


def memory_function(size_kb):
    """Memory-bound function - allocates and processes data"""
    data = [i for i in range(size_kb * 128)]  # ~1KB per 128 ints
    return sum(data)


# ============== FaaS Client ==============
class PerfClient:
    """Performance testing client for MPCSFaaS"""

    def __init__(self, server_url: str = SERVER_URL):
        self.server_url = server_url
        self.registered_functions: Dict[str, str] = {}  # fn_name -> fn_id

    def health_check(self) -> bool:
        """Check if service is running"""
        try:
            response = requests.get(f"{self.server_url}/", timeout=5)
            return response.status_code == 200
        except:
            return False

    def register_function(self, fn: Callable) -> str:
        """Register a function, caching the ID"""
        fn_name = fn.__name__

        if fn_name in self.registered_functions:
            return self.registered_functions[fn_name]

        serialized = serialize(fn)
        response = requests.post(
            f"{self.server_url}/register_function/",
            json={
                "id": "perf_test",
                "name": fn_name,
                "payload": serialized,
                "functionMethod": "python",
                "params": [],
            },
        )
        response.raise_for_status()

        fn_id = response.json()["function_id"]
        self.registered_functions[fn_name] = fn_id
        return fn_id

    def execute_function(self, fn_id: str, args: Any) -> str:
        """Execute a function and return task_id"""
        payload = serialize(args)
        response = requests.post(
            f"{self.server_url}/execute_function/",
            json={"function_id": fn_id, "payload": payload},
        )
        response.raise_for_status()
        return response.json()["task_id"]

    def get_result(self, task_id: str) -> Tuple[str, Any]:
        """Get task result, returns (status, result)"""
        response = requests.get(f"{self.server_url}/result/{task_id}")
        response.raise_for_status()
        data = response.json()
        return data.get("status", "UNKNOWN"), data.get("result")

    def wait_for_result(
        self, task_id: str, timeout: float = TASK_TIMEOUT
    ) -> Tuple[float, str, Any]:
        """
        Wait for task completion.
        Returns (elapsed_time, status, result)
        """
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                return elapsed, "TIMEOUT", None

            status, result = self.get_result(task_id)

            if status in ["COMPLETE", "COMPLETED"]:
                return elapsed, "COMPLETE", result
            elif status == "FAILED":
                return elapsed, "FAILED", result

            time.sleep(POLL_INTERVAL)

    def run_task(self, fn: Callable, args: Any) -> Dict:
        """
        Run a single task end-to-end.
        Returns dict with timing info.
        """
        # Register (cached)
        reg_start = time.time()
        fn_id = self.register_function(fn)
        reg_time = time.time() - reg_start

        # Execute
        exec_start = time.time()
        task_id = self.execute_function(fn_id, args)
        exec_time = time.time() - exec_start

        # Wait for result
        wait_time, status, result = self.wait_for_result(task_id)

        total_time = reg_time + exec_time + wait_time

        return {
            "task_id": task_id,
            "status": status,
            "result": result,
            "reg_time": reg_time,
            "exec_time": exec_time,
            "wait_time": wait_time,
            "total_time": total_time,
        }

    def run_tasks_concurrent(
        self, fn: Callable, args_list: List[Any], max_concurrent: int = 10
    ) -> List[Dict]:
        """Run multiple tasks concurrently"""
        # Pre-register function
        fn_id = self.register_function(fn)

        results = []
        submit_times = {}

        # Submit all tasks
        submit_start = time.time()
        task_ids = []
        for args in args_list:
            task_id = self.execute_function(fn_id, args)
            task_ids.append(task_id)
            submit_times[task_id] = time.time()
        submit_end = time.time()

        # Wait for all results
        for task_id in task_ids:
            wait_time, status, result = self.wait_for_result(task_id)
            total_time = time.time() - submit_times[task_id]

            results.append(
                {
                    "task_id": task_id,
                    "status": status,
                    "result": result,
                    "total_time": total_time,
                    "wait_time": wait_time,
                }
            )

        return results


# ============== Experiment Classes ==============
class LatencyExperiment:
    """Measure single-task latency for different task types"""

    def __init__(self, client: PerfClient):
        self.client = client

    def run(self, num_trials: int = 10) -> Dict:
        """Run latency experiments"""
        print("\n" + "=" * 60)
        print("LATENCY EXPERIMENT")
        print("=" * 60)

        results = {
            "noop": [],
            "sleep_100ms": [],
            "sleep_500ms": [],
            "cpu_light": [],
            "cpu_heavy": [],
        }

        # Warm up
        print("Warming up...")
        self.client.run_task(noop_function, 1)

        # No-op tasks
        print(f"\nRunning {num_trials} no-op tasks...")
        for i in range(num_trials):
            result = self.client.run_task(noop_function, i)
            results["noop"].append(result)
            print(
                f"  Trial {i+1}: {result['total_time']*1000:.2f}ms ({result['status']})"
            )

        # Sleep 100ms
        print(f"\nRunning {num_trials} sleep(0.1s) tasks...")
        for i in range(num_trials):
            result = self.client.run_task(sleep_function, 0.1)
            results["sleep_100ms"].append(result)
            print(
                f"  Trial {i+1}: {result['total_time']*1000:.2f}ms ({result['status']})"
            )

        # Sleep 500ms
        print(f"\nRunning {num_trials} sleep(0.5s) tasks...")
        for i in range(num_trials):
            result = self.client.run_task(sleep_function, 0.5)
            results["sleep_500ms"].append(result)
            print(
                f"  Trial {i+1}: {result['total_time']*1000:.2f}ms ({result['status']})"
            )

        # CPU light (1000 iterations)
        print(f"\nRunning {num_trials} CPU-light tasks...")
        for i in range(num_trials):
            result = self.client.run_task(cpu_function, 1000)
            results["cpu_light"].append(result)
            print(
                f"  Trial {i+1}: {result['total_time']*1000:.2f}ms ({result['status']})"
            )

        # CPU heavy (100000 iterations)
        print(f"\nRunning {num_trials} CPU-heavy tasks...")
        for i in range(num_trials):
            result = self.client.run_task(cpu_function, 100000)
            results["cpu_heavy"].append(result)
            print(
                f"  Trial {i+1}: {result['total_time']*1000:.2f}ms ({result['status']})"
            )

        return self._summarize(results)

    def _summarize(self, results: Dict) -> Dict:
        """Calculate summary statistics"""
        summary = {}

        print("\n" + "-" * 60)
        print("LATENCY SUMMARY")
        print("-" * 60)
        print(
            f"{'Task Type':<15} {'Mean (ms)':<12} {'Std (ms)':<12} {'Min (ms)':<12} {'Max (ms)':<12}"
        )
        print("-" * 60)

        for task_type, trials in results.items():
            times = [
                t["total_time"] * 1000 for t in trials if t["status"] == "COMPLETE"
            ]

            if times:
                mean = statistics.mean(times)
                std = statistics.stdev(times) if len(times) > 1 else 0
                min_t = min(times)
                max_t = max(times)

                summary[task_type] = {
                    "mean_ms": mean,
                    "std_ms": std,
                    "min_ms": min_t,
                    "max_ms": max_t,
                    "num_success": len(times),
                    "num_failed": len(trials) - len(times),
                }

                print(
                    f"{task_type:<15} {mean:<12.2f} {std:<12.2f} {min_t:<12.2f} {max_t:<12.2f}"
                )
            else:
                summary[task_type] = {"error": "No successful trials"}
                print(f"{task_type:<15} {'FAILED':<12}")

        return summary


class ThroughputExperiment:
    """Measure throughput under concurrent load"""

    def __init__(self, client: PerfClient):
        self.client = client

    def run(self, task_counts: List[int] = None) -> Dict:
        """Run throughput experiments"""
        if task_counts is None:
            task_counts = [5, 10, 20, 50, 100]

        print("\n" + "=" * 60)
        print("THROUGHPUT EXPERIMENT")
        print("=" * 60)

        results = {}

        # Warm up
        print("Warming up...")
        self.client.run_task(noop_function, 1)

        for count in task_counts:
            print(f"\nSubmitting {count} concurrent no-op tasks...")

            args_list = list(range(count))

            start_time = time.time()
            task_results = self.client.run_tasks_concurrent(noop_function, args_list)
            total_time = time.time() - start_time

            successful = [r for r in task_results if r["status"] == "COMPLETE"]
            failed = [r for r in task_results if r["status"] != "COMPLETE"]

            throughput = len(successful) / total_time if total_time > 0 else 0
            avg_latency = (
                statistics.mean([r["total_time"] for r in successful])
                if successful
                else 0
            )

            results[count] = {
                "total_tasks": count,
                "successful": len(successful),
                "failed": len(failed),
                "total_time_s": total_time,
                "throughput_tps": throughput,
                "avg_latency_ms": avg_latency * 1000,
            }

            print(f"  Completed: {len(successful)}/{count}")
            print(f"  Total time: {total_time:.2f}s")
            print(f"  Throughput: {throughput:.2f} tasks/sec")
            print(f"  Avg latency: {avg_latency*1000:.2f}ms")

        self._print_summary(results)
        return results

    def _print_summary(self, results: Dict):
        """Print throughput summary table"""
        print("\n" + "-" * 60)
        print("THROUGHPUT SUMMARY")
        print("-" * 60)
        print(
            f"{'Tasks':<10} {'Success':<10} {'Time (s)':<12} {'Throughput':<15} {'Avg Lat (ms)':<12}"
        )
        print("-" * 60)

        for count, data in sorted(results.items()):
            print(
                f"{count:<10} {data['successful']:<10} {data['total_time_s']:<12.2f} "
                f"{data['throughput_tps']:<15.2f} {data['avg_latency_ms']:<12.2f}"
            )


class WeakScalingExperiment:
    """
    Weak scaling study: increase tasks proportionally with workers

    Note: This requires manually starting different numbers of workers
    for each trial. The experiment will prompt for worker count.
    """

    def __init__(self, client: PerfClient):
        self.client = client

    def run(
        self,
        tasks_per_worker: int = DEFAULT_TASKS_PER_WORKER,
        worker_counts: List[int] = None,
    ) -> Dict:
        """Run weak scaling experiment"""
        if worker_counts is None:
            worker_counts = [1, 2, 4, 8]

        print("\n" + "=" * 60)
        print("WEAK SCALING EXPERIMENT")
        print("=" * 60)
        print(f"Tasks per worker: {tasks_per_worker}")
        print("\nThis experiment requires manually adjusting worker count.")
        print("For each trial, ensure the correct number of workers are running.\n")

        results = {}

        for num_workers in worker_counts:
            num_tasks = num_workers * tasks_per_worker

            print(f"\n--- Trial: {num_workers} worker(s), {num_tasks} tasks ---")
            input(f"Press Enter when {num_workers} worker(s) are running...")

            # Warm up
            self.client.run_task(noop_function, 1)
            time.sleep(0.5)

            # Run tasks
            args_list = list(range(num_tasks))

            start_time = time.time()
            task_results = self.client.run_tasks_concurrent(
                sleep_function, [0.1] * num_tasks
            )
            total_time = time.time() - start_time

            successful = [r for r in task_results if r["status"] == "COMPLETE"]

            throughput = len(successful) / total_time if total_time > 0 else 0
            avg_latency = (
                statistics.mean([r["total_time"] for r in successful])
                if successful
                else 0
            )

            # Ideal time if perfectly parallel
            ideal_time = (num_tasks / num_workers) * 0.1  # sleep duration
            efficiency = ideal_time / total_time if total_time > 0 else 0

            results[num_workers] = {
                "num_workers": num_workers,
                "num_tasks": num_tasks,
                "successful": len(successful),
                "total_time_s": total_time,
                "ideal_time_s": ideal_time,
                "throughput_tps": throughput,
                "avg_latency_ms": avg_latency * 1000,
                "efficiency": efficiency,
            }

            print(f"  Completed: {len(successful)}/{num_tasks}")
            print(f"  Total time: {total_time:.2f}s (ideal: {ideal_time:.2f}s)")
            print(f"  Throughput: {throughput:.2f} tasks/sec")
            print(f"  Efficiency: {efficiency*100:.1f}%")

        self._print_summary(results)
        return results

    def _print_summary(self, results: Dict):
        """Print scaling summary"""
        print("\n" + "-" * 70)
        print("WEAK SCALING SUMMARY")
        print("-" * 70)
        print(
            f"{'Workers':<10} {'Tasks':<10} {'Time (s)':<12} {'Ideal (s)':<12} "
            f"{'Throughput':<12} {'Efficiency':<12}"
        )
        print("-" * 70)

        for num_workers, data in sorted(results.items()):
            print(
                f"{num_workers:<10} {data['num_tasks']:<10} {data['total_time_s']:<12.2f} "
                f"{data['ideal_time_s']:<12.2f} {data['throughput_tps']:<12.2f} "
                f"{data['efficiency']*100:<11.1f}%"
            )


class AutoScalingExperiment:
    """
    Automated scaling study without manual worker adjustment.
    Uses fixed worker setup and varies task count.
    """

    def __init__(self, client: PerfClient):
        self.client = client

    def run(self, task_counts: List[int] = None) -> Dict:
        """Run automated scaling experiment"""
        if task_counts is None:
            task_counts = [5, 10, 20, 40, 80]

        print("\n" + "=" * 60)
        print("AUTOMATED SCALING EXPERIMENT")
        print("=" * 60)
        print("Testing throughput with varying task counts")
        print("Using sleep(0.1s) tasks to measure parallelism\n")

        results = {"noop": {}, "sleep": {}}

        # Warm up
        print("Warming up...")
        self.client.run_task(noop_function, 1)

        # No-op tasks
        print("\n--- No-op Tasks ---")
        for count in task_counts:
            print(f"\nSubmitting {count} no-op tasks...")

            start_time = time.time()
            task_results = self.client.run_tasks_concurrent(
                noop_function, list(range(count))
            )
            total_time = time.time() - start_time

            successful = len([r for r in task_results if r["status"] == "COMPLETE"])
            throughput = successful / total_time if total_time > 0 else 0

            results["noop"][count] = {
                "tasks": count,
                "successful": successful,
                "total_time_s": total_time,
                "throughput_tps": throughput,
            }

            print(f"  Time: {total_time:.2f}s, Throughput: {throughput:.2f} tasks/sec")

        # Sleep tasks
        print("\n--- Sleep(0.1s) Tasks ---")
        for count in task_counts:
            print(f"\nSubmitting {count} sleep tasks...")

            start_time = time.time()
            task_results = self.client.run_tasks_concurrent(
                sleep_function, [0.1] * count
            )
            total_time = time.time() - start_time

            successful = len([r for r in task_results if r["status"] == "COMPLETE"])
            throughput = successful / total_time if total_time > 0 else 0

            # Calculate effective parallelism
            # If all tasks run in parallel: time ≈ 0.1s
            # If all tasks run sequentially: time ≈ count * 0.1s
            sequential_time = count * 0.1
            effective_parallelism = (
                sequential_time / total_time if total_time > 0 else 0
            )

            results["sleep"][count] = {
                "tasks": count,
                "successful": successful,
                "total_time_s": total_time,
                "throughput_tps": throughput,
                "sequential_time_s": sequential_time,
                "effective_parallelism": effective_parallelism,
            }

            print(f"  Time: {total_time:.2f}s (sequential: {sequential_time:.2f}s)")
            print(f"  Throughput: {throughput:.2f} tasks/sec")
            print(f"  Effective parallelism: {effective_parallelism:.1f}x")

        self._print_summary(results)
        return results

    def _print_summary(self, results: Dict):
        """Print summary"""
        print("\n" + "-" * 70)
        print("SCALING SUMMARY - Sleep Tasks")
        print("-" * 70)
        print(
            f"{'Tasks':<10} {'Time (s)':<12} {'Sequential (s)':<15} "
            f"{'Throughput':<15} {'Parallelism':<12}"
        )
        print("-" * 70)

        for count, data in sorted(results["sleep"].items()):
            print(
                f"{count:<10} {data['total_time_s']:<12.2f} "
                f"{data['sequential_time_s']:<15.2f} "
                f"{data['throughput_tps']:<15.2f} "
                f"{data['effective_parallelism']:<11.1f}x"
            )


# ============== Results Export ==============
def export_results(results: Dict, mode: str, experiment: str):
    """Export results to CSV and JSON"""
    import os

    os.makedirs(RESULTS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON export
    json_file = f"{RESULTS_DIR}/{mode}_{experiment}_{timestamp}.json"
    with open(json_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {json_file}")

    return json_file


# ============== Main ==============
def main():
    parser = argparse.ArgumentParser(description="MPCSFaaS Performance Evaluation")
    parser.add_argument(
        "--mode",
        choices=["local", "pull", "push", "all"],
        default="all",
        help="Dispatcher mode to test (default: all)",
    )
    parser.add_argument(
        "--experiment",
        choices=["latency", "throughput", "scaling", "auto-scaling", "all"],
        default="all",
        help="Experiment to run (default: all)",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=10,
        help="Number of trials for latency experiment (default: 10)",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        nargs="+",
        default=None,
        help="Task counts for throughput experiment",
    )
    parser.add_argument(
        "--tasks-per-worker",
        type=int,
        default=DEFAULT_TASKS_PER_WORKER,
        help="Tasks per worker for scaling experiment",
    )
    parser.add_argument(
        "--server", default=SERVER_URL, help=f"Server URL (default: {SERVER_URL})"
    )
    parser.add_argument("--export", action="store_true", help="Export results to files")

    args = parser.parse_args()

    # Create client
    client = PerfClient(args.server)

    # Health check
    print("Checking server connection...")
    if not client.health_check():
        print(f"ERROR: Cannot connect to server at {args.server}")
        print(
            "Make sure the server is running: uvicorn server_faas:app --host 127.0.0.1 --port 8000"
        )
        sys.exit(1)
    print("Server is running!\n")

    all_results = {}

    # Note about mode
    if args.mode != "all":
        print(f"Testing mode: {args.mode.upper()}")
        print(
            f"Make sure the dispatcher is running with: python3 task_dispatcher.py -m {args.mode}"
        )
        print("And appropriate workers are running.\n")
    else:
        print(
            "Mode set to 'all' - you'll need to run experiments separately for each mode"
        )
        print("and restart the dispatcher between runs.\n")
        args.mode = input("Enter current mode (local/pull/push): ").strip().lower()

    # Run experiments
    experiments_to_run = []
    if args.experiment == "all":
        experiments_to_run = ["latency", "throughput", "auto-scaling"]
    else:
        experiments_to_run = [args.experiment]

    for exp in experiments_to_run:
        if exp == "latency":
            experiment = LatencyExperiment(client)
            results = experiment.run(num_trials=args.trials)
            all_results["latency"] = results

        elif exp == "throughput":
            experiment = ThroughputExperiment(client)
            results = experiment.run(task_counts=args.tasks)
            all_results["throughput"] = results

        elif exp == "scaling":
            experiment = WeakScalingExperiment(client)
            results = experiment.run(tasks_per_worker=args.tasks_per_worker)
            all_results["scaling"] = results

        elif exp == "auto-scaling":
            experiment = AutoScalingExperiment(client)
            results = experiment.run(task_counts=args.tasks)
            all_results["auto_scaling"] = results

    # Export results
    if args.export:
        export_results(all_results, args.mode, args.experiment)

    print("\n" + "=" * 60)
    print("PERFORMANCE EVALUATION COMPLETE")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
