# MPCSFaaS Integration Tests - Extended Test Cases

# Additional tests covering:
# - Multiple function types
# - Concurrent execution
# - Error handling
# - Status transitions
# - Different argument formats


import requests
from mpcsfaas.serialize import serialize, deserialize
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

base_url = "http://127.0.0.1:8000/"
valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]


# ============== Helper Functions ==============


def register_function(fn):
    """Helper to register a function"""
    resp = requests.post(
        base_url + "register_function",
        json={"name": fn.__name__, "payload": serialize(fn)},
    )
    resp.raise_for_status()
    return resp.json()["function_id"]


def execute_function(fn_id, args):
    """Helper to execute a function"""
    resp = requests.post(
        base_url + "execute_function",
        json={"function_id": fn_id, "payload": serialize(args)},
    )
    resp.raise_for_status()
    return resp.json()["task_id"]


def wait_for_result(task_id, timeout=30):
    """Helper to wait for task completion"""
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(f"{base_url}result/{task_id}")
        data = resp.json()
        if data["status"] in ["COMPLETED", "FAILED"]:
            return data
        time.sleep(0.1)
    return {"status": "TIMEOUT", "task_id": task_id}


# ============== Test Functions ==============


def add(x, y):
    return x + y


def multiply(x, y):
    return x * y


def square(x):
    return x * x


def sleep_task(duration):
    import time

    time.sleep(duration)
    return f"slept {duration}s"


# CPU-bound task
def compute_sum(n):
    total = 0
    for i in range(n):
        total += i
    return total


def failing_function(x):
    raise ValueError(f"Intentional error with input {x}")


# Returns input unchanged
def identity(x):
    return x


# ============== Registration Tests ==============


# Tests for function registration
class TestRegistration:

    def test_register_simple_function(self):

        fn_id = register_function(square)
        assert fn_id is not None
        assert len(fn_id) == 36  # UUID format

    def test_register_multiple_functions(self):
        fn_ids = []
        for fn in [add, multiply, square, identity]:
            fn_id = register_function(fn)
            fn_ids.append(fn_id)

        # All should be unique
        assert len(set(fn_ids)) == 4

    # Test registering with empty payload fails
    def test_register_empty_payload(self):
        resp = requests.post(
            base_url + "register_function",
            json={"name": "test", "payload": ""},
        )
        assert resp.status_code in [400, 500]

    # Test registering with invalid base64 fails
    def test_register_invalid_base64(self):
        resp = requests.post(
            base_url + "register_function",
            json={"name": "test", "payload": "not-valid-base64!!!"},
        )
        assert resp.status_code in [400, 500]


# ============== Execution Tests ==============


class TestExecution:

    def test_execute_single_arg(self):
        fn_id = register_function(square)
        task_id = execute_function(fn_id, ((5,), {}))

        result = wait_for_result(task_id)
        assert result["status"] == "COMPLETED"
        assert deserialize(result["result"]) == 25

    def test_execute_multiple_args(self):
        fn_id = register_function(add)
        task_id = execute_function(fn_id, ((3, 7), {}))

        result = wait_for_result(task_id)
        assert result["status"] == "COMPLETED"
        assert deserialize(result["result"]) == 10

    # Test executing function with keyword arguments
    def test_execute_with_kwargs(self):
        fn_id = register_function(add)
        task_id = execute_function(fn_id, ((), {"x": 10, "y": 20}))

        result = wait_for_result(task_id)
        assert result["status"] == "COMPLETED"
        assert deserialize(result["result"]) == 30

    # Test executing with invalid function ID fails
    def test_execute_invalid_function_id(self):
        resp = requests.post(
            base_url + "execute_function",
            json={
                "function_id": "00000000-0000-0000-0000-000000000000",
                "payload": serialize(((1,), {})),
            },
        )
        assert resp.status_code == 404

    def test_execute_malformed_function_id(self):
        resp = requests.post(
            base_url + "execute_function",
            json={
                "function_id": "not-a-uuid",
                "payload": serialize(((1,), {})),
            },
        )
        assert resp.status_code == 400


# ============== Status Tests ==============


# Tests for status and result endpoints
class TestStatus:

    # Test status endpoint returns valid status
    def test_status_endpoint(self):
        fn_id = register_function(square)
        task_id = execute_function(fn_id, ((2,), {}))

        resp = requests.get(f"{base_url}status/{task_id}")
        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        assert resp.json()["status"] in valid_statuses

    # Test result endpoint returns result when complete
    def test_result_endpoint(self):
        fn_id = register_function(square)
        task_id = execute_function(fn_id, ((3,), {}))

        result = wait_for_result(task_id)
        assert result["status"] == "COMPLETED"
        assert "result" in result

    def test_invalid_task_id(self):
        resp = requests.get(f"{base_url}status/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404

    def test_malformed_task_id(self):
        resp = requests.get(f"{base_url}status/not-a-uuid")
        assert resp.status_code == 400


# ============== Concurrent Execution Tests ==============


class TestConcurrent:

    def test_concurrent_same_function(self):
        fn_id = register_function(square)

        # Submit 10 tasks
        task_ids = []
        for i in range(10):
            task_id = execute_function(fn_id, ((i,), {}))
            task_ids.append((task_id, i))

        # Wait for all results
        for task_id, input_val in task_ids:
            result = wait_for_result(task_id)
            assert result["status"] == "COMPLETED"
            assert deserialize(result["result"]) == input_val * input_val

    def test_concurrent_different_functions(self):
        add_id = register_function(add)
        mul_id = register_function(multiply)
        sq_id = register_function(square)

        tasks = [
            (execute_function(add_id, ((2, 3), {})), 5),
            (execute_function(mul_id, ((4, 5), {})), 20),
            (execute_function(sq_id, ((6,), {})), 36),
        ]

        for task_id, expected in tasks:
            result = wait_for_result(task_id)
            assert result["status"] == "COMPLETED"
            assert deserialize(result["result"]) == expected

    def test_many_concurrent_tasks(self):
        fn_id = register_function(identity)
        num_tasks = 50

        # Submit all tasks
        task_ids = []
        for i in range(num_tasks):
            task_id = execute_function(fn_id, ((i,), {}))
            task_ids.append((task_id, i))

        # Wait for all
        completed = 0
        for task_id, expected in task_ids:
            result = wait_for_result(task_id)
            if result["status"] == "COMPLETED":
                assert deserialize(result["result"]) == expected
                completed += 1

        # At least 90% should complete
        assert completed >= num_tasks * 0.9


# ============== Error Handling Tests ==============


class TestErrorHandling:

    # Test that function exceptions are handled
    def test_function_exception(self):
        fn_id = register_function(failing_function)
        task_id = execute_function(fn_id, ((42,), {}))

        result = wait_for_result(task_id)
        assert result["status"] == "FAILED"
        # Error message should be serialized in result
        assert "result" in result


# ============== Data Type Tests ==============


class TestDataTypes:

    # Test function with string input
    def test_string_input(self):
        fn_id = register_function(identity)
        task_id = execute_function(fn_id, (("hello world",), {}))

        result = wait_for_result(task_id)
        assert result["status"] == "COMPLETED"
        assert deserialize(result["result"]) == "hello world"

    # Test function with list input
    def test_list_input(self):
        fn_id = register_function(identity)
        task_id = execute_function(fn_id, (([1, 2, 3, 4, 5],), {}))

        result = wait_for_result(task_id)
        assert result["status"] == "COMPLETED"
        assert deserialize(result["result"]) == [1, 2, 3, 4, 5]

    # Test function with dict input
    def test_dict_input(self):
        fn_id = register_function(identity)
        test_dict = {"key": "value", "num": 42}
        task_id = execute_function(fn_id, ((test_dict,), {}))

        result = wait_for_result(task_id)
        assert result["status"] == "COMPLETED"
        assert deserialize(result["result"]) == test_dict

    # test function with nested data structures
    def test_nested_data(self):
        fn_id = register_function(identity)
        nested = {"list": [1, 2, {"inner": "dict"}], "tuple": (1, 2, 3)}
        task_id = execute_function(fn_id, ((nested,), {}))

        result = wait_for_result(task_id)
        assert result["status"] == "COMPLETED"
        result_data = deserialize(result["result"])
        assert result_data["list"] == [1, 2, {"inner": "dict"}]


# ============== API Tests ==============


class TestAPI:
    """Tests for API behavior"""

    # Test root endpoint returns service status
    def test_root_endpoint(self):
        resp = requests.get(base_url)
        assert resp.status_code == 200
        data = resp.json()
        assert "message" in data or "redis_status" in data

    # Test that API accepts JSON content type
    def test_content_type_json(self):
        fn_id = register_function(square)
        resp = requests.post(
            base_url + "execute_function",
            json={"function_id": fn_id, "payload": serialize(((2,), {}))},
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
