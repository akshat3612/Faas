import requests
import dill
import codecs
import time
import sys

# fastAPI service on default port 8000
SERVER_URL = "http://127.0.0.1:8000"


# dill pickling
def serialize(obj) -> str:
    '''convert object to string w dill'''
    return codecs.encode(dill.dumps(obj), "base64").decode().strip()

def deserialize(s: str):
    '''convert base64 string back to object'''
    return dill.loads(codecs.decode(s.encode(), "base64"))

# register function with FaaS service
# sends to FastAPI to store in redis
def register_function(fn):
    serialized = serialize(fn)
    return requests.post(f"{SERVER_URL}/register_function/", json={
        "id": "ignored",
        "name": fn.__name__,
        "payload": serialized,
        "functionMethod": "python",
        "params": []
    }).json()

# submitting job to workers to be executed later
def execute_function(fn_id, args):
    payload = serialize(args)
    return requests.post(f"{SERVER_URL}/execute_function/", json={
        "function_id": fn_id,
        "payload": payload
    }).json()

# get status of a task (using result endpoint since status endpoint does not exist)
def get_status(task_id):
    return requests.get(f"{SERVER_URL}/result/{task_id}").json()

# get final result of a task
def get_result(task_id):
    return requests.get(f"{SERVER_URL}/result/{task_id}").json()

        
if __name__ == "__main__":
    
    # parse cl arguments
    if len(sys.argv) < 3:
        print("Usage: python3 client_faas.py '<function>' <arg1> [arg2] ...")
        sys.exit(1)
    
    # convert function string to actual function
    fn_string = sys.argv[1]
    fn = eval(fn_string)
    
    # parse remaining arguments
    args_for_fn = []
    for arg in sys.argv[2:]:
        try:
            # try convert to int/float
            if '.' in arg:
                args_for_fn.append(float(arg))
            else:
                args_for_fn.append(int(arg))
        except ValueError:
            # keep as string if not a number
            args_for_fn.append(arg)
    
    # convert to single value if only one arg, otherwise tuple
    if len(args_for_fn) == 1:
        args_for_fn = args_for_fn[0]
    else:
        args_for_fn = tuple(args_for_fn)
    
    # register function
    reg_response = register_function(fn)
    print(f"Registration: {reg_response}")
    fn_id = reg_response.get("function_id")

    # execute function
    if fn_id:
        exec_response = execute_function(fn_id, args_for_fn)
        print(f"Execution Response: {exec_response}")
        task_id = exec_response.get("task_id")

        # wait for results
        if task_id:
            status = "QUEUED"
            print(f"Waiting for task {task_id} to complete")
            while status not in ["COMPLETE", "FAILED"]:
                time.sleep(1)
                status_response = get_status(task_id)
                status = status_response.get("status", "UNKNOWN")
                print(f"Current status: {status}")

            # get results
            if status == "COMPLETE":
                result_response = get_result(task_id)
                final_result = result_response.get("result")
                print(f"Task {task_id} completed. Result: {final_result}")

            elif status == "FAILED":
                result_response = get_result(task_id)
                exception_info = result_response.get("exception")
                print(f"Task {task_id} failed: {exception_info}")

    else:
        print("Could not get function_id")