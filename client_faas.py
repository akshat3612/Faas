import requests
import dill
import codecs
import time


'''
Client responsible for REST
Dill:
- functions
- arguments
- results
- exceptions
(workers and dispatchers will use this too)
'''

'''
Client Action (HTTP)	Service Endpoint	        Service Action (Redis)
Register function	    /register_function (POST)	Service calls r.set()
Execute function	    /execute_function (POST)	Service calls r.set() and r.publish()
Get result	            /result/<task_id> (GET)	    Service calls r.get()
'''

# fastAPI service on default port 8000
SERVER_URL = "http://127.0.0.1:8000"


# dill pickling
def serialize(obj) -> str:
    '''convert object to string w dill'''
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(s: str):
    '''convert base64 string back to object'''
    return dill.loads(codecs.decode(s.encode(), "base64"))

# register function with FaaS service
# sends to FastAPI to store in redis
def register_function(fn):
    payload = serialize(fn)
    return requests.post(f"{SERVER_URL}/register_function", json={
        "name": fn.__name__,
        "payload": payload
    }).json()

# submitting job to workers to be executed later
def execute_function(fn_id, args):
    payload = serialize(args)
    return requests.post(f"{SERVER_URL}/execute_function", json={
        "function_id": fn_id,
        "payload": payload
    }).json()

# get status of a task
def get_status(task_id):
    return requests.get(f"{SERVER_URL}/status/{task_id}").json()

# get result of a task
# once status == completed
def get_result(task_id):
    return requests.get(f"{SERVER_URL}/result/{task_id}").json()

        
if __name__ == "__main__":
    
    def add_numbers(a, b):
        """simple function to be executed by workers"""
        time.sleep(2) 
        return a + b
    
    # register function
    reg_response = register_function(add_numbers)
    print(f"Registration: {reg_response}")
    # return the function's unique id
    fn_id = reg_response.get("function_id")

    # execute function
    if fn_id:
        args_for_fn = (10, 20)
        exec_response = execute_function(fn_id, args_for_fn)
        print(f"Execution Response: {exec_response}")
        # return task ID for the task
        task_id = exec_response.get("task_id")

    # wait for results
        if task_id:
            status = "PENDING"
            print(f"Waiting for task {task_id} to complete")
            while status not in ["COMPLETED", "FAILED"]:
                time.sleep(1)
                status_response = get_status(task_id)
                status = status_response.get("status", "UNKNOWN")
                print(f"Current status: {status}")

        # get results
            if status == "COMPLETED":
                        result_response = get_result(task_id)
                        final_result = deserialize(result_response.get("result"))
                        print(f"Task {task_id} completed. Result: {final_result}")
            elif status == "FAILED":
                        result_response = get_result(task_id)
                        exception_info = deserialize(result_response.get("exception"))
                        print(f"Task {task_id} failed: {exception_info}")
            else:
                print("Could not get task_id.")
        else:
            print("Could not get function_id.")