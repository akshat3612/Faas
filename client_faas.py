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
    return requests.post(f"{SERVER_URL}/register_function/", json={
        "id": "",                     # minimal required field
        "name": fn.__name__,
        "payload": payload,
        "functionMethod": payload,    # stored as pickled function body
        "params": []                  # default empty
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
    
    def add_numbers(a, b):
        """simple function to be executed by workers"""
        time.sleep(2) 
        return a + b
    
    # register function
    reg_response = register_function(add_numbers)
    print(f"Registration: {reg_response}")
    fn_id = reg_response.get("function_id")

    # execute function
    if fn_id:
        args_for_fn = (10, 20)
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
                if final_result is not None:
                    final_result = deserialize(final_result)
                print(f"Task {task_id} completed. Result: {final_result}")

            elif status == "FAILED":
                result_response = get_result(task_id)
                exception_info = result_response.get("exception")
                if exception_info is not None:
                    exception_info = deserialize(exception_info)
                print(f"Task {task_id} failed: {exception_info}")

    else:
        print("Could not get function_id")