import requests
import dill
import codecs


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
def register_function(fn):
    payload = serialize(fn)
    return requests.post(f"{SERVER_URL}/register_function", json={
        "name": fn.__name__,
        "payload": payload
    }).json()

# execute function
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
def get_result(task_id):
    return requests.get(f"{SERVER_URL}/result/{task_id}").json()

        
if __name__ == "__main__":
    print("Client is ready.")