import zmq
import time
import multiprocessing
import dill
import codecs

DISPATCHER_ADDR = "tcp://127.0.0.1:5555"

# dill pickling
def serialize(obj) -> str:
    '''convert object to string w dill'''
    return codecs.encode(dill.dumps(obj), "base64").decode().strip()

def deserialize(s: str):
    '''convert base64 string back to object'''
    return dill.loads(codecs.decode(s.encode(), "base64"))

# worker runs task
def run_task(data):
    fn = deserialize(data["fn"])
    args = deserialize(data["args"])
    try:
        # handle different arg types
        if isinstance(args, tuple):
            result = fn(*args)
        elif isinstance(args, dict):
            result = fn(**args)
        else:
            # single arg
            result = fn(args)
        
        return ("COMPLETE", serialize(result))
    except Exception as e:
        return ("FAILED", serialize(str(e)))
    
# start pull worker
if __name__ == "__main__":

    worker_id = f"pull-{int(time.time())}"  # unique name

    context = zmq.Context()
    socket = context.socket(zmq.REQ)

    socket.setsockopt(zmq.RCVTIMEO, 5000)   # 5 second timeout
    socket.connect(DISPATCHER_ADDR)

    print(f"Pull worker {worker_id} connected to {DISPATCHER_ADDR}")

    pool = multiprocessing.Pool(processes=3)

    while True:
        # see dispatcher for work
        socket.send_multipart([b"PULL_REQUEST", worker_id.encode()]) #sends pull request

        msg = socket.recv_multipart()
        response = msg[0] # tasks or no tasks

        if response == b"NO_TASK":  # if dispatcher has no task 
            time.sleep(2)
            continue

        if response == b"TASK": # if dispatcher has task 
            task_id = msg[1].decode()
            fn_payload = msg[2].decode().strip()  
            args_payload = msg[3].decode().strip()  

            print(f"Pull worker {worker_id} executing task {task_id}")

            status, result = run_task({"fn": fn_payload, "args": args_payload})

            # remove newlines in result payload
            result = result.replace("\n", "")

            # return result to dispatcher
            socket.send_multipart([b"PULL_RESULT", task_id.encode(), status.encode(), result.encode()])
            socket.recv()  # wait for ack
            print(f"Pull task {task_id} completed with status: {status}")
