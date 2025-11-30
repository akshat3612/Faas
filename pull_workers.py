import zmq
import time
import multiprocessing
import dill
import codecs

DISPATCHER_ADDR = ""

# dill pickling
def serialize(obj) -> str:
    '''convert object to string w dill'''
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(s: str):
    '''convert base64 string back to object'''
    return dill.loads(codecs.decode(s.encode(), "base64"))

# worker runs task
def run_task(data):
    fn = deserialize(data["fn"])
    args = deserialize(data["args"])
    try:
        result = fn(*args)
        return ("COMPLETED", serialize(result))
    except Exception as e:
        return ("FAILED", serialize(str(e)))
    
# start pull worker
if __name__ == "__main__":

    worker_id = f"pull-{int(time.time())}"   # unique name

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(DISPATCHER_ADDR)

    pool = multiprocessing.Pool(processes=3)

    while True:
        # see dispatcher for work
        socket.send_multipart([b"PULL_REQUEST", worker_id.encode()]) #sends pull request

        msg = socket.recv_multipart()
        response = msg[0] # tasks or no tasks

        if response == b"NO_TASK":    # if dispatcher has no task 
            time.sleep(2)
            continue

        if response == b"TASK":   # if dispatcher has task 
            task_id = msg[1].decode()
            fn_payload = msg[2]
            args_payload = msg[3]

            print(f"Pull worker executing task {task_id}")

            status, result = run_task({"fn": fn_payload, "args": args_payload})

            # return result to dispatcher
            socket.send_multipart([b"PULL_RESULT", task_id.encode(), status.encode(), result])
            socket.recv()  # wait for ack