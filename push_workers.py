import zmq
import time
import multiprocessing
import dill
import codecs

DISPATCHER_ADDR = "tcp://127.0.0.1:5555"
HEARTBEAT_INTERVAL = 0.2  #seconds

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
        # handle different argument types
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


if __name__ == "__main__":

    worker_id = f"push-{int(time.time())}"  # unique name

    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect(DISPATCHER_ADDR)

    print(f"Push worker {worker_id} connecting to {DISPATCHER_ADDR}")

    # register immediately
    socket.send_multipart([b"REGISTER", worker_id.encode()])
    print(f"Push worker {worker_id} registered")

    pool = multiprocessing.Pool(processes=3)  # concurrent tasks
    last_heartbeat = time.time()

    while True:

        # receive messages from dispatcher
        if socket.poll(200) & zmq.POLLIN:  # check for msg every 200ms
            msg = socket.recv_multipart()
            
            if len(msg) >= 2:
                msg_type = msg[0]
                
                if msg_type == b"TASK":
                    task_id = msg[1].decode()
                    fn_payload = msg[2].decode().strip()
                    args_payload = msg[3].decode().strip()

                    print(f"Push worker {worker_id} executing task {task_id}")
                    
                    # sends result back to dispatcher
                    def callback(result_tuple): 
                        status, result = result_tuple
                        # remove newlines from result
                        result = result.replace("\n", "")
                        socket.send_multipart([
                            b"PUSH_RESULT", 
                            task_id.encode(), 
                            status.encode(), 
                            result.encode()
                        ])
                        print(f"Task {task_id} completed with status: {status}")

                    pool.apply_async(
                        run_task, 
                        args=({"fn": fn_payload, "args": args_payload},), 
                        callback=callback
                    )

        # send heartbeat
        if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
            socket.send_multipart([b"HEARTBEAT", worker_id.encode()])
            last_heartbeat = time.time()