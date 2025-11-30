import zmq
import time
import multiprocessing
import dill
import codecs

DISPATCHER_ADDR = ""
HEARTBEAT_INTERVAL = 3 #sedonds

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


if __name__ == "__main__":

    worker_id = f"push-{int(time.time())}" # unique name

    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect(DISPATCHER_ADDR)

    # register immediately
    socket.send_multipart([b"REGISTER", worker_id.encode()])

    pool = multiprocessing.Pool(processes=3)  # concurrent tasks
    last_heartbeat = time.time()

    while True:

        # receive messages from dispatcher
        if socket.poll(200) & zmq.POLLIN: # check for msg every 200ms
            identity, msg_type, *payload = socket.recv_multipart() # [identity, msg_type, task_id, fn_payload, args_payload]

            if msg_type == b"TASK":
                task_id = payload[0].decode()
                fn_payload = payload[1]
                args_payload = payload[2]

                print(f"Push worker executing task {task_id}")
                
                # sends result back to dispatcher
                def callback(result_tuple): 
                    status, result = result_tuple
                    socket.send_multipart([identity, b"PUSH_RESULT", task_id.encode(), status.encode(), result])

                pool.apply_async(run_task, args=({"fn": fn_payload, "args": args_payload},), callback=callback)

        # Send heartbeat
        if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
            socket.send_multipart([b"HEARTBEAT", worker_id.encode()])
            last_heartbeat = time.time()
            print("Heartbeat sent")