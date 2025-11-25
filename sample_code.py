import dill
import codecs


def sampleFunction(a, b):
    result = a + b
    return result


def bruteforce_password(hashed_pin, startrange, endrange):
    import hashlib

    for i in range(startrange, endrange):
        if hashlib.md5(("%d" % i).encode()).hexdigest() == hashed_pin:
            return i
    return None


pw = "3cab3e11c1104722a842f0095235881f"


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()


def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


encoded = serialize(bruteforce_password)
print(encoded)

functionCode = deserialize(encoded)
print(functionCode)

print(sampleFunction(8, 9))
