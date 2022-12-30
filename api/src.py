import time
import datetime
import math

def wait(seconds):
    try:
        print((f"sleeping started at {datetime.datetime.now()}. Click ctrl+c to stop me"))
        time.sleep(seconds)
        print(f"sleeping ended at {datetime.datetime.now()}")

    except KeyboardInterrupt:
        print("Keyboard interrupted")
        exit()


def to_unix(dt: str):
    'This function takes in a timestamp and returns the unixtime'
    if dt is None:
        return None
    else:
        unix = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").timestamp()

    return math.ceil(unix)