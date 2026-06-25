import base64
import os
import uuid


def generate_id():
    return base64.b64encode(os.urandom(16)).rstrip(b"=").decode()

if __name__ == "__main__":
    print(f"KAFKA_CLUSTER_ID={generate_id()}")
    print(f"KAFKA_CONTROLLER_DIRECTORY_ID={generate_id()}")
