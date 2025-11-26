from locust import HttpUser, task, constant_pacing
import random
import string
import os
import csv

def random_cid():
    """產生隨機 client ID"""
    return "user_" + "".join(random.choices(string.ascii_letters + string.digits, k=8))

class PixelTrackerUser(HttpUser):
    wait_time = constant_pacing(0.1)

    @task
    def track_mapping(self):
        cid = random_cid()
        partner = "OneAD"
        url = f"/track?cid={cid}&partner={partner}"

        with self.client.get(url, name="/track", headers={}, catch_response=True) as response:
            if response.status_code == 200:
                json_data = response.json()
                if "mapping_id" not in json_data:
                    response.failure("No mapping_id returned")
            else:
                response.failure(f"HTTP {response.status_code}")
