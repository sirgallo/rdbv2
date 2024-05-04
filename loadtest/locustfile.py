import os
import random
import string

from locust import FastHttpUser, task, between


ENDPOINT = '/command'
HOSTNAME = os.getenv('HOSTNAME')
HOST = f'https://{HOSTNAME}'


def randomString(length):
  characters = string.ascii_letters + string.digits
  randomString = ''.join(random.choice(characters) for _ in range(length))
  
  return randomString


class MyUser(FastHttpUser):
  host = HOST
  wait_time = between(0.1, 0.5)  # Random wait time between requests

  @task(2)
  def insert(self):
    payload = {
      'action': 'put',
      'payload': {
        'collection': 'test',
        'value': randomString(30)
      }
    }

    response = self.client.post(ENDPOINT, json=payload, verify=False)

  @task(8)
  def find(self):
    payload = {
      'action': 'get',
      'payload': {
        'collection': 'test',
        'value': 'test1'
      }
    }

    response = self.client.post(ENDPOINT, json=payload, verify=False)