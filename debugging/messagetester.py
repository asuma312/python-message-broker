import socket
import pickle
from executor.client import client
from utils.message import  message
import config

class testobj:
    def __init__(self, data):
        self.data = data

if __name__ == "__main__":
    client = client(config.ip, config.port,'testqueue')
    testmessage = message('testmessage')
    client.add_message(testmessage)
    answer = None
    while not answer:
        answer = client.run()
    print(answer.answer)