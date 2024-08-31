import socket
from executor.service import service
from utils.message import message
import threading
from time import sleep
import config

class testservice:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.basequeue = 'testqueue'
        self.queue = self.basequeue
        self.service = service(self.host, self.port, self.queue)

    def run(self):
        data = self.service.startup()
        if data:
            data = self.answer(data)
            self.service.finish(data)


    def answer(self,message):
        answerstring = "Teste concluido" + "*"*900
        message.answer_message(answerstring)
        return message

if __name__ == "__main__":
    testservice = testservice(config.ip, config.port)
    while True:
        testservice.run()
