import socket
import pickle
from utils.message import *
from time import sleep


class service:
    endstring = b"__end__"


    def __init__(self, host, port, queue:str):
        self.actions = {
            0: self.setup,
            1: self.getdata,
            2: self.answerdata,
            3: self.close
        }


        self.host = host
        self.port = port
        self.basequeue = queue.encode('utf-8')
        self.queue = self.basequeue

        self.fulldata = b""
        self.action = 0

        self.questions = []

        self.answer = None


    def create_queue(self):
        print("Creating queue")
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))
        msgobj = message(self.basequeue)
        pickledata = pickle.dumps(msgobj)
        fullbytes = b"new_queue++service" + b"___" + pickledata + self.endstring
        self.s.send(fullbytes)
        print("Queue sent")

    def receive_queue(self):
        msgobj = None
        while not msgobj:
            print("Waiting for queue")
            data = self.receive()
            msgobj = self.handle_response(data)
        _queue = msgobj.answer
        self.queue = _queue.name.encode('utf-8') + b"++service"
        print(f"Queue received {_queue.name}")





    def setup(self):
        """
        Create queue before starting the service
        :return:
        """
        self.create_queue()
        self.receive_queue()

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))

        queueobj = queue(self.queue.decode('utf-8'))
        self.send(queueobj)
        self.action += 1



    def send(self, message):
        pickledata = pickle.dumps(message)
        fullbytes = self.queue + b"___" + pickledata + self.endstring
        self.s.send(fullbytes)



    def startaction(self):
        action = self.actions.get(self.action)
        if action:
            print(f"Running action {self.action}")
            data = action()
            print(f"Action {self.action} done")
            return data
        else:
            print("Invalid action")
            self.close()




    def getdata(self):
        fulldata = None
        while not fulldata:
            data = self.receive()
            print(f"Data received {data}")
            fulldata = self.handle_response(data)
            print(f"Data handled {self.fulldata}")
            if fulldata:
                break
        return fulldata

    def startup(self):
        print("Starting service")
        setup = self.setup()
        print("Setup done, waiting for data")
        data = self.getdata()
        print(f"Data {data} received, answering")
        return data


    def finish(self,answer):
        self.answer = answer
        print("Running answer, closing and restarting")
        answering = self.answerdata()
        print("Answering done")
        sucessmsg = self.getdata()
        print(sucessmsg)
        if sucessmsg.message == "sucess":
            print("Sucess")
            closing = self.close()
            print("Closing done")
            return closing


    def answerdata(self):
        newsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        newsocket.connect((self.host, self.port))
        self.s = newsocket
        answer = pickle.dumps(self.answer)
        fullbytes = self.queue + b"___" + answer + self.endstring
        self.send(self.answer)
        self.action += 1

    def close(self):
        self.s.close()
        self.action = 0

    def handle_response(self,data):
        print(self.fulldata[-len(self.endstring):])
        self.fulldata += data
        if self.fulldata[-len(self.endstring):] == self.endstring:
            fulldata = self.fulldata[:-len(self.endstring)]
            fulldata = pickle.loads(fulldata)
            self.fulldata = b""
            self.action += 1
            return fulldata


    def receive(self):
        return self.s.recv(1)


if __name__ == "__main__":
    s = service('localhost',1046,'testqueue')
    s.run()