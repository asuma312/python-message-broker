import socket
import pickle


class client:
    endstring = b"__end__"

    def __init__(self, host, port,queue):
        self.actions = {
            0: self.send,
            1: self.get_data,
        }

        self.host = host
        self.port = port
        self.queue = queue.encode('utf-8') + b"++client"
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))
        self.action = 0
        self.fulldata = b""
        self.answer = None

    def add_message(self,message):
        self.message = message

    def send(self):
        pickledata = pickle.dumps(self.message)
        fullbytes = self.queue + b"___" + pickledata + b"__end__"
        self.s.send(fullbytes)

    def run(self):
        action = self.actions.get(self.action)
        if action:
            data = action()
            self.action += 1
            return data
        else:
            print("Invalid action")

    def get_data(self):
        answer = None
        while not answer:
            data = self.receive()
            answer = self.handle_response(data)
        return answer


    def handle_response(self,data):
        self.fulldata += data
        if self.fulldata[-len(self.endstring):] == self.endstring:
            objdata = self.fulldata[:-len(self.endstring)]
            self.answer = pickle.loads(objdata)
            self.action += 1
            return self.answer
        return None


    def receive(self):
        return self.s.recv(1)

    def close(self):
        self.s.close()