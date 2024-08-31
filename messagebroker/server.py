import socket
import threading
import pickle
from utils.message import *
from dataclasses import dataclass
import config

@dataclass
class Queue:
    queue: queue
    connection: socket.socket



class Server:
    bytesreceiving = 1
    endstring = b"__end__"

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((self.host, self.port))
        self.s.listen(10)
        print(f"Server listening on {self.host}:{self.port}")

        self.queues = queues()
        self.offqueues = queues()

        self.create_base_queues()

    def create_base_queues(self):
        basequeues = ["new_queue"]
        for __queue in basequeues:
            newqueue = queue(__queue)
            self.queues.add_queueobj(newqueue)

    def create_queue(self,queuename:str):
        queuename = queuename
        queuename = f"{queuename.replace("++service","")}{len(self.queues.list_queues())}"
        print(queuename)
        newqueue = queue(queuename)
        self.queues.add_queueobj(newqueue)
        return newqueue

    def get_queue(self,data):
        if len(data.split("__"))<2:
            return False, False
        queue,type = data.split("++")
        return queue,type


    def handle_data(self,data):
        obj = pickle.loads(data)
        return obj

    def verify_queue(self,queuename):
        queue = self.queues.verify_queue(queuename)
        if queue:
            print(f"Queue {queue.name} found")
            return queue
        return False


    def verify_obj(self,obj):
        if isinstance(obj,message):
            return 0
        elif isinstance(obj,queue):
            return 1
        return obj

    def handle_request(self,__queue,type,object,connection):
        handler = {
            0: self.handle_messageobj,
            1: self.handle_queueobj,
        }
        queueobj = self.verify_queue(__queue)
        objinstance = self.verify_obj(object)

        queuename = __queue.split("++")[0]

        print(f"Handling request {object} from {queuename}")

        if not queueobj and objinstance != 1:
            print(f"Queue {queuename} not found")
            object.userconnection = connection
            if not self.offqueues.verify_queue(queuename):
                print(f"Creating offqueue {queuename}")
                newqueue = queue(queuename)
                newmessage = object
                print(f"Adding message {object} to offqueue {queuename}")
                newqueue.add_message(newmessage)
                self.offqueues.add_queueobj(newqueue)
                return
            else:
                print(f"Adding message {object} to offqueue {queuename}")
                queueobj = self.offqueues.get_queue(queuename)
                queueobj.add_message(object)
                return
        if type == "client__":
            obj = self.verify_obj(object)
            if handler.get(obj):
                handler[obj](object,connection,type,queueobj)
        elif type == "service__":
            obj = self.verify_obj(object)
            if handler.get(obj):
                handler[obj](object,connection,type,queueobj)


    def handle_messageobj(self,object:message,clientconnection,type,queue):
        '''
        Service normally sends a message to the userconnection inside message object
        if it refers to clientconnection then it sends the message to the service
        '''
        if type == "client__":
            senddata = object
            senddata.userconnection = clientconnection
            queue.add_message(senddata)
            connection = queue.connection
            print(f"Sending message {senddata} to {queue.name}")
            self.send(senddata,connection)
        elif type=="service__":
            messages = queue.messages
            print(queue)
            if queue.name == 'new_queue':
                print("b")
                queuename = object.message.decode('utf-8')
                queue = self.create_queue(queuename)
                object.answer = queue
                print(f"Queue {queuename} created")
                self.send(object,clientconnection)
            elif object.id in [message.id for message in messages]:
                message = [message for message in messages if message.id == object.id][0]
                userconnection = message.userconnection
                queue.connection = clientconnection
                self.send(object,userconnection,queue)
                queue.messages.remove(message)
                self.queues.remove_queue(queue.name)

    def handle_queueobj(self,object:queue,connection,type="service__",queue=None):
        if type == "service__":
            object.connection = connection
            object.name = object.name.split("++")[0]

            self.queues.add_queueobj(object)
            print(f"Queue {object.name} added")

            if self.offqueues.verify_queue(object.name):
                messages = self.offqueues.get_queue(object.name).messages
                for message in messages:
                    self.handle_messageobj(message,message.userconnection,"client__",object)
                self.offqueues.remove_queue(object.name)
            return True
        return False


    def handle_client(self, conn, addr):
        print(f"Connected by {addr}")
        queuedata = ""
        fulldata = b""

        queue = None
        type = None

        try:
            while True:
                data = self.receive(conn)
                if not queue or not type:
                    queue,type = self.get_queue(queuedata)
                    queuedata += data.decode("utf-8")
                else:
                    fulldata += data
                    if fulldata.endswith(self.endstring):
                        objdata = fulldata[:-len(self.endstring)]
                        break
            obj = self.handle_data(objdata)
            response = self.handle_request(queue,type,obj,conn)
            # if response:
            #     self.send(response, conn)
        except Exception as e:
            print(f"Error handling client {addr}: {e}")

    def run(self):
        print("Server is running")
        while True:
            conn, addr = self.s.accept()
            client_thread = threading.Thread(target=self.handle_client, args=(conn, addr))
            client_thread.start()

    def send(self, data:message, conn,queue=None):
        #Queue is used to verify if its a service sending a message,then it comunicates back to the server the sucess status

        print(f"Sending {data.message if data.userconnection else data.answer} to {conn}")

        userconnection = data.userconnection
        datasent = data.answer
        data.userconnection = None
        bytesdata = pickle.dumps(data)
        fullbytes = bytesdata + self.endstring
        conn.send(fullbytes)
        if queue:
            print("Sending sucess message")
            conn = queue.connection
            sucessmsg = message("sucess")
            bytesdata = pickle.dumps(sucessmsg)
            fullbytes = bytesdata + self.endstring
            conn.send(fullbytes)
            print("Sucess message sent")

        data.userconnection = userconnection
        print(f"Sent {data.message if data.userconnection else data.answer} to {conn}")

    def receive(self, conn):
        return conn.recv(self.bytesreceiving)

    def close(self, conn):
        conn.close()


if __name__ == "__main__":
    server = Server(config.host, config.port)
    server.run()