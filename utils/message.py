import socket


class message:
    def __init__(self,message,id=None,queue="basic",answer=None):
        self.answer = answer
        self.queue = queue
        self.message = message
        self.id = id
        self.used = False
        self.userconnection = None

    def as_dict(self):
        return {
            "message":self.message,
            "id":self.id,
            "queue":self.queue.name,
            "answer":self.answer,
            "used":self.used
        }

    def as_dict_queue(self):
        return {
            "message":self.message,
            "id":self.id,
            "answer":self.answer,
            "used":self.used
        }

    def answer_message(self,message):
        self.answer = message
        return self



class queue:
    def __init__(self,name,connection:socket.socket=None):
        self.name = name
        self.connection = connection
        self.messages = []

    def as_dict(self):
        return {
            "name":self.name,
            "messages":[message.as_dict_queue() for message in self.messages]
        }



    def add_message(self,message):
        self.messages.append(message)
        return message

    def get_queue_size(self):
        return len(self.messages)

    def get_non_answered_message(self):
        message = [message for message in self.messages if not message.used]
        if message==[]:
            return None
        message = message[0]
        message.used = True
        return message

class queues:
    def __init__(self):
        self.queues = {}
        self.add_queue("basic")
    def list_queues(self):
        return [queue.name for queue in self.queues.values()]

    def as_dict(self):
        return {
            "queues":[queue.as_dict() for queue in self.queues.values()]
        }

    def add_queue(self,name):
        new_queue = queue(name)
        self.queues[name] = new_queue
        return new_queue

    def add_queueobj(self,obj):
        self.queues[obj.name] = obj
        return obj

    def get_queue(self,name):
        return self.queues.get(name)

    def remove_queue(self,name):
        self.queues.pop(name)

    def verify_queue(self,queuename):
        print(list(self.queues.keys()))
        queue = [q for q in list(self.queues.keys()) if str(q).startswith(queuename)]
        if queue == []:
            return None
        queue = self.queues[queue[0]]
        return queue



class messages:
    def __init__(self):
        self.queues = queues()
        self.queues.add_queue("basic")

    def add_message(self,_message,queue="basic"):
        if queue not in self.queues.list_queues():
            raise ValueError("Queue not found")
        queue = self.queues.get_queue(queue)
        all_msgs = queue.get_queue_size()
        new_message = message(_message,all_msgs,queue)
        queue.add_message(new_message)
        return new_message


    def change_message_state(self,id,queue="basic"):
        queue = self.queues.get_queue(queue)
        message = queue.messages[id]
        message.used = False
        return message


    def answer_message(self,queue,answer,id):
        queue = self.queues.get_queue(queue)
        message = queue.messages[int(id)]
        message.answer = answer
        return message


    def get_answered_message(self,id,queue="basic"):
        queue = self.queues.get_queue(queue)
        message = queue.messages[id]
        return message



    def get_next_message(self,queue="basic"):
        queue = self.queues.get_queue(queue)
        return queue.get_non_answered_message()

    def __enter__(self):
        return self

