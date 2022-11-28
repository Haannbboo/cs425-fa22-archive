import socket
import time
import pickle

from .utils import Message, socket_should_stop
from ..config import *

"""
Have a DNSserver which can do following jobs:
  1. Store who is introducer
  2. Once received the failure message, if introducer is curshed, update the message src as a new introducer
  3. Tell others who is introducer
"""

"""
Message: FailureMessage
{
  id: src id
  host: src host
  port: src port
  time_stamp: ANY
  message_type: LEAVE
  content: {
    id: curshed process' id
  }
}
"""

"""
Message: Get Introducer
{
  id: ANY
  host: host who want to know who is introducer
  port: port 
  time_stamp: ANY
  message_type: GET_INTRODUCER
  content: None
}
"""

"""
Message: RETURN Introducer
{
  id: ANY (Do we need to guaranteed the order of it?)
  host: introducer host
  port: port
  time_stamp: ANY
  message_type: RETURN_INTRODUCER
  content: None
}
"""

"""
Message: Turn on Introducer?
{
  id: ANY
  host: host who want to know who is introducer
  port: port 
  time_stamp: ANY
  message_type: Turn on Intruducer
  content: None
}
"""


class DNSserver:
    def __init__(self) -> None:
        self.introducer_host = ""
        self.introducer_port = -1
        self.introducer_id = -1
        self.id = -1
        self.host = DNS_SERVER_HOST
        self.port = DNS_SERVER_PORT

        self.global_unique_id = 0

    def server(self, s: socket.socket = ...):
        if s is Ellipsis:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        s.bind(("", self.port))
        print("I am DNSServer...")
        while True:
            try:
                data, addr = s.recvfrom(1024)
                if data:
                    
                    if socket_should_stop(data):
                        break
                    
                    msg: Message = pickle.loads(data)

                    # there is a process want to know who is introducer
                    if msg.message_type == "GET_INTRODUCER":

                        if self.introducer_host == "":
                            self.introducer_host = msg.host
                            self.introducer_port = PORT_FDINTRODUCER
                            self.introducer_id = msg.id

                        content = {
                            "introducer_host": self.introducer_host,
                            "introducer_port": self.introducer_port,
                            "introducer_id": self.introducer_id,
                            "assigned_id": self.global_unique_id,
                        }
                        self.global_unique_id += 1
                        introducer_info = self.__generate_message("RESP_INTRODUCER", content)
                        s.sendto(pickle.dumps(introducer_info), addr)
                        print(f"Respond to {msg.host} w/: {self.introducer_host}:{self.introducer_port}")

                    # receive a message that there is a processes is down
                    else:
                        crushed = msg.content["id"]
                        if crushed == self.introducer_id:
                            self.introducer_host = msg.host
                            self.introducer_port = msg.port
                            self.introducer_id = msg.id
                            print(f"Now the introducer is {self.introducer_host}, {self.introducer_id}")
            except Exception as e:
                s.close()
                raise e from None
        s.close()

    def __generate_message(self, m_type: str, content=None) -> Message:
        """Generates message for all communications."""
        return Message(self.id, self.host, self.port, time.time(), m_type, content)

    def run(self):
        self.server()


if __name__ == "__main__":
    d = DNSserver()
    d.run()
