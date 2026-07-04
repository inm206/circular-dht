#COMP9331 DHT Assignment
#By Isaac Ngotho Mbira

import sys
import os
import socket
import threading
from time import sleep
import ast

IDENTITY = 0
NEXTPEER1 = 0
NEXTPEER2 = 0
PREVPEER1 = -1
PREVPEER2 = -1
PINGPORT = 0
PREV1READY = False
PREV2READY = False
RUNNER = True
SEQ = 0

# Peer class to hold identity and successors
class Peer():
    def __init__(self, identity, nextPeer1, nextPeer2):
        global PINGPORT
        self.identity = identity
        self.nextPeer1 = nextPeer1
        self.nextPeer2 = nextPeer2
        PINGPORT = 50000 + identity
        
# Class to send Ping UDP Messages
class SendPing(threading.Thread):
    def __init__(self):
        super(SendPing, self).__init__()
        global IDENTITY
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.message = ''

    
    def run(self):
        global NEXTPEER1, NEXTPEER2, SEQ
        
        # Send ping message to both successors
        while RUNNER:
            self.message = "{'from': " + str(IDENTITY) + ", 'seq': " + str(SEQ) + ", 'next1': " + str(NEXTPEER1) + ", 'next2': " + str(NEXTPEER2) + "}"
            self.sock.sendto(self.message.encode(), ("127.0.0.1", 50000 + NEXTPEER1))
            self.sock.sendto(self.message.encode(), ("127.0.0.1", 50000 + NEXTPEER2))
            sleep(5)
            SEQ += 1
        
# Class to receive Ping UDP messages 
class ReceivePing(threading.Thread):
    def __init__(self):
        super(ReceivePing, self).__init__()
        self.sockR = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sockR.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sockR.bind(("127.0.0.1", PINGPORT))
    
    def run(self):
        global NEXTPEER1, NEXTPEER2, PREVPEER1, PREVPEER2, SEQ
        responseSeq1 = {'peer': -1, 'received': -1}
        responseSeq2 = {'peer': -1, 'received': -1}
        while RUNNER:
            data, addr = self.sockR.recvfrom(1024)
            data = data.decode()
            data_dict = ast.literal_eval(data)

            # Handle receipt of ping response message
            if 'r' in data_dict:
                print("A ping response message was received from Peer", data_dict['r'])

                if data_dict['r'] == NEXTPEER1:
                    responseSeq1['peer'] = int(data_dict['r'])
                    responseSeq1['received'] = int(data_dict['seq'])
                if data_dict['r'] == NEXTPEER2:
                    responseSeq2['peer'] = int(data_dict['r'])
                    responseSeq2['received'] = int(data_dict['seq'])

            # Handle receipt of ping request message
            else:	
                message = "{'r': " + str(IDENTITY) + ", 'seq': " + str(data_dict['seq']) + "}"
                message = message.encode()
                print("A ping request message was received from Peer " + str(data_dict['from']))
                
                if int(data_dict['next1']) == IDENTITY:
                    PREVPEER1 = int(data_dict['from'])

                if int(data_dict['next2']) == IDENTITY:
                    PREVPEER2 = int(data_dict['from'])
                # Send ping response message
                self.sockR.sendto(message, ("127.0.0.1", 50000 + int(data_dict['from'])))

            if SEQ - responseSeq1['received'] >= 4:
                print("Peer " + str(NEXTPEER1) + " is no longer alive.")
                NEXTPEER1 = NEXTPEER2
                NEXTPEER2 = -1
                responseSeq1['received'] = SEQ
                print("My first successor is now peer " + str(NEXTPEER1) + ".")
                killedPeer()

            if SEQ - responseSeq2['received'] >= 4:
                print("Peer " + str(NEXTPEER2) + " is no longer alive.")
                NEXTPEER2 = -1
                responseSeq2['received'] = SEQ
                print("My first successor is now peer " + str(NEXTPEER1) + ".")
                killedPeer()
            


# Class to send file request TCP messages        
class FileRequest(threading.Thread):
    def __init__(self, fileNum):
        global IDENTITY
        super(FileRequest, self).__init__()
        self.fileNum = fileNum
        self.fileNumHash = fileNum % 256
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(("127.0.0.1", 50000 + NEXTPEER1))
        self.request = "{'source': " + str(IDENTITY) + ", 'file': " + str(self.fileNum) + ", 'hash': " + str(self.fileNumHash) + "}"

    def run(self):
        self.sock.send(self.request.encode())
        print("\nFile request message for " + str(self.fileNum) + " has been sent to my successor.")


# Class to receive all TCP messages  
class FileReceive(threading.Thread):
    def __init__(self):
        global IDENTITY
        super(FileReceive, self).__init__()
        self.sockR = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sockR.bind(("127.0.0.1", 50000 + IDENTITY))
        self.sockR.listen(5)

    # Function to decide if file is at current peer
    def isFileHere(self, fileHash):
        # Case if first peer in circle
        if IDENTITY < NEXTPEER1 < PREVPEER2 < PREVPEER1:
            if fileHash > PREVPEER1:
                return 1
            else: return 0
        # Case if last peer
        elif NEXTPEER1 < PREVPEER2 < PREVPEER1 < IDENTITY:
            if PREVPEER1 < fileHash < IDENTITY:
                return 1
            else: return 0
        else:
            if PREVPEER1 < fileHash < IDENTITY:
                return 1
            else: return 0

    def run(self):
        global PREVPEER1, PREVPEER2, PREV1READY, PREV2READY, NEXTPEER1, NEXTPEER2, IDENTITY
        while RUNNER:
            conn, addr = self.sockR.accept()
            data = ''
            data = conn.recv(1024)
            data = data.decode()
            data_dict = ast.literal_eval(data)
            
            # Handle file found message
            if 'fileAt' in data_dict:
                print("\nReceived a response from peer " + str(data_dict['fileAt']) + ", which has the file " + str(data_dict['file']))
            
            # Handle predecessor update message
            elif 'update' in data_dict:
                if 'prev1'in data_dict:
                    PREVPEER1 = int(data_dict['prev1'])
                PREVPEER2 = int(data_dict['prev2'])

            # Handle graceful peer departure message
            elif 'leaving' in data_dict:
                if 'ready' in data_dict:
                    
                    if int(data_dict['ready']) == PREVPEER1:
                        PREV1READY = True
                    elif int(data_dict['ready']) == PREVPEER2:
                        PREV2READY = True
                else:
                    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.sock.connect(("127.0.0.1", 50000 + int(data_dict['leaving'])))
                    print("\nPeer " + str(data_dict['leaving']) + " will depart from the network.")
                    responseMsg = "{'leaving': 'response', 'ready': " + str(IDENTITY) + "}"
                    self.sock.send(responseMsg.encode())
                    if 'next1' in data_dict:
                        NEXTPEER1 = int(data_dict['next1'])
                    NEXTPEER2 = int(data_dict['next2'])
                    print("My first successor is now peer " + str(NEXTPEER1) + ".")
                    print("My second successor is now peer " + str(NEXTPEER2) + ".")
            
            #Handle successor request message
            elif 'request' in data_dict:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect(("127.0.0.1", 50000 + int(data_dict['request'])))
                responseMsg = "{'successor': " + str(NEXTPEER1) + "}"
                self.sock.send(responseMsg.encode())

            #Handle successor response (udpate) message
            elif 'successor' in data_dict:
                if NEXTPEER2 == -1:
                    NEXTPEER2 = int(data_dict['successor'])
                    print("My second successor is now peer " + str(NEXTPEER2) + ".")
            
            # Handle file request message
            elif 'source' in data_dict:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                res = self.isFileHere(int(data_dict['hash']))
                # If file not at current peer
                if res == 0:
                    print("\nFile " + str(data_dict['file']) + " is not stored here.")
                    self.sock.connect(("127.0.0.1", 50000 + NEXTPEER1))
                    self.sock.send(data.encode())
                    print("\nFile request message has been forwarded to my successor.")
                # If file is at current peer
                elif res == 1:
                    print("\nFile " + str(data_dict['file']) + " is here.")
                    msg = "{'fileAt': " + str(IDENTITY) + ", 'file': " + str(data_dict['file']) + "}"
                    self.sock.connect(("127.0.0.1", 50000 + int(data_dict['source'])))
                    self.sock.send(msg.encode())
                    print("\nA response message, destined for peer " + str(data_dict['source']) + ", has been sent.")

# Graceful exit function that updates successors and predecessors
def gracefulExit():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.connect(("127.0.0.1", 50000 + PREVPEER1))
    request1 = "{'leaving': " + str(IDENTITY) + ", 'next1': " + str(NEXTPEER1) + ", 'next2': " + str(NEXTPEER2) + "}"
    sock.send(request1.encode())
    request2 = "{'leaving': " + str(IDENTITY) + ", 'next2': " + str(NEXTPEER1) + "}"
    sock1.connect(("127.0.0.1", 50000 + PREVPEER2))
    sock1.send(request2.encode())
    request3 = "{'update': 'predecessors', 'prev1': " + str(PREVPEER1) + ", 'prev2': " + str(PREVPEER2) + "}"
    sock2.connect(("127.0.0.1", 50000 + NEXTPEER1))
    sock2.send(request3.encode())
    request4 = "{'update': 'predecessors', 'prev2': " + str(PREVPEER1) + "}"
    sock3.connect(("127.0.0.1", 50000 + NEXTPEER2))
    sock3.send(request4.encode())

# Killed peer function that requests successor
def killedPeer():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    request = "{'request': " + str(IDENTITY) + "}"
    sock.connect(("127.0.0.1", 50000 + NEXTPEER1))
    sock.send(request.encode())
    

# Initial argument check
def init():
    global IDENTITY, NEXTPEER1, NEXTPEER2
    if len(sys.argv) != 4:
        print("Error: wrong number of arguments")
        exit(1)
        
    identity = int(sys.argv[1])
    nextPeer1 = int(sys.argv[2])
    nextPeer2 = int(sys.argv[3])
    
    if not (0 <= identity <= 255):
        print("Error: identity argument out of range [0, 255]")
        exit(1)
    if not (0 <= nextPeer1 <= 255):
        print("Error: first next peer argument out of range [0, 255]")
        exit(1)
    if not (0 <= nextPeer2 <= 255):
        print("Error: second next peer argument out of range [0, 255]")
        exit(1)
    
    IDENTITY = identity
    NEXTPEER1 = nextPeer1
    NEXTPEER2 = nextPeer2 


if __name__ == "__main__":
    init()
    peer = Peer(IDENTITY, NEXTPEER1, NEXTPEER2)
    print("identity:", peer.identity, "/ next 1:", peer.nextPeer1, "/ next 2:", peer.nextPeer2, "/ ping port:", PINGPORT)
    
    sleep(3)
    thread1 = ReceivePing()
    thread1.start()
    thread2 = SendPing()
    thread2.start()
    thread3 = FileReceive()
    thread3.start()

    
    while True:
        try:
            key_input = input()
            # Handle file request input
            if key_input[0:7] == "request":
                file = key_input[8:]
                if len(file) == 4:
                    try:
                        file = int(file)
                    except:
                        print("File name non integer error")
                    thread4 = FileRequest(file)
                    thread4.start()
                    
                elif len(file) != 4:
                    print("File name length error")

            # Handle quit input
            if key_input == "quit":
                gracefulExit()
                print("Departing gracefully, please wait...")
                sleep(5)
                if (PREV1READY):
                    if(PREV2READY):
                        RUNNER = False
                        print("Departed")
                        os._exit(0)
            
        except KeyboardInterrupt:
            print("\nPeer killed.")
            os._exit(1)
