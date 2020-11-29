import os
import pickle
import socket
import struct
import sys
import threading
from time import sleep
ROOT_DIR = os.path.dirname(os.path.abspath("/Users/fazeli/Desktop/code/Online-Shop-master/client")) #root path of project  --> .../ChatRoom/
sys.path.append(ROOT_DIR)    #Add path to project root otherwise imports will fail

from cluster.ports import MULTICAST_PORT_CLIENT

HEADER = 64 #First message to the server tells how long message is. Represents amount of bytes of msg
FORMAT = 'utf-8'

port = 8080 #Server Port

#Input IP Address of Server
#host = input("Please enter the hostname of the server : ")

class client():
    def __init__(self):
        self.leader_server_found = False    #stops multicasting when leader server is found
        self.client_message = []            #stores messages in list

        self.client_socket = ''

        self.invalidmessage = True          #Checks if message are placed correctly
        self.connectedToLeader = False      #Checks if leader server is still online
        self.msg_port = 0                   #This is port of the client where connection to the leader establishes. When running client user has to input a port
        self.leader_msg_request_port = 0    #New Leader uses this port to get missed messages from client and restore message history

    def discoverLeaderServer(self):
        message = ('Client Multicast Message')
        multicast_group = ('224.3.29.71', MULTICAST_PORT_CLIENT)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)       # Create the datagram socket
        sock.settimeout(2)                  # Set a timeout so the socket does not block indefinitely when trying # to receive data. (in sec flaot)
        ttl = struct.pack('b', 1)           # Set the time-to-live for messages to 1 so they do not go past the local network segment.
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        while(not self.leader_server_found):
            try:
                # Send data to the multicast group
                print('Client Multicast message sending "%s"' % message)
                sent = sock.sendto(message.encode(), multicast_group)

                # Look for responses from all recipients
                while True:
                    print('Client Multicast: Waiting to receive respond to sent multicast message')
                    try:
                        data, server_addr = sock.recvfrom(128)  # receive 128 bytes at once

                        if data.decode() == "True":
                            leader_server, port = server_addr  # split up server_address into ip address and port
                            self.leader_server_found = True    # Leader Server discovered stop multicasting

                    except socket.timeout:
                        print('Timed out, no more responses')
                        break
                    else:
                        print('received "%s" from %s' % (data.decode(), server_addr))

            except KeyboardInterrupt:   #on CTRL-C
                break
            except:
                print("Failed to send multicast message")


        sock.close()

        self.connectToLeaderServer(leader_server)

    def connectToLeaderServer(self, leader_server):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind(('', self.msg_port))      #Connection port for the client

        self.client_socket.connect((leader_server, port))
        print("Client connected to Leader Server: ", leader_server)
        self.connectedToLeader = True
        print("Welcome to the Chat room! Enter a message or Enter 'disconnect' to close the client connection")

        while self.connectedToLeader:

            while self.invalidmessage:                      #Check valid input
                msg = input(">>Type your message: ")

                if msg == 'disconnect':
                        print('Disconnecting Client')
                        self.invalidmessage = False
                else:
                        print(msg)

            #After a valid input the message can be sent
            self.invalidmessage=True            #Change value to True so next msg can checked before send

            #try except einbauen falls connection nicht klappt break
            try:
                print('Trying to send msg')
                self.client_socket.send(msg.encode()) #send msg
            except:
                print('Message could not be sent')
                self.connectedToLeader = False #leader crashed
                break

            response = self.client_socket.recv(1024)  #wait for msg confirmation

            if len(response)==0:    #0 bytes means leader crashed
                print("Leader Server not online, starting leader discovery again")
                self.connectedToLeader = False
            else:
                response = pickle.loads(response)
                self.client_list.append(response)
                print('Received msg confirmation', response)

        self.client_socket.close()     #Close the client socket and start discovery again for leader server

        self.leader_server_found=False #since we have no leader set value back to initial
        self.discoverLeaderServer()

    def disconnectToLeaderServer(self):
        msg = 'disconnect'
        msg = msg.encode()
        self.client_socket.send(msg)

        self.client_socket.close()

    def listenforLeaderRequest(self):
        server_address = ('', self.leader_request_port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a TCP/IP socket
        sock.bind(server_address)  # Bind to the server address
        sock.listen()

        while True:
            missed_msg_list = []            #create everytime empty chatroom

            connection, server_address = sock.accept()          # Wait for a connection
            received_message = connection.recv(1024)

            received_message = pickle.loads(received_message)   #unpickle message from leader

            if len(self.client_list)==0:    #no message send to leader server
                response = 'no'
                connection.send(response.encode())

            else:  # look in list if there is a message

                for x in range(len(self.client_list)):

                    id, myconnection, order = self.client_list[x]

                    if id > received_message:
                        missed_msg_list.append(self.client_list[x])  # Put into list
                    else:
                        pass    #do nothing
                if len(missed_msg_list) > 0:        #send missed message to server
                    response = pickle.dumps(missed_msg_list)
                    connection.send(response)
                else:                               #client has no missed message to send
                    response = 'no'
                    connection.send(response.encode())

if __name__ == '__main__':
    try:
        c = client()

        valid_input = False
        while not valid_input:
            c.msg_port = int(input(">>Type in port number between 9000-9100: "))
            if c.msg_port>=9000 and c.msg_port <= 9100:
                c.leader_msg_request_port = c.msg_port + 200
                valid_input=True
            else:
                print('Invalid port number. Try again')

        thread = threading.Thread(target=c.listenforLeaderRequest)
        thread.start()

        c.discoverLeaderServer()    #Look for Leader Server and connect

    except KeyboardInterrupt:
        print('Disconnecting client')
        c.diseconnectToLeaderServer()
