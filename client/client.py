
# based on https://www.neuralnine.com/tcp-chat-in-python/

import socket
import threading
import time
import struct

# BROADCAST_IP = "192.168.178.255"
MULTICAST_GROUP_IP = '224.1.1.100'

# Ports
MULTICAST_PORT_CLIENT = 7000    # Port for clients to discover servers

CLIENT_MESSAGE_TO_LEADER_PORT = 9100        # Port for Clients to send Messages to Leader


###################################################################################################################
###################################################################################################################
###################################################################################################################

class ChatClient():
    def __init__(self):
        self.connection_established = False   # Variable to mark established connection with leader
        self.server_port = ''
        self.server_ip = ''

    def ChooseNickname(self):

        # Choosing Nickname
        self.nickname = input("Choose your nickname: ")

        self.LookForChatroom()


    def LookForChatroom(self):

        while self.connection_established == False:

            message = ('Looking for Chatroom')
            multicast_group = (MULTICAST_GROUP_IP, MULTICAST_PORT_CLIENT)

            # Create Socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Timeout socket from spamming
            sock.settimeout(2)
            # Set time to live for message (network hops; 1 for local)
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

            sock.sendto(message.encode(), multicast_group)

            # look for chatroom leader
            while True:
                try:
                    data, server_addr = sock.recvfrom(2048)

                    if data:
                        print('received "%s" from %s' % (data.decode(), server_addr))
                        print(data.decode())
                        self.server_ip, self.server_port = server_addr
                        self.connection_established = True
                        break

                    else:
                        pass

                except socket.timeout:
                    print('Timed out, no more responses')
                    self.server_port = ''
                    self.server_ip = ''
                    self.connection_established = False
                    break

            time.sleep(1)

        if self.connection_established == True:

            # TCP Connection to leader/chatroom
            print("Connecting to Server...")
            print("Server IP:" + self.server_ip)

            # Connecting To Server
            self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # self.client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.client.connect((self.server_ip, CLIENT_MESSAGE_TO_LEADER_PORT))

            # Starting Threads For Listening And Writing
            receive_thread = threading.Thread(target=ChatClient.receive)
            receive_thread.start()

            write_thread = threading.Thread(target=ChatClient.write)
            write_thread.start()

    def receive(self):
        while True:
            try:
                # Receive Message From Server
                # If 'NICK' Send Nickname
                message = self.client.recv(1024).decode('UTF-8')
                if message == 'NICK':
                    self.client.send(self.nickname.encode('UTF-8'))
                else:
                    print(message)
            except:
                # Close Connection When Error
                print("An error occured!")
                self.client.close()
                break

        self.connection_established = False
        self.LookForChatroom()

    # Sending Messages To Server
    def write(self):
        print("Connected to Chatroom, please type your message...")
        while True:
            message = '{}: {}'.format(self.nickname, input(''))
            self.client.send(message.encode('UTF-8'))



if __name__ == '__main__':
    ChatClient = ChatClient()

    thread1 = threading.Thread(target=ChatClient.ChooseNickname)
    thread1.start()