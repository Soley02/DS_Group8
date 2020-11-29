import socket
import threading
import time
import struct
import concurrent

# Broadcast IP Fritzbox 192.168.178.255
# BROADCAST_IP = "192.168.178.255"
MULTICAST_GROUP_IP = '224.1.1.100'

# Ports
MULTICAST_PORT_SERVER = 5000    # Port for server to server Multicasts
UNICAST_PORT_SERVER = 6000      # Port for server to server Unicasts
MULTICAST_PORT_CLIENT = 7000    # Port for clients to discover servers

CLIENT_CONNECTION_TO_LEADER_PORT = 9000

SERVER_CLIENTLIST_PORT = 5100
SERVER_SERVERLIST_PORT = 5200
SERVER_MESSAGELIST_PORT = 5300
SERVER_LEADER_ELECTION_PORT = 5400
SERVER_NEW_LEADER_PORT = 5500


# Local host information
MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)

class Server():
    def __init__(self):
        self.isLeader = False
        self.serverlist = []    # List of Servers and their addresses
        self.clientlist = []    # List of Clients and their addresses

    # ------------- Multicast -------------


    def MulticastSendMessage(self):

        leader_server_found = False
        leader_search_try = 0
        message = ('Multicast Message looking for Leader')
        multicast_group = (MULTICAST_GROUP_IP, MULTICAST_PORT_SERVER)

        # Create Socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Timeout socket from spamming
        sock.settimeout(5)
        # Set time to live for message (network hops; 1 for local)
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        # look for leader 5 times, if no leader found, start new election
        while (not leader_server_found):

            while leader_search_try < 5:
                print("Looking for leader")
                sock.sendto(message.encode(), multicast_group)

                while True:
                    leader_search_try = leader_search_try + 1
                    print('Server multicast: Waiting to receive response to sent multicast message from the leader')

                    try:
                        data, server_addr = sock.recvfrom(128)
                        print('received "%s" from %s' % (data.decode(), server_addr))
                        print(data.decode())
                        if data.decode() == "True":
                            print('received "%s" from %s' % (data.decode(), server_addr))
                            print("LEADER FOUND")
                            leader_server_found = True # Leader Server discovered stop multicasting
                            leader_search_try = 7
                            break

                    except socket.timeout:
                        print('Timed out, no more responses')
                        break

                time.sleep(2)

            # Start leader election after 5 tries
            if leader_search_try == 6:
                self.isLeader = True
                isLeader = self.isLeader
                print('I am the leader now')
                return isLeader
               # break
            if leader_search_try == 7:
                leader_server_found = True
                break


    def MulticastListenMessage(self):

        server_address = ('', MULTICAST_PORT_SERVER)

        # Create Socket and bind to server address
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(server_address)

        # Tell the operating system to add the socket to the multicast group
        group = socket.inet_aton(MULTICAST_GROUP_IP)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        while True:
            data, address = sock.recvfrom(128)

            print('Listen multicast: received {} bytes from {}'.format(len(data), address))
            print('Listen multicast: sending acknowledgement to {}'.format(address))

            if self.isLeader:
                return_message = 'True'
                print('I am the leader')
                self.serverlist.append((address, False))
                print(self.serverlist)
            else:
                return_message = 'False'
                print('I am not the leader')

            sock.sendto(return_message.encode(), address)
            #return self.serverlist


    def UpdateServerList(self):
        print(self.serverlist)

        while True: # debug config

            if len(self.serverlist) == 0:
                print('Serverlist is empty')
                print(self.serverlist)

            elif len(self.serverlist) > 0:
                print('Serverlist is filled')
                print(self.serverlist)
                self.HeartbeatSend()

            time.sleep(3)


    def HeartbeatSend(self):
        message = ('Hearbeat: Are you alive?')

        dead_host = -1

        time.sleep(3)

        for x in range(len(self.serverlist)):

            heartbeat_connection = self.serverlist[x]
            server_address, isLeader = heartbeat_connection
            ip, port = server_address

            # TCP connection for each server
            HBsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            HBsocket.settimeout(2)

            try:
                HBsocket.connect((ip, UNICAST_PORT_SERVER))  # Connect each socket to ip adress and UNICAST Port
                HBsocket.send(message.encode())
                print("Sending Heartbeat: Heartbeat message sent to: {},{} ".format(ip, UNICAST_PORT_SERVER))
                try:
                    response = HBsocket.recv(1024)
                    print("Sending Heartbeat: Received Heartbeat response: {}".format(response.decode()))
                except socket.timeout:
                    print('Sending Heartbeat: No response to heartbeat from: {} '.format(ip))
            except:
                print('Connection failed')
                dead_host = x

                if isLeader == True:
                    print('Leader crashed')
                    self.MulticastSendMessage()

            finally:
                HBsocket.close()

        if dead_host >= 0:

            newserverlist = self.serverlist
            del newserverlist[dead_host]
            print('Removed crashed server', ip, 'from serverlist')

            self.serverlist = newserverlist


    def HeartbeatListen(self):

        server_address = ('', UNICAST_PORT_SERVER)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a TCP/IP socket
        sock.bind(server_address)  # Bind to the server address
        sock.listen()
        print('Listening to Heartbeat on Port: {} '.format(UNICAST_PORT_SERVER))
        while True:
            connection, server_address = sock.accept()  # Wait for a connection
            heartbeat_msg = connection.recv(1024)
            heartbeat_msg = heartbeat_msg.decode()
            print('Listening Heartbeat: received Heartbeat from: {} '.format(server_address))
            if heartbeat_msg:
                print('Listening Heartbeat: sending Heartbeat back to: {} '.format(server_address))
                connection.sendall(
                    heartbeat_msg.encode())  # sendall sends the entire buffer you pass until everything has been sent or an error occurs





if __name__ == '__main__':
    server = Server()

    thread1 = threading.Thread(target=server.MulticastListenMessage)
    thread1.start()

    #time.sleep(10)

    thread2 = threading.Thread(target=server.MulticastSendMessage)
    thread2.start()

    thread3 = threading.Thread(target=server.HeartbeatListen)
    thread3.start()

    thread4 = threading.Thread(target=server.UpdateServerList)
    thread4.start()