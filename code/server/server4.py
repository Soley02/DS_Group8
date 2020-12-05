import socket
import threading
import time
import struct
import concurrent
import pickle
import uuid

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
# create a unique ID based on host ID and current time
MY_ID = uuid.uuid1()

class Server():
    def __init__(self):
        self.isLeader = False
        self.serverlist = []    # List of Servers and their addresses
        self.clientlist = []    # List of Clients and their addresses
        self.election_message = MY_ID

    # ------------- Multicast -------------

    def MulticastSendMessage(self):

        leader_server_found = False
        leader_search_try = 0

        if self.isLeader == True:
            leader_server_found = True
            leader_search_try = 7

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
                        # ip, port = server_addr
                        if data.decode() == "True":
                            print('received "%s" from %s' % (data.decode(), server_addr))
                            print("LEADER FOUND")
                            # Address of leader
                            # self.server_address_leader = server_addr
                            # print(self.server_address_leader)
                            self.serverlist.append((server_addr, True))

                            leader_server_found = True # Leader Server discovered stop multicasting
                            leader_search_try = 7
                            break
                        # if data.decode() == "False" and MY_IP != ip:
                        #     self.serverlist.append((server_addr, False))

                    except socket.timeout:
                        print('Timed out, no more responses')
                        break

                time.sleep(2)

            # Start leader election after 5 tries
            # currently debug function, remove once leader election is coded
            if leader_search_try == 6:
                # self.isLeader = True
                # isLeader = self.isLeader
                # print('I am the leader now')
                # return isLeader
                self.election_message = MY_ID
                self.LeaderElection()

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

    # ------------- Serverlist -------------

    def UpdateServerList(self):

        while True:

            while self.isLeader == True:
            # while True: # debug config

                if len(self.serverlist) == 0:
                    print('Serverlist is empty')
                    print(self.serverlist)

                if len(self.serverlist) > 0:
                    print('Serverlist is filled')
                    print(self.serverlist)

                    # Send Serverlist updates

                    if len(self.serverlist) > 0:
                        for x in range (len(self.serverlist)):
                            servers_and_leader = self.serverlist[x]
                            server_address, isLeaderServer = servers_and_leader
                            ip, port = server_address

                            SLsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            SLsock.settimeout(3)

                            try:
                                SLsock.connect((ip, SERVER_SERVERLIST_PORT))
                                newserverlist = pickle.dumps(self.serverlist)
                                SLsock.send(newserverlist)

                                try:
                                    answer = SLsock.recv(1024)
                                    answer = answer.decode()
                                    print("Serverlist sent to: {} ".format(ip))
                                except socket.timeout:
                                    print("Connection failed: {}".format(ip))

                            except:
                                print("Connection failed: {}".format(ip))

                            finally:
                                SLsock.close()

                    self.HeartbeatSend()

                time.sleep(3)

            while self.isLeader == False:

                # Listen to Serverlist updates

                server_address = ('', SERVER_SERVERLIST_PORT)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(server_address)
                sock.listen()
                # Socket timeout so server doesn't get stuck
                # sock.settimeout(5)
                print("Listening for serverlist...")
                print(self.serverlist)

                while self.isLeader == False:

                    try:
                        connection, server_address_leader = sock.accept()

                        serverlistleader = []

                        serverlistleader = connection.recv(2048)

                        serverlistleader = pickle.loads(serverlistleader)

                        newserverlistreceive = []
                        newserverlistreceive = serverlistleader

                        serverlist_lenght = len(newserverlistreceive)

                        for x in range(serverlist_lenght):
                            servers_and_leader = newserverlistreceive[x]
                            server_address, isLeaderServer = servers_and_leader
                            ip, port = server_address
                            if ip == MY_IP:
                                del newserverlistreceive[x]
                                newserverlistreceive.append((server_address_leader, True))
                                self.serverlist = newserverlistreceive
                                sock.close()
                                # self.HeartbeatSend()
                        sock.close()

                    except self.isLeader == True:
                        sock.close()
                        self.HeartbeatSend()

                    finally:
                        sock.close()
                        self.HeartbeatSend()

            time.sleep(2)


    # ------------- Heartbeat -------------

    def HeartbeatSend(self):
        message = ('Hearbeat: Are you alive?')

        dead_host = -1

        time.sleep(3)

        for x in range(len(self.serverlist)):

            heartbeat_connection = self.serverlist[x]
            server_address, ServerisLeader = heartbeat_connection
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

                # if isLeader == True:
                #     print('Leader crashed')
                #     # Start leader election
                #     # self.MulticastSendMessage()
                #     self.LeaderElection()

            finally:
                HBsocket.close()

        if dead_host >= 0:

            newserverlist = self.serverlist
            del newserverlist[dead_host]
            print('Removed crashed server', ip, 'from serverlist')

            if ServerisLeader == True:
                print('Leader crashed')
                # Start leader election
                # self.MulticastSendMessage()
                self.serverlist = newserverlist
                self.LeaderElection()
                return self.serverlist


            self.serverlist = newserverlist

        self.UpdateServerList()


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
                connection.sendall(heartbeat_msg.encode())  # sendall sends the entire buffer you pass until everything has been sent or an error occurs

    # ------------- Serverlist -------------

    def LeaderElection(self):

        while True:

            if self.isLeader == True:
                break

            # Form ring; fill ring list from serverlist and own ip
            ring_members = []
            ring_members.append(MY_IP)
            # election_message = MY_ID

            leader_found = False

            for x in range (len(self.serverlist)):
                election_connection = self.serverlist[x]
                server_address, isLeader = election_connection
                ip, port = server_address
                ring_members.append(ip)

            # Get right neighbour
            print("unsorted neighbours")
            print(ring_members)

            ring_members.sort()

            print("sorted neighbours")
            print(ring_members)

            # find my IP in the sorted list
            index_MY_IP = ring_members.index(MY_IP)
            # print(index_MY_IP)

            x = index_MY_IP + 1

            print(len(ring_members))

            if len(ring_members) == 1:
                self.isLeader = True
                print("I AM THE LEADER")
                return self.isLeader


            if len(ring_members) > 1:

                if x == len(ring_members):
                    neighbor_IP = ring_members[0]
                else:
                    neighbor_IP = ring_members[index_MY_IP + 1]

                print("Neighbor IP: ")
                print(neighbor_IP)

                # if neighbor_IP is empty, that means current server is the last one in the list, so set index to the first one in the list
                if neighbor_IP == '':
                    neighbor_IP = ring_members[0]

                # send message to the neighbor
                ELsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ELsock.settimeout(2)

                election_ballot = str(self.election_message)

                try:
                    ELsock.connect((neighbor_IP, SERVER_LEADER_ELECTION_PORT))
                    ELsock.send(election_ballot.encode())
                    print("Sending Election Message to: {},{} ".format(neighbor_IP, SERVER_LEADER_ELECTION_PORT))

                    try:
                        response = ELsock.recv(1024)
                        print("Received Election response: {}".format(response.decode()))
                    except socket.timeout:
                        print("Cannot connect to: {}".format(neighbor_IP))
                except:
                    print("Connection failed")
                finally:
                    ELsock.close()

    def LeaderElectionListen(self):

        server_address = ('', SERVER_LEADER_ELECTION_PORT)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a TCP/IP socket
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(server_address)  # Bind to the server address
        sock.listen()
        print('Listening to Election Message on Port: {} '.format(SERVER_LEADER_ELECTION_PORT))

        while True:
            # Wait for connection
            connection, client_address = sock.accept()
            election_ballot = connection.recv(1024)
            election_ballot = election_ballot.decode()
            MY_ID_string = str(MY_ID)

            print('Received Election Message from: {} '.format(server_address))
            print(self.election_message)
            print('Election Ballot')
            print(election_ballot)

            # compare own uuid with received uuid
            # if own uuid > received uuid: send my uuid
            if MY_ID_string > election_ballot:
                sock.close()
                self.election_message = MY_ID
                # self.LeaderElection()

            # if own uuid < received uuid: send received uuid
            if MY_ID_string < election_ballot:
                sock.close()
                self.election_message = election_ballot
                # self.LeaderElection()

            # if own uuid == received uuid: make myself leader; leader elected
            if MY_ID_string == election_ballot:
                sock.close()
                self.election_message = 'New Leader elected'
                self.isLeader = True
                return self.isLeader

            if election_ballot == 'New Leader elected':
                sock.close()




if __name__ == '__main__':
    server = Server()

    thread1 = threading.Thread(target=server.MulticastListenMessage)
    thread1.start()

    thread2 = threading.Thread(target=server.MulticastSendMessage)
    thread2.start()

    thread3 = threading.Thread(target=server.HeartbeatListen)
    thread3.start()

    thread4 = threading.Thread(target=server.UpdateServerList)
    thread4.start()

    thread5 = threading.Thread(target=server.LeaderElectionListen)
    thread5.start()
