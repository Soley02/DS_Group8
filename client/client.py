import socket
import threading
import time
import struct

# BROADCAST_IP = "192.168.178.255"
MULTICAST_GROUP_IP = '224.1.1.100'

# Ports
MULTICAST_PORT_CLIENT = 7000    # Port for clients to discover servers

CLIENT_CONNECTION_TO_LEADER_PORT = 9000

# Listening to Server and Sending Nickname
def receive():
    while True:
        try:
            # Receive Message From Server
            # If 'NICK' Send Nickname
            message = client.recv(1024).decode('ascii')
            if message == 'NICK':
                client.send(nickname.encode('ascii'))
            else:
                print(message)
        except:
            # Close Connection When Error
            print("An error occured!")
            client.close()
            break

# Sending Messages To Server
def write():
    while True:
        message = '{}: {}'.format(nickname, input(''))
        client.send(message.encode('ascii'))



# Choosing Nickname
nickname = input("Choose your nickname: ")


# Look for leader/chatroom
connection_established = False
server_port = ''
server_ip = ''

while connection_established == False:

    message = ('Multicast Message looking for Chatroom')
    multicast_group = (MULTICAST_GROUP_IP, MULTICAST_PORT_CLIENT)

    # Create Socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Timeout socket from spamming
    sock.settimeout(5)
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
                server_ip, server_port = server_addr
                connection_established = True
                break

            else:
                pass

        except socket.timeout:
            print('Timed out, no more responses')
            break

    time.sleep(1)

if connection_established == True:
    # TCP Connection to leader/chatroom

    print("Connecting to Server...")
    print(server_ip)

    # Connecting To Server
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((server_ip, CLIENT_CONNECTION_TO_LEADER_PORT))

# Starting Threads For Listening And Writing
receive_thread = threading.Thread(target=receive)
receive_thread.start()

write_thread = threading.Thread(target=write)
write_thread.start()