from typing import List
from pmerge_sort import parallel_merge_sort

import socket
import argparse
import threading
import logging
import random


# Global list storing all peers in the network.
peers = []

def _handle_spread(data: str, local_ip: str, local_port: int, peer_ip: str, peer_port: int) -> None:
    global peers

    recv_peers = data.split(' ')[1:]
    logging.info(f'Current Peers:{recv_peers}')

    before = len(peers)
    for sp in recv_peers:
        if sp not in peers:
            peers.append(sp)
    
    if f'{local_ip}:{local_port}' not in recv_peers:
        recv_peers.append(f'{local_ip}:{local_port}')

    peers = recv_peers
    if before < len(peers):
        _spread_routes(peer_ip, peer_port)

def _handle_sort(data: str, conn: socket.SocketType) -> None:
    splitted_data = data.split(' ')
    print(splitted_data)

    _ = splitted_data[0]
    numbers = [int(num) for num in splitted_data[1:]]

    sorted_numbers = parallel_merge_sort(numbers)
    str_sorted_numbers = ' '.join([str(num) for num in sorted_numbers])

    logging.info('Sending the following sorted chunk: \'' + str_sorted_numbers + '\'')
    conn.send(str_sorted_numbers.encode('utf-8'))

def _distribute_work(data: List[int], num_peers: int):
    distributed_work = []
    chunk_size = len(data) // num_peers

    for _ in range(num_peers):
        distributed_work.append([int(num) for num in data[:chunk_size]])
        data = data[chunk_size:]

    distributed_work[-1].extend(data)
    return distributed_work

def _print_menu(rol : str) -> List[str]:
    options = [
        'Generate and sort numbers',
        'Exit'
        ]
    print('SELECT AN OPTION')

    if rol == 'S': 
        options.insert(1, 'Spread Routes')

    for idx, option in enumerate(options):
        print(f"[{idx + 1}] {option}")
    
    return options

def _listen_as_a_server(local_ip: str, local_port: int, peer_ip: str, peer_port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as local_socket:
        local_socket.bind((local_ip, local_port))
        local_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        local_socket.listen(2)

        # Keep listening to peer if connected
        while True:
            conn, addr = local_socket.accept()
            logging.info("Peer connected from '%s'" % (addr, ))

            with conn:
                data = conn.recv(1024, 0)
                decoded_data = data.decode('utf-8')
                logging.info("Message from %s:\n%s" % (addr, decoded_data))

                if decoded_data.startswith('SPREAD'):
                    _handle_spread(decoded_data, local_ip, local_port,
                                   peer_ip, peer_port)

                elif decoded_data.startswith('SORT'):
                    _handle_sort(decoded_data, conn)

def _send_data(local_ip: str, local_port: int, data: List[int]) -> None:
    global peers

    peers = [peer for peer in peers if len(peer)]

    logging.info(f'{data}')
    unsorted_chunks = _distribute_work(data, len(peers))
    sorted_nums = []

    sorted_nums.extend(parallel_merge_sort(unsorted_chunks[0]))

    chunk_id = 1
    for peer in peers:
        if peer == f'{local_ip}:{local_port}':
            continue

        peer_ip, peer_port = peer.split(':')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as ps:
            ps.connect((peer_ip, int(peer_port)))

            str_chunk = ' '.join([str(num) for num in unsorted_chunks[chunk_id]])
            logging.info(f'Sending "{str_chunk}" to peer {peer_ip}:{peer_port}...')
            ps.sendall(f'SORT {str_chunk}'.encode('utf-8'))

            decoded_data = ps.recv(1024, 0).decode('utf-8')
            logging.info(f"Received data from peer ({peer_ip, peer_port}):\n{decoded_data}")
            sorted_chunk = [int(num) for num in decoded_data.split(' ')]
            
            sorted_nums.extend(sorted_chunk)
            chunk_id += 1

    sorted_nums = parallel_merge_sort(sorted_nums)
    logging.info(f'Sorted nums: {sorted_nums}')

def _spread_routes(peer_ip: str, peer_port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as peer_socket:
        peer_socket.connect((peer_ip, int(peer_port)))
        peer_socket.sendall('SPREAD '.encode('utf-8'))
        
        peers_str = ' '.join(peers)
        peer_socket.sendall(peers_str.encode('utf-8'))

def _generate_numbers(size: int) -> List[int]:
    return [random.randint(1, 32000) for _ in range(size)]

def main(args):
    local_ip = args.LOCAL_IP[0]
    local_port = args.LOCAL_PORT[0]

    peer_ip = args.PEER_IP[0]
    peer_port = args.PEER_PORT[0]

    rol = args.rol[0]

    # Use a daemon lightweight process to handle the peer connections
    # since we need user input to send the numbers to the peers.
    t1 = threading.Thread(target=_listen_as_a_server, args=(local_ip, local_port,
                                                            peer_ip, peer_port))
    t1.daemon = True
    t1.start()
    logging.info(f"Server daemon listening on {local_ip}:{local_port}...")


    peers.append(f'{local_ip}:{local_port}')

    user_selection = None
    while user_selection != 'Exit':
        aval_options = _print_menu(rol)
        user_selection = aval_options[int(input("> "))]

        if user_selection == 'Generate and sort numbers':
            print("Enter the number's size: ")
            nums_size = input("> ")
            numbers = _generate_numbers(int(nums_size))

            t2 = threading.Thread(target=_send_data, args=(local_ip, local_port, numbers))
            t2.start()
            t2.join()
        elif user_selection == 'Spread Routes' and rol == 'S':
            _spread_routes(peer_ip, peer_port)

    return 0


if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s:%(message)s", level=logging.INFO,
                        datefmt="%H:%M:%S")

    parser = argparse.ArgumentParser()
    parser.add_argument('LOCAL_IP', nargs=1, type=str)
    parser.add_argument('LOCAL_PORT', nargs=1, type=int)
    parser.add_argument('PEER_IP', nargs=1, type=str)
    parser.add_argument('PEER_PORT', nargs=1, type=str)
    parser.add_argument('-r', '--rol', nargs=1, type=str, default='P')

    args = parser.parse_args()

    main(args)
