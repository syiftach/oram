import math
import sys
import os
import psutil
import matplotlib.pyplot as plt
import time
import pickle

# user packages
from ex1 import *

EXAMPLE_DATA_FILE = './example_file.txt'


def load_pickle(filename):
    with open(filename, 'rb') as pkl:
        return pickle.load(pkl)


def save_pickle(filename, data):
    with open(filename, 'wb') as pkl:
        pickle.dump(data, pkl)


def client_read_write_throughput(num_files, num_request, read_arr, write_arr):
    # num_request = 8
    delay = 1e-6  # sleep for delay seconds, to avoid zero division error
    # init client and server
    server = Server(num_files)
    client = Client(server)
    # start evaluating writing process
    with open(f'{EXAMPLE_DATA_FILE}', 'r') as file:
        data = file.readlines()
    print(f'(throughput) starting write procedure with n={num_files} leaves...')
    start_write = time.time()
    time.sleep(delay)
    for i in range(num_request):
        client.write(server, f'file{i}', data[i].replace('\n', ''))
        if i % math.ceil(num_request / 4) == 0:
            print(f'write progress...{((i + 1) / num_request) * 100}%')
    end_write = time.time()
    # append throughput: num_request/sec
    write_arr.append(num_request / (end_write - start_write))
    # start evaluating read process
    print(f'(throughput) starting read procedure with n={num_files} leaves...')
    start_read = time.time()
    time.sleep(delay)
    for i in range(num_request):
        client.read(server, f'file{i}')
        if i % math.ceil(num_request / 4) == 0:
            print(f'read progress...{((i + 1) / num_request) * 100}%')
    end_read = time.time()
    # append throughput: num_request/sec
    read_arr.append(num_request / (end_read - start_read))


def client_read_write_latency(num_files, num_request, read_arr, write_arr):
    # num_request=8
    delay = 1e-6  # sleep for delay seconds, to avoid zero division error
    # init client and server
    server = Server(num_files)
    client = Client(server)
    # open example file
    with open(f'{EXAMPLE_DATA_FILE}', 'r') as file:
        data = file.readlines()

    # start evaluating writing process
    print(f'(latency) starting write procedure with n={num_files} leaves...')
    write_latecy_sum = 0
    for i in range(num_request):
        start_write = time.time()
        client.write(server, f'file{i}', data[i].replace('\n', ''))
        end_write = time.time()
        write_latecy_sum += (end_write - start_write)
        if i % math.ceil(num_request / 4) == 0:
            print(f'write progress...{((i + 1) / num_request) * 100}%')
    # append averaged write latency
    write_arr.append(write_latecy_sum / num_request)

    # start evaluating read process
    print(f'(latency) starting read procedure with n={num_files} leaves...')
    read_latency_sum = 0
    for i in range(num_request):
        start_read = time.time()
        client.read(server, f'file{i}')
        read_latency_sum += (time.time() - start_read)
        if i % math.ceil(num_request / 4) == 0:
            print(f'read progress...{((i + 1) / num_request) * 100}%')
    # append averaged read latency
    read_arr.append(read_latency_sum / num_request)


def plot_throughput(num_request=2, save=False, filename=None):
    read_arr = []
    write_arr = []
    num_files_max = 13
    x = [2 ** n for n in range(num_files_max)]
    for n in x:
        assert n > 0
        client_read_write_throughput(n, num_request, read_arr, write_arr)
    fig, axis = plt.subplots(1, 2)
    axis[0].set_title('throughput: read')
    axis[0].set_xlabel('N (num files)')
    axis[0].set_ylabel('requests/second')
    axis[0].plot(x, read_arr, marker='.', lw=1.5, color='orange')

    axis[1].set_title('throughput: write')
    axis[1].set_xlabel('N (num files)')
    axis[1].set_ylabel('requests/second')
    axis[1].plot(x, write_arr, marker='.', lw=1.5, color='purple')
    fig.show()
    if save:
        if filename is None:
            fig.savefig('./png/throughput.png')
            save_pickle('./pkl/throughput.pkl', (read_arr, write_arr))
        else:
            fig.savefig(f'./png/{filename}.png')
            save_pickle(f'./pkl/{filename}.pkl', (read_arr, write_arr))


def plot_latency(num_request=2, save=False, filename=None):
    read_arr = []
    write_arr = []
    num_files_max = 13
    x = [2 ** n for n in range(num_files_max)]
    for n in x:
        assert n > 0
        client_read_write_latency(n, num_request, read_arr, write_arr)
    fig, axis = plt.subplots(1, 2)
    axis[0].set_title('latency: read')
    axis[0].set_xlabel('N (num files)')
    axis[0].set_ylabel('latency')
    axis[0].plot(x, read_arr, marker='.', lw=1.5, color='orange')

    axis[1].set_title('latency: write')
    axis[1].set_xlabel('N (num files)')
    axis[1].set_ylabel('latency')
    axis[1].plot(x, write_arr, marker='.', lw=1.5, color='purple')
    fig.show()
    if save:
        if filename is None:
            fig.savefig('./png/latency.png')
            save_pickle('./pkl/latency.pkl', (read_arr, write_arr))
        else:
            fig.savefig(f'./png/{filename}.png')
            save_pickle(f'./pkl/{filename}.pkl', (read_arr, write_arr))


def plot_throughput_vs_latency(save=False, filename=None, pkl_names=None):
    try:
        if pkl_names is None:
            r_latency_arr, w_latency_arr = load_pickle('./pkl/latency.pkl')
            r_throughput_arr, w_throughput_arr = load_pickle('./pkl/throughput.pkl')
        else:
            r_latency_arr, w_latency_arr = load_pickle(f'./pkl/{pkl_names[0]}.pkl')
            r_throughput_arr, w_throughput_arr = load_pickle(f'./pkl/{pkl_names[1]}.pkl')
    except FileNotFoundError:
        print(f'EVAL: failed to load pkl files', file=sys.stderr)
        return
    fig, axis = plt.subplots(1, 2)
    axis[0].set_title('latency vs throughput: read')
    axis[0].set_xlabel('latency')
    axis[0].set_ylabel('throughput')
    axis[0].plot(r_latency_arr, r_throughput_arr, marker='.', lw=1.5, color='orange')

    axis[1].set_title('latency vs throughput: write')
    axis[1].set_xlabel('latency')
    axis[1].set_ylabel('throughput')
    axis[1].plot(w_latency_arr, w_throughput_arr, marker='.', lw=1.5, color='purple')
    fig.show()
    if save:
        if filename is None:
            fig.savefig('./png/throughput_vs_latency.png')
        else:
            fig.savefig(f'./png/{filename}.png')


if __name__ == '__main__':
    should_save = True
    if not os.path.exists('./ex1'):
        raise Exception('program should run from root folder!')
    if not os.path.exists('./keys'):
        os.mkdir('keys')
    if not os.path.exists('./pkl'):
        os.mkdir('pkl')
    if not os.path.exists('./png'):
        os.mkdir('png')
    print(f'debug mode? {DEBUG}')
    ps = psutil.Process()
    arr = ps.cpu_affinity()
    # run with percentage of the available cpu in the current system
    for i in [0.1, 0.25, 0.5, 0.75, 1]:
        ps.cpu_affinity(arr[:max([int(i * len(arr)), 1])])
        print(f'*** running with {len(ps.cpu_affinity())} cores ***')
        tname = f'throughput_c={len(ps.cpu_affinity())}'
        lname = f'latency_c={len(ps.cpu_affinity())}'
        plot_throughput(num_request=2, filename=tname, save=should_save)
        plot_latency(num_request=2, filename=lname, save=should_save)
        plot_throughput_vs_latency(filename=f'throughput_vs_latency_c={len(ps.cpu_affinity())}',
                                   pkl_names=[lname, tname], save=should_save)
