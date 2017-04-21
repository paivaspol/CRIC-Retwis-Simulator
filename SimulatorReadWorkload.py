from argparse import ArgumentParser
from ConfigReader import ConfigurationReader
from LatencyManager import LatencyManager
from collections import defaultdict
from multiprocessing import Pool

import os
import numpy

CLIENT_SIM_LIMIT = 10000
# CLIENT_SIM_LIMIT = 1
READ_TIMELINE = 'READ'
TWEET = 'TWEET'
NUM_CPUS = 32

def perform_retwis_operation(client, op):
    # This function should be configurable such that the number of servers
    # we are sending requests to and the number of requests we wait for are
    # different.

    follower_count = config_reader.get_client_follower_count(client, args.follower_scale_factor)
    
    # First, get access set and datacenter for that client
    src, access_set = config_reader.get_client_access_set(client)
    src = latency_manager.index_to_server(src) # Convert from index to string
    
    # First, get the replication config.
    replication_config = config_reader.read_replication_config(f, n, 'ec2', access_set)
    replicas_in_access_set = [ latency_manager.index_to_server(r) for r in replication_config ]
    closest_replicas = latency_manager.get_closest_dcs(src)
    target_replicas = [ r for r in closest_replicas if r in replicas_in_access_set ]
    latency = 0

    # CRIC
    repeat = 3
    waits_within_dc = 2

    # Baseline
    if args.mode == 'baseline':
        repeat = 1
        waits_within_dc = 0
    if op == TWEET:
        # Translate TWEET to storage server operation:
        latency += perform_single_op(src, target_replicas, 'put', repeat, waits_within_dc=waits_within_dc) # Put to user timeline
        latency += perform_single_op(src, target_replicas, 'get', repeat, waits_within_dc=waits_within_dc) # Get all followers
        latency += perform_parallel_ops(src, target_replicas, 'put', repeat, follower_count, waits_within_dc=waits_within_dc) # Put post to all follower timelines.
    elif op == READ_TIMELINE:
        # A read timeline translates to 2 GETs.
        latency += perform_single_op(src, target_replicas, 'get', repeat, waits_within_dc=waits_within_dc)
        latency += perform_parallel_ops(src, target_replicas, 'get', repeat, follower_count, waits_within_dc=waits_within_dc)
    return latency

# Performs the operation on n servers.
# This will send operation to multiple dsts in parallel with n repetitions.
# This will return the max latency.
def perform_single_op(src, dsts, op, repeats, waits_within_dc=0, waits_overall=0):
    if waits_within_dc == 0:
        waits_within_dc = repeats
    if waits_overall == 0:
        waits_overall = len(dsts)

    latencies = []
    for dst in dsts:
        tmp_latencies = []
        for i in range(0, repeats):
            network = latency_manager.get_network_latency(src, dst)
            storage = latency_manager.get_storage_latency(dst, op)
            latency = network + storage
            tmp_latencies.append(latency)
        tmp_latencies.sort()
        latencies.append(tmp_latencies[waits_within_dc - 1]) # Wait for only 
    latencies.sort()
    return latencies[f] # Get the max

# Perform n operations in parallel.
# Perform an operation n times and pick the one with max latency.
def perform_parallel_ops(src, dsts, op, repeats, n_parallel, waits_within_dc=0, waits_overall=0):
    max_latency = -1
    for i in range(0, n_parallel):
        max_latency = max(max_latency, perform_single_op(src, dsts, op, repeats, waits_within_dc, waits_overall))
    return max_latency

def run_workload(workload_filename):
    print 'Processing: ' + workload_filename
    write_latencies = { client: [] for client in config_reader.get_clients() }
    read_latencies = { client: [] for client in config_reader.get_clients() }
    uniq_client = set()
    with open(workload_filename, 'rb') as input_file:
        for line in input_file:
            _, client, operation, _ = line.strip().split()
            uniq_client.add(client)
            if client not in write_latencies or \
                client not in read_latencies:
                continue
            # print 'Performing ' + operation + ' for client: ' + str(client)
            if operation == TWEET:
                latency = perform_retwis_operation(client, operation)
                write_latencies[client].append(latency)
            elif operation == READ_TIMELINE:
                latency = perform_retwis_operation(client, operation)
                read_latencies[client].append(latency)
    return write_latencies, read_latencies

def get_percentiles(latencies, percentiles=[50, 90, 99]):
    retval = defaultdict(list)
    for client in latencies:
        if len(latencies[client]) > 0:
            for q in percentiles:
                retval[client].append(numpy.percentile(latencies[client], q))
    return retval

def output_to_file(latencies, original_latencies, op, output_dir):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    output_filename = os.path.join(output_dir, op + '_latencies_' + str(f) + '_' + str(n))
    with open(output_filename, 'wb') as output_file:
        for l in latencies:
            output_line = l
            output_line += ' ' + str(len(original_latencies[l]))
            for q in latencies[l]:
                output_line += ' ' + str(q)
            output_file.write(output_line + '\n')

def aggregate_dependencies(latencies):
    final_write_latencies = defaultdict(list)
    final_read_latencies = defaultdict(list)
    for l in latencies:
        write_latencies = l[0]
        read_latencies = l[1]
        update_dict(write_latencies, final_write_latencies)
        update_dict(read_latencies, final_read_latencies)
    return final_write_latencies, final_read_latencies

def update_dict(src, dst):
    for key, values in src.iteritems(): # values is a list
        # if len(values) > 0:
        #     print 'key: ' + key + ' len: ' + str(len(values))
        dst[key].extend(values)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('output_dir')
    parser.add_argument('mode', choices=['cric', 'baseline'])
    parser.add_argument('follower_scale_factor', type=int)
    parser.add_argument('workloads', help='The path to the workload files.', nargs='+')
    args = parser.parse_args()
    global config_reader
    global latency_manager
    global f
    global n

    config_reader = ConfigurationReader()
    latency_manager = LatencyManager()
    f = 1
    n = 3

    p = Pool(NUM_CPUS)
    latencies = p.map(run_workload, args.workloads)
    write_latencies, read_latencies = aggregate_dependencies(latencies)

    # write_latencies, read_latencies = run_workload(args.workload)
    write_percentiles = get_percentiles(write_latencies)
    read_percentiles = get_percentiles(read_latencies)
    output_to_file(write_percentiles, write_latencies, 'write', args.output_dir)
    output_to_file(read_percentiles, read_latencies, 'read', args.output_dir)
