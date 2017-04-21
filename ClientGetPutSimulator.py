from argparse import ArgumentParser
from ConfigReader14AWS import ConfigurationReader
from LatencyManager14AWS import LatencyManager
from collections import defaultdict
from multiprocessing import Pool
from collections import defaultdict

import os
import numpy
import subprocess

CLIENT_SIM_LIMIT = 10000
NUM_CPUS = 32

def main(output_dir, clients):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    if args.debug:
        if os.path.exists('/w/parallel_get'):
            os.remove('/w/parallel_get')
            os.remove('/w/parallel_put')
        global parallel_get_file
        global parallel_put_file
        parallel_get_file = open('/w/parallel_get', 'wb')
        parallel_put_file = open('/w/parallel_put', 'wb')

        latencies = perform_ops(clients)
        if type(latencies) is not list:
            latencies = [ latencies ]

        get_latencies, put_latencies = aggregate_dependencies(latencies)

        get_percentiles = find_percentiles(get_latencies)
        put_percentiles = find_percentiles(put_latencies)
        output_to_file(output_dir, get_latencies, get_percentiles, put_latencies, put_percentiles)

        parallel_get_file.close()
        parallel_put_file.close()
        subprocess.call('cp /w/parallel_get parallel_get_sim_only_storage'.split())
        subprocess.call('cp /w/parallel_put parallel_put_sim_only_storage'.split())
    else:
        p = Pool(NUM_CPUS)
        size = len(clients) / NUM_CPUS
        clients_list = list(split_data(clients, size))
        latencies = p.map(perform_ops, clients_list)

        get_latencies, put_latencies = aggregate_dependencies(latencies)

        get_percentiles = find_percentiles(get_latencies)
        put_percentiles = find_percentiles(put_latencies)
        output_to_file(output_dir, get_latencies, get_percentiles, put_latencies, put_percentiles)

def split_data(l, split_factor):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), split_factor):
        yield l[i:i + split_factor]

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

def output_to_file(output_dir, get_latencies, get_percentiles, put_latencies, put_percentiles):
    get_output_filename = os.path.join(output_dir, 'get_latencies')
    with open(get_output_filename, 'wb') as output_file:
        for key, values in get_percentiles.iteritems():
            output_line = str(key) + ' ' + str(len(get_latencies[key]))
            # output_line = str(key[0]) + ',' + str(key[1])
            for v in values:
                output_line += ' ' + str(v)
            output_file.write(output_line + '\n')

    put_output_filename = os.path.join(output_dir, 'put_latencies')
    with open(put_output_filename, 'wb') as output_file:
        for key, values in put_percentiles.iteritems():
            output_line = str(key) + ' ' + str(len(put_latencies[key]))
            for v in values:
                output_line += ' ' + str(v)
            output_file.write(output_line + '\n')

def find_percentiles(latencies, percentiles=[50, 90, 99]):
    retval = defaultdict(list)
    for key, values in latencies.iteritems():
        if len(values) > 0:
            for q in percentiles:
                retval[key].append(numpy.percentile(values, q))
    return retval

def perform_ops(clients):
    get_latencies = defaultdict(list)
    put_latencies = defaultdict(list)
    for client in clients:
        iterations = CLIENT_SIM_LIMIT
        if args.debug:
            follower_count, followee_count = config_reader.get_client_follower_count(client)
            iterations = iterations * followee_count

        src, access_set = config_reader.get_client_access_set(client)
        # src = latency_manager.index_to_server(src) # Convert from index to string
        print 'Working on client: ' + str(client) + ' access set: ' + str(access_set)
        # Get replication configuration and perform PUT/GET
        replication_config = config_reader.generate_replication_config_from_as(str(access_set))
        # print 'rep config: ' + str(replication_config)
        # replicas_in_access_set = [ latency_manager.index_to_server(r) for r in replication_config ]
        target_replicas = replication_config

        # CRIC
        repeat = 3
        waits_within_dc = 2
        if args.mode == 'baseline':
            repeat = 1
            waits_within_dc = 0

        for i in range(0, CLIENT_SIM_LIMIT):
            put_latency = perform_single_op(src, target_replicas, 'put', repeat, waits_within_dc=waits_within_dc, debug=args.debug) # Put to user timeline
            get_latency = perform_single_op(src, target_replicas, 'get', repeat, waits_within_dc=waits_within_dc, debug=args.debug) # Put to user timeline
            put_latencies[client].append(put_latency)
            get_latencies[client].append(get_latency)
    return get_latencies, put_latencies

# Gets the datacenters from this file.
def get_dc(access_set_filename):
    dc = []
    with open(access_set_filename, 'rb') as input_file:
        for l in input_file:
            dc.append(l.strip())
    return dc

# Performs the operation on n servers.
# This will send operation to multiple dsts in parallel with n repetitions.
# This will return the max latency.
def perform_single_op(src, dsts, op, repeats, waits_within_dc=0, waits_overall=0, debug=False):
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
        if debug:
            output_file = parallel_put_file if op == 'put' else parallel_get_file
            output_file.write('[SINGLE ' + op + ' WITHIN DC] at ' + dst + ' ' + str(tmp_latencies) + ' picking at ' + str((waits_within_dc - 1)) + ' got latency: ' + str(tmp_latencies[waits_within_dc - 1]) + '\n')
    latencies.sort()
    if debug:
        output_file = parallel_put_file if op == 'put' else parallel_get_file
        output_file.write('[SINGLE ' + op + ' CROSS DCs] ' + str(latencies) + ' picking at ' + str(f) + ' got latency: ' + str(latencies[f]) + '\n')
    return latencies[f] # Get the max

def get_clients(clients_filename):
    result = []
    with open(clients_filename, 'rb') as input_file:
        for raw_line in input_file:
            result.append(raw_line.strip())
    return result

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('output_dir')
    parser.add_argument('client_dir')
    parser.add_argument('clients')
    parser.add_argument('mode', choices=['cric', 'baseline'])
    parser.add_argument('--debug', default=False, action='store_true')
    parser.add_argument('--path-to-rep-config', default=None)
    args = parser.parse_args()
    global config_reader
    global latency_manager
    global f
    global n

    config_reader = ConfigurationReader(args.client_dir, args.path_to_rep_config)
    latency_manager = LatencyManager()
    f = 1
    n = 3
    clients = get_clients(args.clients)
    main(args.output_dir, clients)
