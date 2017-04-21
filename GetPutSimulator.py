from argparse import ArgumentParser
from ConfigReader import ConfigurationReader
from LatencyManager import LatencyManager
from collections import defaultdict
from multiprocessing import Pool
from collections import defaultdict

import os
import numpy

CLIENT_SIM_LIMIT = 10000
NUM_CPUS = 32

def main(output_dir):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    p = Pool(NUM_CPUS)
    size = 247 / NUM_CPUS
    access_sets = list(generate_access_sets(range(0, 247), size))
    latencies = p.map(perform_ops, access_sets)

    get_latencies, put_latencies = aggregate_dependencies(latencies)

    get_percentiles = find_percentiles(get_latencies)
    put_percentiles = find_percentiles(put_latencies)
    output_to_file(output_dir, get_percentiles, put_percentiles)

def generate_access_sets(l, split_factor):
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

def output_to_file(output_dir, get_percentiles, put_percentiles):
    get_output_filename = os.path.join(output_dir, 'get_latencies')
    with open(get_output_filename, 'wb') as output_file:
        for key, values in get_percentiles.iteritems():
            output_line = str(key[0]) + ',' + str(key[1])
            for v in values:
                output_line += ' ' + str(v)
            output_file.write(output_line + '\n')

    put_output_filename = os.path.join(output_dir, 'put_latencies')
    with open(put_output_filename, 'wb') as output_file:
        for key, values in put_percentiles.iteritems():
            output_line = str(key[0]) + ',' + str(key[1])
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

def perform_ops(access_sets):
    get_latencies = defaultdict(list)
    put_latencies = defaultdict(list)
    for access_set in access_sets:
        print 'Working on access set ' + str(access_set)
        # Get the data centers in each of the access set.
        access_set_filename = os.path.join('/vault-home/vaspol/zhe_strong-consistency/cric_study_data', 'access_sets', 'as_all_ec2_storage_' + str(access_set))
        dcs = get_dc(access_set_filename)
        for dc in dcs:
            dc_index = dc
            dc = latency_manager.index_to_server(dc_index)
            # Get replication configuration and perform PUT/GET
            replication_config = config_reader.generate_replication_config_from_as(str(access_set))
            replicas_in_access_set = [ latency_manager.index_to_server(r) for r in replication_config ]
            closest_replicas = latency_manager.get_closest_dcs(dc)
            target_replicas = [ r for r in closest_replicas if r in replicas_in_access_set ]

            map_key = (access_set, dc_index) 
            # CRIC
            repeat = 3
            waits_within_dc = 2
            for i in range(0, CLIENT_SIM_LIMIT):
                put_latency = perform_single_op(dc, target_replicas, 'put', repeat, waits_within_dc=waits_within_dc) # Put to user timeline
                get_latency = perform_single_op(dc, target_replicas, 'get', repeat, waits_within_dc=waits_within_dc) # Put to user timeline
                put_latencies[map_key].append(put_latency)
                get_latencies[map_key].append(get_latency)
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
            with open('parallel_' + op, 'ab') as output_file:
                output_file.write('[SINGLE ' + op + ' WITHIN DC] at ' + dst + ' ' + str(tmp_latencies) + '\n')
    latencies.sort()
    if debug:
        with open('parallel_' + op, 'ab') as output_file:
            output_file.write('[SINGLE ' + op + ' CROSS DCs] ' + str(latencies) + '\n')
    return latencies[f] # Get the max

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('output_dir')
    args = parser.parse_args()
    global config_reader
    global latency_manager
    global f
    global n

    config_reader = ConfigurationReader(None)
    latency_manager = LatencyManager()
    f = 1
    n = 3

    main(args.output_dir)
