from argparse import ArgumentParser
# from ConfigReader14AWS import ConfigurationReader
from ConfigReader14AWS import ConfigurationReader
from LatencyManager14AWS import LatencyManager
from collections import defaultdict
from multiprocessing import Pool

import os
import json
import numpy
import itertools
import subprocess

CLIENT_SIM_LIMIT = 5
READ_TIMELINE = 'READ'
TWEET = 'TWEET'
NUM_CPUS = 32

GET = 'get'
PUT = 'put'
PARALLEL_GET = 'pGet'
PARALLEL_PUT = 'pPut'

# Returns the cache key.
def get_cache_key(client):
    follower_count, _ = config_reader.get_client_follower_count(client)
    src, access_set = config_reader.get_client_access_set(client)
    return ( src, access_set, follower_count )

def perform_retwis_operation(client, op, mode):
    # This function should be configurable such that the number of servers
    # we are sending requests to and the number of requests we wait for are
    # different.
    
    # First, get access set and datacenter for that client
    src, access_set = config_reader.get_client_access_set(client)
    # src = latency_manager.index_to_server(src) # Convert from index to string
    
    # First, get the replication config.
    # replication_config = config_reader.read_replication_config(f, n, 'ec2', access_set)
    replication_config = config_reader.generate_replication_config_from_as(access_set)
    # print 'rep config: ' + str(replication_config)
    # replication_config = ['0', '12', '15']
    # replicas_in_access_set = [ latency_manager.index_to_server(r) for r in replication_config ]
    # target_replicas = replication_config
    target_replicas = [ 'aws-eu-west-1', 'aws-us-east-1', 'az-australiasoutheast' ]
    latency = 0
    
    # CRIC
    repeat = 3
    waits_within_dc = 2

    # Baseline
    if mode == 'baseline':
        repeat = 1
        waits_within_dc = 0

    log_model = {} if args.log_extensively else None
    if op == TWEET:
        # Translate TWEET to storage server operation:
        if not args.perform_only_parallel:
            latency_1, put_log = perform_single_op(src, target_replicas, 'put', repeat, waits_within_dc=waits_within_dc) # Put to user timeline
            # print '[TWEET Latency (1) PUT] ' + str(latency_1) + ' ' + str(latency)
            latency_2, get_log = perform_single_op(src, target_replicas, 'get', repeat, waits_within_dc=waits_within_dc, from_local=True) # Get all followers
            latency += max(latency_1, latency_2)
            if args.log_extensively:
                log_model[PUT] = put_log
                log_model[GET] = get_log
        # print '[TWEET Latency (2) GET] ' + str(latency_2) + ' ' + str(latency)
        followers = config_reader.get_client_followers(client)
        if args.debug:
            print 'len(followers): ' + str(len(followers))
        latency_3, parallel_put_log = perform_parallel_ops(src, followers, 'put', repeat, waits_within_dc=waits_within_dc) # Put post to all follower timelines.
        latency += latency_3

        if args.log_extensively:
            log_model[PARALLEL_PUT] = parallel_put_log
        # print '[TWEET Latency (3) PUT] ' + str(latency_3) + ' ' + str(latency)
    elif op == READ_TIMELINE:
        # A read timeline translates to 2 GETs.
        if not args.perform_only_parallel:
            latency_1, get_log = perform_single_op(src, target_replicas, 'get', repeat, waits_within_dc=waits_within_dc)
            latency += latency_1
            if args.log_extensively:
                log_model[GET] = get_log
        # print '[READ Latency (1) GET] ' + str(latency_1) + ' ' + str(latency)
        followees = config_reader.get_client_followees(client)
        posts = numpy.random.choice(followees, 100, replace=True)
        if args.debug:
            print 'followees: ' + str(followees)
        latency_2, parallel_get_log = perform_parallel_ops(src, posts, 'get', repeat, waits_within_dc=waits_within_dc)
        latency += latency_2
        if args.log_extensively:
            log_model[PARALLEL_GET] = parallel_get_log
        # print '[READ Latency (2) GET] ' + str(latency_2) + ' ' + str(latency)
    return latency, log_model

# Performs the operation on n servers.
# This will send operation to multiple dsts in parallel with n repetitions.
# This will return the max latency.
def perform_single_op(src, dsts, op, repeats, waits_within_dc=0, waits_overall=0, from_local=False, debug=False):
    if waits_within_dc == 0:
        waits_within_dc = repeats
    if waits_overall == 0:
        waits_overall = len(dsts)

    if from_local:
        tmp_latencies = []
        for i in range(0, repeats):
            latency = latency_manager.get_storage_latency(src, op), { }
            tmp_latencies.append(latency)
        return tmp_latencies[waits_within_dc - 1]

    latencies = []
    log_model = {}
    for dst in dsts:
        tmp_latencies = []
        for i in range(0, repeats):
            network = latency_manager.get_network_latency(src, dst)
            storage = latency_manager.get_storage_latency(dst, op)
            latency = network + storage
            tmp_latencies.append(latency)
        tmp_latencies.sort()
        latencies.append(tmp_latencies[waits_within_dc - 1]) # Wait for only 
        log_model[dst] = tmp_latencies
        if debug:
            output_file = parallel_put_file if op == 'put' else parallel_get_file
            output_file.write('[SINGLE ' + op + ' WITHIN DC] at ' + dst + ' ' + str(tmp_latencies) + ' picking at ' + str((waits_within_dc - 1)) + ' got latency: ' + str(tmp_latencies[waits_within_dc - 1]) + '\n')
    latencies.sort()
    if debug:
        output_file = parallel_put_file if op == 'put' else parallel_get_file
        output_file.write('[SINGLE ' + op + ' CROSS DCs] ' + str(latencies) + ' picking at ' + str(f) + ' got latency: ' + str(latencies[f]) + '\n')
    return latencies[f], log_model # Get the max

# Perform n operations in parallel.
# Perform an operation n times and pick the one with max latency.
def perform_parallel_ops(src, client_relationship, op, repeats, waits_within_dc=0, waits_overall=0):
    max_latency = -1
    parallel_log_model = []
    for client in client_relationship:
        _, access_set = config_reader.get_client_access_set(client)
        # dsts = config_reader.generate_replication_config_from_as(access_set)
        dsts = [ 'aws-eu-west-1', 'aws-us-east-1', 'az-australiasoutheast' ]
        parallel_latency, single_op_log = perform_single_op(src, dsts, op, repeats, waits_within_dc, waits_overall, debug=args.debug)
        max_latency = max(max_latency, parallel_latency)
        parallel_log_model.append(single_op_log)
    if args.debug:
        output_file = parallel_put_file if op == 'put' else parallel_get_file
        output_file.write('[MAX PARALLEL ' + op + '] ' + str(max_latency) + '\n\n')
    return max_latency, parallel_log_model

def run_workload(workload_filename, process_id, mode):
    print 'Processing: ' + workload_filename + ' on ' + str(process_id)
    write_latencies = defaultdict(list)
    read_latencies = defaultdict(list)
    clients = get_clients(workload_filename)
    client_count = 0
    output_file = None
    if args.log_extensively:
        if not os.path.exists('/w/vaspol/client_logs'):
            os.mkdir('/w/vaspol/client_logs')
        output_file = open(os.path.join('/w/vaspol/client_logs/', 'client_log_' + str(process_id)), 'wb')

    for client in clients:
        if len(clients_to_use) > 0 and client not in clients_to_use:
            continue
        client_write_latencies = []
        client_read_latencies = []
        tweet_logs = []
        read_logs = []
        for i in range(0, CLIENT_SIM_LIMIT):
            latency, tweet_log_model = perform_retwis_operation(client, TWEET, mode)
            client_write_latencies.append(latency)

            latency, read_log_model = perform_retwis_operation(client, READ_TIMELINE, mode)
            client_read_latencies.append(latency)
            tweet_logs.append(tweet_log_model)
            read_logs.append(read_log_model)

        client_log = { 'client': client, READ_TIMELINE: read_logs, TWEET: tweet_logs }
        if args.log_extensively:
            output_file.write(json.dumps(client_log) + '\n')

        if args.debug:
            print '\twrite samples: ' + str(len(client_write_latencies)) + ' read samples: ' + str(len(client_read_latencies))
        write_percentiles = get_percentiles(client_write_latencies)
        if len(write_percentiles) > 0:
            write_latencies[client] = write_percentiles

        read_percentiles = get_percentiles(client_read_latencies)
        if len(read_percentiles) > 0:
            read_latencies[client] = read_percentiles
        client_count += 1
        print 'Process: ' + str(process_id) + ' client ' + str(client) + ' done. Progress: ' + str(client_count) + '/' + str(len(clients))
    if args.log_extensively:
        output_file.close()
        src = os.path.join('/w/vaspol/client_logs/', 'client_log_' + str(process_id))
        dst = os.path.join(args.output_dir, 'client_logs')
        subprocess.call('cp {0} {1}'.format(src, dst).split())
    if args.dump_latencies:
        dump_cache(write_latency_cache, 'write')
        dump_cache(read_latency_cache, 'read')
    return write_latencies, read_latencies

def dump_cache(cache, op):
    if not os.path.exists('latency_cache'):
        os.mkdir('latency_cache')
    with open(os.path.join('latency_cache', op), 'ab') as output_file:
        for cache_key, latencies in cache.iteritems():
            output_line = ''
            for k in cache_key:
                output_line += ' ' + str(k)
            for l in latencies:
                output_line += ' ' + str(l)
            output_file.write(output_line + '\n')

def get_clients(clients_filename):
    clients = []
    with open(clients_filename, 'rb') as input_file:
        for l in input_file:
            cid = l.strip().split()
            clients.append(cid[0])
    return clients

def get_percentiles(latencies, percentiles=[50, 90, 99]):
    retval = []
    percentiles = [50, 99]
    retval.append(min(latencies))
    if len(latencies) > 0:
        for q in percentiles:
            retval.append(numpy.percentile(latencies, q))
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
        if type(values) is list:
            dst[key].extend(values)
        else:
            dst[key] = values

def run_workload_wrapper(args):
    return run_workload(*args)

def get_clients_set(clients_filename):
    clients = set()
    if clients_filename is not None:
        with open(clients_filename, 'rb') as input_file:
            for l in input_file:
                l = l.strip().split()
                client = l[0]
                clients.add(client)
    return clients

def output_client_logs(client_logs, output_dir):
    output_filename = os.path.join(output_dir, 'client_logs')
    with open(output_filename, 'wb') as output_file:
        output_file.write(json.dumps(client_logs))

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('f', type=int)
    parser.add_argument('n', type=int)
    parser.add_argument('output_dir')
    parser.add_argument('client_dir')
    parser.add_argument('mode', choices=['cric', 'baseline'])
    parser.add_argument('workloads', help='The path to the workload files.', nargs='+')
    parser.add_argument('--dump-latencies', default=False, action='store_true')
    parser.add_argument('--read-latencies', default=False, action='store_true')
    parser.add_argument('--clients', default=None)
    parser.add_argument('--debug', default=False, action='store_true')
    parser.add_argument('--perform-only-parallel', default=False, action='store_true')
    parser.add_argument('--path-to-rep-config', default=None)
    parser.add_argument('--stdout', default=False, action='store_true')
    parser.add_argument('--log-extensively', default=False, action='store_true')
    args = parser.parse_args()
    global config_reader
    global latency_manager
    global f
    global n
    global clients_to_use

    config_reader = ConfigurationReader(args.client_dir, args.path_to_rep_config)
    latency_manager = LatencyManager()
    f = args.f
    n = args.n
    
    clients_to_use = get_clients_set(args.clients)

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    if args.log_extensively:
        if not os.path.exists('/w/vaspol/client_logs'):
            os.mkdir('/w/vaspol/client_logs')
        if not os.path.exists(os.path.join(args.output_dir, 'client_logs')):
            os.mkdir(os.path.join(args.output_dir, 'client_logs'))

    if not args.debug:
        p = Pool(NUM_CPUS)
        latencies = p.map(run_workload_wrapper, itertools.izip(args.workloads, range(0, NUM_CPUS), itertools.repeat(args.mode)))
    else:
        if os.path.exists('/w/vaspol/parallel_get'):
            os.remove('/w/vaspol/parallel_get')
            os.remove('/w/vaspol/parallel_put')
        global parallel_get_file
        global parallel_put_file
        parallel_get_file = open('/w/vaspol/parallel_get', 'wb')
        parallel_put_file = open('/w/vaspol/parallel_put', 'wb')
        workload_filename = ''
        for i in range(0, len(args.workloads)):
            if '21' in args.workloads[i]:
                workload_filename = args.workloads[i]
                break
        latencies = run_workload(workload_filename, 0, args.mode)
        if type(latencies) is not list:
            latencies = [ latencies ]
        parallel_get_file.close()
        parallel_put_file.close()
        subprocess.call('cp /w/vaspol/parallel_get .'.split())
        subprocess.call('cp /w/vaspol/parallel_put .'.split())

    print 'Aggregating latencies...'
    write_latencies, read_latencies = aggregate_dependencies(latencies)
    print 'Aggregating latencies... DONE'

    # write_latencies, read_latencies = run_workload(args.workload)
    print 'Getting percentiles...'
    # write_percentiles = get_percentiles(write_latencies)
    # read_percentiles = get_percentiles(read_latencies)
    print 'Getting percentiles... DONE'
    if args.stdout:
        print 'write: ' + str(write_latencies)
        print 'read: ' + str(read_latencies)
    else:
        output_to_file(write_latencies, write_latencies, 'write', args.output_dir)
        output_to_file(read_latencies, read_latencies, 'read', args.output_dir)
    print 'Done'
