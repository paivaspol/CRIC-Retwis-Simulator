from argparse import ArgumentParser
from ConfigReader import ConfigurationReader
from LatencyManager import LatencyManager

import sys

input_filename = sys.argv[1]

config_reader = ConfigurationReader()
latency_manager = LatencyManager()
f = 1
n = 3

with open(input_filename, 'rb') as input_file:
    for client in input_file:
        client = client.strip()
        src, access_set = config_reader.get_client_access_set(client)
        src = latency_manager.index_to_server(src) # Convert from index to string
        print 'client: ' + client + ' access_set: ' + str(access_set)
        replication_config = config_reader.read_replication_config(f, n, 'ec2', access_set)
        replicas_in_access_set = [ latency_manager.index_to_server(r) for r in replication_config ]
        print '\treplicas in access set: ' + str(replicas_in_access_set)
