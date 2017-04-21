from collections import defaultdict

import random
import os

FOLLOWER_SCALE_FACTOR = 1

def read_client_access_set_mapping(client_dir):
    result = dict()
    path = os.path.join(client_dir, 'client_to_access_set_14_aws')
    with open(path, 'rb') as input_file:
        for l in input_file:
            client_id, data_center, access_set = l.strip().split()
            result[client_id] = (data_center, access_set)
    return result

def read_client_follower_count(client_dir):
    result = dict()
    path = os.path.join(client_dir, 'client_follower_count')
    with open(path, 'rb') as input_file:
        for l in input_file:
            l = l.strip().split()
            client_id = l[0]
            follower_count = int(l[1])
            followee_count = int(2 * l[1])
            if len(l) == 3:
                followee_count = int(l[2])
            result[client_id] = (follower_count, followee_count)
    return result

def get_dc(access_set_filename):
    dc = []
    with open(access_set_filename, 'rb') as input_file:
        for l in input_file:
            dc.append(l.strip())
    return dc

class ConfigurationReader:
    def generate_replication_config_from_as(self, access_set):
        # Return from map.
        return self.access_set_to_rep_policy[access_set]

    def get_client_access_set(self, client_id):
        return self.client_to_access_set[client_id]

    def get_client_follower_count(self, client_id, follower_scale_factor=FOLLOWER_SCALE_FACTOR):
        return tuple([follower_scale_factor * x for x in self.client_follower_count[client_id]])

    def get_clients(self):
        return self.client_follower_count.keys()

    def __init__(self, client_dir=None, access_set_dir=None):
        if client_dir is not None:
            self.client_follower_count = read_client_follower_count(client_dir)
        else:
            self.client_to_access_set = dict()
            self.client_follower_count = dict()
        self.client_to_access_set = dict()
        self.access_set_to_rep_policy = dict()
        with open('client_configs/client_100k/client_to_random_replication', 'rb') as input_file:
            for l in input_file:
                l = l.strip().split()
                client_id = l[0]
                self_dc = l[1]
                dcs = l[2:]
                self.client_to_access_set[client_id] = (self_dc, client_id)
                self.access_set_to_rep_policy[client_id] = dcs
