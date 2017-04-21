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

def read_client_relationship(client_dir, mode):
    result = dict()
    path = os.path.join(client_dir, 'expanded_twitter_graph', mode)
    with open(path, 'rb') as input_file:
        for l in input_file:
            l = l.strip().split()
            client_id = l[0]
            followers = []
            for i in range(2, len(l)):
                followers.append(l[i])
            if mode == 'followee':
                result[client_id] = followers[:100]
            else:
                result[client_id] = followers
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

    def get_client_followers(self, client_id):
        return self.client_to_followers[client_id]

    def get_client_followees(self, client_id):
        return self.client_to_followees[client_id]

    def get_clients(self):
        return self.client_follower_count.keys()

    def __init__(self, client_dir=None, access_set_dir=None):
        if client_dir is not None:
            self.client_to_access_set = read_client_access_set_mapping(client_dir)
            self.client_to_followers = read_client_relationship(client_dir, 'follower')
            self.client_to_followees = read_client_relationship(client_dir, 'followee')
            print 'Read user relationships'
        else:
            self.client_to_access_set = dict()
            self.client_follower_count = dict()
        self.access_set_to_dc_list = dict()
        self.access_set_to_rep_policy = dict()
        for i in range(0, 16368):
            self.access_set_to_dc_list[str(i)] = get_dc(os.path.join('/vault-home/vaspol/zhe_strong-consistency/cric_study_data', 'new_dataset', 'access_sets_14_aws', 'access_set_aws_' + str(i)))
            self.access_set_to_rep_policy[str(i)] = get_dc(os.path.join('/vault-home/wuzhe/cric_app_study/new_dataset/replication_policy_39dc_3_replicas', 'replication_' + str(i)))
        print 'Done reading access sets'
