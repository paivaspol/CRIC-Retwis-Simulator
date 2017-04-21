from collections import defaultdict

import random
import os

FOLLOWER_SCALE_FACTOR = 1

def read_client_access_set_mapping(client_dir):
    result = dict()
    path = os.path.join(client_dir, 'client_to_access_set')
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

def populate_access_set_rep_policy(access_set_dir):
    result = defaultdict(list)
    for as_index in os.listdir(access_set_dir):
        # with open(os.path.join(access_set_dir, 'as_' + str(as_index)), 'rb') as input_file:
        with open(os.path.join(access_set_dir, str(as_index)), 'rb') as input_file:
            for s in input_file:
                result[as_index[len('as_'):]].append(s.strip())
    # print result
    return result

def generate_dc_to_dcs_in_same_region():
    result = {}
    us_dcs = set([ '0', '1', '2', '13', '14' ])
    europe_dcs = set([ '3', '11', '15' ])
    asia_dcs = set([ '5', '6', '10', '12' ])
    result['0'] = us_dcs
    result['1'] = us_dcs
    result['2'] = us_dcs
    result['13'] = us_dcs
    result['14'] = us_dcs
    result['8'] = us_dcs
    result['9'] = us_dcs
    result['3'] = europe_dcs
    result['11'] = europe_dcs
    result['15'] = europe_dcs
    result['5'] = asia_dcs
    result['6'] = asia_dcs
    result['10'] = asia_dcs
    result['12'] = asia_dcs
    result['7'] = asia_dcs
    return result

class ConfigurationReader:
    def read_replication_config(self, f, n, target_cloud, as_index):
        fin = open('/vault-home/vaspol/zhe_strong-consistency/cric_study_data/replication_configuration/minimize_max_get_replications/parsed_all_access_sets_goal_minimize_max_get/parsed_'+target_cloud+'_result_with_storage_f_'+str(f)+'_'+str(n)+'_'+as_index, 'r')
        configuration = []
        for line in fin:
            items = line.strip().split()
            if items[0] == 'C':
                configuration.append(items[1])
        fin.close()
        return configuration

    def generate_replication_config_from_as(self, access_set):
        if self.access_set_to_rep_policy is not None:
            return self.access_set_to_rep_policy[access_set]

        # First, get all dc in access set. If in s. america --> america, if in aus --> asia
        dcs = self.access_set_to_dc_list[access_set]

        # Example, useast
        # Second, randomly pick from those region total 3.
        # random from --> [useast, uswest, .. anything in us across all clouds]
        sampled = set()
        sample_limit = 3
        for dc in dcs:
            new_sample = random.sample(self.dc_to_dcs_in_same_region[dc], 1)[0]
            while new_sample in sampled: # Making sure that we are not picking duplicated dc
                new_sample = random.sample(self.dc_to_dcs_in_same_region[dc], 1)[0]
            sampled.add(new_sample)

        # We haven't got enough samples yet. Use union of all regions and sample.
        if len(sampled) < sample_limit:
            # First, get the union.
            union_dcs = set()
            for dc in dcs:
                union_dcs = union_dcs | self.dc_to_dcs_in_same_region[dc]

            # Keep sampling until we get all samples that we need.
            while len(sampled) < sample_limit:
                new_sample = random.sample(union_dcs, 1)[0]
                if new_sample not in sampled:
                    # Add only if the dc isn't in the sampled set.
                    sampled.add(new_sample)
        return sampled           

    def read_clients(self, target_cloud, as_index):
        if as_index != '-1':
            fin = open('/vault-home/vaspol/zhe_strong-consistency/cric_study_data/access_sets/as_all_'+target_cloud+'_storage_'+as_index,'r')
        else:
            fin = open(data_dir + 'access_sets/as_all_'+target_cloud+'_storage','r')
        clients = []
        for line in fin:
            clients.append(line.strip())
        fin.close()
        return clients

    def get_client_access_set(self, client_id):
        return self.client_to_access_set[client_id]

    def get_client_follower_count(self, client_id, follower_scale_factor=FOLLOWER_SCALE_FACTOR):
        return tuple([follower_scale_factor * x for x in self.client_follower_count[client_id]])

    def get_clients(self):
        # return random.sample(self.client_follower_count.keys(), 50000)
        return self.client_follower_count.keys()

    def __init__(self, client_dir=None, access_set_dir=None):
        if client_dir is not None:
            self.client_to_access_set = read_client_access_set_mapping(client_dir)
            self.client_follower_count = read_client_follower_count(client_dir)
        else:
            self.client_to_access_set = dict()
            self.client_follower_count = dict()
        self.dc_to_dcs_in_same_region = generate_dc_to_dcs_in_same_region()
        self.access_set_to_dc_list = dict()
        for i in range(0, 247):
            path_to_rep_config = os.path.join('/vault-home/vaspol/zhe_strong-consistency/cric_study_data', 'access_sets', 'as_all_ec2_storage_' + str(i))
            self.access_set_to_dc_list[str(i)] = get_dc(path_to_rep_config)
        if access_set_dir is not None:
            self.access_set_to_rep_policy = populate_access_set_rep_policy(access_set_dir)
        else:
            self.access_set_to_rep_policy = None
