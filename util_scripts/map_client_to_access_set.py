import sys
import random
import os

client_filename = sys.argv[1]

access_set_to_dc_list = dict()

def get_dc(access_set_filename):
    dc = []
    with open(access_set_filename, 'rb') as input_file:
        for l in input_file:
            dc.append(l.strip())
    return dc

# for i in range(0, 247):
#     access_set_to_dc_list[i] = get_dc(os.path.join('/vault-home/vaspol/zhe_strong-consistency/cric_study_data', 'access_sets', 'as_all_ec2_storage_' + str(i)))
for i in range(0, 16368):
    access_set_to_dc_list[i] = get_dc(os.path.join('/vault-home/vaspol/zhe_strong-consistency/cric_study_data', 'new_dataset', 'access_sets_14_aws', 'access_set_aws_' + str(i)))

with open(client_filename, 'rb') as input_file:
    for i, l in enumerate(input_file):
        l = l.strip().split()
        if len(l) == 2:
            client_id, _ = l
        elif len(l) == 3:
            client_id, _, _ = l
        access_set = random.randint(0, 16367)
        # print 'access_set_to_dc: ' + str(access_set_to_dc_list[access_set])
        data_center = random.choice(access_set_to_dc_list[access_set])
        print client_id + ' ' + str(data_center) + ' ' + str(access_set)
