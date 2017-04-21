import sys
import os
import random

if len(sys.argv) != 2:
    print 'Usage: ./generate_access_set_replication_policy.py [output_dir]'
    sys.exit(1)
    
output_dir = sys.argv[1]

if not os.path.exists(output_dir):
    os.mkdir(output_dir)

dc_to_dcs_in_same_region = {}
us_dcs = set([ '0', '1', '2', '13', '14' ])
europe_dcs = set([ '3', '11', '15' ])
asia_dcs = set([ '5', '6', '10', '12' ])
dc_to_dcs_in_same_region['0'] = us_dcs
dc_to_dcs_in_same_region['1'] = us_dcs
dc_to_dcs_in_same_region['2'] = us_dcs
dc_to_dcs_in_same_region['13'] = us_dcs
dc_to_dcs_in_same_region['14'] = us_dcs
dc_to_dcs_in_same_region['8'] = us_dcs
dc_to_dcs_in_same_region['9'] = us_dcs
dc_to_dcs_in_same_region['3'] = europe_dcs
dc_to_dcs_in_same_region['11'] = europe_dcs
dc_to_dcs_in_same_region['15'] = europe_dcs
dc_to_dcs_in_same_region['5'] = asia_dcs
dc_to_dcs_in_same_region['6'] = asia_dcs
dc_to_dcs_in_same_region['10'] = asia_dcs
dc_to_dcs_in_same_region['12'] = asia_dcs
dc_to_dcs_in_same_region['7'] = asia_dcs

def get_dc(access_set_filename):
    dc = []
    with open(access_set_filename, 'rb') as input_file:
        for l in input_file:
            dc.append(l.strip())
    return dc

def generate_replication_config_from_as(access_set):
    # First, get all dc in access set. If in s. america --> america, if in aus --> asia
    dcs = access_set_to_dc_list[access_set]

    # Example, useast
    # Second, randomly pick from those region total 3.
    # random from --> [useast, uswest, .. anything in us across all clouds]
    sampled = set()
    sample_limit = 3
    for dc in dcs:
        new_sample = random.sample(dc_to_dcs_in_same_region[dc], 1)[0]
        while new_sample in sampled: # Making sure that we are not picking duplicated dc
            new_sample = random.sample(dc_to_dcs_in_same_region[dc], 1)[0]
        sampled.add(new_sample)

    # We haven't got enough samples yet. Use union of all regions and sample.
    if len(sampled) < sample_limit:
        # First, get the union.
        union_dcs = set()
        for dc in dcs:
            union_dcs = union_dcs | dc_to_dcs_in_same_region[dc]

        # Keep sampling until we get all samples that we need.
        while len(sampled) < sample_limit:
            new_sample = random.sample(union_dcs, 1)[0]
            if new_sample not in sampled:
                # Add only if the dc isn't in the sampled set.
                sampled.add(new_sample)
    return sampled           

access_set_to_dc_list = dict()
for i in range(0, 247):
    access_set_to_dc_list[str(i)] = get_dc(os.path.join('/vault-home/vaspol/zhe_strong-consistency/cric_study_data', 'access_sets', 'as_all_ec2_storage_' + str(i)))

for i in range(0, 247):
    with open(os.path.join(output_dir, 'as_' + str(i)), 'wb') as output_file:
        rep_config = generate_replication_config_from_as(str(i))
        for r in rep_config:
            output_file.write(str(r) + '\n')
