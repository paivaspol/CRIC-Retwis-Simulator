from collections import defaultdict

import random
import sys

client_filename = sys.argv[1]
dc_filename = sys.argv[2]
n = int(sys.argv[3])

dcs = defaultdict(list)
all_dcs = []
with open(dc_filename, 'rb') as dc_file:
    for l in dc_file:
        dc, continent = l.strip().split()
        dcs[continent].append(dc)
        all_dcs.append(dc)

dcs = [ (k, v) for k, v in dcs.iteritems() ]
continent_index = 0
with open(client_filename, 'rb') as input_file:
    for l in input_file:
        client_id = l.strip().split()[0]
        replicas = set()
        self_dc = random.sample(all_dcs, 1)[0]
        output_line = '{0} {1}'.format(client_id, self_dc)
        for i in range(0, n):
            r = random.sample(dcs[continent_index][1], 1)[0]
            while r in replicas:
                r = random.sample(dcs[continent_index][1], 1)[0]
            replicas.add(r)
            output_line += ' ' + r
            continent_index += 1
            if continent_index >= len(dcs):
                continent_index = 0
        print output_line
