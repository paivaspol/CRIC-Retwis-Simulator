from collections import defaultdict

import sys
import os

followers_filename = sys.argv[1]
data_filename = sys.argv[2]
output_dir = sys.argv[3]

if not os.path.exists(output_dir):
    os.mkdir(output_dir)

cur_index = 0
clients = { 100: [], 250: [] }

followers_dict = defaultdict(set)
with open(followers_filename, 'rb') as input_file:
    for l in input_file:
        l = l.strip().split()
        client_id = l[0]
        client_follower = int(l[1])
        for k in clients:
            if client_follower >= k:
                clients[k].append(client_id)

sorted_follower_dict = sorted(followers_dict.iteritems(), key=lambda x: x[0])
cur_index = 0
with open(data_filename, 'rb') as input_file:
    print 'Output: ' + os.path.join(output_dir, 'data_' + str(cur_index))
    output_file = open(os.path.join(output_dir, 'data_' + str(cur_index)), 'wb')
    for l in input_file:
        sl = l.strip().split()
        client_id = sl[0]
        if client_id not in sorted_follower_dict[cur_index][1]:
            output_file.close()
            cur_index += 1
            print 'Output: ' + os.path.join(output_dir, 'data_' + str(cur_index))
            output_file = open(os.path.join(output_dir, 'data_' + str(cur_index)), 'wb')
        output_file.write(l)
    output_file.close()
