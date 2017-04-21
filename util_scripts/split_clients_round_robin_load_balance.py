from collections import defaultdict

import sys
import os

filename = sys.argv[1]
split_factor = int(sys.argv[2])
output_dir = sys.argv[3]

if not os.path.exists(output_dir):
    os.mkdir(output_dir)

clients_to_follower = {}
with open(filename, 'rb') as input_file:
    for l in input_file:
        client_id, num_followers, num_followee = l.strip().split()
        clients_to_follower[client_id] = (int(num_followers), int(num_followee))

sorted_clients_to_follower = sorted(clients_to_follower.iteritems(), key=lambda x: x[1][0], reverse=True)

result = defaultdict(list)
for i, client_follower_pair in enumerate(sorted_clients_to_follower):
    dst = i % split_factor
    client, follower_followee_pair = client_follower_pair
    follower_count, followee_count = follower_followee_pair
    output_line = '{0} {1} {2}\n'.format(client, follower_count, followee_count)
    result[dst].append(output_line)

for i, lines in result.iteritems():
    output_filename = 'data_' + str(i)
    with open(os.path.join(output_dir, output_filename), 'wb') as output_file:
        for l in lines:
            output_file.write(l)
