from argparse import ArgumentParser

import os
import random
import sys

def main(num_clients, output_dir):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    percentiles = [(10, 3), (20, 9), (30, 19), (40, 36), (50, 61), \
           (60, 98), (70, 154), (80, 246), (90, 458), (95, 819), (96, 978), \
           (97, 1211), (98, 1675), (99, 2991), (99.9, 24964)]
    #percentiles = [(10, 1), (20, 1), (30, 1), (40, 1), (50, 1), \
    #       (60, 1), (70, 1), (80, 1), (90, 1), (95, 1), (96, 1), \
    #       (97, 1), (98, 1), (99, 1), (99.9, 1)]
    buckets, bucket_borders = generate_buckets(num_clients, percentiles)
    start_id = 0
    start_follower = 1

    with open(os.path.join(output_dir, 'follower'), 'wb') as follower_output_file, \
        open(os.path.join(output_dir, 'followee'), 'wb') as followee_output_file:
        clients_generated = 0
        for i, end_id in enumerate(bucket_borders):
            print 'Generating: ' + str(bucket_borders[i]) + ' i: ' + str(i) + ' type_i: ' + str(type(i))
            end_follower = percentiles[i][1]
            followers = generate_data_for_interval(start_id, end_id, start_follower, end_follower)
            for client_id, num_follower in followers:
                num_followee = 2 * num_follower
                follower_output_line = '{0} {1}'.format(client_id, num_follower)
                followee_output_line = '{0} {1}'.format(client_id, num_followee)
                followers = set()
                for j in range(0, num_follower):
                    follower = random.randint(0, num_clients - 1)
                    while follower in followers:
                        follower = random.randint(0, num_clients - 1)
                    follower_output_line += ' ' + str(follower)
                follower_output_file.write(follower_output_line + '\n')
                
                followees = set()
                for j in range(0, num_followee):
                    followee = random.randint(0, num_clients - 1)
                    while followee in followees:
                        followee = random.randint(0, num_clients - 1)
                    followee_output_line += ' ' + str(followee)
                followee_output_file.write(followee_output_line + '\n')
            start_id = end_id + 1
            start_follower = end_follower
            clients_generated += len(followers)
        clients_generated = client_id
        for i in range(clients_generated, num_clients):
            client_id = i
            num_follower = percentiles[-1][1]
            num_followee = 2 * follower
            follower_output_line = '{0} {1}'.format(client_id, num_follower)
            followee_output_line = '{0} {1}'.format(client_id, num_followee)
            followers = set()
            for j in range(0, num_follower):
                follower = random.randint(0, num_clients - 1)
                while follower in followers:
                    follower = random.randint(0, num_clients - 1)
                follower_output_line += ' ' + str(follower)
            follower_output_file.write(follower_output_line + '\n')
            followees = set()
            for j in range(0, num_followee):
                followee = random.randint(0, num_clients - 1)
                while followee in followees:
                    followee = random.randint(0, num_clients - 1)
                followee_output_line += ' ' + str(followee)
            followee_output_file.write(followee_output_line + '\n')

def generate_data_for_interval(start_id, end_id, start_follower, end_follower):
    # Linearly growing. Use y = mx + b.
    # First, find m = delta(y) / delta(x) and b (by plugging in)
    m = 1.0 * (end_follower - start_follower) / (end_id - start_id)
    b = end_follower - (end_id * m)
    followers = []
    for i in range(start_id, end_id + 1):
        followers.append((i, int(round(m * i + b))))
    return followers

def find_target_bucket(client_id, bucket_borders):
    for i, b in enumerate(bucket_borders):
        if client >= b:
            return idx, b
    return -1

def generate_buckets(num_clients, percentiles):
    result = dict()
    bucket_borders = []
    for p, _ in percentiles:
        border = int(p / 100.0 * num_clients)
        result[border] = []
        bucket_borders.append(border)
    return result, bucket_borders

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('num_clients', type=int)
    parser.add_argument('output_dir')
    args = parser.parse_args()
    main(args.num_clients, args.output_dir)
