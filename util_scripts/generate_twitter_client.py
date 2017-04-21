from argparse import ArgumentParser

import sys

def main(num_clients):
    percentiles = [(10, 3), (20, 9), (30, 19), (40, 36), (50, 61), \
           (60, 98), (70, 154), (80, 246), (90, 458), (95, 819), (96, 978), \
           (97, 1211), (98, 1675), (99, 2991), (99.9, 24964)]
    buckets, bucket_borders = generate_buckets(num_clients, percentiles)
    start_id = 0
    start_follower = 1

    clients_generated = 0
    for i, end_id in enumerate(bucket_borders):
        end_follower = percentiles[i][1]
        followers = generate_data_for_interval(start_id, end_id, start_follower, end_follower)
        for client_id, follower in followers:
            followee = 2 * follower
            print '{0} {1} {2}'.format(client_id, follower, followee)
        start_id = end_id + 1
        start_follower = end_follower
        clients_generated += len(followers)

    for i in range(clients_generated, num_clients):
        client_id = i
        follower = percentiles[-1][1]
        followee = 2 * follower
        output_line = '{0} {1} {2}'.format(client_id, follower, followee)
        print output_line

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
    args = parser.parse_args()
    main(args.num_clients)
