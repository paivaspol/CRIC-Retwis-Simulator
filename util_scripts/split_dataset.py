import sys
import os

filename = sys.argv[1]
split_factor = int(sys.argv[2])
output_dir = sys.argv[3]

if not os.path.exists(output_dir):
    os.mkdir(output_dir)

line_count = 0
with open(filename, 'rb') as input_file:
    for l in input_file:
        line_count += 1

lines_per_file = line_count / split_factor
lines_processed = 0
with open(filename, 'rb') as input_file:
    for i in range(0, split_factor):
        output_filename = 'data_' + str(i)
        with open(os.path.join(output_dir, output_filename), 'wb') as output_file:
            for i in range(0, lines_per_file):
                l = input_file.readline()
                output_file.write(l)
                lines_processed += 1

    output_filename = 'data_' + str(split_factor)
    with open(os.path.join(output_dir, output_filename), 'wb') as output_file:
        lines_per_file = line_count - lines_processed
        for i in range(0, lines_per_file):
            l = input_file.readline()
            output_file.write(l)
            lines_processed += 1
