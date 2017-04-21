import sys
import json

filename = sys.argv[1]

with open(filename, 'rb') as input_file:
    content = input_file.readline().strip()
    result = json.loads(content)
    counter = 0
    max_val = -1
    wait_index = 0
    for put in result['TWEET'][0]['pPut']:
        # print counter
        vals = []

        for k, v in put.iteritems():
            # print '\t{0}: {1}'.format(k, v)
            # print '\tChose: {0}'.format(v[wait_index])
            vals.append(int(v[wait_index]))
        vals.sort()
        # print '\tCross DC: {0}'.format(vals[1])
        print vals[1]
        max_val = max(max_val, vals[1])
        counter += 1
    # print 'max: ' + str(max_val)

