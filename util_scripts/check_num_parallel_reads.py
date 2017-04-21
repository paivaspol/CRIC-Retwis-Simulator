import sys
import json

filename = sys.argv[1]

with open(filename, 'rb') as input_file:
    for l in input_file:
        l = l.strip()
        obj = json.loads(l)
        print obj.keys()
        print obj['client']

