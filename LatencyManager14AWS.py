import glob
import time
import sys
import random
from time import sleep
import os

# MAX_VALUE = 30000
MAX_VALUE = 30000
USE_SAMPLE_LIMIT = True

data_dir = "/vault-home/vaspol/zhe_strong-consistency/cric_study_data/new_dataset"
def rename(s):
    items = s.split('/')
    return items[0]+'-'+items[1]

class LatencyManager:

    # Returns a sample of the latencies between data centers.
    def get_network_latency(self, src, dst):
        return self.GetOneRandomSample(self.between_dc_latency_list[src][dst])
    
    def get_storage_latency(self, dc, op):
        return self.GetOneRandomSample(self.cloud_storage_latency[dc][op])

    def Percentile(self, sorted_list, percentile):
        if percentile == 0:
            return sorted_list[0]
        if percentile == 100:
            return sorted_list[-1]
        return sorted_list[int(len(sorted_list)*percentile/100)]

    def GetOneRandomSample(self, a_list):
        return a_list[random.randint(0, len(a_list)-1)]

    def output_distribution(the_list, filename):
        fout = open(filename, 'w')
        for item in the_list:
            fout.write('{0}\n'.format(item))
        fout.close()

    def GetMedian(a_list):
        return Percentile(a_list, 50)

    def ReadNetworkLatency(self):
        fin = open(os.path.join(data_dir, 'data_centers'),'r')
        data_centers = []
        for line in fin:
            items = line.strip().split()
            data_centers.append(items[0])
        fin.close()

        fin = open(os.path.join(data_dir, 'network_measurements/located-ping-times.txt'), 'r')
        count = 0
        latencies = {}

        for line in fin:
            items = line.strip().split()
            if len(items) != 3:
                continue
            dc1 = rename(items[1])
            dc2 = rename(items[2])
            try:
                l = float(items[0])*1000
            except Exception as e:
                #sys.stderr.write(str(e)+' '+ line)
                continue
            if dc1 not in latencies:
                latencies[dc1] = {}
            if dc2 not in latencies[dc1]:
                latencies[dc1][dc2] = []
            latencies[dc1][dc2].append(l)
            count += 1
            if not self.silent and count % 100000 == 0:
                sys.stderr.write(str(count)+'\n')
        fin.close()

        for dc1 in latencies:
            for dc2 in latencies[dc1]:
                latencies[dc1][dc2].sort()
            latencies[dc1][dc1] = [1]

        for dc1 in latencies:
            for dc2 in data_centers:
                if dc2 not in latencies[dc1]:
                    if not self.silent:
                        print 'no', dc1, dc2
                    latencies[dc1][dc2] = [1000]

        return latencies

    def ReadStorageLatency(self):
        print 'Reading storage latency'
        fin = open(os.path.join(data_dir, 'data_centers'),'r')
        data_centers = []
        for line in fin:
            items = line.strip().split()
            data_centers.append(items[0])
        fin.close()
        
        storage_latencies = {}
        for dc in data_centers:
            storage_latencies[dc] = {}
            storage_latencies[dc]['get'] = []
            storage_latencies[dc]['put'] = []
            fin = open(os.path.join(data_dir, 'storage_measurements/'+dc+'/store-reads.log'),'r')
            for line in fin:
                items = line.strip().split()
                if items[0] != 'success':
                    continue
                storage_latencies[dc]['get'].append(float(items[3]))
            storage_latencies[dc]['get'].sort()
            fin.close()
            fin = open(os.path.join(data_dir, 'storage_measurements/'+dc+'/store-writes.log'),'r')
            for line in fin:
                items = line.strip().split()
                if items[0] != 'success':
                    continue
                storage_latencies[dc]['put'].append(float(items[3]))
            storage_latencies[dc]['put'].sort()
            fin.close()
            if not self.silent:
                print 'done reading storage ', dc
        return storage_latencies

    def __init__(self):
        self.sample_limit = MAX_VALUE
        self.sample_time = MAX_VALUE
        self.silent = True

        self.between_dc_latency_list = self.ReadNetworkLatency()
        # _, _, self.between_dc_latency_list, self.closest_dcs = self.ReadNetworkLatency()
        self.cloud_storage_latency = self.ReadStorageLatency()
