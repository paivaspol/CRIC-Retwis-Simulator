import glob
import time
import sys
import random
from time import sleep

MAX_VALUE = 30000
USE_SAMPLE_LIMIT = True

data_dir = "/d01/old_vault_backup/home/csgrads/zwu005/research/consistency_availability_and_latency/analysis/inter_datacenter_quorum_storage_latency/"
class LatencyManager:

    # Returns a sample of the latencies between data centers.
    def get_network_latency(self, src, dst):
        return self.GetOneRandomSample(self.between_dc_latency_list[src][dst])
    
    def get_storage_latency(self, dc, op):
        return self.GetOneRandomSample(self.cloud_storage_latency[dc][op])

    # Returns a list of dc sorted from closest to furthest
    def get_closest_dcs(self, dc):
        return [ r[1] for r in self.closest_dcs[dc] ]

    def index_to_server(self, index):
        return self.index2server[index]

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
        dc_list = glob.glob("/d01/old_vault_backup/zwu/dns_measurement_data.1/between_cloud_measurement/*")
        between_dc_latency = {}
        dc_round_latency = {}
        between_dc_latency_list = {}
        dst_list = {}
        print 'Reading network latency'
        for dc in dc_list:
            if dc.find('google') > -1:
                continue
            current_dc = (dc.split('/'))[-1]
            folder_list = glob.glob(dc+'/*')

            total_measurement = 0
            dst_list[current_dc] = []

            sample_index = 0
            skip_index = 0
            for a_folder in folder_list:
                try:
                    timestamp = float((a_folder.split('/data.'))[1])
                    timestamp_str = (a_folder.split('/data.'))[1]

                    fin = open(a_folder+'/log_time_no_stop','r')
                    current_round = 0
                    for line in fin:
                        items = line.strip().split('||')
                        round = int(items[0])
                        round_str = items[0]

                        dst = items[1]

                        type = items[2]
                        if type != 'site':
                            continue
                        time = items[3]
                        ip = items[4]
                        if ip == 'ERROR':
                            continue
                        time_total = float(items[5])
                        time_namelookup = float(items[6])
                        time_connect = float(items[7])
                        latency = (time_connect - time_namelookup)*1000

                        if current_dc not in between_dc_latency:
                            between_dc_latency[current_dc] = {}
                            between_dc_latency_list[current_dc] = {}
                        if dst not in between_dc_latency[current_dc]:
                            between_dc_latency[current_dc][dst] = {}
                            between_dc_latency_list[current_dc][dst] = []
                        between_dc_latency[current_dc][dst][round_str+timestamp_str] = (latency, time)
                        between_dc_latency_list[current_dc][dst].append(latency)

                        if current_dc not in dc_round_latency:
                            dc_round_latency[current_dc] = {}
                        if round_str+timestamp_str not in dc_round_latency[current_dc]:
                            dc_round_latency[current_dc][round_str+timestamp_str] = {}
                        dc_round_latency[current_dc][round_str+timestamp_str][dst] = latency

                        sample_index += 1
                        if USE_SAMPLE_LIMIT and sample_index == self.sample_limit:
                            break

                    fin.close()
                    if USE_SAMPLE_LIMIT and sample_index == self.sample_limit:
                        break
                except Exception as e:
                    print e

            #Find closest DCs
            for dst in between_dc_latency[current_dc]:
                between_dc_latency_list[current_dc][dst].sort()
                dst_list[current_dc].append((self.Percentile(between_dc_latency_list[current_dc][dst], 50), dst))
            dst_list[current_dc].sort()


            print 'Done', current_dc

        return between_dc_latency, dc_round_latency, between_dc_latency_list, dst_list

    def ReadStorageLatency(self):
        print 'Reading storage latency'
        storage_list = glob.glob("/d01/old_vault_backup/zwu/costlo_public_data_set/vm_measurements/*")
        cloud_storage_latency = {}
        for cloud_storage in storage_list:
            current_storage = self.storage2server[(cloud_storage.split('/'))[-1]]
            cloud_storage_latency[current_storage] = {}
            for op in ['get', 'put']:
                cloud_storage_latency[current_storage][op] = []
                fin = open(cloud_storage+'/'+op+'_same_1')
                sample_index = 0
                skip_index = 0
                for line in fin:
                    items = line.strip().split(';')
                    cloud_storage_latency[current_storage][op].append(float(items[0]))
                    sample_index += 1
                    if USE_SAMPLE_LIMIT and sample_index == self.sample_limit:
                        break
                fin.close()
                cloud_storage_latency[current_storage][op].sort()
                print current_storage, op, len(cloud_storage_latency[current_storage][op])
        return cloud_storage_latency


    def GetClientLatencyReadingMajority(clients, configuration, between_dc_latency_list, cloud_storage_latency):
        client_latency = {}
        for client in clients:
            latency_list = []
            latency_list_f2 = []
            for k in range(sample_time):
                parallel_latencies = []
                for replica in configuration:
                    network_latency = GetOneRandomSample(between_dc_latency_list[index2server[client]][index2server[replica]])
                    storage_latency = GetOneRandomSample(cloud_storage_latency[index2server[replica]]['get'])
                    parallel_latencies.append(network_latency+storage_latency)
                parallel_latencies.sort()
                latency_list.append(parallel_latencies[len(parallel_latencies)/2])
                latency_list_f2.append(parallel_latencies[len(parallel_latencies)/2+1])
            latency_list.sort()
            latency_list_f2.sort()
            client_latency[client] = {}
            client_latency[client]['50'] = Percentile(latency_list, 50)
            client_latency[client]['99'] = Percentile(latency_list, 99)
            client_latency[client]['f250'] = Percentile(latency_list_f2, 50)
            client_latency[client]['f299'] = Percentile(latency_list_f2, 99)
        return client_latency

    def Test2Configurations(clients, configuration1, configuration2, between_dc_latency_list, cloud_storage_latency):
        for client in clients:
            for k in range(sample_time_test):
                parallel_latencies = []
                for replica in configuration1:
                    network_latency = GetOneRandomSample(between_dc_latency_list[index2server[client]][index2server[replica]])
                    storage_latency = GetOneRandomSample(cloud_storage_latency[index2server[replica]]['get'])
                    parallel_latencies.append((network_latency+storage_latency, replica))
                parallel_latencies.sort()
                print client, parallel_latencies

                parallel_latencies = []
                for replica in configuration2:
                    network_latency = GetOneRandomSample(between_dc_latency_list[index2server[client]][index2server[replica]])
                    storage_latency = GetOneRandomSample(cloud_storage_latency[index2server[replica]]['get'])
                    parallel_latencies.append((network_latency+storage_latency, replica))
                parallel_latencies.sort()
                print client, parallel_latencies

    def __init__(self):
        self.sample_limit = MAX_VALUE
        self.sample_time = MAX_VALUE

        fin = open(data_dir + 'server_list_with_name','r')
        self.server_list = []
        self.server2index = {}
        self.index2server = {}
        self.server2storage = {}
        self.storage2server = {}

        for line in fin:
            self.server_list.append((line.strip().split())[1])
            self.server2index[(line.strip().split())[1]] = (line.strip().split())[0]
            self.index2server[(line.strip().split())[0]] = (line.strip().split())[1]
            self.server2storage[(line.strip().split())[1]] = (line.strip().split())[2]
            self.storage2server[(line.strip().split())[2]] = (line.strip().split())[1]
        fin.close()
        print self.server2index
        print self.index2server
        print self.server2storage
        print self.storage2server
        _, _, self.between_dc_latency_list, self.closest_dcs = self.ReadNetworkLatency()
        self.cloud_storage_latency = self.ReadStorageLatency()
