import time
import os
import os.path
import csv
from collections import Counter

# global
data_dir = './data/'
data = []
FromNodeId = {}
ToNodeId = {}

def Writer(filename,data):
    f = open(filename,'w')
    w = csv.writer(f)
    for x in data:
        w.writerow(x)
    f.close()
    

if __name__ == '__main__':
    
    # open file.
    print('Start input file ...')
    start_time = time.time()
    for file in os.listdir(data_dir):
        if file.endswith(".txt"):
            filename = os.path.join(data_dir, file)
            f = open(filename, 'r')
            temp = csv.reader(f,delimiter = '\t')
            for x in temp:
                data.append(x)

    for x in data[4:]:
        # x[0] -- FromNodeId attribute.
        if x[0] not in FromNodeId:
            FromNodeId[x[0]]=1
        else:
            FromNodeId[x[0]]=FromNodeId[x[0]]+1
        # x[1] -- ToNodeId attribute.
        if x[1] not in ToNodeId:
            ToNodeId[x[1]]=1
        else:
            ToNodeId[x[1]]=ToNodeId[x[1]]+1

    # sorted in descending order.
    sorted_descend_fromNode = Counter(FromNodeId).most_common(None)
    sorted_descend_toNode = Counter(FromNodeId).most_common(None)
    # write in file.
    Writer('outlinks.txt',sorted_descend_fromNode)
    Writer('inlink.txt',sorted_descend_fromNode)
    end_time1 = time.time()
    print('Done.')