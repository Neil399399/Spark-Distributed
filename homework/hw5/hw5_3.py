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

    # input node ID.
    print('Please input node ID ...')
    input_node = input('input node:')
    
    # search.
    fromNode_temp_list = []
    toNode_temp_list = []
    for x in data[4:10]:
        if x[1] == input_node:
            fromNode_temp_list.append(x[0])
        if x[0] == input_node:
            toNode_temp_list.append(x[1])

    FromNodeId[input_node] = fromNode_temp_list
    ToNodeId[input_node] = toNode_temp_list

    print('To this node :')
    print(FromNodeId)
    print('From this node :')
    print(ToNodeId)
    end_time1 = time.time()
    print('Done.')