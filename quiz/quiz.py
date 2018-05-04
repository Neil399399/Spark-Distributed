# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from collections import Counter, OrderedDict
import csv
import re


def protocol_type(line):
    dict={}
    titleWords = re.findall('[a-zA-z]+', str(line[1]))
    for x in range(0,len(titleWords)):
        if titleWords[x] in dict:
            dict[titleWords[x]]=dict[titleWords[x]]+1
        else:
            dict[titleWords[x]]=1
    return dict

def service(line):
    dict={}
    titleWords = re.findall('[a-zA-z]+', str(line[2]))
    for x in range(0,len(titleWords)):
        if titleWords[x] in dict:
            dict[titleWords[x]]=dict[titleWords[x]]+1
        else:
            dict[titleWords[x]]=1
    return dict

def flag(line):
    dict={}
    titleWords = re.findall('[a-zA-z]+', str(line[3]))
    for x in range(0,len(titleWords)):
        if titleWords[x] in dict:
            dict[titleWords[x]]=dict[titleWords[x]]+1
        else:
            dict[titleWords[x]]=1
    return dict

def logged_in(line):
    dict={}
    titleWords = re.findall('[a-zA-z]+', str(line[11]))
    for x in range(0,len(titleWords)):
        if titleWords[x] in dict:
            dict[titleWords[x]]=dict[titleWords[x]]+1
        else:
            dict[titleWords[x]]=1
    return dict

def intrusion_type(line):
    dict={}
    titleWords = re.findall('[a-zA-z]+', str(line[41]))
    for x in range(0,len(titleWords)):
        if titleWords[x] in dict:
            dict[titleWords[x]]=dict[titleWords[x]]+1
        else:
            dict[titleWords[x]]=1
    return dict


def Dict(dictionary,newDictionary):
    for (key, value) in dictionary.items():
        if key in newDictionary:
            newDictionary[key]=newDictionary[key]+1
        else:
            newDictionary[key]=1
    return newDictionary

# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="quiz"
sparkExecutorMemory="3g"
sparkDriverMemory="3g"
sparkCoreMax="4"
# outputFile = "result.txt"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory).set("spark.driver.memory",sparkDriverMemory)
sc = SparkContext(conf=conf)

# Input data.
print("start")
# decode dataset.
with open ("/root/homework/dataset/quiz/kddcup_data/kddcup_data_all.txt",'r',encoding = 'utf8') as file:
    data = csv.reader(file,delimiter = ",")
    dataset = list(data)
# print("dataset long:",len(dataset))

# print(len(dataset[0]))
dataset1 = sc.parallelize(dataset)

# Q1
print("/***Q1***/")
duration = dataset1.map(lambda x: float(x[0])).stats()
src_bytes = dataset1.map(lambda x: float(x[4])).stats()
dst_bytes = dataset1.map(lambda x: float(x[5])).stats()
num_failed_logins = dataset1.map(lambda x:float(x[10])).stats()


print("duration: ",duration)
print("src_bytes: ",src_bytes)
print("dst_bytes: ",dst_bytes)
print("num_failed_logins: ",num_failed_logins)
print("Q1 done.")

# # Q2
# print("/***Q2***/")
# protocol_type_dict = {}
# service_dict = {}
# flag_dict = {}
# logged_in_dict = {}

# protocol_type = dataset1.map(protocol_type).take(dataset1.count())
# for x in range(0,len(protocol_type)):
#     Dict(protocol_type[x],protocol_type_dict)

# service = dataset1.map(service).take(dataset1.count())
# for x in range(0,len(service)):
#     Dict(service[x],service_dict)

# flag = dataset1.map(flag).take(dataset1.count())
# for x in range(0,len(flag)):
#     Dict(flag[x],flag_dict)

# logged_in = dataset1.map(logged_in).take(dataset1.count())
# for x in range(0,len(logged_in)):
#     Dict(logged_in[x],logged_in_dict)

# # sort
# protocol_type_result = OrderedDict(sorted(protocol_type_dict.items(), key=lambda t: t[1], reverse=True))
# service_result = OrderedDict(sorted(service_dict.items(), key=lambda t: t[1], reverse=True))
# flag_result = OrderedDict(sorted(flag_dict.items(), key=lambda t: t[1], reverse=True))
# logged_in_result = OrderedDict(sorted(logged_in_dict.items(), key=lambda t: t[1], reverse=True))

# print('protocol_type_result: ',protocol_type_result)
# print('service_result: ',service_result)
# print('flag_result: ',flag_result)
# print('logged_in_result: ',logged_in_result)

# print("Q2 done.")

# # Q3
# print("/***Q3***/")

# back_dict = {}
# buffer_overflow_dict = {}
# ftp_write_dict = {}
# guess_passwd_dict = {}
# imap_dict = {}
# ipsweep_dict = {}
# land_dict = {}
# loadmodule_dict = {}
# multihop_dict = {}
# neptune_dict = {}
# nmap_dict = {}
# normal_dict = {}
# perl_dict = {}
# phf_dict = {}
# pod_dict = {}
# portsweep_dict = {}
# rootkit_dict = {}
# satan_dict = {}
# smurf_dict = {}
# spy_dict = {}
# teardrop_dict = {}
# warezclient_dict = {}
# warezmaster_dict = {}

# back = dataset1.filter(lambda x: x[41]=='back.').map(service).collect()
# for x in range(0,len(back)):
#     Dict(back[x],back_dict)
# back_result = Counter(back_dict).most_common(1)

# buffer_overflow = dataset1.filter(lambda x: x[41]=='buffer_overflow.').map(service).collect()
# for x in range(0,len(buffer_overflow)):
#     Dict(buffer_overflow[x],buffer_overflow_dict)
# buffer_overflow_result = Counter(buffer_overflow_dict).most_common(1)

# ftp_write = dataset1.filter(lambda x: x[41]=='ftp_write.').map(service).collect()
# for x in range(0,len(ftp_write)):
#     Dict(ftp_write[x],ftp_write_dict)
# ftp_write_result = Counter(ftp_write_dict).most_common(1)

# guess_passwd = dataset1.filter(lambda x: x[41]=='guess_passwd.').map(service).collect()
# for x in range(0,len(guess_passwd)):
#     Dict(guess_passwd[x],guess_passwd_dict)
# guess_passwd_result = Counter(guess_passwd_dict).most_common(1)

# imap = dataset1.filter(lambda x: x[41]=='imap.').map(service).collect()
# for x in range(0,len(imap)):
#     Dict(imap[x],imap_dict)
# imap_result = Counter(imap_dict).most_common(1)

# ipsweep = dataset1.filter(lambda x: x[41]=='ipsweep.').map(service).collect()
# for x in range(0,len(ipsweep)):
#     Dict(ipsweep[x],ipsweep_dict)
# ipsweep_result = Counter(ipsweep_dict).most_common(1)

# land = dataset1.filter(lambda x: x[41]=='land.').map(service).collect()
# for x in range(0,len(land)):
#     Dict(land[x],land_dict)
# land_result = Counter(land_dict).most_common(1)

# loadmodule = dataset1.filter(lambda x: x[41]=='loadmodule.').map(service).collect()
# for x in range(0,len(loadmodule)):
#     Dict(loadmodule[x],loadmodule_dict)
# loadmodule_result = Counter(loadmodule_dict).most_common(1)

# multihop = dataset1.filter(lambda x: x[41]=='multihop.').map(service).collect()
# for x in range(0,len(multihop)):
#     Dict(multihop[x],multihop_dict)
# multihop_result = Counter(multihop_dict).most_common(1)

# neptune = dataset1.filter(lambda x: x[41]=='neptune.').map(service).collect()
# for x in range(0,len(neptune)):
#     Dict(neptune[x],neptune_dict)
# neptune_result = Counter(neptune_dict).most_common(1)

# nmap = dataset1.filter(lambda x: x[41]=='nmap.').map(service).collect()
# for x in range(0,len(nmap)):
#     Dict(nmap[x],nmap_dict)
# nmap_result = Counter(nmap_dict).most_common(1)

# normal = dataset1.filter(lambda x: x[41]=='normal.').map(service).collect()
# for x in range(0,len(normal)):
#     Dict(normal[x],normal_dict)
# normal_result = Counter(normal_dict).most_common(1)

# perl = dataset1.filter(lambda x: x[41]=='perl.').map(service).collect()
# for x in range(0,len(perl)):
#     Dict(perl[x],perl_dict)
# perl_result = Counter(perl_dict).most_common(1)

# phf = dataset1.filter(lambda x: x[41]=='phf.').map(service).collect()
# for x in range(0,len(phf)):
#     Dict(phf[x],phf_dict)
# phf_result = Counter(phf_dict).most_common(1)

# pod = dataset1.filter(lambda x: x[41]=='pod.').map(service).collect()
# for x in range(0,len(pod)):
#     Dict(pod[x],pod_dict)
# pod_result = Counter(pod_dict).most_common(1)

# portsweep = dataset1.filter(lambda x: x[41]=='portsweep.').map(service).collect()
# for x in range(0,len(portsweep)):
#     Dict(portsweep[x],portsweep_dict)
# portsweep_result = Counter(portsweep_dict).most_common(1)

# rootkit = dataset1.filter(lambda x: x[41]=='rootkit.').map(service).collect()
# for x in range(0,len(rootkit)):
#     Dict(rootkit[x],rootkit_dict)
# rootkit_result = Counter(rootkit_dict).most_common(1)

# satan = dataset1.filter(lambda x: x[41]=='satan.').map(service).collect()
# for x in range(0,len(satan)):
#     Dict(satan[x],satan_dict)
# satan_result = Counter(satan_dict).most_common(1)

# smurf = dataset1.filter(lambda x: x[41]=='smurf.').map(service).collect()
# for x in range(0,len(smurf)):
#     Dict(smurf[x],smurf_dict)
# smurf_result = Counter(smurf_dict).most_common(1)

# spy = dataset1.filter(lambda x: x[41]=='spy.').map(service).collect()
# for x in range(0,len(spy)):
#     Dict(spy[x],spy_dict)
# spy_result = Counter(spy_dict).most_common(1)

# teardrop = dataset1.filter(lambda x: x[41]=='teardrop.').map(service).collect()
# for x in range(0,len(teardrop)):
#     Dict(teardrop[x],teardrop_dict)
# teardrop_result = Counter(teardrop_dict).most_common(1)

# warezclient = dataset1.filter(lambda x: x[41]=='warezclient.').map(service).collect()
# for x in range(0,len(warezclient)):
#     Dict(warezclient[x],warezclient_dict)
# warezclient_result = Counter(warezclient_dict).most_common(1)

# warezmaster = dataset1.filter(lambda x: x[41]=='warezmaster.').map(service).collect()
# for x in range(0,len(warezmaster)):
#     Dict(warezmaster[x],warezmaster_dict)
# warezmaster_result = Counter(warezmaster_dict).most_common(1)


# print('back_result:',back_result)
# print('buffer_overflow_result:',buffer_overflow_result)
# print('ftp_write_result:',ftp_write_result)
# print('guess_passwd_result:',guess_passwd_result)
# print('imap_result:',imap_result)
# print('ipsweep_result:',ipsweep_result)
# print('land_result:',land_result)
# print('loadmodule_result:',loadmodule_result)
# print('multihop_result:',multihop_result)
# print('neptune_result:',neptune_result)
# print('nmap_result:',nmap_result)
# print('normal_result:',normal_result)
# print('perl_result:',perl_result)
# print('phf_result:',phf_result)
# print('pod_result:',pod_result)
# print('portsweep_result:',portsweep_result)
# print('rootkit_result:',rootkit_result)
# print('satan_result:',satan_result)
# print('smurf_result:',smurf_result)
# print('spy_result:',spy_result)
# print('teardrop_result:',teardrop_result)
# print('warezclient_result:',warezclient_result)
# print('warezmaster_result:',warezmaster_result)

# print("Q3 done.")