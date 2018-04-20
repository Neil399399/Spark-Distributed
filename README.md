# Spark-Distributed
Big Data Mining and Applications, Spring 2018.          
This course discusses issues in scalable mining of big data in various applications, which is gaining more popularity in recent years. 
The major focus in this course will be utilizing parallel progamming in distributed platforms for efficient mining of different kinds of big data. 

## Work-Detail
### HW1
Hadoop/Spark distributed mode setup & simple calculation in MapReduce.
### HW2
Statistics of various data types in MapReduce (co-occurrence).          
Dataset link : https://archive.ics.uci.edu/ml/datasets/News+Popularity+in+Multiple+Social+Media+Platforms       
Question :     
1. Find title and headline most frequent words in news data.    
2. Calculate the average popularity of each news by hour, and by day for each platform.
3. Calculate the sum and average sentiment score of each topic.
4. For the top-100 frequent words per topic in titles and headlines, calculate their co-occurrence matrices (100x100).
## Establish Spark Cluster(Standalone Mode)
If you want to have one spark cluster in local, please follow the step to setting your enviroment.

### Download and Install
Please downloads or clone the project in local, and install [Docker](https://docs.docker.com/install/) first.
#### Step1
After complete install ans start docker, please open the project and run the command to build the docker image.
```zsh
docker build -t spark/hadoop .
```
#### Step2
After build the docker image, please run the command to create the docker containers(one master and one slave or more).
```zsh
#for master
docker run -it --name spark-master -p 8088:8088 -p 50070:50070 -p 50010:50010 -p 4040:4040 -p 8042:8042 -p 8888:8888 -p 8080:8080 -v `the folder you want share.`:/root/ spark/hadoop bash
```
```zsh
#for slaves
docker run -it --name spark-slave1 -v `the folder you want share.`:/root/ --link spark-master spark/hadoop bash
```
#### Step3
After finished create container, we should set the hosts to each container. Attach the container and start setting.     
```zsh
# Set hosts
cd ~/etc && vim hosts # go set the docker ID and IP address to each containers.
```
next we need to open the `PermitRootLogin` to access the ssh can connect with the root authority.
```zsh
# open PermitRootLogin
cd etc/ssh
 vim sshd_config  # PermitRootLogin => yes.
...
# If finish changed, go run:
service ssh restart
```
  ![ssh setting](https://i.imgur.com/FThJ9LH.png)
#### Setp4
Started setting spark environments(both master and slaves):
```zsh
# Setting spark-env.sh.
cd usr/local/spark/conf
vim spark-env.sh
```
add this setting:
>export SPARK_MASTER_IP={your master IP}  
export SPARK_MASTER_CORES=1     
export SPARK_WORKER_MEMORY=512m     
export SPARK_EXECUTOR_INSTANCES=4   

and here add your slaves(only master).

```zsh
# Setting slaves file.
cd usr/local/spark/conf
cp slaves.template slaves  #copy and rename.
vim slaves
```
#### Setp5
Started setting hadoop environments(both master and slaves):    
core-site.xml
```zsh
# Setting core-site.xml
cd usr/local/hadoop/etc/hadoop
vim core-site.xml
```
add this setting :
```zsh
<property>
    <name>fs.default.name</name>
    <value>hdfs://{your master ID or name}:9000</value></property>
</property>
```
yarn-site.xml
```zsh
# Setting yarn-site.xml
cd usr/local/hadoop/etc/hadoop
vim yarn-site.xml
```
add this setting :
```zsh
<property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
        </property>
<property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred/ShuffleHandler</value>
</property>
<property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>{your master ID or name}:8025</value>
</property>
<property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>{your master ID or name}:8030</value>
</property>
<property>
        <name>yarn.resourcemanager.address</name>
        <value>{your master ID or name}:8050</value>
</property>
```
mapred-site.xml
```zsh
# Setting mapred-site.xml
cd usr/local/hadoop/etc/hadoop
cp mapred-site.xml.templete mapred-site.xml
vim mapred-site.xml
```
add this setting :
```zsh
<property>
        <name>mapred.job.tracker</name>
        <value>{your master ID or name}:54311</value>
</property>
```
hdfs-site.xml
```zsh
# Setting hdfs-site.xml
cd usr/local/hadoop/etc/hadoop
vim hdfs-site.xml
```
add this setting(for master) :
```
<property>
        <name>dfs.replication</name>
        <value>{your slaves amount }</value>
</property>
<property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/usr/local/hadoop/hadoop_data/hdfs/datanode</value>
</property>
```
add this setting(for slaves) :
```
<property>
        <name>dfs.replication</name>
        <value>{your slaves amount }</value>
</property>
<property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/usr/local/hadoop/hadoop_data/hdfs/namenode</value>
</property>
```
<!-- Slaves
```zsh
cd usr/local/hadoop/etc/hadoop
vim slaves
``` -->
#### Step5 
Create hdfs folder and format(both master and slaves).
```zsh
# For master
cd usr/local/hadoop/hadoop_data/hdfs
rm -r datanode

# For slaves
cd usr/local/hadoop/hadoop_data/hdfs
rm -r namenode
```
After setting, go to master container and format hdfs folder.
```zsh
hadoop namenode -format
```
#### Step6
Run Spark and test.
```zsh
pyspark --master spark://172.17.0.2:7077 --num-executors 1 --total-executor-cores=1 --executor-memory 512m
```
