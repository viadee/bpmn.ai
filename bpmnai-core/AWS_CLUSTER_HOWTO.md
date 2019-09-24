# How to run a Spark cluster on AWS
This document describes how to use Flintrock to run bpmn.ai in an AWS cluster and get the resuts. Flintrock is a tool for managing Spark and Hadoop clusters in Amazons AWS. It can be found under 
[Github](https://github.com/nchammas/flintrock).

## Configure Flintrock
In order to start a clust with Flintrock it needs to be configured, so Flintrock knows what type of cluster to build. Start by typing
	
	flintrock configure
	
This opens the configuration file in your default text editor. Change it so it matches the configuration example below.

```yaml
	services:
	  spark:
	    version: 2.3.1
	    # git-commit: latest  # if not 'latest', provide a full commit SHA; e.g. d6dc12ef0146ae409834c78737c116050961f350
	    # git-repository:  # optional; defaults to https://github.com/apache/spark
	    # optional; defaults to download from from the official Spark S3 bucket
	    #   - must contain a {v} template corresponding to the version
	    #   - Spark must be pre-built
	    #   - must be a tar.gz file
	    # download-source: "https://www.example.com/files/spark/{v}/spark-{v}.tar.gz"
	    # executor-instances: 1
	  hdfs:
	    version: 2.8.4
	    # optional; defaults to download from a dynamically selected Apache mirror
	    #   - must contain a {v} template corresponding to the version
	    #   - must be a .tar.gz file
	    # download-source: "https://www.example.com/files/hadoop/{v}/hadoop-{v}.tar.gz"
	    # download-source: "http://www-us.apache.org/dist/hadoop/common/hadoop-{v}/hadoop-{v}.tar.gz"
	
	provider: ec2
	
	providers:
	  ec2:
	    key-name: bpmn.ai-keypair # the keypair used for bmpn.ai activities
	    identity-file: <path_to_the_bpmnai-keypair.pem_file>
	    instance-type: m5.large # list of available types can be found here: https://aws.amazon.com/ec2/instance-types/
	    region: eu-central-1 # use it for Frankfurt data center
	    # availability-zone: <name>
	    ami: ami-0bfa8b98c4ff06eed   # Amazon Linux 2, eu-central-1, can be found here and might change in future: https://aws.amazon.com/amazon-linux-2/release-notes/
	    user: ec2-user
	    # ami: ami-61bbf104   # CentOS 7, us-east-1
	    # user: centos
	    # spot-price: <price>
	    # vpc-id: <id>
	    # subnet-id: <id>
	    # placement-group: <name>
	    # security-groups:
	    #   - group-name1
	    #   - group-name2
	    # instance-profile-name:
	    tags:
			- viadee Projektnummer,9312 # used to allocate the AWS consts in viadee
	    #   - key2, value2  # leading/trailing spaces are trimmed
	    #   - key3,  # value will be empty
	    # min-root-ebs-size-gb: <size-gb>
	    tenancy: default  # default | dedicated
	    ebs-optimized: no  # yes | no
	    instance-initiated-shutdown-behavior: terminate  # terminate | stop
	    # user-data: /path/to/userdata/script
	
	launch:
	  num-slaves: 1 # how many salves should be launched in t he cluster?
	  install-hdfs: True # Istall and cond´figure Hadoop cluster as well
	  # install-spark: False
	
	debug: false
```

These are the default settings for Flintrock. But single parameters can be orverwritten when launching a cluster.


## Launch a cluster
A cluster according to the default configuration can be launched by running

	flintrock launch <cluster-name>
	
When the command is finished is gives you the hostname of the master server.

If you e.g. want to launch a different instance type or njumber of slaves you can also run the following command to overwrite the default settings

	flintrock launch <cluster-name> --ec2-instance-type m5.2xlarge --num-slaves 2

## Login to the cluster master server
To login to the master of the cluster via ssh you can simply run

	flintrock login <cluster-name>

## Upload files to the cluster 

### Upload the Spark application jar
In order for the application to run on the Spark cluster, you need to disibute the applcation jar to all nodes. Thios can easiky be done by using the following command

	flintrock copy-file <cluster-name> <path_to_bpmnai-core.jar> /home/ec2-user/

This copies the files into the home directoy of the ec2-user account with which Flintrock logs you in.
		
### Upload input data
As we are using the Hadoop cluster for input and output data we need to upload the input data to Hadoop.

It is more stable to first copy the file onto the master and then into the HDFS from there. To upload the input data run the following command

	scp -i <path_to_the_bpmnai-keypair.pem_file> -r <path_to_input_data> ec2_user@<ec2-cluster-master-hostname>:/home/ec2-user/

Now we need to create directories in the Hadoop file system (HDFS) and copy the input data into it. You need to login to the cluster master server and run the following commands on it.

First we create a data and a result folder

	hadoop/bin/hadoop fs -mkdir /data /result
	
???CHMOD REQUIRED????
	hadoop/bin/hadoop fs -chmod 777 /result
	
We can confirm that they have been created by running

	hadoop/bin/hadoop fs -ls /
	
Now we copy the input data into the data folder in HDFS

	hadoop/bin/hadoop fs -put /home/ec2-user/<input_data> /data



## Start slave on the master server
If you are only running one slave you might need to run a slave on the master as well. This can be done by the following command on the master server.

	spark/sbin/start-slave.sh spark://<ec2-cluster-master-hostname>:7077


## Submit Spark job
As we now have the input data uploaded and the result folder created in HDFS and the Spark application jar is uploaded to all nodes we can submit the Spark job. This is done by running the spark-submit command. The command below is an example for running the CSV Importer and Processing Application. The pplication parameters are descibed in the [README](./README.MD).

	spark-submit --class de.viadee.bpmnai.core.CSVImportAndProcessingApplication \
		--master spark://<ec2-cluster-master-hostname>:7077 \
		--deploy-mode cluster /home/ec2-user/bpmnai-core-1.2.0-SNAPSHOT.jar \
		-fs hdfs://<ec2-cluster-master-hostname>:9000/data/integration_test_file.csv \
		-fd hdfs://<ec2-cluster-master-hostname>:9000/result -d "|"
		
The cluster state can be seen under [http://\<ec2-cluster-master-hostname\>:8080](http://<ec2-cluster-master-hostname>:8080).

Once the job is finished the result can be found in the HDFS under the /result folder.

## Download files from HDFS
Again the most stable way is to first download the file from HDFS onto the master server and then to your local machine. To download the result from HDFS to the master run the following command

	hadoop/bin/hadoop fs -get /result /home/ec2-user
	
This will get the complete result folder into the home directory of the ec2-user.

Now you can copy the file onto you local machine by running the follwing command from your local machine

	scp -i <path_to_the_bpmnai-keypair.pem_file> -r ec2_user@<ec2-cluster-master-hostname>:/home/ec2-user/result <path_to_local_destination>

## Stop or terminate cluster
In order to avoid unnecesary cost for not used but running AWS instances you can stop (if you still need it) or even destroy the cluster.

To stop the cluster run

	flintrock stop <cluster-name>
	
It can be started later again by running

	flintrock start <cluster-name>
	
To complete terminate the cluster run

	flintrock destroy <cluster-name>