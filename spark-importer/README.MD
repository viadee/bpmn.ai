# viadee Spark BPMN process data importer

This project contains three Apache Spark applications that serve the task of taking data from Camunda engine and aggreating them to a data mining table containing one line per process instance including additional columns for every process variable. This data mining table is then used to train a machine learning algorithm in order to predict certain events of the process in the future.

The following applications three applications are available:

* SparkImporterCSVApplication
* SparkImporterKafkaImportApplication
* SparkImporterKafkaDataProcessingApplication

Each of those applications serves a different purpose.

## Data pipeline
The picture below shows the pipeline through which the data flows from Camunda to the Machine Learning engine. Each of the three application serves a specific purpose and use cases around importing into, data aggregation and transformation inside and exporting data from Apache Spark. 
![alt text](./doc/SparkImporterApplicationFlow.png "SparkImporterCSVApplication Pipeline")


## CSV Import and Processing

This application (application class: CSVImportAndProcessingApplication) takes data from a CSV export of Camunda history database tables and aggreates them to a data mining table. The result is also a CSV file containing the data mining table structure.

The SQL statement for exporting the data from Camunda is as follows:

	SELECT * FROM ACT_HI_PROCINST a JOIN ACT_HI_VARINST v ON a.PROC_INST_ID_ = v.PROC_INST_ID_ AND a.proc_def_key_ = 'XYZ'
	
### Parameters

Parameter                 | Description                | mandatory
--------------------------|----------------------------|------------
-fs or --file-source      | Path an name of the CSV file to be processed. | yes
-fd or --file-destination | The name of the target folder, where the resulting CSV file is being stored (the data mining table). | yes
-d or --delimiter         | Character or string that separates fields such as [ ;, &#124; or &#124;&#124;&#124; ]. Please make sure that these are not contained in your data. | yes
-rc or --revision-count   | Boolean toggle to enable the counting of changes to a variable. It results in a number of columns named <VARIABLE_NAME>_rev. | no (default is true)
-sr or --step-results     | Defines if intermediate results be written into CSV files. | no (default is false)
-wd or --working-directory| Folder where the configuration files are stored or should be stored. | no (default is the current directory)
-ld or --log-directory| Folder where the log files should be stored. | no (default is the current directory)


## Kafka Import

This application (application class: KafkaImportApplication) retrieves data from Kafka in which three queues have been made available and are filled with data from the history event handler of Camunda:

* **processInstance**: filled by events on the process instance level
* **activityInstance**: filled by events on the activity instance level
* **variableUpdate**: filled by events happening when a variable gets updated in any way

It then stores the retrieved data in a defined location as parquet files. No data processing is happening by this application as it can run as a Spark application constantly receiving data from Kafka streams.

### Parameters

Parameter                 | Description	                | mandatory
--------------------------|-----------------------------|----------------------
-kb or --kafka-broker     | server and port of the Kafka instance to query for data | yes
-fd or --file-destination | The name of the target folder, where the received data should be stored to (e.g. a local folder or a hdfs target). | yes
-bm or --batch-mode.      | in batch mode the application stops receiving data, once every queue has returned zero entries at least aonce during the stream. Otherwise it keeps the stream running until it is aborted. | no (default is false)
-sr or --step-results     | Defines if intermediate results be written into CSV files. | no (default is false)
-wd or --working-directory| Folder where the configuration files are stored or should be stored. | no (default is the current directory)
-ld or --log-directory| Folder where the log files should be stored. | no (default is the current directory)

## Kafka Processing

This application (application class: KafkaImportApplication) retrieves data from a Kafka Import. The --file-source parameter should match the --file-destination parameter of a Kafka Import run. The data runs through the same steps as in the CSV Import and Processing application, it is just a separate application as it has a different input than the CSV case.

### Parameters

Parameter                 | Description                | mandatory
--------------------------|----------------------------|----------------------
-fs or --file-source      | Path of the folder containing the data from a SparkImporterKafkaImportApplication run. | yes
-fd or --file-destination | The name of the target folder, where the resulting CSV file is being stored (the data mining table). | yes
-d or --delimiter         | Character or string that should be used in the resulting CSV file as a separator for fields such as [ ;, &#124; or &#124;&#124;&#124; ]. Please make sure that these are not contained in your data. | yes
-rc or --revision-count   | Boolean toggle to enable the counting of changes to a variable. It results in a number of columns named <VARIABLE_NAME>_rev. | no (default is true)
-sr or --step-results     | Defines if intermediate results be written into CSV files. | no (default is false)
-wd or --working-directory| Folder where the configuration files are stored or should be stored. | no (default is the current directory)
-ld or --log-directory| Folder where the log files should be stored. | no (default is the current directory)

## Data processing example
This (to a minimum reduced) example should show what happens to the data that goes through the processing steps.

Let's assume the following (simplified) data coming from Camunda.

processInstanceId  	|  variableName 	|  serializer | text | long |  double  |  revision
--------------------|---------------	|-------------|------|------|----------|------------
1						| a					| string      | hello|      |          | 0
1						| b					| boolean     |      | 1    |          | 1
1						| c		     		| double      |      |      | 1.5      | 0
2						| a					| string      | hi   |      |          | 0
2						| b					| boolean     |      | 0    |          | 2
2						| c		     		| double      |      |      | 12.5     | 1

After procesing the data the following data mining table is produced which can be used to train a machine learning algorithm.

processInstanceId  	|  a    |  b | c    | a_rev | b_rev | c_rev
--------------------|-------|----|------|-------|-------|------
1						| hello | 1  | 1.5  | 0     | 1     | 0
1						| hi    | 0  | 12.5 | 0     | 2     | 1


## Setup and run in IntelliJ / Eclipse
In order for the SparkImporterApplications to run in IntelliJ the run configuration needs to be amended.
Try to run the Applicaiton once as a Java Application and the add the following parameters in the run configuration:

### VM arguments

#### mandatory
This defines that Spark should run in local standalone mode and utilise all CPUs.

	-Dspark.master=local[*]

### optional 
	-Dspark.executor.memoryOverhead=1g 
	-Dspark.driver.memory=2g

### Program arguments
Here you define the parameters of the respective Spark application. You need to define the parameters as listed above, e.g.

	-fs <path_to_input_csv> -fd <path_to_target_folder_for_results> -d <field_delimiter>
	
Now you can run the application via the run configuration.

## Run with spark-submit
In order to run the application with spark-submit you first need to package the applcation to a JAR file with maven.

	mvn clean package
	
Then you can run the spark-submit command from the bin folder of your Apache Spark installation by referencing the created JAR file and the Spark application class you would like to run including its parameters.

	bin/spark-submit --class de.viadee.ki.sparkimporter.SparkImporterCSVApplication --master "local[*]" --deploy-mode client --name ViadeeSparkImporter <path_to_packaged_jar> -fs <path_to_input_csv> -fd <path_to_target_folder_for_results> -d <field_delimiter>

