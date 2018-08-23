# viadee Spark BPMN process data importer

This project contains three Apache Spark applications that serve the task of taking data from Camunda engine and aggreating them to a data mining table containing one line per process instance including additional columns for every process variable. This data mining table is then used to train a machine learning algorithm in order to predict certain events of the process in the future.

The following applications three applications are available:

* SparkImporterCSVApplication
* SparkImporterKafkaImportApplication
* SparkImporterKafkaDataProcessingApplication

Each of those applications serves a different purpose.

## CSV Import and Processing

This application ( takes data from an CSV export of Camunda history database tables and aggreates them to a data mining table. The result is also a CSV file containing the data mining table structure.

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


## SparkImporterKafkaImportApplication

This application retrieves data from Kafka in which three queues have been made available and are filled by data from the history event handler of Camunda:

* **processInstance**: filled by events on the process instance level
* **activityInstance**: filled by events on the activity instance level
* **variableUpdate**: filled by events happening when a variable gets updated in any way

### Parameters

Parameter                 | Description	                | mandatory
--------------------------|-----------------------------|----------------------
-kb or --kafka-broker     | server and port of the Kafka instance to query for data | yes
-fd or --file-destination | The name of the target folder, where the received data should be stored to (e.g. a local folder or a hdfs target). | yes
-bm or --batch-mode.      | in batch mode the application stops receiving data, once every queue has returned zero entries at least aonce during the stream. Otherwise it keeps the stream running until it is aborted. | no (default is false)
-sr or --step-results     | Defines if intermediate results be written into CSV files. | no (default is false)
-wd or --working-directory| Folder where the configuration files are stored or should be stored. | no (default is the current directory)
-ld or --log-directory| Folder where the log files should be stored. | no (default is the current directory)

## SparkImporterKafkaDataProcessingApplication

This application retrieves data from the SparkImporterKafkaImportApplication. The --file-source parameter should match the --file-destination parameter of a SparkImporterKafkaImportApplication run.

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

