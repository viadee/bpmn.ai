# viadee Spark BPMN process data importer

This is a Apache Spark application that takes data from an CSV export of Camunda history database tables and aggreates them to a data mining table containing one line per process instance including additional columns for every process variable.

The SQL statement for exporting the data from Camunda is as follows:

	SELECT * FROM ACT_HI_PROCINST a JOIN ACT_HI_VARINST v ON a.PROC_INST_ID_ = v.PROC_INST_ID_ AND a.proc_def_key_ = 'XYZ'
	
## Parameters

Parameter                 | Description
--------------------------|------------------
-fs or --file-source      | Path an name of the CSV-File to be processed.
-fd or --file-destination | The name of the target folder, where the resulting csv files are being stored, i.e. the data mining table.
-d or --delimiter         | Character or string that separates fields such as [ ;, &#124; or &#124;&#124;&#124; ]. Please make sure that these are not contained in your data.
-rc or --revision-count   | Boolean toggle to enable the counting of changes to a variable. It results in a number of columns named <VARIABLE_NAME>_rev.
-sr or --step-results     | Defines if intermediate results be written into CSV files.


## Setup and run in IntelliJ / Eclipse
In order for the SparkImporterApplication to run in IntelliJ the run configuration needs to be amended.
Try to run the Applicaiton once as a Java Application and the add the following parameters in the run configuration:

### VM arguments

#### mandatory
This defines that Spark should run in local standalone mode

	-Dspark.master=local

### optional 
	-Dspark.executor.extraJavaOptions="-Dnumbers=4"
	-Dspark.executor.memoryOverhead=1g 
	-Dspark.driver.memory=5g

### Program arguments
Here you define the parameters of the Spark application. You need to define at least the parameters -fs, -fd and --d.

	-fs <path_to_input_csv> -fd <path_to_target_folder_for_results> -d <field_delimiter>
	
Now you can run the application via the run configuration.

## Run with spark-submit
In order to run the application with spark-submit you first need to package the applcation to a JAR file with maven.

	mvn clean package
	
Then you can run the spark-submit command from the bin folder of your Apache Spark installation by referencing the created JAR file.

	bin/spark-submit --class de.viadee.ki.sparkimporter.SparkImporterCSVApplication --master "local" --deploy-mode client --name ViadeeSparkImporter <path_to_packaged_jar> -fs <path_to_input_csv> -fd <path_to_target_folder_for_results> -d <field_delimiter>

