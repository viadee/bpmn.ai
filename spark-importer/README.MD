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
-devtcc or --dev-type-cast-check | Development feature: Check for type casting errors of columns. | no (default is false)

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
-devtcc or --dev-type-cast-check | Development feature: Check for type casting errors of columns. | no (default is false)

## Configuration
Some steps in the processing can be controlled by the user (see chapter "Data processing steps" for details). The configuration is done in a file named "pipeline_configuration.json" which should be located in the working-directory. If there is no configuration file the applications will create one, filled the variables found in the data and default configurations for them, which would result is no change if run again with this configuration.

The configuration file has the following structure:

```json
	{
  		"data_extraction": {
	  		"filter_query": ""
  		},
  		"preprocessing": {
    		"variable_configuration": [
      			{
        			"variable_name": "",
        			"variable_type": "",
        			"use_variable": true,
        			"comment": ""
        		}
        	],
        	"variable_name_mapping": [
      			{
        			"old_name": "",
        			"new_name": ""
      			}
    		]
      },
      "model_learning": {}
	}`
```

It contains three sections:

	* Data extraction
	* Preprocessing
	* Model learning
	
### Data extraction
This section contains configuration applied to the data during loading it for processing.

#### filter_query
By setting a filter query the data is filtered by this filter before it is run through the processing.

It takes a SQL expression as an input which is applied to the filter method as defined in the [Spark documentation](https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/Dataset.html#filter-java.lang.String-)

Example:

	processDefinitionId == 'p1'
	
### Preprocessing
This section contains configuration applied to the data during processing.

### variable_configuration
After the first run this array is filled with all variables found in the data and can be amended by the user for the following runs.

Property      |  Description
--------------|-------------
variable_name | The name of the variable
variable_type | Which type the variable should be casted to after the data mining table has been created. In the processing all variables are handled as strings.
parse_format  | if variable_type is set to date or timestamp this optional property defines how spark should parse the value while casting it. This follows the [SimpleDateFormat patterns](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html).
use_variable  | Should the variable be considered during the run. If set to false it will filtered out before the processing runs through.
comment       | A comment if changes have been done to this variable, which will be written into the application log during processing.

Example:

```json
	...
	"variable_configuration": [
		{
			"variable_name": "a",
			"variable_type": "string",
			"parse_format": "yyy"
			"use_variable": true,
			"comment": ""
		},
		{
			"variable_name": "b",
			"variable_type": "boolean",
			"use_variable": false,
			"comment": "Not required"
		},
		{
			"variable_name": "c",
			"variable_type": "double",
			"use_variable": true,
			"comment": ""
		},
		{
			"variable_name": "d",
			"variable_type": "date",
			"parse_format": "yyyy/MM/dd"
			"use_variable": true,
			"comment": ""
		},
	],
	...
```

### variable\_name\_mapping

In case variable names have changed in the dataset (e.g. in a new process version) a mapping can be added in this section which old variable name should be renamed to which new variable name.

Property      |  Description
--------------|-------------
old_value     | The (old) name of the variable to be renamed
new_value     | The (new) name of the variable to be renamed to

Example:

```json
	...
	"variable_name_mapping": [
      {
        "old_name": "amount",
        "new_name": "amountInEUR"
      }
    ],
    ...
```
    
### column\_configuration

After the first run this array is filled with all columns found in the data and can be amended by the user for the following runs. Works analogous to the variableConfiguration.

Property      |  Description
--------------|-------------
column_name | The name of the column
column_type | Which type the column should be casted to after the data mining table has been created.
parse_format  | if column_type is set to date or timestamp this optional property defines how spark should parse the value while casting it. This follows the [SimpleDateFormat patterns](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html).
use_column  | Should the column be considered during the run. If set to false it will filtered out before the processing runs through.
comment       | A comment if changes have been done to this column, which will be written into the application log during processing.

Example: 

```json
    ...
    "column_configuration": [
      {
        "column_name": "case_execution_id_",
        "column_type": "string"
        "use_column": false,
        "comment": ""
      }
    ],
    ...
```

### column\_hash\_configuration

Before writing the final result into a CSV-File, columns of data can be selected to make them unrecognisable to protect personal data. Using sha1 at the moment.

Property      |  Description
--------------|-------------
column_name | The name of the column
hash_column  | Should the column be hashed. If set to true, the column will be hashed.
comment       | A comment if changes have been done to this variable, which will be written into the application log during processing.

Example: 

```json
    ...
    "column_hash_configuration": [
      {
        "column_name": "state_",
        "hash_column": true,
        "comment": ""
      }
    ]
    ...
```


### Model learning
Section reserved for settings related to the machine learning model.

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

## More details
For more details and in-depth infomation on the Spark importer application please see [this document](./README_details.md).