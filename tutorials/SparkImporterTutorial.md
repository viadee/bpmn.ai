

# Tutorial Spark Importer

The following section of this tutorial explains how data from the Camunda process engine can be preprocessed as a csv file using the Spark Importer pipeline.

## 1. Export Camunda data
If you do not have a Camunda database available yet, you can skip this step and use the example file [camundaExport]() as a data basis.

To export the data from Camunda the tables *"ACT_HI_PROCINST"* and *"ACT_HI_WARINST"* have to be selected from the Camunda h2 database. For example the tool [razorsql](https://razorsql.com/) can be used for this.

The SQL statement for exporting the data from Camunda is as follows:

	SELECT * FROM ACT_HI_PROCINST a JOIN ACT_HI_VARINST v ON a.PROC_INST_ID_ = v.PROC_INST_ID_ AND a.proc_def_key_ = 'XYZ'

After selecting the tables, the data must be stored in the format csv.


## 2. Preprocess the exported data

In the following step the exported file will be preprocessed with the help of the spark-importer. Since we have a csv file, this will be done with the CSVImportAMDProcessingApplication.

The following installations have to be done in advance:
*  Java 8 (JRE, JDK)
*  Development environment (Eclipse or IntelliJ)
*  Maven
*  Hadoop

Now the application can be executed via the Run Configuration in the development environment.  Therefore  the following arguments have be passed:

**Program Arguments:**

```
-fs ".\bpmn.ai.github\tutorials\camundaExport.csv" -fd ".\bpmn.ai.github\tutorials" -d ";" -of "csv"
```
**VM Arguments:**

```
-Dspark.master=local[*]
```

Your working directory should now contain a result folder containing the preprocessed data in csv and parquet format. To compare your results, the [result](result.csv) file can also be found in the tutorial folder. Beside this file a default configuration file is generated listing all steps that have been performed during the last run. If you want to modify certain steps or add new steps, you have to adjust this file accordingly and run the application again.


## 3. Create new step

In the following we will implement a new step and integrate it into the pipeline.

We want to add a column to the dataset to indicate whether the process has been approved or not. As a basis the column *approved* can be used. If its entry is *false* or *null* the entry of the column *approved2* should be "OK" and otherwise "NOT OK".

First a new java class has to be created and the step has to be implemented. The column (in this case "approved") has to be passed as parameter via the configuration file.

```java
public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel,Map<String, Object> parameters) {		
		
		if (parameters == null) {
            SparkImporterLogger.getInstance().writeWarn("No parameters found");
            return dataset;
		}			

		String colName = (String) parameters.get("column");
				
		return dataset.withColumn("approved2", functions.when(dataset.col(colName).equalTo("true"), "OK").otherwise("NOT OK"));

}
```
To integrate the step into the pipeline, it has to be added to the pipeline configuration.

```json
{      
"id": "CheckApprovedStep",   "className":"de.viadee.ki.sparkimporter.processing.steps.userconfig.CheckApprovedStep",
"dependsOn": "TypeCastStep",
    "parameters":{
       "column":  "approved"
    }
},
```
The result can also be found in the [result2.csv](result2.csv).