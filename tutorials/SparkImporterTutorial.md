

# Tutorial Spark Importer

The following section of this tutorial explains how data from the Camunda process engine can be preprocessed as a csv file using the Spark-Importer pipeline.

## 1. Export Camunda data
If you do not have a Camunda database available yet, you can skip this step and use the example file [camundaExport](camundaExport.csv) as a data basis.

To export the data from Camunda the tables *ACT_HI_PROCINST* and *ACT_HI_WARINST* have to be selected from the Camunda h2 database. For example the tool [razorsql](https://razorsql.com/) can be used for this.

The SQL statement for exporting the data from Camunda is as follows:

```sql
SELECT * FROM ACT_HI_PROCINST a JOIN ACT_HI_VARINST v ON a.PROC_INST_ID_ = v.PROC_INST_ID_ AND a.proc_def_key_ = 'XYZ'
```

After selecting the tables, the data must be stored in the format csv. The pipeline currently expects all column names for the *CSVImportAndProcessingApplication* to be lowercase. If this is not the case in the exported CSV, remember to convert the column names in advance (the example file is already in the correct format).


## 2. Preprocess the exported data

In the following step the exported file will be preprocessed with the help of the spark-importer. Since we have a csv file, this will be done with the *CSVImportAndProcessingApplication*.

The following installations have to be done in advance:
*  Java 8 (JRE, JDK)
*  Development environment (Eclipse or IntelliJ)
*  Maven
*  Hadoop (only required for Windows)

Now the application can be executed via the Run Configuration in the development environment.  Therefore, the following arguments have to be passed:

**Program Arguments:**

```arg
-fs ".\bpmn.ai.github\tutorials\camundaExport.csv" -fd ".\bpmn.ai.github\tutorials" -d ";" -of "csv"
```
**VM Arguments:**

```
-Dspark.master=local[*]
```

Your working directory should now contain a result folder containing the preprocessed data in csv and parquet format. To compare your results, the [result](result.csv) file can also be found in the tutorial folder. Beside this file a default configuration file is generated listing all steps that have been performed during the last run. If you want to modify certain steps or add new steps, you have to adjust this file accordingly and run the application again.


## 3. Create new step

In the following we will implement a new step and integrate it into the pipeline.

An important part of *bpmn.ai* is to use the preprocessed data for the data mining process. If we look at the column *approved*, it is noticeable that there are three possible values that would also be included as such in the data mining model. To avoid this we will create a new step in the following, which creates a new column. Since only one table is needed for machine learning, the new column must be added to the existing dataset and can thus be selected for the model in the subsequent data mining process. The entry of the new column *approved2* has to be "OK" if the entry of the column *approved* is TRUE and otherwise it has to be "NOT OK".

| approved |approved2|
| -------- | -------- |
| null     |NOT OK|
| true     |OK|
| false    |NOT OK|
| true    |OK|
| true    |OK|
| false    |NOT OK|

First a new java class has to be created and the step has to be implemented. The respective column name (in this case *approved*) has to be passed as a parameter via the configuration file.

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