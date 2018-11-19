

# Tutorial Spark Importer

The following section of this tutorial explains how data from the Camunda process engine can be preprocessed as a csv file using the Spark Importer pipeline.

## 1. Export Camunda data
If you do not have a Camunda database available yet, you can skip this step and use the example file [camundaExport]() as a data basis.

To export the data from Camunda the tables *"ACT_HI_PROCINST"* and *"ACT_HI_WARINST"* have to be selected from the Camunda h2 database. For example the tool [razorsql](https://razorsql.com/) can be used for this.

The SQL statement for exporting the data from Camunda is as follows:

	SELECT * FROM ACT_HI_PROCINST a JOIN ACT_HI_VARINST v ON a.PROC_INST_ID_ = v.PROC_INST_ID_ AND a.proc_def_key_ = 'XYZ'

After selecting the tables, the data must now be stored in the format csv.


## 2. Preprocess the exported data

In the following step the exported files will be preprocessed with the help of the spark-importer. Since we have a csv file, this will be done with the CSVImportAMDProcessingApplication.

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

Your working directory should now contain an output.csv file containing the preprocessed data. Beside this file a default configuration file is generated listing all steps that have been performed during the lust run. If you want to modify certain steps or add new steps, you have to adjust this file accordingly and run the application again.


## 3. Edit configuration file

This step is optional and shows how to add an additional own step and to integrate it into the pipeline.

To do this, we want to create a step that adds a column in which Boolean values are specified to indicate whether the process has already been completed.