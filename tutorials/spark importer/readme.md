

The following section of this tutorial explains how a new Spark application can be created by integrating the spark importer pipeline in a new java project and adding new customized steps to the pipeline.



## 1. Create a new java project and integrate spark-importer

First, a new Maven project must be created in the provided workspace. The Group Id and Version has to be set as follows:

`Group Id: de.viadee.ki`
`Version: 1.0.0-SNAPSHOT`

whereas the Artifact ID can be chosen arbitrarily.


Next the file pom.xml must be adapted. Therefore, one has to create the dependency to the spark-importer and include the viadee-snapshot repository.


    <repositories>
            <repository>
                <id>viadee-snapshots</id>
                <name>viadee Snapshots</name>
                <url>http://nexus.intern.viadee.de/repository/maven-snapshots/</url>
                <snapshots>
                    <enabled>true</enabled>
                </snapshots>
            </repository>
            <repository>
                <id>viadee-releases</id>
                <name>viadee releases</name>
                <url>http://nexus.intern.viadee.de/repository/maven-releases/</url>
                <snapshots>
                    <enabled>false</enabled>
                </snapshots>
            </repository>
    </repositories>
```
<dependencies>
    <dependency>
        <groupId>de.viadee.ki</groupId>
        <artifactId>spark-importer</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```



## 2. Create project specific step

In the following we will implement a new step and integrate it into the pipeline.

An important part of *bpmn.ai* is to use the preprocessed data for the data mining process. If we look at the column *approved*, it is noticeable that there are three possible values that would also be included as such in the data mining model. To avoid this we will create a new step in the following, which creates a new column. Since only one table is needed for machine learning, the new column must be added to the existing dataset and can thus be selected for the model in the subsequent data mining process. The entry of the new column *approved2* has to be "OK" if the entry of the column *approved* is TRUE and otherwise it has to be "NOT OK".

| approved | approved2 |
| -------- | --------- |
| null     | NOT OK    |
| true     | OK        |
| false    | NOT OK    |
| true     | OK        |
| true     | OK        |
| false    | NOT OK    |

First a new java class has to be created and the step has to be implemented (e.g. class name: CheckApprovedStep). The respective column name (in this case *approved*) has to be passed as a parameter via the configuration file.

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



## 3. Run Application

In order to insert the new step into the pipeline, the default configuration must  be generated first. For this reason, the default pipeline must be executed once to create this configuration. Then the new step can be integrated.

In the main method of the application, a CSVImportAndProcessingRunner is created.

```java
public class Application {
    
    public static void main(String[] args){

        CSVImportAndProcessingRunner runner = new CSVImportAndProcessingRunner()		
        try {
            runner.run(args);
        } catch (FaultyConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
```
After setting the program arguments and VM arguments in the run configuration the application can be run.

**Program Arguments:**

```arg
-fs "camundaExport.csv" -fd ".\workspace" -d ";" -of "csv"
```
**VM Arguments:**

```
-Dspark.master=local[*]
```

The configuration file is now in the folder of your project. The new step can now be integrated into the pipeline as follows:

```json
{      
	"id": "CheckApprovedStep",
	"className":"de.viadee.ki.TestApp.CheckApprovedStep",
	"dependsOn": "TypeCastStep",
	"parameters":{
		"column":  "approved"
	}
},
```
 When executing the pipeline again, this step is applied to the dataset as well. The result can be found [here](/viadee/bpmn.ai/blob/develop/tutorials/spark%20importer/result2.csv).