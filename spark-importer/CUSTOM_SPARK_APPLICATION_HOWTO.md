# How to write a project specific Spark pipeline application

This document desribes how one can write a custom / project specific spark application that is build on top of the generic spark application. In the custom aplication new steps can be created and added into the overall processing pipeline.

## Create a new Java project

First, a new Maven project must be created in the provided workspace. The Group Id and Version has to be set as follows:

`Group Id: de.viadee.ki`

`Version: 1.0.0-SNAPSHOT`

whereas the Artifact ID can be chosen arbitrarily.

## Set project dependencies

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

## Create project specific steps

Optionally, additional preprocessing steps can now be added that refer specifically to the use case.

The steps must be created as a Java file as follows:


```java
public class AddGeodataStep implements PreprocessingStepInterface {

    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean 		writeStepResultIntoFile, String dataLevel, Map<String,Object> parameters) {	

        //TODO - step has to be implemented here
        
    	return dataset;
	}
}
```

## Run application

To run the application in a user adapted way, the configuration file must first be created and edited.

In the main method of the application, a runner is created as shown in the following example for the KafkaProcessingRunner. Depending on the application, a CSVImportAndProcessingRunner or a KafkaImportRunner can also be created. 

```java
public class Application {
    
    public static void main(String[] args){

        KafkaProcessingRunner kafkaProcessingRunner = new KafkaProcessingRunner();
        try {
            kafkaProcessingRunner.run(args);
        } catch (FaultyConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
```

The application can now be started. Therefore the following program arguments and VM arguments should be specified via the run configuration. These correspond to the parameters from the Spark Importer. Details can be found [here](./README_details.md).


The first time you run the application, a configuration file called *"pipeline_configuration.json"* is created which is located in the selected workspace folder. This json file can be used to determine which preprocessing steps from the Spark Importer will be executed and in which order. In addition, the previously created project-specific steps can be integrated in the preprocessing. The corresponding parameters can also be defined.

In the following, a project-specific step is shown as an example. The variable "dependsOn" determines the predecessor of the step and the parameter "parameters" requires to pass the function specific parameters.

```xml
{
    "id": "MatchBrandsStep",
    "className": "de.viadee.ki.sparkprovi.MatchBrandsStep",
    "dependsOn": "AddGeodataStep",
    "parameters":{
        "brandlist_csv": "../brands.csv",
        "regexp_csv": "../regexp.csv",
        "column":  ["int_fahrzeugHerstellernameAusVertrag"]
    }
}
```
Finally, the application can be executed again with the new configuration.