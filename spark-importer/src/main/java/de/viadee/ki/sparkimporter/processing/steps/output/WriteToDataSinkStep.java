package de.viadee.ki.sparkimporter.processing.steps.output;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterKafkaImportArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

public class WriteToDataSinkStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {

        /*
        TODO: Not working yet
    	// if output format is set to "csv" create both: csv and parquet 
    	if(SparkImporterKafkaImportArguments.getInstance().getOutputFormat().equals(SparkImporterVariables.OUTPUT_FORMAT_CSV)) {
    		dataset
            .write()
            .option("header", "true")
            .option("delimiter", ";")
            .option("ignoreLeadingWhiteSpace", "false")
            .option("ignoreTrailingWhiteSpace", "false")
            .mode(SparkImporterVariables.getSaveMode())
            .csv(SparkImporterVariables.getTargetFolder());
    	}
    	*/
  
    	dataset
                //we repartition the data by process instances, which allows spark to better distribute the data between workers as the operations are related to a process instance
                .repartition(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID))
                .write()
                .mode(SaveMode.Append)
                .save(SparkImporterVariables.getTargetFolder());

        return dataset;
    }
    
}
