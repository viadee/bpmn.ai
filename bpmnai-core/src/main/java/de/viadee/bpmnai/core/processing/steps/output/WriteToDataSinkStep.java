package de.viadee.bpmnai.core.processing.steps.output;

import de.viadee.bpmnai.core.annotation.PreprocessingStepDescription;
import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.BpmnaiVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

@PreprocessingStepDescription(name = "Write to data sink", description = "The resulting dataset is written into a file. It could e.g. also be written to a HDFS filesystem.")
public class WriteToDataSinkStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

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
                .repartition(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID))
                .write()
                .mode(SaveMode.Append)
                .save(config.getTargetFolder());

        return dataset;
    }
    
}
