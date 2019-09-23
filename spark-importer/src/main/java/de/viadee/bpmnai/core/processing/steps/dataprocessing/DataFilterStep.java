package de.viadee.bpmnai.core.processing.steps.dataprocessing;

import de.viadee.bpmnai.core.annotation.PreprocessingStepDescription;
import de.viadee.bpmnai.core.annotation.PreprocessingStepParameter;
import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.logging.SparkImporterLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

@PreprocessingStepDescription(name = "Data filter", description = "If the configuration contains a filter query (e.g. by limiting the processing only to one process definition id, it is applied in this step to reduce the input data.")
@PreprocessingStepParameter(name = "query", description = "The Apache Spark filter query to execute in this step", required = false, dataType = PreprocessingStepParameter.DATA_TYPE.STRING)
public class DataFilterStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

    	if (parameters == null || parameters.size() == 0) {
            SparkImporterLogger.getInstance().writeWarn("No parameters found for the DataFilterStep");
            return dataset;
        }
    	
    	String query = (String) parameters.get("query");               
        SparkImporterLogger.getInstance().writeInfo("Filtering data with filter query: " + query + ".");
        dataset = dataset.filter(query);

        dataset.cache();
        if(dataset.count() == 0) {
            SparkImporterLogger.getInstance().writeInfo("Filtering resulted in zero lines of data. Aborting. Please check your filter query.");
            System.exit(1);
        }
               
        return dataset;
    }  
}
