package de.viadee.ki.sparkimporter.processing.steps.importing;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterCSVArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ColumnsPreparationStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters, SparkRunnerConfig config) {


        List<String> predictionVariables = new ArrayList<>();
        if(config.getPipelineMode().equals(SparkImporterVariables.PIPELINE_MODE_PREDICT)) {
            Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(config);
            List<VariableConfiguration> variableConfigurations = configuration.getPreprocessingConfiguration().getVariableConfiguration();
            for(VariableConfiguration vc : variableConfigurations) {
                predictionVariables.add(vc.getVariableName());
            }
        }

        //rename columns
        for(String columnName : dataset.columns()) {
            if(config.getPipelineMode().equals(SparkImporterVariables.PIPELINE_MODE_LEARN)
                    || !predictionVariables.contains(columnName)) {
                dataset = dataset.withColumnRenamed(columnName, columnName.replaceAll("([A-Z])","_$1").concat("_").toLowerCase());
            }

        }

        dataset = dataset
                .withColumnRenamed("process_instance_id_", "proc_inst_id_")
                .withColumnRenamed("duration_in_millis_", "duration_")
                .withColumnRenamed("variable_name_", "name_")
                .withColumnRenamed("long_value_", "long_")
                .withColumnRenamed("double_value_", "double_")
                .withColumnRenamed("text_value_", "text_")
                .withColumnRenamed("complex_value_", "text2_")
                .withColumnRenamed("serializer_name_", "var_type_")
                .withColumnRenamed("revision_", "rev_")
                .withColumnRenamed("process_definition_key_", "proc_def_key_")
                .withColumnRenamed("process_definition_id_", "proc_def_id_")
                .withColumnRenamed("activity_instance_id_", "act_inst_id_")
                .withColumnRenamed("revision_", "rev_");

        //convert all columns to string in order to be able to select the correct value for variables and to extract json structure in variables
        for(String columnName : dataset.columns()) {
            dataset = dataset.withColumn(columnName, dataset.col(columnName).cast("string").as(columnName));
        }

        // write imported CSV structure to file for debugging
        if (SparkImporterCSVArguments.getInstance().isWriteStepResultsToCSV()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "import_result", config);
        }
        
        return dataset;
    }
}
