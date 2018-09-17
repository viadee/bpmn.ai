package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableNameMapping;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class VariableNameMappingStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {

        Map<String, String> variableNameMappings = new HashMap<>();

        // getting variable name mappings from configuration
        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            if(preprocessingConfiguration != null) {
                for(VariableNameMapping vm : preprocessingConfiguration.getVariableNameMappings()) {
                    if(!vm.getOldName().equals("") && !vm.getNewName().equals("")) {
                        variableNameMappings.put(vm.getOldName(), vm.getNewName());
                    } else {
                        SparkImporterLogger.getInstance().writeWarn("Ignoring variable name mapping '" + vm.getOldName() + "' -> '" + vm.getNewName() + "'.");
                    }
                }
            }
        }

        // rename all variables
        for(String oldName : variableNameMappings.keySet()) {
            String newName = variableNameMappings.get(oldName);

            SparkImporterLogger.getInstance().writeInfo("Renaming variable '" + oldName + "' to '" + newName + "' as per user configuration.");

            dataset = dataset.withColumn(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                    when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(oldName), lit(newName))
                            .otherwise(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)));
        }

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "variable_name_mapping");
        }

        return dataset;
    }
}
