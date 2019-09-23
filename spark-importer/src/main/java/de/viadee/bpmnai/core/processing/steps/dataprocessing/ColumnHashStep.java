package de.viadee.bpmnai.core.processing.steps.dataprocessing;

import de.viadee.bpmnai.core.configuration.util.ConfigurationUtils;
import de.viadee.bpmnai.core.util.SparkImporterUtils;
import de.viadee.bpmnai.core.annotation.PreprocessingStepDescription;
import de.viadee.bpmnai.core.configuration.Configuration;
import de.viadee.bpmnai.core.configuration.preprocessing.ColumnHashConfiguration;
import de.viadee.bpmnai.core.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.logging.SparkImporterLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.sha1;

@PreprocessingStepDescription(name = "Hash column", description = "In this step the columns that are configured to be hashed for anonymization are run through a SHA-1 hash operation.")
public class ColumnHashStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, Map<String, Object> parameters, SparkRunnerConfig config) {

        //check if all variables that should be hashed actually exist, otherwise log a warning
        List<String> existingColumns = new ArrayList<>(Arrays.asList(dataSet.columns()));

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(config);
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            if(preprocessingConfiguration != null) {
                for(ColumnHashConfiguration chc : preprocessingConfiguration.getColumnHashConfiguration()) {
                    if(chc.isHashColumn()) {
                        if(!existingColumns.contains(chc.getColumnName())) {
                            // log the fact that a column that should be hashed does not exist
                            SparkImporterLogger.getInstance().writeWarn("The column '" + chc.getColumnName() + "' is configured to be hashed, but does not exist in the data.");
                        } else {
                            dataSet = dataSet.withColumn(chc.getColumnName(), sha1(dataSet.col(chc.getColumnName())));
                            SparkImporterLogger.getInstance().writeInfo("The column '" + chc.getColumnName() + "' is being hashed.");
                        }
                    }

                }
            }
        }

        if(config.isWriteStepResultsIntoFile()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataSet, "column_hash_step", config);
        }

        return dataSet;
    }
}