package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.ColumnHashConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.sha1;

public class ColumnHashStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile, String dataLevel) {

        //check if all variables that should be hashed actually exist, otherwise log a warning
        List<String> existingColumns = new ArrayList<>(Arrays.asList(dataSet.columns()));

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
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

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataSet, "column_hash_step");
        }

        return dataSet;
    }
}