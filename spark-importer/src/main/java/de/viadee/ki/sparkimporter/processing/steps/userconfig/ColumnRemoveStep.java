package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.ColumnConfiguration;
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
import java.util.function.Consumer;

public class ColumnRemoveStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile) {

        List<String> columnsToRemove = new ArrayList<>();

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            if(preprocessingConfiguration != null) {
                for(ColumnConfiguration cc : preprocessingConfiguration.getColumnConfiguration()) {
                    if(!cc.isUseColumn()) {
                        columnsToRemove.add(cc.getColumnName());
                        SparkImporterLogger.getInstance().writeWarn("The column '" + cc.getColumnName() + "' will be removed. Comment: " + cc.getComment());
                    }
                }
            }
        }

        //check if all variables that should be filtered actually exist, otherwise log a warning
        List<String> existingColumns = new ArrayList<>(Arrays.asList(dataSet.columns()));

        columnsToRemove
                .stream()
                .forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        if(!existingColumns.contains(s)) {
                            // log the fact that a variable that should be filtered does not exist
                            SparkImporterLogger.getInstance().writeWarn("The column '" + s + "' is configured to be filtered, but does not exist in the data.");
                        }
                    }
                });

        dataSet = dataSet.drop(SparkImporterUtils.getInstance().asSeq(columnsToRemove));

        return dataSet;
    }
}