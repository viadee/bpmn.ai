package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.dataextraction.DataExtractionConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataFilterStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
        if(configuration != null) {
            DataExtractionConfiguration dataExtractionConfiguration = configuration.getDataExtractionConfiguration();
            if(dataExtractionConfiguration != null) {
                String filterQuery = dataExtractionConfiguration.getFilterQuery();
                if(filterQuery != null && !filterQuery.equals("")) {
                    SparkImporterLogger.getInstance().writeInfo("Filtering data with filter query: " + filterQuery + ".");
                    dataset = dataset.filter(filterQuery);

                    if(dataset.count() == 0) {
                        SparkImporterLogger.getInstance().writeInfo("Filtering resulted in zero lines of data. Aborting. Please check your filter query.");
                        System.exit(1);
                    }
                } else {
                    SparkImporterLogger.getInstance().writeWarn("Ignoring empty filter query.");
                }
            }

        }

        return dataset;
    }
}
