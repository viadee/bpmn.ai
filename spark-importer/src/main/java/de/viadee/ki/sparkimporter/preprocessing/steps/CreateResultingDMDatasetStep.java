package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.exceptions.WrongCacheValueTypeException;
import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterCache;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateResultingDMDatasetStep implements PreprocessingStepInterface {

    private static final Logger LOG = LoggerFactory.getLogger(CreateResultingDMDatasetStep.class);

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) throws WrongCacheValueTypeException {

        //add variable column names that contains the value to dataset
        Dataset<Row> mapDataset = SparkImporterUtils.getInstance().getVariableTypeValueColumnMappingDataset();
        dataset = dataset.join(mapDataset,SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE);

        //get variables from Ignite cache
        java.util.Map<String, String[]> variables = SparkImporterCache.getInstance().getAllCacheValues(SparkImporterCache.CACHE_VARIABLE_NAMES_AND_TYPES, String[].class);

        for(String key : variables.keySet()) {
            String variableName = key;
            String variableType = variables.get(key)[0];

            dataset = dataset.withColumn(variableName, SparkImporterUtils.getInstance().createVariableValueColumnFromVariableNameAndType(dataset, variableName, variableType));

            if(SparkImporterArguments.getInstance().isRevisionCount()) {
                dataset = dataset.withColumn(variableName+"_rev", SparkImporterUtils.getInstance().createVariableRevColumnFromVariableName(dataset, variableName));
            }
        }

        //cleanup help columns
        dataset = dataset.drop("valueField");

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "dm_dataset");
        }

        //returning prepocessed dataset
        return dataset;
    }
}
