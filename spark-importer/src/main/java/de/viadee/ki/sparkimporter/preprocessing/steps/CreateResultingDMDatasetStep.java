package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.SparkImporterApplication;
import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterCache;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CreateResultingDMDatasetStep implements PreprocessingStepInterface {

    private static final Logger LOG = LoggerFactory.getLogger(CreateResultingDMDatasetStep.class);

    //default columns that should always be there
    private final String[] DEFAULT_COLUMNS = new String []{
            "id_",
            "proc_inst_id_",
            "proc_def_key_",
            "proc_def_id_",
            "start_time_",
            "end_time_",
            "duration_",
            "start_act_id_",
            "end_act_id_",
            "state_",
            "name_",
            "var_type_",
            "rev_",
            "bytearray_id_",
            "double_",
            "long_"
    };

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        //create List for new column names
        List<StructField> columnsOfMlDataset = new ArrayList<>();

        //add variable column names that contains the value to dataset
        Dataset<Row> mapDataset = SparkImporterUtils.getInstance().getVariableTypeValueColumnMappingDataset();
        dataset = dataset.join(mapDataset,SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE);

        //get variables from Ignite cache
        java.util.Map<String, String> variables = SparkImporterCache.getInstance().getAllCacheValues(SparkImporterCache.CACHE_VARIABLE_NAMES_AND_TYPES);

        for(String key : variables.keySet()) {
            String variableName = key;
            String variableType = variables.get(key);

            dataset = dataset.withColumn(variableName, SparkImporterUtils.getInstance().createVariableValueColumnFromVariableNameAndType(dataset, variableName, variableType));

            if(SparkImporterArguments.getInstance().isRevisionCount()) {
                //TODO: add counter to show real count
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
