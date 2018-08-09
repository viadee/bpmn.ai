package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterCache;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static de.viadee.ki.sparkimporter.util.SparkImporterVariables.*;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class VariablesTypeEscalationStep implements PreprocessingStepInterface {

    private static final Logger LOG = LoggerFactory.getLogger(VariablesTypeEscalationStep.class);


    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        //get all distinct variable names
        Map<String, String> variables = SparkImporterCache.getInstance().getAllCacheValues(SparkImporterCache.CACHE_VARIABLE_NAMES_AND_TYPES);

        Map<String, String> filteredVariables = new HashMap<>();

        String lastVariableName = "";
        String lastVariableType = "";
        int variableOccurences = 0;
        for (String key : variables.keySet()) {
            String variable = key;
            String type = variables.get(key);

            processVariable(variable, type, lastVariableName, lastVariableType, variableOccurences, filteredVariables);


            if (!variable.equals(lastVariableName)) {
                //prepare for next variable
                lastVariableName = variable;
                lastVariableType = type;
                variableOccurences = 1;
            }
        }
        //handle last line
        processVariable("", "", lastVariableName, lastVariableType, variableOccurences, filteredVariables);


        //create new Dataset
        //write column names into list
        List<Row> filteredVariablesRows = new ArrayList<>();
        for (String variableName : filteredVariables.keySet()) {
            filteredVariablesRows.add(RowFactory.create(variableName, filteredVariables.get(variableName)));
        }

        StructType schema = new StructType(new StructField[] {
            new StructField(VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                    DataTypes.StringType, false,
                    Metadata.empty()),
            new StructField(VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                    DataTypes.StringType, false,
                    Metadata.empty())
            });

        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> helpDataSet = sparkSession.createDataFrame(filteredVariablesRows, schema).toDF().orderBy(VAR_PROCESS_INSTANCE_VARIABLE_NAME);

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(helpDataSet, "variable_types_escalated_help");
        }

        //cleanup initial data
        Dataset<Row> newDataSet = dataset.withColumn(VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                when(dataset.col(VAR_PROCESS_INSTANCE_VARIABLE_TYPE).isin("null",""),
                        lit(helpDataSet
                                .select(VAR_PROCESS_INSTANCE_VARIABLE_NAME, VAR_PROCESS_INSTANCE_VARIABLE_TYPE)
                                .filter(VAR_PROCESS_INSTANCE_VARIABLE_NAME+" == "+dataset.col(VAR_PROCESS_INSTANCE_VARIABLE_NAME))
                                .first().getString(1)))
                        //lit(filteredVariables.get(preprocessedDataSet.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME))))
        .otherwise(dataset.col(VAR_PROCESS_INSTANCE_VARIABLE_TYPE)));

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(newDataSet, "variable_types_escalated");
        }

        //returning prepocessed dataset
        return newDataSet;
    }

    private void processVariable(String variable, String type, String lastVariableName, String lastVariableType, int variableOccurences, Map<String, String> filteredVariables) {
        if (variable.equals(lastVariableName)) {
            //multiple types for the same variable detected, escalation needed
            if (lastVariableType.equals("null") || lastVariableName.equals("")) {
                //last one was null or empty, so we can use this one, even is this is also null it does not change anything
                lastVariableType = type;
            } else {
                //check which one to be used --> escalation
                //TODO: currently only done for null and empty strings, should be done for multiple types with a type hierarchy
                if (!type.equals(null) && !type.equals("")) {
                    lastVariableType = type;
                }
            }
        } else {
            //new variable being processed
            //first decide on what to do with last variable and add to filtered list
            if (variableOccurences == 1) {
                //only occurs once so add to list with correct tyoe
                if (lastVariableType.equals("null") || lastVariableName.equals("")) {
                    filteredVariables.put(lastVariableName, "string");
                    SparkImporterCache.getInstance().addValueToCache(SparkImporterCache.CACHE_VARIABLE_NAMES_AND_TYPES, lastVariableName, "string");
                } else {
                    filteredVariables.put(lastVariableName, lastVariableType);
                    SparkImporterCache.getInstance().addValueToCache(SparkImporterCache.CACHE_VARIABLE_NAMES_AND_TYPES, lastVariableName, lastVariableType);
                }
            } else if(variableOccurences > 1) {
                //occurred multiple types
                filteredVariables.put(lastVariableName, lastVariableType);
                SparkImporterCache.getInstance().addValueToCache(SparkImporterCache.CACHE_VARIABLE_NAMES_AND_TYPES, lastVariableName, lastVariableType);
            }
        }
    }
}
