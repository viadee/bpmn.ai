package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.configuration.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static de.viadee.ki.sparkimporter.util.SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME;
import static de.viadee.ki.sparkimporter.util.SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE;

public class VariablesTypeEscalationStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        //get all distinct variable names
        Map<String, String> variables = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_RAW);

        String lastVariableName = "";
        String lastVariableType = "";
        int lastVariableMaxRevision = 0;
        int variableOccurences = 0;
        for (String variable : variables.keySet()) {
            String type = variables.get(variable);
            int revision = 0;

            processVariable(variables, variable, type, revision, lastVariableName, lastVariableType, lastVariableMaxRevision, variableOccurences);


            if (!variable.equals(lastVariableName)) {
                //prepare for next variable
                lastVariableName = variable;
                lastVariableType = type;
                lastVariableMaxRevision = revision;
                variableOccurences = 1;
            }
        }
        //handle last line
        processVariable(variables, "", "",0, lastVariableName, lastVariableType, lastVariableMaxRevision, variableOccurences);

        //update broadcasted variable
        SparkBroadcastHelper.getInstance().broadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED, variables);

        //create new Dataset
        //write column names into list
        List<Row> filteredVariablesRows = new ArrayList<>();

        for (String key : variables.keySet()) {
            filteredVariablesRows.add(RowFactory.create(key, variables.get(key)));
        }

        //if there is no configuration file yet, write one
        if(ConfigurationUtils.getInstance().getConfiguration() == null) {
            ConfigurationUtils.getInstance().writeConfiguration(variables);
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

        //returning prepocessed dataset
        return dataset;
    }

    private void processVariable(Map<String, String> variables, String variableName, String variableType, int revision, String lastVariableName, String lastVariableType, int lastVariableMaxRevision, int variableOccurences) {
        if (variableName.equals(lastVariableName)) {
            variableOccurences++;

            //multiple types for the same variableName detected, escalation needed
            if (lastVariableType.equals("null") || lastVariableType.equals("")) {
                //last one was null or empty, so we can use this one, even is this is also null it does not change anything
                lastVariableType = variableType;
            } else {
                //check which one to be used --> escalation
                //TODO: currently only done for null and empty strings, should be done for multiple types with a variableType hierarchy
                if (!variableType.equals("null") && !variableType.equals("")) {
                    lastVariableType = variableType;
                }
            }

            //keep max revision
            lastVariableMaxRevision = Math.max(revision, lastVariableMaxRevision);

        } else {
            //new variableName being processed
            //first decide on what to do with last variableName and add to filtered list
            if (variableOccurences == 1) {
                //only occurs once so add to list with correct tyoe
                if (lastVariableType.equals("null") || lastVariableType.equals("")) {
                    variables.put(lastVariableName, "string");
                } else {
                    variables.put(lastVariableName, lastVariableType);
                }
            } else if(variableOccurences > 1) {
                //occurred multiple types
                variables.put(lastVariableName, lastVariableType);
            }
        }
    }
}
