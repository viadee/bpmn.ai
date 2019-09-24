package de.viadee.bpmnai.core.processing.steps.dataprocessing;

import de.viadee.bpmnai.core.util.BpmnaiUtils;
import de.viadee.bpmnai.core.annotation.PreprocessingStepDescription;
import de.viadee.bpmnai.core.configuration.Configuration;
import de.viadee.bpmnai.core.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.bpmnai.core.configuration.preprocessing.VariableConfiguration;
import de.viadee.bpmnai.core.configuration.preprocessing.VariableNameMapping;
import de.viadee.bpmnai.core.configuration.util.ConfigurationUtils;
import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.BpmnaiVariables;
import de.viadee.bpmnai.core.util.helper.SparkBroadcastHelper;
import de.viadee.bpmnai.core.util.logging.BpmnaiLogger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static de.viadee.bpmnai.core.util.BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME;
import static de.viadee.bpmnai.core.util.BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE;
import static org.apache.spark.sql.functions.*;

@PreprocessingStepDescription(name = "Determine process variables", description = "Determines all process variables.")
public class DetermineProcessVariablesStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

        // FILTER VARIABLES
        dataset = doFilterVariables(dataset, config.isWriteStepResultsIntoFile(), config);

        // VARIABLE NAME MAPPING
        dataset = doVariableNameMapping(dataset, config.isWriteStepResultsIntoFile(), config);

        // DETERMINE VARIABLE TYPES
        dataset = doVariableTypeDetermination(dataset, config.isWriteStepResultsIntoFile(), config);

        // VARIABLE TYPE ESCALATAION
        dataset = doVariableTypeEscalation(dataset, config);

        //return preprocessed data
        return dataset;
    }

    private Dataset<Row> doFilterVariables(Dataset<Row> dataset, boolean writeStepResultIntoFile, SparkRunnerConfig config) {
        List<String> variablesToFilter = new ArrayList<>();

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(config);
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            if(preprocessingConfiguration != null) {
                for(VariableConfiguration vc : preprocessingConfiguration.getVariableConfiguration()) {
                    if(!vc.isUseVariable()) {
                        variablesToFilter.add(vc.getVariableName());
                        BpmnaiLogger.getInstance().writeInfo("The variable '" + vc.getVariableName() + "' will be filtered out. Comment: " + vc.getComment());
                    }
                }
            }

        }

        //check if all variables that should be filtered actually exist, otherwise log a warning
        List<Row> existingVariablesRows = dataset.select(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).distinct().collectAsList();
        List<String> existingVariables = existingVariablesRows
                .stream()
                .map(r -> r.getString(0)).collect(Collectors.toList());

        variablesToFilter
                .stream()
                .forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        if(!existingVariables.contains(s)) {
                            // log the fact that a variable that should be filtered does not exist
                            BpmnaiLogger.getInstance().writeWarn("The variable '" + s + "' is configured to be filtered, but does not exist in the data.");
                        }
                    }
                });

        dataset = dataset.filter((FilterFunction<Row>) row -> {
            // keep the row if the variable name column does not contain a value that should be filtered
            String variable = row.getAs(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);

            //TODO: cleanup
            boolean keep = !variablesToFilter.contains(variable);
            if(variable != null && variable.startsWith("_CORRELATION_ID_")) {
                keep = false;
            }

            return keep;
        });

        if(writeStepResultIntoFile) {
            BpmnaiUtils.getInstance().writeDatasetToCSV(dataset, "variable_filter", config);
        }

        return dataset;
    }

    private Dataset<Row> doVariableNameMapping(Dataset<Row> dataset, boolean writeStepResultIntoFile, SparkRunnerConfig config) {
        Map<String, String> variableNameMappings = new HashMap<>();

        // getting variable name mappings from configuration
        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(config);
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            if(preprocessingConfiguration != null) {
                for(VariableNameMapping vm : preprocessingConfiguration.getVariableNameMappings()) {
                    if(!vm.getOldName().equals("") && !vm.getNewName().equals("")) {
                        variableNameMappings.put(vm.getOldName(), vm.getNewName());
                    } else {
                        BpmnaiLogger.getInstance().writeWarn("Ignoring variable name mapping '" + vm.getOldName() + "' -> '" + vm.getNewName() + "'.");
                    }
                }
            }
        }

        // rename all variables
        for(String oldName : variableNameMappings.keySet()) {
            String newName = variableNameMappings.get(oldName);

            BpmnaiLogger.getInstance().writeInfo("Renaming variable '" + oldName + "' to '" + newName + "' as per user configuration.");

            dataset = dataset.withColumn(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                    when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(oldName), lit(newName))
                            .otherwise(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)));
        }

        if(writeStepResultIntoFile) {
            BpmnaiUtils.getInstance().writeDatasetToCSV(dataset, "variable_name_mapping", config);
        }

        return dataset;
    }

    private Dataset<Row> doVariableTypeDetermination(Dataset<Row> dataset, boolean writeStepResultIntoFile, SparkRunnerConfig config) {
        //Determine the process instances with their variable names and types
        Dataset<Row> variablesTypesDataset = dataset.select(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME, BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE, BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION)
                .groupBy(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME, BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE)
                .agg(max(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION).alias(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
                .filter(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME+" <> 'null'"); // don't consider null variables'

        //create broadcast variable for variables list
        Map<String, String> variablesAndTypes = new HashMap<>();
        Iterator<Row> it = variablesTypesDataset.toLocalIterator();
        while(it.hasNext()) {
            Row row = it.next();
            String name = row.getString(0);
            String type = row.getString(1);
            if(type == null)
                type = "string";
            variablesAndTypes.put(name, type);
        }

        //broadcast variable in Spark
        SparkBroadcastHelper.getInstance().broadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_RAW, variablesAndTypes);


        if(writeStepResultIntoFile) {
            BpmnaiUtils.getInstance().writeDatasetToCSV(variablesTypesDataset, "variables_types_help", config);
        }

        return dataset;
    }

    private Dataset<Row> doVariableTypeEscalation(Dataset<Row> dataset, SparkRunnerConfig config) {
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

        //if there is no configuration file yet, write variables into the empty one
        if(config.isInitialConfigToBeWritten()) {
            Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(config);
            for(String name : variables.keySet()) {
                String type = variables.get(name);
                VariableConfiguration variableConfiguration = new VariableConfiguration();
                variableConfiguration.setVariableName(name);
                variableConfiguration.setVariableType(type);
                variableConfiguration.setUseVariable(true);
                variableConfiguration.setComment("");
                configuration.getPreprocessingConfiguration().getVariableConfiguration().add(variableConfiguration);
            }

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

        dataset.cache();
        helpDataSet.cache();
        BpmnaiLogger.getInstance().writeInfo("Found " + helpDataSet.count() + " process variables.");

        if(config.isWriteStepResultsIntoFile()) {
            BpmnaiUtils.getInstance().writeDatasetToCSV(helpDataSet, "variable_types_escalated", config);
        }

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
