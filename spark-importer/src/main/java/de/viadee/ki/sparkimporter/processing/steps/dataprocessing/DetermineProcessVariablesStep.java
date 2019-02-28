package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableNameMapping;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
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

import static de.viadee.ki.sparkimporter.util.SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME;
import static de.viadee.ki.sparkimporter.util.SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE;
import static org.apache.spark.sql.functions.*;

@PreprocessingStepDescription(name = "Determine process variables", description = "Determines all process variables.")
public class DetermineProcessVariablesStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {

        // FILTER VARIABLES
        dataset = doFilterVariables(dataset, writeStepResultIntoFile);

        // VARIABLE NAME MAPPING
        dataset = doVariableNameMapping(dataset, writeStepResultIntoFile);

        // DETERMINE VARIABLE TYPES
        dataset = doVariableTypeDetermination(dataset, writeStepResultIntoFile);

        // VARIABLE TYPE ESCALATAION
        dataset = doVariableTypeEscalation(dataset);

        //return preprocessed data
        return dataset;
    }

    private Dataset<Row> doFilterVariables(Dataset<Row> dataset, boolean writeStepResultIntoFile) {
        List<String> variablesToFilter = new ArrayList<>();

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            if(preprocessingConfiguration != null) {
                for(VariableConfiguration vc : preprocessingConfiguration.getVariableConfiguration()) {
                    if(!vc.isUseVariable()) {
                        variablesToFilter.add(vc.getVariableName());
                        SparkImporterLogger.getInstance().writeInfo("The variable '" + vc.getVariableName() + "' will be filtered out. Comment: " + vc.getComment());
                    }
                }
            }

        }

        //check if all variables that should be filtered actually exist, otherwise log a warning
        List<Row> existingVariablesRows = dataset.select(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).distinct().collectAsList();
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
                            SparkImporterLogger.getInstance().writeWarn("The variable '" + s + "' is configured to be filtered, but does not exist in the data.");
                        }
                    }
                });

        dataset = dataset.filter((FilterFunction<Row>) row -> {
            // keep the row if the variable name column does not contain a value that should be filtered
            String variable = row.getAs(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);

            //TODO: cleanup
            boolean keep = !variablesToFilter.contains(variable);
            if(variable != null && variable.startsWith("_CORRELATION_ID_")) {
                keep = false;
            }

            return keep;
        });

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "variable_filter");
        }

        return dataset;
    }

    private Dataset<Row> doVariableNameMapping(Dataset<Row> dataset, boolean writeStepResultIntoFile) {
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

    private Dataset<Row> doVariableTypeDetermination(Dataset<Row> dataset, boolean writeStepResultIntoFile) {
        //Determine the process instances with their variable names and types
        Dataset<Row> variablesTypesDataset = dataset.select(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION)
                .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE)
                .agg(max(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION).alias(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
                .filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME+" <> 'null'"); // don't consider null variables'

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
            SparkImporterUtils.getInstance().writeDatasetToCSV(variablesTypesDataset, "variables_types_help");
        }

        return dataset;
    }

    private Dataset<Row> doVariableTypeEscalation(Dataset<Row> dataset) {
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
        if(PreprocessingRunner.initialConfigToBeWritten) {
            Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
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

        SparkImporterLogger.getInstance().writeInfo("Found " + helpDataSet.count() + " process variables.");

        SparkImporterUtils.getInstance().writeDatasetToCSV(helpDataSet, "variable_types_escalated");

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
