package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.*;

import static de.viadee.ki.sparkimporter.util.SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME;
import static de.viadee.ki.sparkimporter.util.SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE;

@PreprocessingStepDescription(value = "In this step each variable column is checked if it contains a json and if so, the first level of attributes is transformed into separate columns. No object or array parameters are converted.")
public class CreateColumnsFromJsonStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameterss) {

        // CREATE COLUMNS FROM DATASET
        dataset = doCreateColumnsFromJson(dataset, writeStepResultIntoFile);

        // FILTER JSON VARIABLES
        dataset = doFilterJsonVariables(dataset);

        //return preprocessed data
        return dataset;
    }

    private Dataset<Row> doCreateColumnsFromJson(Dataset<Row> dataset, boolean writeStepResultIntoFile) {
        // get variables
        Map<String, String> varMap = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED);

        //if broadcast variable is not there then we create an empty map. This resolves the dependency from DetermineProcessVariablesStep
        if(varMap == null) {
            varMap = new HashMap<>();
        }

        String[] vars = null;
        if(SparkImporterVariables.getPipelineMode().equals(SparkImporterVariables.PIPELINE_MODE_LEARN)) {
            //convert to String array so it is serializable and can be used in map function
            Set<String> variables = varMap.keySet();
            vars = new String[variables.size()];
            int vc = 0;
            for(String v : variables) {
                vars[vc++] = v;
            }
        }

        final String[] finalVars = vars;

        String[] columns = dataset.columns();
        StructType schema = dataset.schema();
        StructType newColumnsSchema = new StructType().add("column", DataTypes.StringType);

        //first iteration to collect all columns to be added
        Dataset<Row> newColumnsDataset = dataset.flatMap((FlatMapFunction<Row, Row>) row -> {
            List<Row> newColumns = new ArrayList<>();
            for (String c : columns) {
                if (SparkImporterVariables.getPipelineMode().equals(SparkImporterVariables.PIPELINE_MODE_PREDICT) || Arrays.asList(finalVars).contains(c)) {
                    //it was a variable, so try to parse as json
                    ObjectMapper mapper = new ObjectMapper();
                    JsonFactory factory = mapper.getFactory();
                    JsonParser parser = null;
                    JsonNode jsonParsed = null;
                    try {
                        String varColumn = row.getAs(c);
                        if(varColumn != null) {
                            parser = factory.createParser(varColumn);
                            jsonParsed = mapper.readTree(parser);
                        }
                    } catch (IOException e) {
                        // do nothing as we check if the result is null later
                    }

                    //check if parsing was successful and if so, handle it
                    if (jsonParsed != null && jsonParsed.fieldNames().hasNext()) {
                        //it is a json with fields
                        Iterator<String> fieldNames = jsonParsed.fieldNames();
                        while (fieldNames.hasNext()) {
                            String fieldName = fieldNames.next();
                            JsonNode value = jsonParsed.get(fieldName);
                            //handle only first level and no object or array elements
                            if (!value.isObject() && !value.isArray()) {
                                String columnName = c + "_" + fieldName;
                                newColumns.add(RowFactory.create(columnName));
                            }
                        }
                    }
                }
            }
            return newColumns.iterator();
        }, RowEncoder.apply(newColumnsSchema));

        //get distinct names of new columns
        newColumnsDataset = newColumnsDataset.select(newColumnsDataset.col("column")).distinct();

        List<Row> newColumnsAsRow = Arrays.asList((Row[]) newColumnsDataset.select(newColumnsDataset.col("column")).collect());
        List<String> newColumns = new ArrayList<>();
        StructType newSchema = schema;

        //create new schema for resulting dataset
        for(Row newColumnRow : newColumnsAsRow) {
            newColumns.add(newColumnRow.getString(0));
            newSchema = newSchema.add(newColumnRow.getString(0), DataTypes.StringType);
        }
        final StructType newSchema1 = newSchema;

        //iterate through dataset and add all columns determined in step before
        dataset = dataset.map(row -> {
            List<String> newRowStrings = new ArrayList<>();
            Map<String, String> newColumnValues = new HashMap<>();

            for(String c : columns) {
                String columnValue = null;
                if (SparkImporterVariables.getPipelineMode().equals(SparkImporterVariables.PIPELINE_MODE_PREDICT) || Arrays.asList(finalVars).contains(c)) {
                    //it was a variable, so try to parse as json
                    ObjectMapper mapper = new ObjectMapper();
                    JsonFactory factory = mapper.getFactory();
                    JsonParser parser = null;
                    JsonNode jsonParsed = null;
                    try {
                        if(row.getAs(c) != null) {
                            parser = factory.createParser((String)row.getAs(c));
                            jsonParsed = mapper.readTree(parser);
                        }
                    } catch (IOException e) {
                        //do nothing
                    }

                    // remember initial value
                    columnValue = row.getAs(c);
                    newColumnValues.put(c, columnValue);

                    if(jsonParsed != null && jsonParsed.fieldNames().hasNext()) {

                        //also remember initial value
                        columnValue = row.getAs(c);
                        newColumnValues.put(c, columnValue);

                        //it is a json with fields
                        Iterator<String> fieldNames = jsonParsed.fieldNames();
                        while(fieldNames.hasNext()) {
                            String fieldName = fieldNames.next();
                            JsonNode value = jsonParsed.get(fieldName);
                            if(!value.isObject() && !value.isArray()) {
                                //handle only first level and no object or array elements
                                String columnName = c + "_" + fieldName;
                                if(newColumns.contains(columnName)) {
                                    newColumnValues.put(columnName, value.asText());
                                } else {
                                    //should not happen found column not detected in step before
                                    SparkImporterLogger.getInstance().writeError("Found column in json not found in step before: "+columnName);
                                }
                            }
                        }
                    }
                } else {
                    //it was a column, just use the value as it is
                    columnValue = row.getAs(c);
                    newColumnValues.put(c, columnValue);
                }
            }

            for(String f : newSchema1.fieldNames()) {
                newRowStrings.add(newColumnValues.get(f));
            }

            return RowFactory.create(newRowStrings.toArray());
        }, RowEncoder.apply(newSchema1));


        if (SparkImporterVariables.getPipelineMode().equals(SparkImporterVariables.PIPELINE_MODE_LEARN)) {
            //create new Dataset
            //write column names into list
            List<Row> filteredVariablesRows = new ArrayList<>();

            for(String name : newColumns) {
                //these are always string in the beginning
                String type = "string";

                // add new column to variables list for later processing
                filteredVariablesRows.add(RowFactory.create(name, type));

                // add new variables to configuration
                if(PreprocessingRunner.initialConfigToBeWritten) {
                    Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
                    VariableConfiguration variableConfiguration = new VariableConfiguration();
                    variableConfiguration.setVariableName(name);
                    variableConfiguration.setVariableType(type);
                    variableConfiguration.setUseVariable(true);
                    variableConfiguration.setComment("");
                    configuration.getPreprocessingConfiguration().getVariableConfiguration().add(variableConfiguration);
                }
            }

            StructType schemaVars = new StructType(new StructField[] {
                    new StructField(VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                            DataTypes.StringType, false,
                            Metadata.empty()),
                    new StructField(VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                            DataTypes.StringType, false,
                            Metadata.empty())
            });

            SparkImporterLogger.getInstance().writeInfo("Found " + newColumns.size() + " additional process variables during Json processing.");

            SparkSession sparkSession = SparkSession.builder().getOrCreate();
            Dataset<Row> helpDataSet = sparkSession.createDataFrame(filteredVariablesRows, schemaVars).toDF().orderBy(VAR_PROCESS_INSTANCE_VARIABLE_NAME);
            SparkImporterUtils.getInstance().writeDatasetToCSV(helpDataSet, "variable_types_after_json_escalated");

            if(writeStepResultIntoFile) {
                SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "create_columns_from_json");
            }
        }

        return dataset;
    }

    private Dataset<Row> doFilterJsonVariables(Dataset<Row> dataset) {
        //read all variables to filter again. They contain also variables that resulted from Json parsing and are not columns, so they can just be dropped
        List<String> variablesToFilter = new ArrayList<>();

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            if(preprocessingConfiguration != null) {
                for(VariableConfiguration vc : preprocessingConfiguration.getVariableConfiguration()) {
                    if(!vc.isUseVariable()) {
                        variablesToFilter.add(vc.getVariableName());

                        if(Arrays.asList(dataset.columns()).contains(vc.getVariableName())) {
                            SparkImporterLogger.getInstance().writeInfo("The variable '" + vc.getVariableName() + "' will be filtered out after json processing. Comment: " + vc.getComment());
                        }
                    }
                }
            }
        }

        dataset = dataset.drop(SparkImporterUtils.getInstance().asSeq(variablesToFilter));

        return dataset;
    }
}
