package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.*;

public class CreateColumnsFromJsonStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {

        // get variables
        Map<String, String> varMap = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED);

        //convert to String array so it is serializable and can be used in map function
        Set<String> variables = varMap.keySet();
        String[] vars = new String[variables.size()];
        int vc = 0;
        for(String v : variables) {
            vars[vc++] = v;
        }

        String[] columns = dataset.columns();
        StructType schema = dataset.schema();
        StructType newColumnsSchema = new StructType().add("column", DataTypes.StringType);

        //first iteration to collect all columns to be added
        Dataset<Row> newColumnsDataset = dataset.flatMap((FlatMapFunction<Row, Row>) row -> {
            List<Row> newColumns = new ArrayList<>();
            for (String c : columns) {
                if (Arrays.asList(vars).contains(c)) {
                    //it was a variable, so try to parse as json
                    ObjectMapper mapper = new ObjectMapper();
                    JsonFactory factory = mapper.getFactory();
                    JsonParser parser = null;
                    JsonNode jsonParsed = null;
                    try {
                        parser = factory.createParser((String) row.getAs(c));
                        jsonParsed = mapper.readTree(parser);
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
                if(Arrays.asList(vars).contains(c)) {
                    //it was a variable, so try to parse as json
                    ObjectMapper mapper = new ObjectMapper();
                    JsonFactory factory = mapper.getFactory();
                    JsonParser parser = null;
                    JsonNode jsonParsed = null;
                    try {
                        parser = factory.createParser((String)row.getAs(c));
                        jsonParsed = mapper.readTree(parser);
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

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "create_columns_from_json");
        }

        //return preprocessed data
        return dataset;
    }
}
