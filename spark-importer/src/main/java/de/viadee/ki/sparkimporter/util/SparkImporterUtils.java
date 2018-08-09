package de.viadee.ki.sparkimporter.util;

import de.viadee.ki.sparkimporter.preprocessing.PreprocessingRunner;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.when;

public class SparkImporterUtils {

    private SparkImporterArguments args = SparkImporterArguments.getInstance();

    private Dataset<Row> variableTypeValueColumnMappingDataset = null;

    private static SparkImporterUtils instance;

    private SparkImporterUtils(){}

    public static synchronized SparkImporterUtils getInstance(){
        if(instance == null){
            instance = new SparkImporterUtils();
        }
        return instance;
    }

    public void writeDatasetToCSV(Dataset<Row> dataSet, String subDirectory) {
        //save dataset into CSV file
        dataSet.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save(args.getFileDestination()+"/"+ String.format("%02d", PreprocessingRunner.getInstance().getNextCounter()) + "_" + subDirectory);
    }

    public Dataset<Row> removeDuplicatedColumnsFromCSV(Dataset<Row> dataset) {
        Dataset<Row> newDataset = null;
        //remove duplicated columns
        //find duplicated columns and their first name under which they occurred
        String[] columns = dataset.columns();
        Map<String, Column> uniqueColumnNameMapping = new HashMap<>();

        Pattern p = Pattern.compile("(\\w+_)\\d*");
        for(String col : columns) {
            Matcher m = p.matcher(col);
            if(m.matches()) {
                if(!uniqueColumnNameMapping.keySet().contains(m.group(1))) {
                    uniqueColumnNameMapping.put(m.group(1), new Column(col));
                }
            }
        }

        Seq<Column> selectionColumns =  JavaConverters.asScalaIteratorConverter(uniqueColumnNameMapping.values().iterator()).asScala().toSeq();

        //create new dataset if necessary
        if(columns.length != uniqueColumnNameMapping.size()) {

            newDataset = dataset.select(selectionColumns).toDF();

            //rename columns
            Map<String, String> swappedUniqueColumnNameMapping = new HashMap<>();
            for(String key : uniqueColumnNameMapping.keySet()) {
                swappedUniqueColumnNameMapping.put(uniqueColumnNameMapping.get(key).toString(), key);
            }

            for(String column : newDataset.columns()) {
                newDataset = newDataset.withColumnRenamed(column, swappedUniqueColumnNameMapping.get(column));
            }

            return newDataset;
        } else {
            return  dataset;
        }
    }

    public Column createVariableValueColumnFromVariableNameAndType(Dataset<Row> sourceDataSet, String variableName, String variableType) {
        createVariableTypeValueColumnMappingDatasetIfNecessary();

        Column col = when(sourceDataSet.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(variableName),
//                            sourceDataSet.col("text_"))
                            when(sourceDataSet.col("valueField").equalTo("long_"), sourceDataSet.col("long_"))
                            .when(sourceDataSet.col("valueField").equalTo("text_"), sourceDataSet.col("text_"))
                            .when(sourceDataSet.col("valueField").equalTo("text_2"), sourceDataSet.col("text2_")))
                    .otherwise("");
        return col;
    }

    public Column createVariableRevColumnFromVariableName(Dataset<Row> sourceDataSet, String variableName) {
        createVariableTypeValueColumnMappingDatasetIfNecessary();

        Column col = when(sourceDataSet.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(variableName),
                sourceDataSet.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
                .otherwise("0");
        return col;
    }

    private void createVariableTypeValueColumnMappingDatasetIfNecessary() {
        if(variableTypeValueColumnMappingDataset == null) {
            SparkSession sparkSession = SparkSession.builder()
                    .getOrCreate();

            Map<String, String> mappings = new HashMap<>();
            mappings.put("boolean", "long_");
            mappings.put("long", "long_");
            mappings.put("string", "text_");
            mappings.put("serializable", "text2_");

            //create schema
            StructType schema = new StructType(new StructField[] {
                    new StructField("var_type_",
                            DataTypes.StringType, false,
                            Metadata.empty()),
                    new StructField("valueField",
                            DataTypes.StringType, false,
                            Metadata.empty())
            });

            List<Row> data = new ArrayList<>();
            for(String key : mappings.keySet()) {
                data.add(RowFactory.create(new String[]{key, mappings.get(key)}));
            }

            variableTypeValueColumnMappingDataset = sparkSession.createDataFrame(data, schema).toDF();

            variableTypeValueColumnMappingDataset.show();
        }
    }

    public Dataset<Row> getVariableTypeValueColumnMappingDataset() {
        createVariableTypeValueColumnMappingDatasetIfNecessary();
        return variableTypeValueColumnMappingDataset;
    }
}
