package de.viadee.ki.sparkimporter.util;

import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkImporterUtils {

    private SparkImporterArguments args = SparkImporterArguments.getInstance();

    private static SparkImporterUtils instance;

    private SparkImporterUtils(){}

    public static synchronized SparkImporterUtils getInstance(){
        if(instance == null){
            instance = new SparkImporterUtils();
        }
        return instance;
    }

    public void writeDatasetToCSV(Dataset<Row> dataSet, String subDirectory) {
        writeDatasetToCSV(dataSet, subDirectory, "|");
    }

    private void writeDatasetToCSV(Dataset<Row> dataSet, String subDirectory, String delimiter) {

        boolean aggreateCSVToOneFile = true;

        if(aggreateCSVToOneFile) {
            dataSet = dataSet.coalesce(1);
        }

        //save dataset into CSV file
        dataSet
                .write()
                .option("header", "true")
                .option("delimiter", delimiter)
                .option("ignoreLeadingWhiteSpace", "false")
                .option("ignoreTrailingWhiteSpace", "false")
                .mode(SaveMode.Overwrite)
                .csv(args.getFileDestination()+"/"+ String.format("%02d", PreprocessingRunner.getInstance().getNextCounter()) + "_" + subDirectory);

        if(aggreateCSVToOneFile)
            renameResultFile();
    }

    private void renameResultFile() {
        //rename result file to deterministic name
        SparkImporterArguments args = SparkImporterArguments.getInstance();
        File dir = new File(args.getFileDestination()+"/"+ String.format("%02d", PreprocessingRunner.getInstance().getCounter()) + "_result");
        if(!dir.isDirectory()) throw new IllegalStateException("Cannot find result folder!");
        for(File file : dir.listFiles()) {
            if(file.getName().startsWith("part-0000")) {
                try {
                    Files.copy(file.toPath(), new File(dir + "/../result.csv").toPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Dataset<Row> removeDuplicatedColumns(Dataset<Row> dataset) {
        Dataset<Row> newDataset;
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

    /**
     * removes lines with no process instance id
     * @param dataset dataset to be cleaned
     * @return the cleaned dataset
     */
    public Dataset<Row> removeEmptyLinesAfterImport(Dataset<Row> dataset) {
        return dataset.filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID + " <> 'null'")
                .filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID + " <> ''");
    }
}
