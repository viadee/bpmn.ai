package de.viadee.ki.sparkimporter.util;

import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkImporterUtils {

    private static String workingDirectory = ".";

    private static SparkImporterUtils instance;

    private SparkImporterUtils(){}

    public static synchronized SparkImporterUtils getInstance(){
        if(instance == null){
            instance = new SparkImporterUtils();
        }
        return instance;
    }

    public static String getWorkingDirectory() {
        return workingDirectory;
    }

    public static void setWorkingDirectory(String workingDirectory) {
        SparkImporterUtils.workingDirectory = workingDirectory;
    }

    public String md5CecksumOfObject(Object obj) throws IOException, NoSuchAlgorithmException {
        if (obj == null) {
            return "";
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();

        MessageDigest m = MessageDigest.getInstance("MD5");
        m.update(baos.toByteArray());

        Base64 codec = new Base64();
        byte[] encoded = codec.encode(m.digest());

        return DigestUtils.md5Hex(new String(encoded)).toUpperCase();
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
                .csv(SparkImporterVariables.getTargetFolder()+"/"+ String.format("%02d", PreprocessingRunner.getNextCounter()) + "_" + subDirectory);

        if(aggreateCSVToOneFile && subDirectory.equals("result"))
            moveResultFile();
    }

    private void moveResultFile() {
        //rename result file to deterministic name
        String targetFolder = SparkImporterVariables.getTargetFolder()+"/"+ String.format("%02d", PreprocessingRunner.getCounter()) + "_result";
        if(targetFolder.startsWith("hdfs")) {
            // data stored in Hadoop
            Path path = new Path(targetFolder);
            Configuration conf = new Configuration();
            FileSystem fileSystem = null;
            try {
                fileSystem = FileSystem.get(conf);
                if(!fileSystem.isDirectory(path)) throw new IllegalStateException("Cannot find result folder!");
                RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, false);
                while(files.hasNext()) {
                    LocatedFileStatus file = files.next();
                    if(file.isFile() && file.getPath().getName().contains("part-0000")) {
                        FileUtil.copy(fileSystem, file.getPath(), fileSystem, new Path(targetFolder + "/../result.csv"), false, conf);
                    }
                }
            } catch (IOException e) {
                SparkImporterLogger.getInstance().writeError("An error occurred during the renaming of the result file in HDFS. Exception: " + e.getMessage());
            }
        } else {
            File dir = new File(targetFolder);
            if(!dir.isDirectory()) throw new IllegalStateException("Cannot find result folder!");
            for(File file : dir.listFiles()) {
                if(file.getName().startsWith("part-0000")) {
                    try {
                        Files.move(file.toPath(), new File(dir + "/../result.csv").toPath());
                    } catch (IOException e) {
                        SparkImporterLogger.getInstance().writeError("An error occurred during the renaming of the result file. Exception: " + e.getMessage());
                    }
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

    /**
     * implemented help method as per https://stackoverflow.com/questions/40741459/scala-collection-seq-doesnt-work-on-java
     * @param values List to be convert to Scala Seq
     * @param <T> Type of objects to be converted
     * @return the Scala Seq
     */
    public <T> Seq<T> asSeq(List<T> values) {
        return JavaConversions.asScalaBuffer(values);
    }
}
