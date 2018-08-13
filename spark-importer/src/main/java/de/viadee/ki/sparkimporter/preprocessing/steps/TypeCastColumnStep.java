package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class TypeCastColumnStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile) {
        String fileName = "/resources/TypeCast.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {

            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("#")) {
                    String arr[] = line.split(" ", 2);
                    String column= arr[0];
                    String type=arr[1];
                    if (Arrays.asList(dataSet.columns()).contains(column)) {
                        dataSet.withColumn("Tmp", dataSet.col(column).cast(type)).drop(column).withColumnRenamed("Tmp", column);
                    } else {
                        System.out.println("The column "+column+ " does not exist!");
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataSet;
    }
}
