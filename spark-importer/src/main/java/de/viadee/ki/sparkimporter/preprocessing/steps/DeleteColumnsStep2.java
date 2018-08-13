package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class DeleteColumnsStep2 implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile) {
        String fileName = "Columns.txt";

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {

            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("#")) {
                    String arr[] = line.split(" ", 2);
                    String column = arr[0];
                    String reason = "no reason given";
                    try {
                        reason = arr[1];
                    } catch (Exception e) {
                    }
                    if (Arrays.asList(dataSet.columns()).contains(column)) {
                        dataSet = dataSet.drop(column);
                        System.out.println("The column" + column + " was deleted. The reason is: " + reason);
                    } else {
                        System.out.println("The column " + column + " does not exist!");
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataSet;
    }
}
