package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class WriteToCSVStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "result");

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

        return dataset;
    }
}
