package de.viadee.ki.sparkimporter.preprocessing;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreprocessingRunner {

    private final List<PreprocessingStepInterface> preprocessorSteps = new ArrayList<>();

    private static int stepCounter = 0;

    public final static String DATASET_INITIAL = "initial";

    public static final Map<String, Dataset<Row>> helper_datasets = new HashMap<>();

    private static PreprocessingRunner instance;

    private PreprocessingRunner(){}

    public static synchronized PreprocessingRunner getInstance(){
        if(instance == null){
            instance = new PreprocessingRunner();
        }
        return instance;
    }

    public void run(Dataset<Row> dataset, boolean writeStepResultsIntoFile) {
        helper_datasets.clear();
        helper_datasets.put(DATASET_INITIAL, dataset);

        for(PreprocessingStepInterface ps : this.preprocessorSteps) {
            dataset = ps.runPreprocessingStep(dataset, writeStepResultsIntoFile);
        }

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
    }

    public void addPreprocessorStep(PreprocessingStepInterface step) {
        this.preprocessorSteps.add(step);
    }

    public synchronized int getNextCounter() {
        return ++stepCounter;
    }

    public synchronized int getCounter() {
        return stepCounter;
    }
}
