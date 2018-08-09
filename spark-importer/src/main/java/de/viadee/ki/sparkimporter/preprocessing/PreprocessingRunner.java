package de.viadee.ki.sparkimporter.preprocessing;

import de.viadee.ki.sparkimporter.exceptions.WrongCacheValueTypeException;
import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class PreprocessingRunner {

    private List<PreprocessingStepInterface> preprocessorSteps = new ArrayList<>();

    private static int stepCounter = 0;

    private static PreprocessingRunner instance;

    private PreprocessingRunner(){}

    public static synchronized PreprocessingRunner getInstance(){
        if(instance == null){
            instance = new PreprocessingRunner();
        }
        return instance;
    }

    public void run(Dataset<Row> dataset, boolean writeStepResultsIntoFile) throws WrongCacheValueTypeException {
        for(PreprocessingStepInterface ps : this.preprocessorSteps) {
            dataset = ps.runPreprocessingStep(dataset, writeStepResultsIntoFile);
        }
        SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "result");
    }

    public void addPreprocessorStep(PreprocessingStepInterface step) {
        this.preprocessorSteps.add(step);
    }

    public synchronized int getNextCounter() {
        return ++stepCounter;
    }
}
