package de.viadee.ki.sparkimporter.preprocessing;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class PreprocessingRunner {

    private List<PreprocessingStepInterface> preprocessorSteps = new ArrayList<>();

    public static int stepCounter = 1;

    private static PreprocessingRunner instance;

    private PreprocessingRunner(){}

    public static synchronized PreprocessingRunner getInstance(){
        if(instance == null){
            instance = new PreprocessingRunner();
        }
        return instance;
    }

    public void run(Dataset<Row> initialDataset, boolean writeStepResultsIntoFile) {
        stepCounter = 1;

        Dataset<Row> dataset = initialDataset;
        for(PreprocessingStepInterface ps : this.preprocessorSteps) {
            dataset = ps.runPreprocessingStep(dataset, writeStepResultsIntoFile);
        }
    }

    public void addPreprocessorStep(PreprocessingStepInterface step) {
        this.preprocessorSteps.add(step);
    }

    public void removePreprocessorStep(PreprocessingStepInterface step) {
        this.preprocessorSteps.remove(step);
    }
}
