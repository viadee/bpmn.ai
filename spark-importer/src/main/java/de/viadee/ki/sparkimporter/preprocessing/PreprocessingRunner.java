package de.viadee.ki.sparkimporter.preprocessing;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class PreprocessingRunner {

    private List<PreprocessingStep> preprocessorSteps = new ArrayList<>();

    private static PreprocessingRunner instance;

    private PreprocessingRunner(){}

    public static synchronized PreprocessingRunner getInstance(){
        if(instance == null){
            instance = new PreprocessingRunner();
        }
        return instance;
    }

    public void run(Dataset<Row> initialDataset, boolean writeStepResultsIntoFile) {
        for(PreprocessingStep ps : this.preprocessorSteps) {
            ps.runPreprocessingStep(initialDataset, writeStepResultsIntoFile);
        }
    }

    public void addPreprocessorStep(PreprocessingStep step) {
        this.preprocessorSteps.add(step);
    }

    public void removePreprocessorStep(PreprocessingStep step) {
        this.preprocessorSteps.remove(step);
    }
}
