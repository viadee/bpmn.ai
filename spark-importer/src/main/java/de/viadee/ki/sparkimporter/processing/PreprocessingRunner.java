package de.viadee.ki.sparkimporter.processing;

import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreprocessingRunner {

    private final List<PipelineStep> pipelineSteps = new ArrayList<>();

    private static int stepCounter = 0;

    public final static String DATASET_INITIAL = "initial";

    public static final Map<String, Dataset<Row>> helper_datasets = new HashMap<>();

    public static boolean writeStepResultsIntoFile = false;

    public static boolean initialConfigToBeWritten = false;

    public PreprocessingRunner(){}

    public void run(Dataset<Row> dataset, String dataLevel) {
        helper_datasets.clear();
        helper_datasets.put(DATASET_INITIAL + "_" + dataLevel, dataset);

        for(PipelineStep ps : this.pipelineSteps) {
            if(ps.getPreprocessingStep() != null)
            dataset = ps.getPreprocessingStep().runPreprocessingStep(dataset, writeStepResultsIntoFile, dataLevel, ps.getStepParameters());
        }

    }

    public void addPreprocessorStep(PipelineStep step) {
        this.pipelineSteps.add(step);
    }

    public static synchronized int getNextCounter() {
        return ++stepCounter;
    }

    public static synchronized int getCounter() {
        return stepCounter;
    }
}
