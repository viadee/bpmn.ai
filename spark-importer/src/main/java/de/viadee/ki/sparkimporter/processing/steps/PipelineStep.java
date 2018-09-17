package de.viadee.ki.sparkimporter.processing.steps;

import de.viadee.ki.sparkimporter.configuration.preprocessing.CustomStep;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;

import java.util.HashMap;
import java.util.Map;

public class PipelineStep {

    private PreprocessingStepInterface preprocessingStep;
    private String id;
    private String dependsOn;
    private Map<String, Object> stepParameters = new HashMap<>();

    public PipelineStep(CustomStep cs) {
        Class<? extends PreprocessingStepInterface> step = null;
        try {
            step =
                    (Class<PreprocessingStepInterface>) Class.forName(cs.getClassName());
        } catch (ClassNotFoundException e) {
            SparkImporterLogger.getInstance().writeError("Could not find the class '" + cs.getClassName() + "' for custom step '" + cs.getId() + "' " + e.getMessage());
        }
        if(step != null) {
            try {
                preprocessingStep = step.newInstance();
            } catch (InstantiationException e) {
                SparkImporterLogger.getInstance().writeError("Could not find instantiate class '" + cs.getClassName() + "' for custom step '" + cs.getId() + "' " + e.getMessage());
            } catch (IllegalAccessException e) {
                SparkImporterLogger.getInstance().writeError("Could not find instantiate class '" + cs.getClassName() + "' for custom step '" + cs.getId() + "'. " + e.getMessage());
            }
        }

        dependsOn = cs.getDependsOn();
        id = cs.getId();
        stepParameters = cs.getParameters();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(String dependsOn) {
        this.dependsOn = dependsOn;
    }

    public boolean hasPredecessor() {
        return this.dependsOn != null;
    }

    public PreprocessingStepInterface getPreprocessingStep() {
        return preprocessingStep;
    }

    public Map<String, Object> getStepParameters() {
        return stepParameters;
    }

    public void setStepParameters(Map<String, Object> stepParameters) {
        this.stepParameters = stepParameters;
    }
}
