package de.viadee.bpmnai.core.processing.steps;

import de.viadee.bpmnai.core.configuration.preprocessing.Step;
import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.util.logging.BpmnaiLogger;

import java.util.Map;
import java.util.Objects;

public class PipelineStep {

    private PreprocessingStepInterface preprocessingStep;
    private String id;
    private String className;
    private String dependsOn;
    private Map<String, Object> stepParameters;

    public PipelineStep(Step cs) {
        Class<? extends PreprocessingStepInterface> step = null;
        try {
            step =
                    (Class<PreprocessingStepInterface>) Class.forName(cs.getClassName());
        } catch (ClassNotFoundException e) {
            BpmnaiLogger.getInstance().writeError("Could not find the class '" + cs.getClassName() + "' for custom step '" + cs.getId() + "' " + e.getMessage());
        }
        if(step != null) {
            try {
                preprocessingStep = step.newInstance();
            } catch (InstantiationException e) {
                BpmnaiLogger.getInstance().writeError("Could not find instantiate class '" + cs.getClassName() + "' for custom step '" + cs.getId() + "' " + e.getMessage());
            } catch (IllegalAccessException e) {
                BpmnaiLogger.getInstance().writeError("Could not find instantiate class '" + cs.getClassName() + "' for custom step '" + cs.getId() + "'. " + e.getMessage());
            }
        }

        id = cs.getId();
        className = cs.getClassName();
        dependsOn = cs.getDependsOn();
        stepParameters = cs.getParameters();
    }

    public PipelineStep(PreprocessingStepInterface s, String dependsOn) {
        preprocessingStep = s;
        id = s.getClass().getSimpleName();
        className = s.getClass().getCanonicalName();
        this.dependsOn = dependsOn;
        stepParameters = null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClassName() {
        return className;
    }

    public String getDependsOn() {
        return dependsOn;
    }

    public boolean hasPredecessor() {
        return this.dependsOn != null && !this.dependsOn.equals("");
    }

    public PreprocessingStepInterface getPreprocessingStep() {
        return preprocessingStep;
    }

    public Map<String, Object> getStepParameters() {
        return stepParameters;
    }

    @Override
    public String toString() {
        return getId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PipelineStep that = (PipelineStep) o;
        return Objects.equals(getClassName(), that.getClassName()) &&
                Objects.equals(getId(), that.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClassName(), getId());
    }
}
