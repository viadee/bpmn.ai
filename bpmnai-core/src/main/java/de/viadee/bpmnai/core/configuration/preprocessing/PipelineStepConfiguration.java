package de.viadee.bpmnai.core.configuration.preprocessing;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class PipelineStepConfiguration {

    @SerializedName("steps")
    private List<Step> steps;

    public List<Step> getSteps() {
        return steps;
    }

    public void setSteps(List<Step> steps) {
        this.steps = steps;
    }
}
