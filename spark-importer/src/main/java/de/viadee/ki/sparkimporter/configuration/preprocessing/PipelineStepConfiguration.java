package de.viadee.ki.sparkimporter.configuration.preprocessing;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class PipelineStepConfiguration {

    @SerializedName("custom_steps")
    private List<CustomStep> customSteps;

    public List<CustomStep> getCustomSteps() {
        return customSteps;
    }

    public void setCustomSteps(List<CustomStep> customSteps) {
        this.customSteps = customSteps;
    }
}
