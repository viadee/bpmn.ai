package de.viadee.ki.sparkimporter.configuration.modelprediction;

import com.google.gson.annotations.SerializedName;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PipelineStepConfiguration;

import java.util.ArrayList;
import java.util.List;

public class ModelPredictionConfiguration {

    @SerializedName("prediction_variables")
    private List<String> predictionVariables = new ArrayList<>();

    @SerializedName("predicition_output_type")
    private String predictionOutputType;

    @SerializedName("pipeline_step_configuration")
    private PipelineStepConfiguration pipelineStepConfiguration = new PipelineStepConfiguration();

    public List<String> getPredictionVariables() {
        return predictionVariables;
    }

    public String getPredictionOutputType() {
        return predictionOutputType;
    }

    public PipelineStepConfiguration getPipelineStepConfiguration() {
        return pipelineStepConfiguration;
    }
}
