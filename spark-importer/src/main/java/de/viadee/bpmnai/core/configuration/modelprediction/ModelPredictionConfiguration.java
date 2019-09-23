package de.viadee.bpmnai.core.configuration.modelprediction;

import com.google.gson.annotations.SerializedName;
import de.viadee.bpmnai.core.configuration.preprocessing.PipelineStepConfiguration;

import java.util.ArrayList;
import java.util.List;

public class ModelPredictionConfiguration {

    @SerializedName("prediction_variables")
    private List<String> predictionVariables = new ArrayList<>();

    @SerializedName("model_configuration")
    private ModelConfiguration modelConfiguration;

    @SerializedName("pipeline_step_configuration")
    private PipelineStepConfiguration pipelineStepConfiguration = new PipelineStepConfiguration();

    @SerializedName("rolling_data")
    private RollingDataConfiguration rollingDataConfiguration;

    public List<String> getPredictionVariables() {
        return predictionVariables;
    }


    public ModelConfiguration getModelConfiguration() {
        return modelConfiguration;
    }

    public PipelineStepConfiguration getPipelineStepConfiguration() {
        return pipelineStepConfiguration;
    }

    public RollingDataConfiguration getRollingDataConfiguration() {
        return rollingDataConfiguration;
    }
}
