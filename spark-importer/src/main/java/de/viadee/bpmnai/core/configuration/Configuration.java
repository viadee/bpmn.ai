package de.viadee.bpmnai.core.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.gson.annotations.SerializedName;
import de.viadee.bpmnai.core.configuration.dataextraction.DataExtractionConfiguration;
import de.viadee.bpmnai.core.configuration.modellearning.ModelLearningConfiguration;
import de.viadee.bpmnai.core.configuration.modelprediction.ModelPredictionConfiguration;
import de.viadee.bpmnai.core.configuration.preprocessing.PreprocessingConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Configuration {

    @SerializedName("data_extraction")
    private DataExtractionConfiguration dataExtractionConfiguration;

    @SerializedName("preprocessing")
    private PreprocessingConfiguration preprocessingConfiguration;

    @SerializedName("model_learning")
    private ModelLearningConfiguration modelLearningConfiguration;

    @SerializedName("model_prediction")
    private ModelPredictionConfiguration modelPredictionConfiguration;

    public DataExtractionConfiguration getDataExtractionConfiguration() {
        return dataExtractionConfiguration;
    }

    public void setDataExtractionConfiguration(DataExtractionConfiguration dataExtractionConfiguration) {
        this.dataExtractionConfiguration = dataExtractionConfiguration;
    }

    public PreprocessingConfiguration getPreprocessingConfiguration() {
        return preprocessingConfiguration;
    }

    public void setPreprocessingConfiguration(PreprocessingConfiguration preprocessingConfiguration) {
        this.preprocessingConfiguration = preprocessingConfiguration;
    }

    public ModelLearningConfiguration getModelLearningConfiguration() {
        return modelLearningConfiguration;
    }

    public void setModelLearningConfiguration(ModelLearningConfiguration modelLearningConfiguration) {
        this.modelLearningConfiguration = modelLearningConfiguration;
    }

    public ModelPredictionConfiguration getModelPredictionConfiguration() {
        return modelPredictionConfiguration;
    }

    public void setModelPredictionConfiguration(ModelPredictionConfiguration modelPredictionConfiguration) {
        this.modelPredictionConfiguration = modelPredictionConfiguration;
    }

    @JsonIgnore
    public boolean isEmpty() {
        boolean result = false;

        if(this.preprocessingConfiguration == null
                ||
                (this.preprocessingConfiguration.getVariableConfiguration().size() == 0
                        && this.preprocessingConfiguration.getColumnConfiguration().size() == 0
                        && this.preprocessingConfiguration.getPipelineStepConfiguration().getSteps() == null
                )
        ) {
            result = true;
        }

        return result;
    }
}

