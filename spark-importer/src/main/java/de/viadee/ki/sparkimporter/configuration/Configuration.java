package de.viadee.ki.sparkimporter.configuration;

import com.google.gson.annotations.SerializedName;

public class Configuration {

    @SerializedName("data_extraction")
    private DataExtractionConfiguration dataExtractionConfiguration;

    @SerializedName("preprocessing")
    private PreprocessingConfiguration preprocessingConfiguration;

    @SerializedName("model_learning")
    private ModelLearningConfiguration modelLearningConfiguration;

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
}

