package de.viadee.ki.sparkimporter.configuration;

import com.google.gson.annotations.SerializedName;
import de.viadee.ki.sparkimporter.configuration.dataextraction.DataExtractionConfiguration;
import de.viadee.ki.sparkimporter.configuration.modellearning.ModelLearningConfiguration;
import de.viadee.ki.sparkimporter.configuration.modelprediction.ModelPredictionConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;

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
}

