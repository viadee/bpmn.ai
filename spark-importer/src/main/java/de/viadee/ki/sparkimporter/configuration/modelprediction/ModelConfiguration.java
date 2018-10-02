package de.viadee.ki.sparkimporter.configuration.modelprediction;

import com.google.gson.annotations.SerializedName;

public class ModelConfiguration {

    @SerializedName("mojo_path")
    private String mojoPath;

    @SerializedName("prediction_type")
    private String predictionType = "binomial";

    public String getMojoPath() {
        return mojoPath;
    }

    public String getPredictionType() {
        return predictionType;
    }
}
