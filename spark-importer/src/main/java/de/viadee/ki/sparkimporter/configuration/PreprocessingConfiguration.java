package de.viadee.ki.sparkimporter.configuration;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

public class PreprocessingConfiguration {

    @SerializedName("variable_configuration")
    List<VariableConfiguration> variableConfiguration = new ArrayList<>();

    public List<VariableConfiguration> getVariableConfiguration() {
        return variableConfiguration;
    }

    public void setVariableConfiguration(List<VariableConfiguration> variableConfiguration) {
        this.variableConfiguration = variableConfiguration;
    }
}
