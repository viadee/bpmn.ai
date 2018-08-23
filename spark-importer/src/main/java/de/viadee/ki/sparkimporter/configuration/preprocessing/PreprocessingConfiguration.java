package de.viadee.ki.sparkimporter.configuration.preprocessing;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

public class PreprocessingConfiguration {

    @SerializedName("variable_configuration")
    List<VariableConfiguration> variableConfiguration = new ArrayList<>();

    @SerializedName("variable_name_mapping")
    List<VariableNameMapping> variableNameMappings = new ArrayList<>();

    public List<VariableConfiguration> getVariableConfiguration() {
        return variableConfiguration;
    }

    public List<VariableNameMapping> getVariableNameMappings() {
        return variableNameMappings;
    }
}
