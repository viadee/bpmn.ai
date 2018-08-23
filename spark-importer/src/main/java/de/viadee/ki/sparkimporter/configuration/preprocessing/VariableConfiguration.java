package de.viadee.ki.sparkimporter.configuration.preprocessing;

import com.google.gson.annotations.SerializedName;

public class VariableConfiguration {

    @SerializedName("variable_name")
    private String variableName;

    @SerializedName("variable_type")
    private String variableType;

    @SerializedName("use_variable")
    private boolean useVariable;

    @SerializedName("comment")
    private String comment;

    public String getVariableName() {
        return variableName;
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableType() {
        return variableType;
    }

    public void setVariableType(String variableType) {
        this.variableType = variableType;
    }

    public boolean isUseVariable() {
        return useVariable;
    }

    public void setUseVariable(boolean useVariable) {
        this.useVariable = useVariable;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
