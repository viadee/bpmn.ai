package de.viadee.bpmnai.core.processing.steps;

import de.viadee.bpmnai.core.annotation.PreprocessingStepParameter;

public class ParameterDefinition {
    private String name;
    private String description;
    private PreprocessingStepParameter.DATA_TYPE dataType = PreprocessingStepParameter.DATA_TYPE.STRING;
    private boolean required = false;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public PreprocessingStepParameter.DATA_TYPE getDataType() {
        return dataType;
    }

    public void setDataType(PreprocessingStepParameter.DATA_TYPE dataType) {
        this.dataType = dataType;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}
