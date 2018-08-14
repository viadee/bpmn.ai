package de.viadee.ki.sparkimporter.events;

public class VariableUpdateEvent extends DetailEvent {
    private int revision;

    private String variableName;
    private String variableInstanceId;
    private String scopeActivityInstanceId;

    private String serializerName;

    private Long longValue;
    private Double doubleValue;
    private String textValue;

    private Object complexValue;

    public int getRevision() {
        return revision;
    }

    public void setRevision(int revision) {
        this.revision = revision;
    }

    public String getVariableName() {
        return variableName;
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableInstanceId() {
        return variableInstanceId;
    }

    public void setVariableInstanceId(String variableInstanceId) {
        this.variableInstanceId = variableInstanceId;
    }

    public String getScopeActivityInstanceId() {
        return scopeActivityInstanceId;
    }

    public void setScopeActivityInstanceId(String scopeActivityInstanceId) {
        this.scopeActivityInstanceId = scopeActivityInstanceId;
    }

    public String getSerializerName() {
        return serializerName;
    }

    public void setSerializerName(String serializerName) {
        this.serializerName = serializerName;
    }

    public Long getLongValue() {
        return longValue;
    }

    public void setLongValue(Long longValue) {
        this.longValue = longValue;
    }

    public Double getDoubleValue() {
        return doubleValue;
    }

    public void setDoubleValue(Double doubleValue) {
        this.doubleValue = doubleValue;
    }

    public String getTextValue() {
        return textValue;
    }

    public void setTextValue(String textValue) {
        this.textValue = textValue;
    }

    public Object getComplexValue() {
        return complexValue;
    }

    public void setComplexValue(Object complexValue) {
        this.complexValue = complexValue;
    }
}
