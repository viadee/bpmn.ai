package de.viadee.ki.sparkimporter.configuration.preprocessing;

import java.util.HashMap;
import java.util.Map;

public class Step {

    private String id;
    private String className;
    private String dependsOn;
    private Map<String, Object> parameters = new HashMap<>();
    private String comment;
    private Boolean active = true;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getDependsOn() {
        return dependsOn;
    }
    
    public Boolean getActive() {
        return active;
    }

    public void setDependsOn(String dependsOn) {
        this.dependsOn = dependsOn;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }
    
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
    
    public void setActive(Boolean active) {
        this.active = active;
    }
}
