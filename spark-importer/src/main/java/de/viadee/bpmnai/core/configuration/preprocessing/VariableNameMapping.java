package de.viadee.bpmnai.core.configuration.preprocessing;

import com.google.gson.annotations.SerializedName;

public class VariableNameMapping {

    @SerializedName("old_name")
    private String oldName;

    @SerializedName("new_name")
    private String newName;

    public String getOldName() {
        return oldName;
    }

    public void setOldName(String oldName) {
        this.oldName = oldName;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }
}
