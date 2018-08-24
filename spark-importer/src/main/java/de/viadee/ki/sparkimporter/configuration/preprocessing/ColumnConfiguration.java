package de.viadee.ki.sparkimporter.configuration.preprocessing;

import com.google.gson.annotations.SerializedName;

public class ColumnConfiguration {

    @SerializedName("column_name")
    private String columnName;

    @SerializedName("use_column")
    private boolean useColumn;

    @SerializedName("comment")
    private String comment;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String variableName) {
        this.columnName = variableName;
    }

    public boolean isUseColumn() {
        return useColumn;
    }

    public void setUseVariable(boolean useVariable) {
        this.useColumn = useVariable;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}

