package de.viadee.bpmnai.core.configuration.preprocessing;

import com.google.gson.annotations.SerializedName;

public class ColumnHashConfiguration {

    @SerializedName("column_name")
    private String columnName;

    @SerializedName("hash_column")
    private boolean hashColumn;

    @SerializedName("comment")
    private String comment;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public boolean isHashColumn() {
        return hashColumn;
    }

    public void setHashColumn(boolean hashColumn) {
        this.hashColumn = hashColumn;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}

