package de.viadee.bpmnai.core.configuration.preprocessing;

import com.google.gson.annotations.SerializedName;

public class ColumnConfiguration {

    @SerializedName("column_name")
    private String columnName;

    @SerializedName("column_type")
    private String columnType;

    @SerializedName("parse_format")
    private String parseFormat;

    @SerializedName("use_column")
    private boolean useColumn;

    @SerializedName("comment")
    private String comment;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getParseFormat() {
        return parseFormat;
    }

    public void setParseFormat(String parseFormat) {
        this.parseFormat = parseFormat;
    }

    public boolean isUseColumn() {
        return useColumn;
    }

    public void setUseColumn(boolean useColumn) {
        this.useColumn = useColumn;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}

