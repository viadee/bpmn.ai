package de.viadee.ki.sparkimporter.configuration.modelprediction;

import com.google.gson.annotations.SerializedName;

public class PredictionColumnConfiguration {

    @SerializedName("column_name")
    private String columnName;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}

