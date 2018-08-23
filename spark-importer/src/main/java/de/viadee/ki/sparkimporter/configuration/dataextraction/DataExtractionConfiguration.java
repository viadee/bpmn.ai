package de.viadee.ki.sparkimporter.configuration.dataextraction;

import com.google.gson.annotations.SerializedName;

public class DataExtractionConfiguration {

    @SerializedName("filter_query")
    private String filterQuery;

    public String getFilterQuery() {
        return filterQuery;
    }

    public void setFilterQuery(String filterQuery) {
        this.filterQuery = filterQuery;
    }
}
