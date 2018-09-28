package de.viadee.ki.sparkimporter.configuration.modelprediction;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class RollingDataConfiguration {

    @SerializedName("rolling_data_path")
    private String rollingDataPath;

    @SerializedName("rolling_data_update_cron_expression")
    private String rollingDataUpdateCronExpression;

    @SerializedName("used_variables")
    private List<String> usedVariables;

    public String getRollingDataPath() {
        return rollingDataPath;
    }

    public String getRollingDataUpdateCronExpression() {
        return rollingDataUpdateCronExpression;
    }

    public List<String> getUsedVariables() {
        return usedVariables;
    }
}
