package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Arrays;

public class TypeCastStep  implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile) {
        Configuration config= new Configuration();
        JSONObject configfile= config.readConfigFile();
        JSONObject configDetails= (JSONObject) configfile.get("config");
        JSONObject Preprocessing= (JSONObject) configDetails.get("preprocessing");
        JSONArray variables= (JSONArray) Preprocessing.get("variable_configuration");
        System.out.print(variables);
        for(int i=0; i< variables.size(); i++){
            JSONObject variableJSON=(JSONObject) variables.get(i);
            String column= variableJSON.get("variable_name").toString();
            if (Arrays.asList(dataSet.columns()).contains(column)) {
                dataSet.withColumn("Tmp", dataSet.col(column).cast(variableJSON.get("variable_type").toString())).drop(column).withColumnRenamed("Tmp", column);
            }
        }

        return dataSet;
    }
}
