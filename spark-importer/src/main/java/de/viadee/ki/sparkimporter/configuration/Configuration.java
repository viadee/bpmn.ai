package de.viadee.ki.sparkimporter.configuration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Configuration {


    public static void main(String[] args){

    }

    public void createConfigFile(Dataset<Row> dataset) {
        JSONObject config = new JSONObject();
        JSONObject configDetails = new JSONObject();
        JSONObject extractionDetails = new JSONObject();
        JSONObject preprocessingDetails = new JSONObject();
        JSONObject learningDetails = new JSONObject();
        JSONArray variables= addVariableConfig(dataset);
        preprocessingDetails.put("variable_configuration", variables);
        configDetails.put("data_extraction", extractionDetails);
        configDetails.put("preprocessing", preprocessingDetails);
        configDetails.put("model_learning", learningDetails);
        config.put("config", configDetails);
        System.out.print(config);

        try (FileWriter file = new FileWriter("pipeline_configuration.json")) {

            file.write(config.toJSONString());
            file.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JSONObject readConfigFile() {
        JSONParser parser = new JSONParser();
        JSONObject config= null;
        try {

            Object obj = parser.parse(new FileReader("pipeline_configuration.json"));

            config = (JSONObject) obj;
            System.out.println(config);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return config;
    }

    public JSONArray addVariableConfig (Dataset<Row> dataset) {
        JSONArray variables = new JSONArray();
        String[] columns= dataset.columns();
        for (String column: columns)
        {
            JSONObject variable = new JSONObject();
            variable.put("variable_name", column);
            variable.put("variable_type","STRING");
            variable.put("use_variable", true);
            variable.put("comment", "");
            variables.add(variable);
        }


        return variables;
    }
}

