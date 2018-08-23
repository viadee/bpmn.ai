package de.viadee.ki.sparkimporter.configuration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;

import java.io.*;
import java.util.Map;

public class ConfigurationUtils {

    private final Gson gson;
    private final String CONFIGURATION_FILE_NAME = "pipeline_configuration.json";
    private Configuration configuration = null;

    private static ConfigurationUtils instance;

    private ConfigurationUtils(){
        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    public static synchronized ConfigurationUtils getInstance(){
        if(instance == null){
            instance = new ConfigurationUtils();
        }
        return instance;
    }

    public Configuration getConfiguration() {

        if(this.configuration == null) {
            if (new File(SparkImporterUtils.getWorkingDirectory() +"/"+CONFIGURATION_FILE_NAME).exists()){
                try (Reader reader = new FileReader(SparkImporterUtils.getWorkingDirectory()+"/"+CONFIGURATION_FILE_NAME)) {
                    configuration = gson.fromJson(reader, Configuration.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return configuration;
    }

    public void writeConfiguration(Map<String, String> variables) {
        PreprocessingConfiguration preprocessingConfiguration = new PreprocessingConfiguration();

        for(String key : variables.keySet()) {
            VariableConfiguration variableConfiguration = new VariableConfiguration();
            variableConfiguration.setVariableName(key);
            variableConfiguration.setVariableType(variables.get(key));
            variableConfiguration.setUseVariable(true);
            variableConfiguration.setComment("");

            preprocessingConfiguration.getVariableConfiguration().add(variableConfiguration);
        }

        DataExtractionConfiguration dataExtractionConfiguration = new DataExtractionConfiguration();

        ModelLearningConfiguration modelLearningConfiguration = new ModelLearningConfiguration();

        Configuration configuration = new Configuration();
        configuration.setDataExtractionConfiguration(dataExtractionConfiguration);
        configuration.setPreprocessingConfiguration(preprocessingConfiguration);
        configuration.setModelLearningConfiguration(modelLearningConfiguration);

        try (Writer writer = new FileWriter(SparkImporterUtils.getWorkingDirectory()+"/"+CONFIGURATION_FILE_NAME)) {
            gson.toJson(configuration, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
