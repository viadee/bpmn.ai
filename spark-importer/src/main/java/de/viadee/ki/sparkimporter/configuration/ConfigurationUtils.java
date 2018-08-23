package de.viadee.ki.sparkimporter.configuration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.util.Map;

public class ConfigurationUtils {

    private final Gson gson;
    private String configurationFilePath = ".";
    private String configurationFileName = "pipeline_configuration.json";
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

    public void setConfigurationFilePath(String configurationFilePath) {
        this.configurationFilePath = configurationFilePath;
    }

    public Configuration getConfiguration() {

        if(this.configuration == null) {
            if (new File(configurationFilePath+"/"+configurationFileName).exists()){
                try (Reader reader = new FileReader(configurationFilePath+"/"+configurationFileName)) {
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

        try (Writer writer = new FileWriter(configurationFilePath+"/"+configurationFileName)) {
            gson.toJson(configuration, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
