package de.viadee.ki.sparkimporter.configuration.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.dataextraction.DataExtractionConfiguration;
import de.viadee.ki.sparkimporter.configuration.modellearning.ModelLearningConfiguration;
import de.viadee.ki.sparkimporter.configuration.modelprediction.ModelPredictionConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;

import java.io.*;

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

    public String getConfigurationFileName() {
        return CONFIGURATION_FILE_NAME;
    }

    public Configuration getConfiguration() {
        return this.getConfiguration(false);
    }

    public Configuration getConfiguration(boolean reload) {

        if(reload) {
            this.configuration = null;
        }

        if(this.configuration == null) {
            if (new File(SparkImporterVariables.getWorkingDirectory() +"/"+CONFIGURATION_FILE_NAME).exists()){
                try (Reader reader = new FileReader(SparkImporterVariables.getWorkingDirectory()+"/"+CONFIGURATION_FILE_NAME)) {
                    configuration = gson.fromJson(reader, Configuration.class);
                } catch (IOException e) {
                    SparkImporterLogger.getInstance().writeError("An error occurred while reading the configuration file: " + e.getMessage());
                }
            }
        }

        return configuration;
    }

    public void createEmptyConfig() {

        String pipelineType = "default";
        if(!PreprocessingRunner.runnerMode.equals(PreprocessingRunner.runnerMode.KAFKA_IMPORT)) {
            pipelineType = "minimal";
        }
        SparkImporterLogger.getInstance().writeInfo("No config file found. Creating " + pipelineType + " config file for dataset.");

        PreprocessingConfiguration preprocessingConfiguration = new PreprocessingConfiguration();

        DataExtractionConfiguration dataExtractionConfiguration = new DataExtractionConfiguration();

        ModelLearningConfiguration modelLearningConfiguration = new ModelLearningConfiguration();

        ModelPredictionConfiguration modelPredictionConfiguration = new ModelPredictionConfiguration();

        configuration = new Configuration();
        configuration.setDataExtractionConfiguration(dataExtractionConfiguration);
        configuration.setPreprocessingConfiguration(preprocessingConfiguration);
        configuration.setModelLearningConfiguration(modelLearningConfiguration);
        configuration.setModelPredictionConfiguration(modelPredictionConfiguration);

        try (Writer writer = new FileWriter(SparkImporterVariables.getWorkingDirectory()+"/"+CONFIGURATION_FILE_NAME)) {
            gson.toJson(configuration, writer);
        } catch (IOException e) {
            SparkImporterLogger.getInstance().writeError("An error occurred while writing the configuration file: " + e.getMessage());
        };
    }

    public void writeConfigurationToFile() {
        try (Writer writer = new FileWriter(SparkImporterVariables.getWorkingDirectory()+"/"+CONFIGURATION_FILE_NAME)) {
            gson.toJson(configuration, writer);
        } catch (IOException e) {
            SparkImporterLogger.getInstance().writeError("An error occurred while writing the configuration file: " + e.getMessage());
        }
    }
}
