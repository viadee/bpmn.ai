package de.viadee.bpmnai.core.configuration.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.viadee.bpmnai.core.configuration.Configuration;
import de.viadee.bpmnai.core.configuration.dataextraction.DataExtractionConfiguration;
import de.viadee.bpmnai.core.configuration.modellearning.ModelLearningConfiguration;
import de.viadee.bpmnai.core.configuration.modelprediction.ModelPredictionConfiguration;
import de.viadee.bpmnai.core.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.bpmnai.core.runner.SparkRunner;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.logging.BpmnaiLogger;

import java.io.*;

public class ConfigurationUtils {

    private final Gson gson;
    private final String CONFIGURATION_FILE_NAME = "pipeline_configuration";
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

    public String getConfigurationFileName(SparkRunnerConfig config) {
        return CONFIGURATION_FILE_NAME + "_" + config.getRunningMode().getModeString() + ".json";
    }

    public String getConfigurationFilePath(SparkRunnerConfig config) {
        return config.getWorkingDirectory()+"/"+getConfigurationFileName(config);
    }

    public Configuration getConfiguration(SparkRunnerConfig config) {
        return this.getConfiguration(false, config);
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration(boolean reload, SparkRunnerConfig config) {

        if(reload) {
            this.configuration = null;
        }

        if(this.configuration == null) {
            if (new File(getConfigurationFilePath(config)).exists()){
                try (Reader reader = new FileReader(getConfigurationFilePath(config))) {
                    configuration = gson.fromJson(reader, Configuration.class);
                } catch (IOException e) {
                    BpmnaiLogger.getInstance().writeError("An error occurred while reading the configuration file: " + e.getMessage());
                }
            }
        }

        return configuration;
    }

    public void createEmptyConfig(SparkRunnerConfig config) {

        String pipelineType = "default";
        if (!config.getRunningMode().equals(SparkRunner.RUNNING_MODE.KAFKA_IMPORT)) {
            pipelineType = "minimal";
        }
        BpmnaiLogger.getInstance().writeInfo("No config file found. Creating " + pipelineType + " config file for dataset at " + config.getWorkingDirectory() + "/" + getConfigurationFileName(config));

        PreprocessingConfiguration preprocessingConfiguration = new PreprocessingConfiguration();

        DataExtractionConfiguration dataExtractionConfiguration = new DataExtractionConfiguration();

        ModelLearningConfiguration modelLearningConfiguration = new ModelLearningConfiguration();

        ModelPredictionConfiguration modelPredictionConfiguration = new ModelPredictionConfiguration();

        configuration = new Configuration();
        configuration.setDataExtractionConfiguration(dataExtractionConfiguration);
        configuration.setPreprocessingConfiguration(preprocessingConfiguration);
        configuration.setModelLearningConfiguration(modelLearningConfiguration);
        configuration.setModelPredictionConfiguration(modelPredictionConfiguration);

        try (Writer writer = new FileWriter(config.getWorkingDirectory()+"/"+getConfigurationFileName(config))) {
            gson.toJson(configuration, writer);
        } catch (IOException e) {
            BpmnaiLogger.getInstance().writeError("An error occurred while writing the configuration file: " + e.getMessage());
        };
    }

    public void writeConfigurationToFile(SparkRunnerConfig config) {
        try (Writer writer = new FileWriter(config.getWorkingDirectory()+"/"+getConfigurationFileName(config))) {
            gson.toJson(configuration, writer);
        } catch (IOException e) {
            BpmnaiLogger.getInstance().writeError("An error occurred while writing the configuration file: " + e.getMessage());
        }
    }

    public void validateConfigurationFileVsSparkRunnerConfig(SparkRunnerConfig config) {
        if(this.configuration.getPreprocessingConfiguration().getDataLevel().equals("")) {
            BpmnaiLogger.getInstance().writeInfo("No data level set in configuration file. Setting it to run parameter (" + config.getDataLevel() + ")");
            this.configuration.getPreprocessingConfiguration().setDataLevel(config.getDataLevel());
            writeConfigurationToFile(config);
            return;
        }
        if(!config.getDataLevel().equals(this.configuration.getPreprocessingConfiguration().getDataLevel())) {
            String message = "The data level from the run parameters ("
                    + config.getDataLevel() + ") does not match the one from the existing configuration file ("
                    + this.configuration.getPreprocessingConfiguration().getDataLevel()
                    + ")! Exiting...";
            BpmnaiLogger.getInstance().writeError(message);
            throw new RuntimeException(message);
        }
    }
}
