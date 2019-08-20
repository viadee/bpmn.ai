package de.viadee.ki.sparkimporter.util.arguments;

import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;

public abstract class AbstractArguments {

    public abstract SparkRunnerConfig createOrUpdateSparkRunnerConfig(SparkRunnerConfig config);

    protected void validateConfig(SparkRunnerConfig config) {
        if(config.isDevProcessStateColumnWorkaroundEnabled() && config.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_ACTIVITY)) {
            try {
                throw new FaultyConfigurationException("Process state workaround option cannot be used with activity data level.");
            } catch (FaultyConfigurationException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
