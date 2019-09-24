package de.viadee.bpmnai.core.util.arguments;

import com.beust.jcommander.Parameter;
import de.viadee.bpmnai.core.exceptions.FaultyConfigurationException;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.BpmnaiVariables;
import org.apache.spark.sql.SaveMode;

public abstract class AbstractArguments {

    @Parameter(names = { "--data-level",
            "-dl" }, required = false, description = "Which level should the resulting data have. It can be process or activity.")
    protected String dataLevel = BpmnaiVariables.DATA_LEVEL_PROCESS;

    @Parameter(names = { "--file-destination",
            "-fd" }, required = true, description = "The name of the target folder, where the resulting csv files are being stored, i.e. the data mining table.")
    protected String fileDestination;

    @Parameter(names = { "--step-results",
            "-sr" }, description = "Should intermediate results be written into CSV files?", arity = 1)
    protected boolean writeStepResultsToCSV = false;

    @Parameter(names = { "--output-format",
            "-of" }, required = false, description = "In which format should the result be written (parquet or csv)?")
    protected String outputFormat = BpmnaiVariables.OUTPUT_FORMAT_PARQUET;

    @Parameter(names = { "--working-directory",
            "-wd" }, required = false, description = "Folder where the configuration files are stored or should be stored.")
    protected String workingDirectory = "./";

    @Parameter(names = { "--log-directory",
            "-ld" }, required = false, description = "Folder where the log files should be stored.")
    protected String logDirectory = "./";

    @Parameter(names = { "--save-mode",
            "-sm" }, required = false, description = "Should the result be appended to the destination or should it be overwritten?")
    protected String saveMode = BpmnaiVariables.SAVE_MODE_APPEND;

    public void createOrUpdateSparkRunnerConfig(SparkRunnerConfig config) {
        if(config == null) {
            config = new SparkRunnerConfig();
        }

        config.setTargetFolder(this.fileDestination);
        config.setWorkingDirectory(this.workingDirectory);
        config.setLogDirectory(this.logDirectory);
        config.setOutputFormat(this.outputFormat);
        config.setSaveMode(this.saveMode == BpmnaiVariables.SAVE_MODE_APPEND ? SaveMode.Append : SaveMode.Overwrite);
        config.setDataLevel(this.dataLevel);
        config.setWriteStepResultsIntoFile(this.writeStepResultsToCSV);
    }

    protected void validateConfig(SparkRunnerConfig config) {
        if(config.isDevProcessStateColumnWorkaroundEnabled() && config.getDataLevel().equals(BpmnaiVariables.DATA_LEVEL_ACTIVITY)) {
            try {
                throw new FaultyConfigurationException("Process state workaround option cannot be used with activity data level.");
            } catch (FaultyConfigurationException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
