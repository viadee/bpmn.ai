package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.SaveMode;

import java.io.Serializable;

public class SparkRunnerConfig implements Serializable {
    private boolean initialConfigToBeWritten = false;
    private boolean minimalPipelineToBeBuild = false;
    private boolean writeStepResultsIntoFile = false;
    private int stepCounter = 1;

    private String workingDirectory = ".";
    private String sourceFolder = "";
    private String targetFolder = "";
    private boolean devTypeCastCheckEnabled = false;
    private boolean devProcessStateColumnWorkaroundEnabled = false;
    private boolean revCountEnabled = false;
    private SaveMode saveMode = SaveMode.Append;
    private String outputFormat = SparkImporterVariables.OUTPUT_FORMAT_PARQUET;


    private String processFilterDefinitionId = null;

    private String pipelineMode = SparkImporterVariables.PIPELINE_MODE_LEARN;

    private SparkRunner.RUNNING_MODE runningMode = null;

    public boolean isInitialConfigToBeWritten() {
        return initialConfigToBeWritten;
    }

    public void setInitialConfigToBeWritten(boolean initialConfigToBeWritten) {
        this.initialConfigToBeWritten = initialConfigToBeWritten;
    }

    public boolean isMinimalPipelineToBeBuild() {
        return minimalPipelineToBeBuild;
    }

    public void setMinimalPipelineToBeBuild(boolean minimalPipelineToBeBuild) {
        this.minimalPipelineToBeBuild = minimalPipelineToBeBuild;
    }

    public boolean isWriteStepResultsIntoFile() {
        return writeStepResultsIntoFile;
    }

    public void setWriteStepResultsIntoFile(boolean writeStepResultsIntoFile) {
        this.writeStepResultsIntoFile = writeStepResultsIntoFile;
    }

    public int getAndRaiseStepCounter() {
        return stepCounter++;
    }

    public String getWorkingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public String getSourceFolder() {
        return sourceFolder;
    }

    public void setSourceFolder(String sourceFolder) {
        this.sourceFolder = sourceFolder;
    }

    public String getTargetFolder() {
        return targetFolder;
    }

    public void setTargetFolder(String targetFolder) {
        this.targetFolder = targetFolder;
    }

    public boolean isDevTypeCastCheckEnabled() {
        return devTypeCastCheckEnabled;
    }

    public void setDevTypeCastCheckEnabled(boolean devTypeCastCheckEnabled) {
        this.devTypeCastCheckEnabled = devTypeCastCheckEnabled;
    }

    public boolean isDevProcessStateColumnWorkaroundEnabled() {
        return devProcessStateColumnWorkaroundEnabled;
    }

    public void setDevProcessStateColumnWorkaroundEnabled(boolean devProcessStateColumnWorkaroundEnabled) {
        this.devProcessStateColumnWorkaroundEnabled = devProcessStateColumnWorkaroundEnabled;
    }

    public boolean isRevCountEnabled() {
        return revCountEnabled;
    }

    public void setRevCountEnabled(boolean revCountEnabled) {
        this.revCountEnabled = revCountEnabled;
    }

    public SaveMode getSaveMode() {
        return saveMode;
    }

    public void setSaveMode(SaveMode saveMode) {
        this.saveMode = saveMode;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String getProcessFilterDefinitionId() {
        return processFilterDefinitionId;
    }

    public void setProcessFilterDefinitionId(String processFilterDefinitionId) {
        this.processFilterDefinitionId = processFilterDefinitionId;
    }

    public String getPipelineMode() {
        return pipelineMode;
    }

    public void setPipelineMode(String pipelineMode) {
        this.pipelineMode = pipelineMode;
    }

    public SparkRunner.RUNNING_MODE getRunningMode() {
        return runningMode;
    }

    public void setRunningMode(SparkRunner.RUNNING_MODE runningMode) {
        this.runningMode = runningMode;
    }
}
