package de.viadee.ki.sparkimporter.util;

import de.viadee.ki.sparkimporter.runner.SparkRunner;
import org.apache.spark.sql.SaveMode;

public final class SparkImporterVariables {

    public static final String VAR_ID = "id_";
    public static final String VAR_SUPER_PROCESS_INSTANCE_ID = "super_process_instance_id_";
    public static final String VAR_SUPER_CASE_INSTANCE_ID = "super_case_instance_id_";
    public static final String VAR_PROCESS_INSTANCE_ID = "proc_inst_id_";
    public static final String VAR_EXCEUTION_ID = "execution_id_";
    public static final String VAR_BUSINESS_KEY = "business_key_";
    public static final String VAR_PROCESS_DEF_KEY = "proc_def_key_";
    public static final String VAR_PROCESS_DEF_ID = "proc_def_id_";
    public static final String VAR_START_TIME = "start_time_";
    public static final String VAR_END_TIME = "end_time_";
    public static final String VAR_DURATION = "duration_";
    public static final String VAR_START_USER_ID = "start_user_id_";
    public static final String VAR_ACT_ID = "activity_id_";
    public static final String VAR_ACT_INST_ID = "act_inst_id_";
    public static final String VAR_ACT_TYPE = "activity_type_";
    public static final String VAR_ACT_NAME = "activity_name_";
    public static final String VAR_START_ACT_ID = "start_act_id_";
    public static final String VAR_END_ACT_ID = "end_act_id_";
    public static final String VAR_CASE_INST_ID = "case_inst_id_";
    public static final String VAR_CASE_EXECUTION_ID = "case_execution_id_";
    public static final String VAR_CASE_DEF_ID = "case_def_id_";
    public static final String VAR_CASE_DEF_KEY = "case_def_key_";
    public static final String VAR_TASK_ID = "task_id_";
    public static final String VAR_DELETE_REASON = "delete_reason_";
    public static final String VAR_TENANT_ID = "tenant_id_";
    public static final String VAR_STATE = "state_";
    public static final String VAR_BYTEARRAY_ID = "bytearray_id_";
    public static final String VAR_DOUBLE = "double_";
    public static final String VAR_LONG = "long_";
    public static final String VAR_TEXT = "text_";
    public static final String VAR_TEXT2 = "text2_";
    public static final String VAR_PROCESS_INSTANCE_VARIABLE_NAME = "name_";
    public static final String VAR_PROCESS_INSTANCE_VARIABLE_INSTANCE_ID = "variable_instance_id_";
    public static final String VAR_PROCESS_INSTANCE_VARIABLE_TYPE = "var_type_";
    public static final String VAR_PROCESS_INSTANCE_VARIABLE_REVISION = "rev_";
    public static final String VAR_TIMESTAMP = "timestamp_";
    public static final String VAR_SEQUENCE_COUNTER = "sequence_counter_";

    public static final String PROCESS_STATE_ACTIVE = "ACTIVE";
    public static final String PROCESS_STATE_COMPLETED = "COMPLETED";

    public static final String DATA_LEVEL_PROCESS = "process";
    public static final String DATA_LEVEL_ACTIVITY = "activity";

    public static final String SAVE_MODE_APPEND = "append";
    public static final String SAVE_MODE_OVERWRITE = "overwrite";

    public static final String OUTPUT_FORMAT_PARQUET = "parquet";
    public static final String OUTPUT_FORMAT_CSV = "csv";
    


    public static final String PIPELINE_MODE_LEARN = "learn";
    public static final String PIPELINE_MODE_PREDICT = "predict";

    private static String workingDirectory = ".";
    private static String targetFolder = "";
    private static boolean devTypeCastCheckEnabled = false;
    private static boolean devProcessStateColumnWorkaroundEnabled = false;
    private static boolean revCountEnabled = false;
    private static SaveMode saveMode = SaveMode.Append;
    private static String outputFormat = SparkImporterVariables.OUTPUT_FORMAT_PARQUET;

    
	private static String processFilterDefinitionId = null;

    private static String pipelineMode = SparkImporterVariables.PIPELINE_MODE_LEARN;

    private static SparkRunner.RUNNING_MODE runningMode = null;

    public static SparkRunner.RUNNING_MODE getRunningMode() {
        return runningMode;
    }

    public static void setRunningMode(SparkRunner.RUNNING_MODE runningMode) {
        SparkImporterVariables.runningMode = runningMode;
    }

    public static String getWorkingDirectory() {
        return workingDirectory;
    }

    public static void setWorkingDirectory(String workingDirectory) {
        SparkImporterVariables.workingDirectory = workingDirectory;
    }

    public static String getTargetFolder() {
        return targetFolder;
    }

    public static void setTargetFolder(String targetFolder) {
        SparkImporterVariables.targetFolder = targetFolder;
    }

    public static boolean isDevTypeCastCheckEnabled() {
        return devTypeCastCheckEnabled;
    }

    public static void setDevTypeCastCheckEnabled(boolean devTypeCastCheckEnabled) {
        SparkImporterVariables.devTypeCastCheckEnabled = devTypeCastCheckEnabled;
    }

    public static boolean isDevProcessStateColumnWorkaroundEnabled() {
        return devProcessStateColumnWorkaroundEnabled;
    }

    public static void setDevProcessStateColumnWorkaroundEnabled(boolean devProcessStateColumnWorkaroundEnabled) {
        SparkImporterVariables.devProcessStateColumnWorkaroundEnabled = devProcessStateColumnWorkaroundEnabled;
    }

    public static boolean isRevCountEnabled() {
        return revCountEnabled;
    }

    public static void setRevCountEnabled(boolean revCountEnabled) {
        SparkImporterVariables.revCountEnabled = revCountEnabled;
    }

    public static SaveMode getSaveMode() {
        return saveMode;
    }

    public static void setSaveMode(SaveMode saveMode) {
        SparkImporterVariables.saveMode = saveMode;
    }

    public static String getOutputFormat() {
        return outputFormat;
    }

    public static void setOutputFormat(String outputFormat) {
        SparkImporterVariables.outputFormat = outputFormat;
    }

    
    public static void setProcessFilterDefinitionId(String processFilterDefinitionId) {
        SparkImporterVariables.processFilterDefinitionId  = processFilterDefinitionId;
    }

    public static String getProcessFilterDefinitionId() {
        return processFilterDefinitionId;
    }

    public static String getPipelineMode() {
        return pipelineMode;
    }

    public static void setPipelineMode(String pipelineMode) {
        SparkImporterVariables.pipelineMode = pipelineMode;

    }
}
