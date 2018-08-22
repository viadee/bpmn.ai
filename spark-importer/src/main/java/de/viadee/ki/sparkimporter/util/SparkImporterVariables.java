package de.viadee.ki.sparkimporter.util;

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
    public static final String VAR_ACT_INST_ID = "act_inst_id_";
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

    public static String targetFolder = "";

    public static String getTargetFolder() {
        return targetFolder;
    }

    public static void setTargetFolder(String targetFolder) {
        SparkImporterVariables.targetFolder = targetFolder;
    }
}
