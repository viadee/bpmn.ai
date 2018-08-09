package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.first;

public class ReduceDatasetToSingleProcessInstanceRowsStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        dataset = dataset.select(
                SparkImporterVariables.VAR_ID,
                SparkImporterVariables.VAR_SUPER_PROCESS_INSTANCE_ID,
                SparkImporterVariables.VAR_SUPER_CASE_INSTANCE_ID,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_ID,
                SparkImporterVariables.VAR_EXCEUTION_ID,
                SparkImporterVariables.VAR_BUSINESS_KEY,
                SparkImporterVariables.VAR_PROCESS_DEF_KEY,
                SparkImporterVariables.VAR_PROCESS_DEF_ID,
                SparkImporterVariables.VAR_START_TIME,
                SparkImporterVariables.VAR_END_TIME,
                SparkImporterVariables.VAR_DURATION,
                SparkImporterVariables.VAR_START_USER_ID,
                SparkImporterVariables.VAR_ACT_INST_ID,
                SparkImporterVariables.VAR_START_ACT_ID,
                SparkImporterVariables.VAR_END_ACT_ID,
                SparkImporterVariables.VAR_CASE_INST_ID,
                SparkImporterVariables.VAR_CASE_EXECUTION_ID,
                SparkImporterVariables.VAR_CASE_DEF_ID,
                SparkImporterVariables.VAR_CASE_DEF_KEY,
                SparkImporterVariables.VAR_TASK_ID,
                SparkImporterVariables.VAR_DELETE_REASON,
                SparkImporterVariables.VAR_TENANT_ID,
                SparkImporterVariables.VAR_STATE,
                SparkImporterVariables.VAR_BYTEARRAY_ID,
                SparkImporterVariables.VAR_DOUBLE,
                SparkImporterVariables.VAR_LONG,
                SparkImporterVariables.VAR_TEXT,
                SparkImporterVariables.VAR_TEXT2,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION)
                .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)
                .agg(
                        first(SparkImporterVariables.VAR_ID).as(SparkImporterVariables.VAR_ID),
                        first(SparkImporterVariables.VAR_SUPER_PROCESS_INSTANCE_ID).as(SparkImporterVariables.VAR_SUPER_PROCESS_INSTANCE_ID),
                        first(SparkImporterVariables.VAR_SUPER_CASE_INSTANCE_ID).as(SparkImporterVariables.VAR_SUPER_CASE_INSTANCE_ID),
                        first(SparkImporterVariables.VAR_EXCEUTION_ID).as(SparkImporterVariables.VAR_EXCEUTION_ID),
                        first(SparkImporterVariables.VAR_BUSINESS_KEY).as(SparkImporterVariables.VAR_BUSINESS_KEY),
                        first(SparkImporterVariables.VAR_PROCESS_DEF_KEY).as(SparkImporterVariables.VAR_PROCESS_DEF_KEY),
                        first(SparkImporterVariables.VAR_PROCESS_DEF_ID).as(SparkImporterVariables.VAR_PROCESS_DEF_ID),
                        first(SparkImporterVariables.VAR_START_TIME).as(SparkImporterVariables.VAR_START_TIME),
                        first(SparkImporterVariables.VAR_END_TIME).as(SparkImporterVariables.VAR_END_TIME),
                        first(SparkImporterVariables.VAR_DURATION).as(SparkImporterVariables.VAR_DURATION),
                        first(SparkImporterVariables.VAR_START_USER_ID).as(SparkImporterVariables.VAR_START_USER_ID),
                        first(SparkImporterVariables.VAR_ACT_INST_ID).as(SparkImporterVariables.VAR_ACT_INST_ID),
                        first(SparkImporterVariables.VAR_START_ACT_ID).as(SparkImporterVariables.VAR_START_ACT_ID),
                        first(SparkImporterVariables.VAR_END_ACT_ID).as(SparkImporterVariables.VAR_END_ACT_ID),
                        first(SparkImporterVariables.VAR_CASE_INST_ID).as(SparkImporterVariables.VAR_CASE_INST_ID),
                        first(SparkImporterVariables.VAR_CASE_EXECUTION_ID).as(SparkImporterVariables.VAR_CASE_EXECUTION_ID),
                        first(SparkImporterVariables.VAR_CASE_DEF_ID).as(SparkImporterVariables.VAR_CASE_DEF_ID),
                        first(SparkImporterVariables.VAR_CASE_DEF_KEY).as(SparkImporterVariables.VAR_CASE_DEF_KEY),
                        first(SparkImporterVariables.VAR_TASK_ID).as(SparkImporterVariables.VAR_TASK_ID),
                        first(SparkImporterVariables.VAR_DELETE_REASON).as(SparkImporterVariables.VAR_DELETE_REASON),
                        first(SparkImporterVariables.VAR_TENANT_ID).as(SparkImporterVariables.VAR_TENANT_ID),
                        first(SparkImporterVariables.VAR_STATE).as(SparkImporterVariables.VAR_STATE),
                        first(SparkImporterVariables.VAR_BYTEARRAY_ID).as(SparkImporterVariables.VAR_BYTEARRAY_ID),
                        first(SparkImporterVariables.VAR_DOUBLE).as(SparkImporterVariables.VAR_DOUBLE),
                        first(SparkImporterVariables.VAR_LONG).as(SparkImporterVariables.VAR_LONG),
                        first(SparkImporterVariables.VAR_TEXT).as(SparkImporterVariables.VAR_TEXT),
                        first(SparkImporterVariables.VAR_TEXT2).as(SparkImporterVariables.VAR_TEXT2),
                        first(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).as(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME),
                        first(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).as(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE),
                        first(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION).as(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION)
                );


        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "reduced_to_line_per_process_instance");
        }

        //return preprocessed data
        return dataset;
    }
}
