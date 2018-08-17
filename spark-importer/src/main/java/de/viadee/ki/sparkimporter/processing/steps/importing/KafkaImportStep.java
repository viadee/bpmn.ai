package de.viadee.ki.sparkimporter.processing.steps.importing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class KafkaImportStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        //drop unimportant columns
        dataset = dataset.drop(
                "caseInstanceId",
                "caseExecutionId",
                "caseDefinitionId",
                "caseDefinitionKey",
                "caseDefinitionName",
                "eventType",
                "sequenceCounter",
                "superProcessInstanceId",
                "superCaseInstanceId",
                "endActivityId",
                "startActivityId",
                "tenantId",
                "activityId",
                "activityName",
                "activityType",
                "activityInstanceId",
                "activityInstanceState",
                "parentActivityInstanceId",
                "calledProcessInstanceId",
                "calledCaseInstanceId",
                "taskId",
                "taskAssignee",
                "timestamp",
                "userOperationId",
                "variableInstanceId",
                "scopeActivityInstanceId"
        );

        //rename columns
        for(String columnName : dataset.columns()) {
            dataset = dataset.withColumnRenamed(columnName, columnName.replaceAll("([A-Z])","_$1").concat("_").toLowerCase());
        }

        dataset = dataset
                .withColumnRenamed("process_instance_id_", "proc_inst_id_")
                .withColumnRenamed("duration_in_millis_", "duration_")
                .withColumnRenamed("variable_name_", "name_")
                .withColumnRenamed("long_value_", "long_")
                .withColumnRenamed("double_value_", "double_")
                .withColumnRenamed("text_value_", "text_")
                .withColumnRenamed("complex_value_", "text2_")
                .withColumnRenamed("serializer_name_", "var_type_")
                .withColumnRenamed("revision_", "rev_")
                .select("id_",
                        "proc_inst_id_",
                        "business_key_",
                        "process_definition_key_",
                        "process_definition_id_",
                        "start_time_",
                        "end_time_",
                        "duration_",
                        "state_",
                        "name_",
                        "var_type_",
                        "rev_",
                        "long_",
                        "double_",
                        "text_",
                        "text2_");

        //convert all columns to string
        for(String columnName : dataset.columns()) {
            dataset = dataset.withColumn(columnName, dataset.col(columnName).cast("string").as(columnName));
        }

        // write imported CSV structure to file for debugging
        if (SparkImporterArguments.getInstance().isWriteStepResultsToCSV()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "import_result");
        }
        
        return dataset;
    }
}
