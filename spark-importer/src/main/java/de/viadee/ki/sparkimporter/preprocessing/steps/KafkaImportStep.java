package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
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

        //create dataset for processInstance stream data
        Dataset<Row> dspi = dataset.filter("topic == 'processInstance'");

        //create dataset for variableUpdate stream data
        Dataset<Row> dsvu = dataset.filter("topic == 'variableUpdate'");


        //rename variableUpdate columns before merge
        Dataset<Row> dsvu_ren = dsvu
                .withColumnRenamed("variableName","name_")
                .withColumnRenamed("longValue","long_")
                .withColumnRenamed("doubleValue","double_")
                .withColumnRenamed("textValue","text_")
                .withColumnRenamed("complexValue","text2_")
                .withColumnRenamed("serializerName","var_type_")
                .withColumnRenamed("revision","rev_");

        //merge
        for(String columnName : dspi.columns()) {
            dspi = dspi.withColumnRenamed(columnName, columnName.replaceAll("([A-Z])","_$1").concat("_").toLowerCase());
        }

        Dataset<Row> merge = dspi.withColumnRenamed("process_instance_id_","processInstanceId").join(dsvu_ren, "processInstanceId");

        //show result
        dataset = merge
                .select("id_",
                        "processInstanceId",
                        "business_key_",
                        "process_definition_key_",
                        "process_definition_id_",
                        "start_time_",
                        "end_time_",
                        "duration_in_millis_",
                        "state_",
                        "name_",
                        "var_type_",
                        "rev_",
                        "long_",
                        "double_",
                        "text_",
                        "text2_")
                .withColumnRenamed("processInstanceId", "proc_inst_id_")
                .withColumnRenamed("duration_in_millis_", "duration_");

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
