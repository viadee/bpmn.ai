package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class DataFilterOnActivityStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {
        //any parameters set?
        if (parameters == null || parameters.size() == 0) {
            SparkImporterLogger.getInstance().writeWarn("No parameters found for the DataFilterOnActivityStep");
            return dataSet;
        }
        System.out.println(parameters.toString());
        //get query parameter
        String query = (String) parameters.get("query");
        SparkImporterLogger.getInstance().writeInfo("Filtering data with activity instance filter query: " + query + ".");

        Dataset<Row> variables = dataSet.filter(col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).isNotNull());
        //find first occurrence of activity instance
        final Dataset<Row> dsTmp = dataSet.filter(dataSet.col(SparkImporterVariables.VAR_ACT_ID).equalTo(query)).filter(dataSet.col(SparkImporterVariables.VAR_END_TIME).isNull()); //TODO: ENSURING THAT THIS ISN'T A VARIABLE ROW

        List<Row> activityRows = dsTmp.select(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_START_TIME).collectAsList();
        Map<String,String> activities = activityRows.stream().collect(Collectors.toMap(
                r -> r.getAs(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID),r -> r.getAs(SparkImporterVariables.VAR_START_TIME)));
        SparkBroadcastHelper.getInstance().broadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_INSTANCE_TIMESTAMP_MAP, activities);

        System.out.println(dataSet.count());

        Dataset<Row> activityDataSet = dataSet.withColumn("data_filter_on_activity", callUDF("activityBeforeTimestamp",dsTmp.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID),dsTmp.col(SparkImporterVariables.VAR_START_TIME)));
        activityDataSet = activityDataSet.filter(col("data_filter_on_activity").like("TRUE"));
        activityDataSet = activityDataSet.drop("data_filter_on_activity");

        System.out.println("ACT: " +activityDataSet.count());
        System.out.println("VAR: "+variables.count());

        activityDataSet = activityDataSet.withColumnRenamed(SparkImporterVariables.VAR_ACT_INST_ID, SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT");

        variables = variables.join(activityDataSet.select(SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT").distinct(), variables.col(SparkImporterVariables.VAR_ACT_INST_ID).equalTo(activityDataSet.col(SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT")),"inner");

        SparkImporterUtils.getInstance().writeDatasetToCSV(variables, "data_filter_on_activity_step");

        activityDataSet = activityDataSet.withColumnRenamed(SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT", SparkImporterVariables.VAR_ACT_INST_ID);
        variables = variables.drop(SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT");
        System.out.println("VAR+ACT: "+variables.count());
        dataSet = activityDataSet.union(variables);
        System.out.println("DS: "+dataSet.count());


        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataSet, "data_filter_on_activity_step");
        }


        //first iteration to collect all columns to be added

   //     Dataset<Row> filteredDataset = dsTmp.flatMap((FlatMapFunction<Row, Row>) row -> {
//
//            // get start time for row
//            Timestamp startTime = new Timestamp(Long.parseLong(row.getAs(SparkImporterVariables.VAR_START_TIME)));
//
//           System.out.println(finalDataset.columns());
//            // filter dataset by process instance id and < start time and activity type not null
//            Dataset<Row> matchedRows = finalDataset.filter(
//                    finalDataset.col(SparkImporterVariables.VAR_START_TIME).isNotNull())
//                    .filter(finalDataset.col(SparkImporterVariables.VAR_START_TIME).lt(startTime))
//                    .filter(finalDataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID).isNotNull());
//
//
//            return matchedRows.toLocalIterator();
//        }, RowEncoder.apply(finalDataset.schema()));
//
//
//        //return preprocessed data
        return dataSet;


    }
}
