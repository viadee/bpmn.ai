package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.when;

public class FillActivityInstancesHistoryStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        // define window for filling values
        WindowSpec windowSpec = Window
                .partitionBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)
                .orderBy(SparkImporterVariables.VAR_START_TIME)
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        Map<String, String> varMap = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED);
        Set<String> variables = varMap.keySet();

        for(String v : variables) {
            // make empty values actually null
            dataset = dataset.withColumn(v, when(dataset.col(v).equalTo(""), null).otherwise(dataset.col(v)));
        }

        for(String v : variables) {
            // fill up values
            Column filled_column = first(dataset.col(v), true).over(windowSpec);
            dataset = dataset.withColumn(v + "_filled", filled_column);

            // drop original column and rename filled column to original
            dataset = dataset.drop(v).withColumnRenamed(v + "_filled", v);

            //TODO: also fill revisions
//            if(SparkImporterVariables.isRevCountEnabled()) {
//                dataset = dataset.withColumn(v+"_rev",
//                        when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(v), dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
//                                .otherwise("0"));
//            }
        }

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "fill_activity_instances_history");
        }

        //return preprocessed data
        return dataset;
    }
}
