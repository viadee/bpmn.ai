package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AggregateToProcessInstanceaStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        Map<String, String> aggregationMap = new HashMap<>();
        for(String column : dataset.columns()) {
            aggregationMap.put(column, "max");
        }
        dataset = dataset.groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID).agg(aggregationMap);

        //cleanup, so renaming columns and dropping not used ones
        dataset = dataset.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID);

        String pattern = "max\\((.+)\\)";
        Pattern r = Pattern.compile(pattern);

        for(String columnName : dataset.columns()) {
            Matcher m = r.matcher(columnName);
            if(m.find()) {
                String newColumnName = m.group(1);
                dataset = dataset.withColumnRenamed(columnName, newColumnName);
            }
        }


        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "agg_to_process_instance");
        }

        //return preprocessed data
        return dataset;
    }
}
