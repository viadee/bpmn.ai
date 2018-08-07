package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStep;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;

public class GetVariablesCountStep implements PreprocessingStep {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> initialDataSet, boolean writeStepResultIntoFile) {

        //Determine number of process variables per instance and start the ones having the most
        Dataset<Row> variablesCountDataset = initialDataSet.groupBy("proc_inst_id_")
                .agg(count("name_").alias("variables_count")).
                        orderBy(desc("variables_count"));

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(variablesCountDataset, "variables_count");
        }

        //just analysing the data
        return initialDataSet;
    }
}
