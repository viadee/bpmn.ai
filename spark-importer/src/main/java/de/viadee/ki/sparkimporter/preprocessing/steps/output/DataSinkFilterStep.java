package de.viadee.ki.sparkimporter.preprocessing.steps.output;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataSinkFilterStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        return dataset;//.filter("proc_inst_id_ == '0ed3e612-b9e7-11e5-8d7d-005056a04102'");
    }
}
