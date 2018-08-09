package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;

public class FilterOutEmptyVariableLinesStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        //filter out empty variable lines from dataset
        Dataset<Row> noEmptyLinesDataset = dataset.filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME+" <> 'null'");

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(noEmptyLinesDataset, "no_empty_variable_lines");
        }

        //returning cleaned dataset
        return noEmptyLinesDataset;
    }
}
