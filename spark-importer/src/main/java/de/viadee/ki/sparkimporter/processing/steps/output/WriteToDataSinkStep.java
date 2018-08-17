package de.viadee.ki.sparkimporter.processing.steps.output;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class WriteToDataSinkStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {
        dataset
                .repartition(dataset.col("proc_inst_id_"))
                .write()
                .mode(SaveMode.Append)
                .save("file:///Users/mim/Desktop/spark_sink");

        return dataset;
    }
}
