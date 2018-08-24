package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.ColumnConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReduceColumnsDatasetStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        // write initial column set into a new dataset to be able add them back again later
        List<String> startColumnsString = Arrays.asList(dataset.columns());

        //these columns have to stay in in order to do the processing
        List<String> columnsToKeep = new ArrayList<>();
        columnsToKeep.add(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID);
        columnsToKeep.add(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);
        columnsToKeep.add(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE);
        columnsToKeep.add(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION);
        columnsToKeep.add(SparkImporterVariables.VAR_STATE);
        columnsToKeep.add(SparkImporterVariables.VAR_LONG);
        columnsToKeep.add(SparkImporterVariables.VAR_DOUBLE);
        columnsToKeep.add(SparkImporterVariables.VAR_TEXT);
        columnsToKeep.add(SparkImporterVariables.VAR_TEXT2);

        //if there is no configuration file yet, write columns into the empty one
        if(PreprocessingRunner.initialConfigToBeWritten) {
            Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
            for(String column : startColumnsString) {
                if(!columnsToKeep.contains(column)) {
                    ColumnConfiguration columnConfiguration = new ColumnConfiguration();
                    columnConfiguration.setColumnName(column);
                    columnConfiguration.setUseColumn(true);
                    columnConfiguration.setComment("");
                    configuration.getPreprocessingConfiguration().getColumnConfiguration().add(columnConfiguration);
                }
            }
        }

        List<Row> startColumns = new ArrayList<>();

        for(String column : startColumnsString) {
            startColumns.add(RowFactory.create(column));
        }

        StructType schema = new StructType(new StructField[] {
                new StructField("column_name",
                        DataTypes.StringType, false,
                        Metadata.empty())
        });

        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> startColumnsDataset = sparkSession.createDataFrame(startColumns, schema).toDF();

        // add helper dataset to PreprocessingRunner so we can access it later when adding the columns back
        PreprocessingRunner.helper_datasets.put("startColumns", startColumnsDataset);

        // select only relevant columns to continue
        List<Column> columns = new ArrayList<>();
        columns.add(new Column(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID));
        columns.add(new Column(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME));
        columns.add(new Column(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE));
        columns.add(new Column(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION));
        columns.add(new Column(SparkImporterVariables.VAR_STATE));
        columns.add(new Column(SparkImporterVariables.VAR_LONG));
        columns.add(new Column(SparkImporterVariables.VAR_DOUBLE));
        columns.add(new Column(SparkImporterVariables.VAR_TEXT));
        columns.add(new Column(SparkImporterVariables.VAR_TEXT2));

        if(Arrays.asList(dataset.columns()).contains(SparkImporterVariables.VAR_TIMESTAMP)) {
            columns.add(new Column(SparkImporterVariables.VAR_TIMESTAMP));
        }

        Seq<Column> selectionColumns = SparkImporterUtils.getInstance().asSeq(columns);

        dataset = dataset
                .select(selectionColumns)
                .filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID + " <> 'null'");

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "reduced_columns");
        }



        //return preprocessed data
        return dataset;
    }
}
