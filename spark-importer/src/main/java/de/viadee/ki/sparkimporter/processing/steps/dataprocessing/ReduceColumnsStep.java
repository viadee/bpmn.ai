package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.ColumnConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@PreprocessingStepDescription(name = "Reduce columns", description = "The columns of the input data is reduced to the minimum required for the processing to speed up the processing. The removed columns are added back in the end.")
public class ReduceColumnsStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters, SparkRunnerConfig config) {

        // get dataset structure for type determination
        List<StructField> datasetFields = Arrays.asList(dataset.schema().fields());

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

        if(dataLevel.equals(SparkImporterVariables.DATA_LEVEL_ACTIVITY)) {
            columnsToKeep.add(SparkImporterVariables.VAR_ACT_INST_ID);
            columnsToKeep.add(SparkImporterVariables.VAR_START_TIME);
            columnsToKeep.add(SparkImporterVariables.VAR_END_TIME);
            columnsToKeep.add(SparkImporterVariables.VAR_DURATION);
        }

        //if there is no configuration file yet, write columns into the empty one
        if(config.isInitialConfigToBeWritten()) {
            Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(config);
            for(String column : startColumnsString) {
                if(!columnsToKeep.contains(column)) {
                    ColumnConfiguration columnConfiguration = new ColumnConfiguration();
                    columnConfiguration.setColumnName(column);
                    columnConfiguration.setColumnType(getColumnTypeString(datasetFields, column));
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
        PreprocessingRunner.helper_datasets.put("startColumns" + "_" + dataLevel, startColumnsDataset);

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

        if(dataLevel.equals(SparkImporterVariables.DATA_LEVEL_ACTIVITY)) {
            columns.add(new Column(SparkImporterVariables.VAR_ACT_INST_ID));
            columns.add(new Column(SparkImporterVariables.VAR_START_TIME));
            columns.add(new Column(SparkImporterVariables.VAR_END_TIME));
            columns.add(new Column(SparkImporterVariables.VAR_DURATION));
        }

        if(Arrays.asList(dataset.columns()).contains(SparkImporterVariables.VAR_TIMESTAMP)) {
            columns.add(new Column(SparkImporterVariables.VAR_TIMESTAMP));
        }

        Seq<Column> selectionColumns = SparkImporterUtils.getInstance().asSeq(columns);

        dataset = dataset
                .select(selectionColumns)
                .filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID + " <> 'null'");

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "reduced_columns", config);
        }



        //return preprocessed data
        return dataset;
    }

    private String getColumnTypeString(List<StructField> datasetFields, String column) {

        DataType currentDatatype = DataTypes.StringType;

        // search current datatype
        for(StructField sf : datasetFields) {
            if(sf.name().equals(column)) {
                currentDatatype = sf.dataType();
                break;
            }
        }

        //determine string representation
        if(currentDatatype.equals(DataTypes.IntegerType)) {
            return "integer";
        } else if(currentDatatype.equals(DataTypes.LongType)) {
            return "long";
        } else if(currentDatatype.equals(DataTypes.DoubleType)) {
            return "double";
        } else if(currentDatatype.equals(DataTypes.BooleanType)) {
            return "boolean";
        } else if(currentDatatype.equals(DataTypes.TimestampType)) {
            return "timestamp";
        } else if(currentDatatype.equals(DataTypes.DateType)) {
            return "date";
        } else if(currentDatatype.equals(DataTypes.FloatType)) {
            return "float";
        } else {
            return "string";
        }
    }
}
