package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.ColumnConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import de.viadee.ki.sparkimporter.util.helper.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.logging.SparkImporterLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@PreprocessingStepDescription(name = "Type cast", description = "In this step the columns are casted into the data type they have been defined in the configuration. If the cast could not be done by Spark the value is null afterwards.")
public class TypeCastStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

        // get variables
        Map<String, String> varMap = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED);

        List<StructField> datasetFields = Arrays.asList(dataset.schema().fields());

        List<ColumnConfiguration> columnConfigurations = null;
        List<VariableConfiguration> variableConfigurations = null;

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(config);
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            columnConfigurations = preprocessingConfiguration.getColumnConfiguration();
            variableConfigurations = preprocessingConfiguration.getVariableConfiguration();
        }

        Map<String, ColumnConfiguration> columnTypeConfigMap = new HashMap<>();
        Map<String, VariableConfiguration> variableTypeConfigMap = new HashMap<>();

        if(columnConfigurations != null) {
            for(ColumnConfiguration cc : columnConfigurations) {
                columnTypeConfigMap.put(cc.getColumnName(), cc);
            }
        }

        if(variableConfigurations != null) {
            for(VariableConfiguration vc : variableConfigurations) {
                variableTypeConfigMap.put(vc.getVariableName(), vc);
            }
        }

        for(String column : dataset.columns()) {

            // skip revision columns as they are handled for each variable column
            if(column.endsWith("_rev")) {
                continue;
            }

            DataType newDataType = null;
            boolean isVariableColumn  = false;
            String configurationDataType = null;
            String configurationParseFormat = null;

            if(variableTypeConfigMap.keySet().contains(column)) {
                // was initially a variable
                configurationDataType = variableTypeConfigMap.get(column).getVariableType();
                configurationParseFormat = variableTypeConfigMap.get(column).getParseFormat();
                if (config.getPipelineMode().equals(SparkImporterVariables.PIPELINE_MODE_LEARN)) {
                    isVariableColumn = varMap.keySet().contains(column);
                } else {
                    isVariableColumn = true;
                }
            } else if(columnTypeConfigMap.keySet().contains(column)){
                // was initially a column
                configurationDataType = columnTypeConfigMap.get(column).getColumnType();
                configurationParseFormat = columnTypeConfigMap.get(column).getParseFormat();
            }

            newDataType = mapDataType(datasetFields, column, configurationDataType);

            // only check for cast errors if dev feature is enabled and if a change in the datatype has been done
            if(config.isDevTypeCastCheckEnabled() && !newDataType.equals(getCurrentDataType(datasetFields, column))) {
                // add a column with casted value to be able to check the cast results
                dataset = castColumn(dataset, column, column+"_casted", newDataType, configurationParseFormat);

                // add a column for cast results and write CAST_ERROR? in it if there might be a cast error
                dataset = dataset.withColumn(column+"_castresult",
                        when(dataset.col(column).isNotNull().and(dataset.col(column).notEqual(lit(""))),
                                when(dataset.col(column+"_casted").isNull(), lit("CAST_ERROR?"))
                                        .otherwise(lit(""))
                        ).otherwise(lit(""))
                );

                // check for cast errors and write warning to application log
                if(dataset.filter(column+"_castresult == 'CAST_ERROR?'").count() > 0) {
                    SparkImporterLogger.getInstance().writeWarn("Column '" + column + "' seems to have cast errors. Please check the data type (is defined as '" + configurationDataType + "')");
                } else {
                    // drop help columns as there are no cast errors for this column and rename casted column to actual column name
                    dataset = dataset.drop(column, column+"_castresult").withColumnRenamed(column+"_casted", column);
                }
            } else {
                // cast without checking the cast result, entries are null is spark can't cast it
                dataset = castColumn(dataset, column, column, newDataType, configurationParseFormat);
            }

            // cast revision columns for former variables, revisions columns only exist on process level
            if(config.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS) && config.isRevCountEnabled() && isVariableColumn) {
                dataset = dataset.withColumn(column+"_rev", dataset.col(column+"_rev").cast("integer"));
            }
        }

        if(config.isWriteStepResultsIntoFile()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "type_cast_columns", config);
        }

        //return preprocessed data
        return dataset;
    }

    private Dataset castColumn(Dataset<Row> dataset, String columnToCast, String castColumnName, DataType newDataType, String parseFormat) {

        Dataset<Row> newDataset = dataset;

        if(newDataType.equals(DataTypes.DateType)) {
            if(parseFormat != null && !parseFormat.equals("")) {
                // parse format given in config, so use it
                newDataset = dataset.withColumn(castColumnName, when(callUDF("isalong", dataset.col(columnToCast)), to_date(from_unixtime(callUDF("timestampstringtolong", dataset.col(columnToCast))), parseFormat)).otherwise(to_date(dataset.col(columnToCast), parseFormat)));
            } else {
                newDataset = dataset.withColumn(castColumnName, when(callUDF("isalong", dataset.col(columnToCast)), to_date(from_unixtime(callUDF("timestampstringtolong", dataset.col(columnToCast))))).otherwise(to_date(dataset.col(columnToCast))));
            }
        } else if(newDataType.equals(DataTypes.TimestampType)) {
            if(parseFormat != null && !parseFormat.equals("")) {
                // parse format given in config, so use it
                newDataset = dataset.withColumn(castColumnName, when(callUDF("isalong", dataset.col(columnToCast)), to_timestamp(from_unixtime(callUDF("timestampstringtolong", dataset.col(columnToCast))), parseFormat)).otherwise(to_timestamp(dataset.col(columnToCast), parseFormat)));
            } else {
                newDataset = dataset.withColumn(castColumnName, when(callUDF("isalong", dataset.col(columnToCast)), to_timestamp(from_unixtime(callUDF("timestampstringtolong", dataset.col(columnToCast))))).otherwise(to_timestamp(dataset.col(columnToCast))));
            }
        } else {
            newDataset = dataset.withColumn(castColumnName, dataset.col(columnToCast).cast(newDataType));
        }

        return newDataset;
    }

    private DataType getCurrentDataType(List<StructField> datasetFields, String column) {

        // search current datatype
        for(StructField sf : datasetFields) {
            if(sf.name().equals(column)) {
                return sf.dataType();
            }
        }

        return null;
    }

    private DataType mapDataType(List<StructField> datasetFields, String column, String typeConfig) {

        DataType currentDatatype = getCurrentDataType(datasetFields, column);

        // when typeConfig is null (no config for this column), return the current DataType
        if(typeConfig == null) {
            return currentDatatype;
        }

        switch (typeConfig) {
            case "integer":
                return DataTypes.IntegerType;
            case "long":
                return DataTypes.LongType;
            case "double":
                return DataTypes.DoubleType;
            case "boolean":
                return DataTypes.BooleanType;
            case "date":
                return DataTypes.DateType;
            case "timestamp":
                return DataTypes.TimestampType;
            default:
                return DataTypes.StringType;
        }
    }
}
