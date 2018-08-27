package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.ColumnConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.ColumnHashConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.util.*;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sha1;
import static org.apache.spark.sql.functions.when;

public class TypeCastStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        List<StructField> datasetFields = Arrays.asList(dataset.schema().fields());

        dataset.printSchema();

        //List<ColumnConfiguration> columnConfigurations = null;
        List<VariableConfiguration> variableConfigurations = null;

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            //columnConfigurations = preprocessingConfiguration.getColumnConfiguration();
            variableConfigurations = preprocessingConfiguration.getVariableConfiguration();
        }

        Map<String, String> columnTypeConfigMap = new HashMap<>();
        Map<String, String> variableTypeConfigMap = new HashMap<>();

        if(variableConfigurations != null) {
            for(VariableConfiguration vc : variableConfigurations) {
                variableTypeConfigMap.put(vc.getVariableName(), vc.getVariableType());
            }
        }

        for(String column : dataset.columns()) {
            if(variableTypeConfigMap.keySet().contains(column)) {
                // was initially a variable
                DataType newDataType = mapDataType(dataset, datasetFields, column, variableTypeConfigMap.get(column));

                dataset = dataset.withColumn(column, dataset.col(column).cast(newDataType).as(column));

                if(SparkImporterArguments.getInstance().isRevisionCount()) {
                    dataset = dataset.withColumn(column+"_rev", dataset.col(column+"_rev").cast("integer").as(column+"_rev"));
                }

            } else {
                // was initially a column
                //mapDataType(dataset, datasetFields, columnn, null);
            }
        }

        dataset.printSchema();

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "cast_columns");
        }

        //return preprocessed data
        return dataset;
    }

    private DataType mapDataType(Dataset<Row> dataset, List<StructField> datasetFields, String column, String typeConfig) {

        DataType currentDatatype = DataTypes.StringType;

        // search current datatype
        for(StructField sf : datasetFields) {
            if(sf.name().equals(column)) {
                currentDatatype = sf.dataType();
            }
            break;
        }


        if(currentDatatype.equals(DataTypes.StringType)) {
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
                    return DataTypes.TimestampType;
                default:
                    return DataTypes.StringType;
            }
        } else {
            return currentDatatype;
        }
    }
}
