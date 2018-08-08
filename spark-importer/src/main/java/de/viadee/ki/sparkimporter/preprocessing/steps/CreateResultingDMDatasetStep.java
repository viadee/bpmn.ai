package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.count;

public class CreateResultingDMDatasetStep implements PreprocessingStepInterface {

    private static final Logger LOG = LoggerFactory.getLogger(CreateResultingDMDatasetStep.class);


    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> initialDataSet, boolean writeStepResultIntoFile) {

        //get all distinct variable names
        Dataset<Row> variableNames = initialDataSet.select("name_", "var_type_")
                .groupBy("name_","var_type_")
                .agg(count("*"))
                .select("name_","var_type_")
                .distinct();


        //create List for new column names
        List<StructField> columnsOfMlDataset = new ArrayList<>();

        //write column names into list
        variableNames.foreach(row -> {
            columnsOfMlDataset.add(DataTypes.createStructField(row.getString(0), DataTypes.StringType, true));
        });

        List<Row> data = new ArrayList<>();
        StructType schema = new StructType(columnsOfMlDataset.toArray(new StructField[columnsOfMlDataset.size()]));
        data.add(RowFactory.create(new String[columnsOfMlDataset.size()]));

        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> newDataset = sparkSession.createDataFrame(data, schema).toDF();

        newDataset.show();

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(newDataset, "new_dataset");
        }

        //returning prepocessed dataset
        return newDataset;
    }
}
