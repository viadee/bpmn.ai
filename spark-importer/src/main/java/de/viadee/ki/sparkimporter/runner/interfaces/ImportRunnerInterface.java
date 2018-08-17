package de.viadee.ki.sparkimporter.runner.interfaces;

import org.apache.spark.sql.SparkSession;

public interface ImportRunnerInterface {

    void run(SparkSession sparkSession);

}
