package de.viadee.ki.sparkimporter.runner;

import org.apache.spark.sql.SparkSession;

public interface ImportRunnerInterface {

    void run(SparkSession sparkSession);

}
