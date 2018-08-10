package de.viadee.ki.sparkimporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.exceptions.NoDataImporterDefinedException;
import de.viadee.ki.sparkimporter.importing.DataImportRunner;
import de.viadee.ki.sparkimporter.importing.implementations.CSVDataImporter;
import de.viadee.ki.sparkimporter.preprocessing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.preprocessing.steps.*;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterCache;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SparkImporterApplicationTest {



}
