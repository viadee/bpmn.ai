package de.viadee.ki.sparkimporter.importing;

import de.viadee.ki.sparkimporter.exceptions.NoDataImporterDefinedException;
import de.viadee.ki.sparkimporter.importing.interfaces.DataImporterInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataImportRunner {

    private DataImporterInterface dataImporter;

    private static DataImportRunner instance;

    private DataImportRunner(){}

    public static synchronized DataImportRunner getInstance(){
        if(instance == null){
            instance = new DataImportRunner();
        }
        return instance;
    }

    public void setDataImporter(DataImporterInterface dataImporter) {
        this.dataImporter = dataImporter;
    }

    public Dataset<Row> runImport(SparkSession sparkSession) throws NoDataImporterDefinedException {
        if(this.dataImporter == null) {
            throw new NoDataImporterDefinedException();
        } else {
            return this.dataImporter.importData(sparkSession);
        }
    }
}
