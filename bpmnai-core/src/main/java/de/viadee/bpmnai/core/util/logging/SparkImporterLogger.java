package de.viadee.bpmnai.core.util.logging;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class SparkImporterLogger {

    private static Logger appLogger;
    private static FileHandler logFileHandler = null;

    private static final String LOG_FILE_NAME = "bpmnai-core.log";

    private String logDirectory = ".";

    private static SparkImporterLogger instance;

    private SparkImporterLogger(){
        setupLogger();
    }

    private void setupLogger() {
        File logDirectory  = new File(getLogDirectory());
        if(!logDirectory.exists()) {
            logDirectory.mkdir();
        }
        if(logFileHandler != null) {
            logFileHandler.close();
        }
        try {

            logFileHandler = new FileHandler(getLogDirectory()+"/"+LOG_FILE_NAME);

            logFileHandler.setFormatter(new SimpleFormatter() {
                private static final String format = "[%1$tF %1$tT] [%2$-7s] %3$s %n";

                @Override
                public synchronized String format(LogRecord lr) {
                    return String.format(format,
                            new Date(lr.getMillis()),
                            lr.getLevel().getLocalizedName(),
                            lr.getMessage()
                    );
                }
            });

            appLogger = Logger.getLogger("de.viadee.bpmnai.core");
            appLogger.addHandler(logFileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static synchronized SparkImporterLogger getInstance(){
        if(instance == null){
            instance = new SparkImporterLogger();
        }
        return instance;
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public void setLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
        setupLogger();
    }

    public void writeInfo(String message) {
        appLogger.info(message);
    }

    public void writeWarn(String message) {
        appLogger.warning(message);
    }

    public void writeError(String message) {
        appLogger.severe(message);
    }
}
