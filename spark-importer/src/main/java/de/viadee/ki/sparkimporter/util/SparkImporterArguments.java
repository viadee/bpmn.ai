package de.viadee.ki.sparkimporter.util;

import com.beust.jcommander.Parameter;
import de.viadee.ki.sparkimporter.util.validation.ParsingMethodValidator;
import de.viadee.ki.sparkimporter.util.validation.ProcessIdValidator;

/**
 * Wrapper für die Kommandozeilenargumente des Programmes.
 */
public class SparkImporterArguments {

    private static SparkImporterArguments sparkImporterArguments = null;

    public static final String ID_ALL = "all";

    public static final String DEFAULT_ENCLOSING = "";

    public static final String PARSING_METHOD_PROCESS = "process";

    public static final String PARSING_METHOD_ACTIVITY = "activity";

    @Parameter(names = {"--parsing-method", "-pm"},
            description = "Angabe der Parsing Methode.",
            validateWith = ParsingMethodValidator.class)
    private String parsingMethod = PARSING_METHOD_PROCESS;

    @Parameter(names = {"--process-processId", "-pid"},
            description = "ID der Prozessinstanz die verarbeitet werden soll. " +
                    "Die Variable darf keine Anführungszeichen (') enthalten.",
            validateWith = ProcessIdValidator.class)
    private String processId = ID_ALL;

    @Parameter(names = {"--file-source", "-fs"}, required = true,
            description = "Datei die eingelesen werden soll.")
    private String fileSource;

    @Parameter(names = {"--delimiter", "-d"}, required = true,
            description = "Trennzeichen das zwischen den Werten verwendet wird.")
    private String delimiter;

    @Parameter(names = {"--line-delimiter", "-ld"}, required = false,
            description = "Trennzeichen das zwischen den Zeilen verwendet wird.")
    private String lineDelimiter;

    @Parameter(names = {"--file-destination", "-fd"}, required = true,
            description = "Datei die geschrieben werden soll.")
    private String fileDestination;

    @Parameter(names = {"--enclosing", "-e"},
            description = "Umschließendes Zeichen für die Werte.")
    private String enclosing = DEFAULT_ENCLOSING;

    @Parameter(names = {"--revision-count", "-rc"}, description = "Angabe ob die Revisionen einer Variable " +
            "als Zähler exportiert werden sollen. Die Zählspalte besitzt den Namen <VARIABLEN_NAME>_rev.", arity = 1)
    private boolean revisionCount = true;

    @Parameter(names = {"--step-results", "-sr"}, description = "Should step results be written into CSV files?")
    private boolean writeStepResultsToCSV = false;

    /**
     * Privater Konstruktor um DataExtractorArguments als Singleton abzubilden
     */
    private SparkImporterArguments() {
    }

    public boolean isRevisionCount() {
        return revisionCount;
    }

    public String getParsingMethod() {
        return parsingMethod;
    }

    public String getProcessId() {
        return processId;
    }

    public String getFileSource() {
        return fileSource;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public String getFileDestination() {
        return fileDestination;
    }

    public String getEnclosing() {
        return enclosing;
    }

    public boolean isWriteStepResultsToCSV() {
        return writeStepResultsToCSV;
    }

    public void setWriteStepResultsToCSV(boolean writeStepResultsToCSV) {
        this.writeStepResultsToCSV = writeStepResultsToCSV;
    }

    /**
     * @return DataExtractorArguments-Instanz als Singleton
     */
    public static SparkImporterArguments getInstance() {
        if (sparkImporterArguments == null) {
            sparkImporterArguments = new SparkImporterArguments();
        }
        return sparkImporterArguments;
    }

    /**
     * Zurücksetzen der Singleton-Instanz
     */
    public static void clearInstance() {
        sparkImporterArguments = null;
    }

    @Override
    public String toString() {
        return "SpringImporterArguments{" +
                "parsingMethod='" + parsingMethod + '\'' +
                ", processId='" + processId + '\'' +
                ", fileSource='" + fileSource + '\'' +
                ", delimiter='" + delimiter + '\'' +
                ", lineDelimiter='" + lineDelimiter + '\'' +
                ", fileDestination='" + fileDestination + '\'' +
                ", enclosing='" + enclosing + '\'' +
                ", revisionCount=" + revisionCount +
                '}';
    }
}
