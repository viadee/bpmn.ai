package de.viadee.ki.sparkimporter.util;

import com.beust.jcommander.Parameter;
import de.viadee.ki.sparkimporter.util.validation.ParsingMethodValidator;
import de.viadee.ki.sparkimporter.util.validation.ProcessIdValidator;

/**
 * Configures command line parameters of the import application.
 */
public class SparkImporterArguments {

	private static SparkImporterArguments sparkImporterArguments = null;

	private static final String ID_ALL = "all";

	public static final String DEFAULT_ENCLOSING = "";

	public static final String PARSING_METHOD_PROCESS = "process";

	public static final String PARSING_METHOD_ACTIVITY = "activity";

	@Parameter(names = { "--parsing-method",
			"-pm" }, description = "Chooses a parsing method (either process or activity) and thereby the level of analysis. ", validateWith = ParsingMethodValidator.class)
	private String parsingMethod = PARSING_METHOD_PROCESS;

	@Parameter(names = { "--process-processId",
			"-pid" }, description = "Optional parameter to only process a single process instance. ", validateWith = ProcessIdValidator.class)
	private String processId = ID_ALL;

	@Parameter(names = { "--file-source",
			"-fs" }, required = true, description = "Path an name of the CSV-File to be processed. You can generate the file with a query such as this one: SELECT *\r\n"
					+ "FROM ACT_HI_PROCINST a\r\n" + "JOIN ACT_HI_VARINST v ON a.PROC_INST_ID_ = v.PROC_INST_ID_ \r\n"
					+ "AND a.proc_def_key_ = 'XYZ' \r\n" + "")
	private String fileSource;

	@Parameter(names = { "--delimiter",
			"-d" }, required = true, description = "Character or string that separates fields such as [ ;,  | or ||| ]. Please make sure that these are not contained in your data.")
	private String delimiter;

	@Parameter(names = { "--line-delimiter",
			"-ld" }, required = false, description = "The line delimiter of the csv file to be imported.")
	private String lineDelimiter;

	@Parameter(names = { "--file-destination",
			"-fd" }, required = true, description = "The name of the target csd file, i.e. the data mining table.")
	private String fileDestination;

	@Parameter(names = { "--enclosing",
			"-e" }, description = "Enclosing characters around fields such as [\"]. For this particular case, please take care of special character escaping by prefixing a backslash.")
	private String enclosing = DEFAULT_ENCLOSING;

	@Parameter(names = { "--revision-count", "-rc" }, description = "Boolean toggle to enable the counting of changes "
			+ "to a variable. It results in a number of columns named <VARIABLE_NAME>_rev.", arity = 1)
	private boolean revisionCount = true;

	@Parameter(names = { "--step-results",
			"-sr" }, description = "Should intermediate results be written into CSV files?", arity = 1)
	private boolean writeStepResultsToCSV = false;

	/**
	 * Singleton.
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
	 * @return DataExtractorArguments-Instanz as Singleton
	 */
	public static SparkImporterArguments getInstance() {
		if (sparkImporterArguments == null) {
			sparkImporterArguments = new SparkImporterArguments();
		}
		return sparkImporterArguments;
	}

	@Override
	public String toString() {
		return "SpringImporterArguments{" + "parsingMethod='" + parsingMethod + '\'' + ", processId='" + processId
				+ '\'' + ", fileSource='" + fileSource + '\'' + ", delimiter='" + delimiter + '\'' + ", lineDelimiter='"
				+ lineDelimiter + '\'' + ", fileDestination='" + fileDestination + '\'' + ", enclosing='" + enclosing
				+ '\'' + ", revisionCount=" + revisionCount + '}';
	}
}
