package de.viadee.bpmnai.core.util.arguments;

import com.beust.jcommander.Parameter;
import de.viadee.bpmnai.core.exceptions.FaultyConfigurationException;
import de.viadee.bpmnai.core.runner.SparkRunner;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.SparkImporterVariables;

/**
 * Configures command line parameters of the import application.
 */
public class CSVImportAndProcessingArguments extends AbstractArguments {

	private static CSVImportAndProcessingArguments CSVImportAndProcessingArguments = null;

	@Parameter(names = { "--file-source",
			"-fs" }, required = true, description = "Path an name of the CSV-File to be processed. You can generate the file with a query such as this one: SELECT *\r\n"
					+ "FROM ACT_HI_PROCINST a\r\n" + "JOIN ACT_HI_VARINST v ON a.PROC_INST_ID_ = v.PROC_INST_ID_ \r\n"
					+ "AND a.proc_def_key_ = 'XYZ' \r\n" + "")
	private String fileSource;

	@Parameter(names = { "--delimiter",
			"-d" }, required = true, description = "Character or string that separates fields such as [ ;,  | or ||| ]. Please make sure that these are not contained in your data.")
	private String delimiter;

	@Parameter(names = { "--revision-count", "-rc" }, description = "Boolean toggle to enable the counting of changes "
			+ "to a variable. It results in a number of columns named <VARIABLE_NAME>_rev.", arity = 1)
	private boolean revisionCount = true;

	@Parameter(names = { "--dev-type-cast-check",
			"-devtcc" }, required = false, description = "Development feature: Check for type casting errors of columns.", arity = 1)
	private boolean devTypeCastCheckEnabled = false;

	@Parameter(names = { "--dev-process-state-column-workaround",
			"-devpscw" }, required = false, description = "Development feature: If the process state column is empty in source data (e.g. due to an older Camunda version) the matching is done on variable name column instead. Only works if data level is process!", arity = 1)
	private boolean devProcessStateColumnWorkaroundEnabled = false;
	
	@Parameter(names = { "--process-filter",
	"-pf" }, required = false, description = "Execute pipeline for a specific processDefinitionId.")
	private String processDefinitionId = null;

	@Parameter(names = { "--output-delimiter",
			"-od" }, required = false, description = "Character or string that separates fields such as [ ;,  | or ||| ] for the written csv file. Please make sure that these are not contained in your data.")
	private String outputDelimiter="|";

	/**
	 * Singleton.
	 */
	private CSVImportAndProcessingArguments() {
	}

	/**
	 * @return DataExtractorArguments-Instanz as Singleton
	 */
	public static CSVImportAndProcessingArguments getInstance() {
		if (CSVImportAndProcessingArguments == null) {
			CSVImportAndProcessingArguments = new CSVImportAndProcessingArguments();
		}
		return CSVImportAndProcessingArguments;
	}

	@Override
	public void createOrUpdateSparkRunnerConfig(SparkRunnerConfig config) {
		super.createOrUpdateSparkRunnerConfig(config);

		config.setRunningMode(SparkRunner.RUNNING_MODE.CSV_IMPORT_AND_PROCESSING);
		config.setSourceFolder(this.fileSource);
		config.setDevTypeCastCheckEnabled(this.devTypeCastCheckEnabled);
		config.setDevProcessStateColumnWorkaroundEnabled(this.devProcessStateColumnWorkaroundEnabled);
		config.setRevCountEnabled(this.revisionCount);
		config.setProcessFilterDefinitionId(this.processDefinitionId);
		config.setDelimiter(this.delimiter);
		config.setOutputDelimiter(this.outputDelimiter);

		this.validateConfig(config);
	}

	@Override
	protected void validateConfig(SparkRunnerConfig config) {
		super.validateConfig(config);

		if(config.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_ACTIVITY)) {
			try {
				throw new FaultyConfigurationException("Date level activity is not supported for CSVImportAndProcessingApplication.");
			} catch (FaultyConfigurationException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}

	@Override
	public String toString() {
		return "CSVImportAndProcessingArguments{" + "fileSource='" + fileSource
				+ '\'' + ", delimiter='" + delimiter
				+ '\'' + ", fileDestination='" + fileDestination
				+ '\'' + ", revisionCount=" + revisionCount
				+ '\'' + ", workingDirectory=" + workingDirectory
				+ '\'' + ", saveMode=" + saveMode
				+ '\'' + ", outputFormat=" + outputFormat
				+ '\'' + ", outputDelimiter='" + outputDelimiter
				+ '\'' + ", devTypeCastCheckEnabled=" + devTypeCastCheckEnabled
				+ '\'' + ", devProcessStateColumnWorkaroundEnabled=" + devProcessStateColumnWorkaroundEnabled
				+ '\'' + ", logDirectory=" + logDirectory + '}';
	}
}
