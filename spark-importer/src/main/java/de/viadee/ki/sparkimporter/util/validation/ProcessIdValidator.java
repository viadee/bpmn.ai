package de.viadee.ki.sparkimporter.util.validation;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import de.viadee.ki.sparkimporter.util.SparkImporterArguments;

/**
 * Validator for process-id aus {@link SparkImporterArguments} .
 */
public class ProcessIdValidator implements IParameterValidator {

	/**
	 * Validiert that command line parameters do not contain special characters.
	 *
	 * @param name
	 * @param value
	 * @throws ParameterException
	 */
	@Override
	public void validate(String name, String value) throws ParameterException {
		if (value.contains("'")) {
			throw new ParameterException("Parameter " + name + " may not contain quotation marks.");
		}
	}
}
