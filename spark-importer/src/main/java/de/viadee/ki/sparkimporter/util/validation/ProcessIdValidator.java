package de.viadee.ki.sparkimporter.util.validation;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;

/**
 * Validator für process-id aus {@link SparkImporterArguments} um sicherzustellen,
 * dass keine Sonderzeichen enthalten sind.
 */
public class ProcessIdValidator implements IParameterValidator {

    /**
     * Validiert dass Parameter keine Sonderzeichen enthält. Konkrekt wird überprüft, dass keine
     * Anführungszeichen enthalten sind.
     *
     * @param name
     * @param value
     * @throws ParameterException
     */
    @Override
    public void validate(String name, String value) throws ParameterException {
        if (value.contains("'")) {
            throw new ParameterException("Parameter " + name + " darf keine Anführungszeichen enthalten.");
        }
    }
}
