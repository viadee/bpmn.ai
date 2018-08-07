package de.viadee.ki.sparkimporter.util.validation;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;

/**
 * Validator für Parameter parsing-method in {@link SparkImporterArguments}, dass nur bestimmte Werte angenommen werden
 */
public class ParsingMethodValidator implements IParameterValidator {

    /**
     * Validiert dass Parameter parsing-method nur die vordefinierten Werte annehmen darf. Akzeptiert werden
     * {@link SparkImporterArguments#PARSING_METHOD_PROCESS} und {@link SparkImporterArguments#PARSING_METHOD_ACTIVITY}
     *
     * @param name  Name des Parameters parsing-method
     * @param value Wert des Parameters parsing-method
     * @throws ParameterException Wenn Wert nicht den vordefinerten Werten entspricht.
     */
    @Override
    public void validate(String name, String value) throws ParameterException {
        if (!(value.equals(SparkImporterArguments.PARSING_METHOD_PROCESS)
                || value.equals(SparkImporterArguments.PARSING_METHOD_ACTIVITY))) {
            throw new ParameterException("Parameter " + name + " kann nur \""
                    + SparkImporterArguments.PARSING_METHOD_PROCESS + "\" und \""
                    + SparkImporterArguments.PARSING_METHOD_ACTIVITY + "\" als Werte annehmen");
        }

    }
}
