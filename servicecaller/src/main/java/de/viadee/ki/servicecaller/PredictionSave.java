package de.viadee.ki.servicecaller;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.jvnet.hk2.annotations.Service;
import java.util.logging.Logger;

/**
 * Klasse soll die Änderungen durch eine Log Warning hervorheben
 *
 */

@Service
public class PredictionSave implements JavaDelegate {

	private final static Logger LOGGER = Logger.getLogger("LOAN-REQUESTS");


	public void execute(DelegateExecution execution) throws Exception {

		JSONUtils ju = new JSONUtils();
		String processVariables = ju.genJSONStr(execution);

		LOGGER.warning("Variablenanzahl im Prozess: " + execution.getVariables().size() + "");
		LOGGER.warning("JSON-Darstellung der Prozessvariablen: ");
		LOGGER.warning(processVariables);
	}

}
