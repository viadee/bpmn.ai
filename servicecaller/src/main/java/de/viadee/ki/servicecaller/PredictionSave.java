package de.viadee.ki.servicecaller;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Klasse soll die Prediction vom Service entgegennehmen und als Variable abspeichern mittels setVariable.
 *
 */
public class PredictionSave implements JavaDelegate {

	private final static Logger LOGGER = Logger.getLogger("LOAN-REQUESTS");


	public void execute(DelegateExecution execution) throws Exception {
		
		Random rand = new Random();
	
		execution.setVariable("schufa-approved", rand.nextBoolean());

		JSONUtils ju = new JSONUtils();
		String processVariables = ju.genJSONStr(execution);

		LOGGER.warning(execution.getVariables().size() + "");
		LOGGER.warning(processVariables);
	}

}
