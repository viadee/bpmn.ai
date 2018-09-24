package de.viadee.ki.servicecaller;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;

import java.util.logging.Logger;

/**
 * Klasse ruft den Service auf und übergibt ein JSON Objekt. (vllt. hier schon setVariable?)
 *
 */
public class PredictionCall implements JavaDelegate {

    private final static Logger LOGGER = Logger.getLogger("LOAN-REQUESTS");

    public void execute(DelegateExecution execution) throws Exception {

        JSONUtils ju = new JSONUtils();
        String processVariables = ju.genJSONStr(execution);

        LOGGER.warning(execution.getVariables().size() + "");
        LOGGER.warning(processVariables);



    }



}
