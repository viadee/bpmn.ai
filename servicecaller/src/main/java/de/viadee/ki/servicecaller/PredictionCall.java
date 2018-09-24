package de.viadee.ki.servicecaller;

import com.google.gson.*;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;

import java.util.logging.Logger;

/**
 * Klasse ruft den Service auf und übergibt ein JSON Objekt. (vllt. hier schon setVariable?)
 *
 */
public class PredictionCall implements JavaDelegate {

    private final static Logger LOGGER = Logger.getLogger("LOAN-REQUESTS");
    private final String SERVICE_ADDRESS = "http://localhost:8080/RESTfulExample/json/product/post";

    public void execute(DelegateExecution execution) throws Exception {

        JSONUtils ju = new JSONUtils();
        String processData = ju.genJSONStr(execution);

        //testing stuff
        LOGGER.warning(execution.getVariables().size() + "");
        LOGGER.warning(processData);


        PostCall pc = new PostCall();
        String prediction = pc.postCall(processData, SERVICE_ADDRESS);

        JsonParser parser = new JsonParser();
        JsonObject predictionObject = parser.parse(prediction).getAsJsonObject();

        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();

        LOGGER.warning(gson.toJson(predictionObject));
    }



}
