package de.viadee.ki.servicecaller;

import com.google.gson.*;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

/**
 * Klasse ruft den Service auf und übergibt ein JSON Objekt. (vllt. hier schon setVariable?)
 *
 */

@Service
public class PredictionCall implements JavaDelegate {

    private final static Logger LOGGER = Logger.getLogger("LOAN-REQUESTS");
    private final static String SERVICE_ADDRESS = "http://192.168.42.14:8090/services/ki/predict";

    public void execute(DelegateExecution execution) throws Exception {

        JSONUtils ju = new JSONUtils();
        String processData = ju.genJSONStr(execution);

        //testing stuff
        LOGGER.warning(execution.getVariables().size() + "");
        LOGGER.warning(processData);


        PostCall pc = new PostCall();
        String prediction = pc.postCall(processData, SERVICE_ADDRESS);

        LOGGER.warning("Prediction: " + prediction);

        JsonParser parser = new JsonParser();
        JsonObject predictionObject = parser.parse(prediction).getAsJsonObject();


        boolean predictionResult = predictionObject.get("predictionResult").getAsBoolean();
        execution.setVariable("predictionResult", predictionResult);

        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();

        LOGGER.warning(gson.toJson(predictionObject));
    }



}
