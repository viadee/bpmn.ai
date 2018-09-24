package de.viadee.ki.servicecaller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.*;
import org.camunda.bpm.engine.delegate.DelegateExecution;

import java.util.logging.Logger;

/**
 *
 */
public class JSONUtils {

    private final static Logger LOGGER = Logger.getLogger("LOAN-REQUESTS");

    public String genJSONStr (DelegateExecution execution) throws Exception {

        JsonObject processData = new JsonObject();

        ObjectMapper mapper = new ObjectMapper();
        String processVariables = mapper.writeValueAsString(execution.getVariables());
        JsonParser parser = new JsonParser();

        JsonObject content = parser.parse(processVariables).getAsJsonObject();
        processData.add("content", content);

        JsonObject meta = new JsonObject();
        meta.addProperty("procDefID", execution.getProcessDefinitionId());
        meta.addProperty("procInstID", execution.getProcessInstanceId());

        processData.add("meta", meta);

        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();

        return gson.toJson(processData);
    }

}
