package de.viadee.ki.sparkimporter.util;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class SparkBroadcastHelper {

    public enum BROADCAST_VARIABLE {
        PROCESS_VARIABLES_RAW,
        PROCESS_VARIABLES_ESCALATED
    }

    private static SparkBroadcastHelper instance;

    private SparkBroadcastHelper(){}

    public static synchronized SparkBroadcastHelper getInstance(){
        if(instance == null){
            instance = new SparkBroadcastHelper();
        }
        return instance;
    }

    private static final Map<BROADCAST_VARIABLE, Broadcast<Object>> BROADCAST_VARIABLES = new HashMap<>();

    public<T> void broadcastVariable(BROADCAST_VARIABLE name,  T varToBroadcast) {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkSession.builder().getOrCreate().sparkContext());
        Broadcast<Object> broadcastedVar = jsc.broadcast(varToBroadcast);
        BROADCAST_VARIABLES.put(name, broadcastedVar);
    }

    public Object getBroadcastVariable(BROADCAST_VARIABLE name) {
        if(BROADCAST_VARIABLES.get(name) != null)
            return BROADCAST_VARIABLES.get(name).value();
        else
            return null;
    }
}
