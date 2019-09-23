package de.viadee.bpmnai.core.listener;

public interface SparkRunnerListener {
    void onProgressUpdate(String message, int tasksDone, int tasksTotal);
    void onFinished(boolean success);
}
