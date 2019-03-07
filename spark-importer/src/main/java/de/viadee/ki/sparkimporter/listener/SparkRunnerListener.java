package de.viadee.ki.sparkimporter.listener;

public interface SparkRunnerListener {
    void onProgressUpdate(String message, int tasksDone, int tasksTotal);
    void onFinished(boolean success);
}
