package de.viadee.ki.sparkimporter.util;

public class SparkImporterGlobalConfigValues {

    private static String targetFolder = "";
    private static boolean devTypeCastCheckEnabled = false;
    private static boolean revCountEnabled = false;
    private static String dataLevel = "process";

    public static String getTargetFolder() {
        return targetFolder;
    }

    public static void setTargetFolder(String targetFolder) {
        SparkImporterGlobalConfigValues.targetFolder = targetFolder;
    }

    public static boolean isDevTypeCastCheckEnabled() {
        return devTypeCastCheckEnabled;
    }

    public static void setDevTypeCastCheckEnabled(boolean devTypeCastCheckEnabled) {
        SparkImporterGlobalConfigValues.devTypeCastCheckEnabled = devTypeCastCheckEnabled;
    }

    public static boolean isRevCountEnabled() {
        return revCountEnabled;
    }

    public static void setRevCountEnabled(boolean revCountEnabled) {
        SparkImporterGlobalConfigValues.revCountEnabled = revCountEnabled;
    }

    public static String getDataLevel() {
        return dataLevel;
    }

    public static void setDataLevel(String dataLevel) {
        SparkImporterGlobalConfigValues.dataLevel = dataLevel;
    }
}
