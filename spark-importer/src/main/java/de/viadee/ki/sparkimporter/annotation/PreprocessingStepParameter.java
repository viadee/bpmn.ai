package de.viadee.ki.sparkimporter.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(value = PreprocessingStepParameters.class)
public @interface PreprocessingStepParameter {
    String name();
    String description();
    boolean required();
    DATA_TYPE dataType();

    enum DATA_TYPE {
        STRING("string"),
        BOOLEAN("boolean"),
        LONG("long");

        private String dataType;

        DATA_TYPE(String dataType) {
            this.dataType = dataType;
        }

        public String getDataType() {
            return dataType;
        }
    }
}