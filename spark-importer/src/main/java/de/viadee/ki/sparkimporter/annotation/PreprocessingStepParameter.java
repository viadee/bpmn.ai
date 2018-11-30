package de.viadee.ki.sparkimporter.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(value = PreprocessingStepParameters.class)
public @interface PreprocessingStepParameter {
    String name();
    String description();
}