package de.viadee.ki.sparkimporter.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
public @interface PreprocessingStepParameters {
    PreprocessingStepParameter[] value() default {};
}