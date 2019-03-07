package de.viadee.ki.sparkimporter.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface PreprocessingStepParameters {
    PreprocessingStepParameter[] value() default {};
}