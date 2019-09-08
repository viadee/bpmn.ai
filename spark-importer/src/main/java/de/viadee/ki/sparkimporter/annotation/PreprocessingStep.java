package de.viadee.ki.sparkimporter.annotation;

import java.lang.annotation.*;

@Documented
@Target(ElementType.TYPE)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface PreprocessingStep{
    String description();
}