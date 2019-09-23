package de.viadee.bpmnai.core.annotation;

import java.lang.annotation.*;

@Documented
@Target(ElementType.TYPE)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface PreprocessingStep{
    String description();
}