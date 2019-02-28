package de.viadee.ki.sparkimporter.annotation;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import org.reflections.Reflections;

import java.util.Set;

public class AnnotationCollectorTest {
    public static void main(String[] args) {


        String packageName = AnnotationCollectorTest.class.getPackage().getName();
        System.out.println(packageName);
        String basePackageName = packageName.substring(0, packageName.lastIndexOf("."));
        System.out.println(basePackageName);

        Reflections reflections = new Reflections("de.viadee");

        Set<Class<? extends PreprocessingStepInterface>> subTypes = reflections.getSubTypesOf(PreprocessingStepInterface.class);

        for(Class<? extends PreprocessingStepInterface> c : subTypes) {
            System.out.println("Class: " + c.getSimpleName());

            PreprocessingStepDescription description = c.getAnnotation(PreprocessingStepDescription.class);
            if(description != null)
                System.out.println("    description: " + description.description());

            PreprocessingStepParameters parameters = c.getAnnotation(PreprocessingStepParameters.class);
            if(parameters != null) {
                PreprocessingStepParameter[] params = parameters.value();
                for(PreprocessingStepParameter parameter : params) {
                    System.out.println("    name: " + parameter.name() + " description: " + parameter.description());
                }
            }

            PreprocessingStepParameter parameter = c.getAnnotation(PreprocessingStepParameter.class);
            if(parameter != null)
                System.out.println("    name: " + parameter.name() + " description: " + parameter.description());
        }



    }
}
