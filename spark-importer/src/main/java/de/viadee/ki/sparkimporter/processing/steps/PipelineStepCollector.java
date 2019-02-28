package de.viadee.ki.sparkimporter.processing.steps;

import de.viadee.ki.sparkimporter.annotation.AnnotationCollectorTest;
import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.annotation.PreprocessingStepParameter;
import de.viadee.ki.sparkimporter.annotation.PreprocessingStepParameters;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import org.reflections.Reflections;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PipelineStepCollector {

    public static List<PipelineStepDefinition> collectAllAvailablePipelineSteps() {
        List<PipelineStepDefinition> pipelineSteps = new ArrayList<>();
        String packageName = AnnotationCollectorTest.class.getPackage().getName();

        Reflections reflections = new Reflections("de.viadee");

        Set<Class<? extends PreprocessingStepInterface>> subTypes = reflections.getSubTypesOf(PreprocessingStepInterface.class);

        for(Class<? extends PreprocessingStepInterface> c : subTypes) {

            PipelineStepDefinition step = new PipelineStepDefinition();
            step.setId(c.getSimpleName());

            PreprocessingStepDescription description = c.getAnnotation(PreprocessingStepDescription.class);
            if(description != null) {
                step.setName(description.name());
                step.setDescription(description.description());
            }

            PreprocessingStepParameters parameters = c.getAnnotation(PreprocessingStepParameters.class);
            if(parameters != null) {
                PreprocessingStepParameter[] params = parameters.value();
                List<ParameterDefinition> stepParameters = new ArrayList<>();
                for(PreprocessingStepParameter parameter : params) {
                    ParameterDefinition stepParam = new ParameterDefinition();
                    stepParam.setName(parameter.name());
                    stepParam.setDescription(parameter.description());
                    stepParam.setDataType(parameter.dataType());
                    stepParam.setRequired(parameter.required());

                    stepParameters.add(stepParam);
                }
                step.setParameters(stepParameters);
            }

            PreprocessingStepParameter parameter = c.getAnnotation(PreprocessingStepParameter.class);
            if(parameter != null) {
                List<ParameterDefinition> stepParameters = new ArrayList<>();

                ParameterDefinition stepParam = new ParameterDefinition();
                stepParam.setName(parameter.name());
                stepParam.setDescription(parameter.description());
                stepParam.setDataType(parameter.dataType());
                stepParam.setRequired(parameter.required());

                stepParameters.add(stepParam);

                step.setParameters(stepParameters);
            }

            if(step.getName() != null)
                pipelineSteps.add(step);
        }

        return pipelineSteps;
    }
}
