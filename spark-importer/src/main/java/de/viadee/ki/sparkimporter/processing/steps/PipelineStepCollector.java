package de.viadee.ki.sparkimporter.processing.steps;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.annotation.PreprocessingStepParameter;
import de.viadee.ki.sparkimporter.annotation.PreprocessingStepParameters;
import io.github.classgraph.*;

import java.util.ArrayList;
import java.util.List;

public class PipelineStepCollector {

    public static List<PipelineStepDefinition> collectAllAvailablePipelineSteps() {
        List<PipelineStepDefinition> pipelineSteps = new ArrayList<>();

        String pkg = "de.viadee";
        try (ScanResult scanResult =
                     new ClassGraph()
                             .enableClassInfo()
                             .enableAnnotationInfo()
                             .whitelistPackages(pkg)      // Scan com.xyz and subpackages (omit to scan all packages)
                             .scan()) {                   // Start the scan
            for (ClassInfo routeClassInfo : scanResult.getClassesWithAnnotation(PreprocessingStepDescription.class.getCanonicalName())) {
                AnnotationInfo annotationInfo = routeClassInfo.getAnnotationInfo(PreprocessingStepDescription.class.getCanonicalName());
                List<AnnotationParameterValue> paramVals = annotationInfo.getParameterValues();

                PipelineStepDefinition step = new PipelineStepDefinition();
                step.setId(routeClassInfo.getSimpleName());
                step.setClassName(routeClassInfo.getPackageName()+"."+routeClassInfo.getSimpleName());

                for(AnnotationParameterValue parameterValue : paramVals) {
                    if(parameterValue.getName().equals("name")) {
                        step.setName((String) parameterValue.getValue());
                    } else if(parameterValue.getName().equals("description")) {
                        step.setDescription((String) parameterValue.getValue());
                    }
                }

                annotationInfo = routeClassInfo.getAnnotationInfo(PreprocessingStepParameter.class.getCanonicalName());
                if(annotationInfo != null) {
                    paramVals = annotationInfo.getParameterValues();
                    List<ParameterDefinition> stepParameters = new ArrayList<>();
                    ParameterDefinition stepParam = new ParameterDefinition();
                    for(AnnotationParameterValue parameterValue : paramVals) {
                        if(parameterValue.getName().equals("name")) {
                            stepParam.setName((String) parameterValue.getValue());
                        } else if(parameterValue.getName().equals("description")) {
                            stepParam.setDescription((String) parameterValue.getValue());
                        } else if(parameterValue.getName().equals("required")) {
                            stepParam.setRequired((Boolean) parameterValue.getValue());
                        } else if(parameterValue.getName().equals("dataType")) {
                            stepParam.setDataType((PreprocessingStepParameter.DATA_TYPE) ((AnnotationEnumValue) parameterValue.getValue()).loadClassAndReturnEnumValue());
                        }
                    }
                    stepParameters.add(stepParam);
                    step.setParameters(stepParameters);
                }

                annotationInfo = routeClassInfo.getAnnotationInfo(PreprocessingStepParameters.class.getCanonicalName());
                if(annotationInfo != null) {
                    paramVals = annotationInfo.getParameterValues();
                    for(AnnotationParameterValue parameterValue : paramVals) {
                        List<ParameterDefinition> stepParameters = new ArrayList<>();
                        Object[] params = (Object[]) parameterValue.getValue();
                        for(Object p : params) {
                            AnnotationInfo annoInfo = (AnnotationInfo) p;
                            ParameterDefinition stepParam = new ParameterDefinition();
                            if(annoInfo != null) {
                                paramVals = annoInfo.getParameterValues();
                                for(AnnotationParameterValue pv : paramVals) {
                                    if(pv.getName().equals("name")) {
                                        stepParam.setName((String) pv.getValue());
                                    } else if(pv.getName().equals("description")) {
                                        stepParam.setDescription((String) pv.getValue());
                                    } else if(pv.getName().equals("required")) {
                                        stepParam.setRequired((Boolean) pv.getValue());
                                    } else if(pv.getName().equals("dataType")) {
                                        stepParam.setDataType((PreprocessingStepParameter.DATA_TYPE) ((AnnotationEnumValue) pv.getValue()).loadClassAndReturnEnumValue());
                                    }
                                }
                            }
                            stepParameters.add(stepParam);
                        }
                        step.setParameters(stepParameters);
                    }
                }
                pipelineSteps.add(step);
            }
        }
        return pipelineSteps;
    }
}
