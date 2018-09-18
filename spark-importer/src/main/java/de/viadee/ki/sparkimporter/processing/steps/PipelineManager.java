package de.viadee.ki.sparkimporter.processing.steps;

import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PipelineManager {

    private List<PipelineStep> pipelineSteps;
    LinkedList<PipelineStep> orderedPipeline = new LinkedList<>();

    public PipelineManager(List<PipelineStep> pipelineSteps) throws FaultyConfigurationException {
        this.pipelineSteps = pipelineSteps;
        buildPipeline();
        SparkImporterLogger.getInstance().writeInfo("Resulting pipeline from configuration: " + orderedPipeline.toString());
    }

    public LinkedList<PipelineStep> getOrderedPipeline() {
        return orderedPipeline;
    }

    private void buildPipeline() throws FaultyConfigurationException {

        // check for unique IDs
        Map<String, String> pipelineStepsIdCheck = new HashMap<>();

        // first element, search for the ones without a predecessor (should only be one)
        for(PipelineStep ps : pipelineSteps) {
            if(!ps.hasPredecessor()) {
                if(orderedPipeline.size() > 0) {
                    SparkImporterLogger.getInstance().writeError("More that one starting processing step found!");
                } else {
                    orderedPipeline.add(ps);
                }
            }
            pipelineStepsIdCheck.put(ps.getId(), ps.getClassName());
        }
        if(orderedPipeline.size() == 0) {
            String message = "No starting processing step found!";
            SparkImporterLogger.getInstance().writeError(message);
            throw new FaultyConfigurationException(message);
        }

        // check for unique IDs - continued
        if(pipelineSteps.size() != pipelineStepsIdCheck.size()) {
            String message = "Duplicate step IDs found in step configuration!";
            SparkImporterLogger.getInstance().writeError(message);
            throw new FaultyConfigurationException(message);
        }

        // add other steps
        while(orderedPipeline.size() != pipelineSteps.size()) {

            boolean elementFound = false;

            pipelineloop:
            for(PipelineStep ps : pipelineSteps) {
                if (!orderedPipeline.contains(ps)) {
                    int listIndex = 0;

                    graphloop:
                    for (PipelineStep psgraph : orderedPipeline) {
                        if (ps.hasPredecessor() && ps.getDependsOn().equals(psgraph.getId())) {
                            orderedPipeline.add(listIndex+1, ps);
                            elementFound = true;
                            continue pipelineloop;
                        }
                        listIndex++;
                    }


                }
            }

            if(!elementFound) {
                String errorMessage = "Could not add all steps to pipeline. Please check the step configuration! Pipeline before exception: " + orderedPipeline.toString();
                SparkImporterLogger.getInstance().writeError(errorMessage);
                throw new FaultyConfigurationException(errorMessage);
            }
        }
    }
}
