package de.viadee.ki.sparkimporter.processing.steps;

import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class PipelineManager {

    private List<PipelineStep> pipelineSteps = new ArrayList<>();
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
        boolean containsCycle = false;

        // first element, search for the ones without a predecessor (should only be one)
        for(PipelineStep ps : pipelineSteps) {
            if(!ps.hasPredecessor()) {
                if(orderedPipeline.size() > 0) {
                    SparkImporterLogger.getInstance().writeError("More that one starting processing step found!");
                } else {
                    orderedPipeline.add(ps);
                }
            }
        }
        if(orderedPipeline.size() == 0) {
            SparkImporterLogger.getInstance().writeError("No starting processing step found!");
            throw new FaultyConfigurationException("No starting processing step found!");
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
