package de.viadee.ki.sparkimporter.events;

import java.util.Date;

public class IncidentEvent extends HistoryEvent {
    private Date createTime;
    private Date endTime;
    private String incidentType;
    private String activityId;
    private String causeIncidentId;
    private String rootCauseIncidentId;
    private String configuration;
    private String incidentMessage;
    private int incidentState;
    private String tenantId;
    private String jobDefinitionId;

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getIncidentType() {
        return incidentType;
    }

    public void setIncidentType(String incidentType) {
        this.incidentType = incidentType;
    }

    public String getActivityId() {
        return activityId;
    }

    public void setActivityId(String activityId) {
        this.activityId = activityId;
    }

    public String getCauseIncidentId() {
        return causeIncidentId;
    }

    public void setCauseIncidentId(String causeIncidentId) {
        this.causeIncidentId = causeIncidentId;
    }

    public String getRootCauseIncidentId() {
        return rootCauseIncidentId;
    }

    public void setRootCauseIncidentId(String rootCauseIncidentId) {
        this.rootCauseIncidentId = rootCauseIncidentId;
    }

    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(String configuration) {
        this.configuration = configuration;
    }

    public String getIncidentMessage() {
        return incidentMessage;
    }

    public void setIncidentMessage(String incidentMessage) {
        this.incidentMessage = incidentMessage;
    }

    public int getIncidentState() {
        return incidentState;
    }

    public void setIncidentState(int incidentState) {
        this.incidentState = incidentState;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getJobDefinitionId() {
        return jobDefinitionId;
    }

    public void setJobDefinitionId(String jobDefinitionId) {
        this.jobDefinitionId = jobDefinitionId;
    }
}
