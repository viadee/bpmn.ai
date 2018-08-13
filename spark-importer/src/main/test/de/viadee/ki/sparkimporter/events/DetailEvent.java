package de.viadee.ki.sparkimporter.events;

import java.util.Date;

public class DetailEvent extends HistoryEvent {
    private String activityInstanceId;
    private String taskId;
    private Date timestamp;
    private String tenantId;
    private String userOperationId;

    public String getActivityInstanceId() {
        return activityInstanceId;
    }

    public void setActivityInstanceId(String activityInstanceId) {
        this.activityInstanceId = activityInstanceId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getUserOperationId() {
        return userOperationId;
    }

    public void setUserOperationId(String userOperationId) {
        this.userOperationId = userOperationId;
    }
}
