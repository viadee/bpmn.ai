package de.viadee.ki.sparkimporter.events;

public class ActivityInstanceEvent extends ScopeInstanceEvent {
    /** the id of the activity */
    private String activityId;

    /** the name of the activity */
    private String activityName;

    /** the type of the activity (startEvent, serviceTask ...) */
    private String activityType;

    /** the id of this activity instance */
    private String activityInstanceId;

    /** the state of this activity instance */
    private int activityInstanceState;

    /** the id of the parent activity instance */
    private String parentActivityInstanceId;

    /** the id of the child process instance */
    private String calledProcessInstanceId;

    /** the id of the child case instance */
    private String calledCaseInstanceId;

    private String taskId;
    private String taskAssignee;

    /** id of the tenant which belongs to the activity instance  */
    private String tenantId;

    public String getActivityId() {
        return activityId;
    }

    public void setActivityId(String activityId) {
        this.activityId = activityId;
    }

    public String getActivityName() {
        return activityName;
    }

    public void setActivityName(String activityName) {
        this.activityName = activityName;
    }

    public String getActivityType() {
        return activityType;
    }

    public void setActivityType(String activityType) {
        this.activityType = activityType;
    }

    public String getActivityInstanceId() {
        return activityInstanceId;
    }

    public void setActivityInstanceId(String activityInstanceId) {
        this.activityInstanceId = activityInstanceId;
    }

    public int getActivityInstanceState() {
        return activityInstanceState;
    }

    public void setActivityInstanceState(int activityInstanceState) {
        this.activityInstanceState = activityInstanceState;
    }

    public String getParentActivityInstanceId() {
        return parentActivityInstanceId;
    }

    public void setParentActivityInstanceId(String parentActivityInstanceId) {
        this.parentActivityInstanceId = parentActivityInstanceId;
    }

    public String getCalledProcessInstanceId() {
        return calledProcessInstanceId;
    }

    public void setCalledProcessInstanceId(String calledProcessInstanceId) {
        this.calledProcessInstanceId = calledProcessInstanceId;
    }

    public String getCalledCaseInstanceId() {
        return calledCaseInstanceId;
    }

    public void setCalledCaseInstanceId(String calledCaseInstanceId) {
        this.calledCaseInstanceId = calledCaseInstanceId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskAssignee() {
        return taskAssignee;
    }

    public void setTaskAssignee(String taskAssignee) {
        this.taskAssignee = taskAssignee;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
}
