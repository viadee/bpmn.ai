package de.viadee.ki.sparkimporter.events;

public class ProcessInstanceEvent extends ScopeInstanceEvent {
    /** the business key of the process instance */
    private String businessKey;

    /** the id of the user that started the process instance */
    private String startUserId;

    /** the id of the super process instance */
    private String superProcessInstanceId;

    /** the id of the super case instance */
    private String superCaseInstanceId;

    /** the reason why this process instance was cancelled (deleted) */
    private String deleteReason;

    /** id of the activity which ended the process instance */
    private String endActivityId;

    /** id of the activity which started the process instance */
    private String startActivityId;

    /** id of the tenant which belongs to the process instance  */
    private String tenantId;

    private String state;

    public String getBusinessKey() {
        return businessKey;
    }

    public void setBusinessKey(String businessKey) {
        this.businessKey = businessKey;
    }

    public String getStartUserId() {
        return startUserId;
    }

    public void setStartUserId(String startUserId) {
        this.startUserId = startUserId;
    }

    public String getSuperProcessInstanceId() {
        return superProcessInstanceId;
    }

    public void setSuperProcessInstanceId(String superProcessInstanceId) {
        this.superProcessInstanceId = superProcessInstanceId;
    }

    public String getSuperCaseInstanceId() {
        return superCaseInstanceId;
    }

    public void setSuperCaseInstanceId(String superCaseInstanceId) {
        this.superCaseInstanceId = superCaseInstanceId;
    }

    public String getDeleteReason() {
        return deleteReason;
    }

    public void setDeleteReason(String deleteReason) {
        this.deleteReason = deleteReason;
    }

    public String getEndActivityId() {
        return endActivityId;
    }

    public void setEndActivityId(String endActivityId) {
        this.endActivityId = endActivityId;
    }

    public String getStartActivityId() {
        return startActivityId;
    }

    public void setStartActivityId(String startActivityId) {
        this.startActivityId = startActivityId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
