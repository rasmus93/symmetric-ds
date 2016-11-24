package org.jumpmind.analytics;

import org.joda.time.LocalDateTime;

public class ResponseData {

    private long id;

    private long formId;

    private int answerGroup;

    private String label;

    private String number;

    private String passwordEmail;

    private LocalDateTime created;

    private LocalDateTime lastSubmitted;

    public ResponseData() {
    }

    public long getId() {
        return id;
    }

    public void setId( long id ) {
        this.id = id;
    }

    public long getFormId() {
        return formId;
    }

    public void setFormId( long formId ) {
        this.formId = formId;
    }

    public int getAnswerGroup() {
        return answerGroup;
    }

    public void setAnswerGroup( int answerGroup ) {
        this.answerGroup = answerGroup;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel( String label ) {
        this.label = label;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber( String number ) {
        this.number = number;
    }

    public String getPasswordEmail() {
        return passwordEmail;
    }

    public void setPasswordEmail( String passwordEmail ) {
        this.passwordEmail = passwordEmail;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void setCreated( LocalDateTime created ) {
        this.created = created;
    }

    public LocalDateTime getLastSubmitted() {
        return lastSubmitted;
    }

    public void setLastSubmitted( LocalDateTime lastSubmitted ) {
        this.lastSubmitted = lastSubmitted;
    }
}
