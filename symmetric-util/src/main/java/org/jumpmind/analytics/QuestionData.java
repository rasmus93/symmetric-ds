package org.jumpmind.analytics;

public class QuestionData {

    private long formId;

    private long id;

    private int type;

    private int subType;

    private boolean isNumeric;

    private int totalColumns;

    public QuestionData() {
    }

    public long getFormId() {
        return formId;
    }

    public void setFormId( long formId ) {
        this.formId = formId;
    }

    public long getId() {
        return id;
    }

    public void setId( long id ) {
        this.id = id;
    }

    public int getType() {
        return type;
    }

    public void setType( int type ) {
        this.type = type;
    }

    public int getSubType() {
        return subType;
    }

    public void setSubType( int subType ) {
        this.subType = subType;
    }

    public boolean isNumeric() {
        return isNumeric;
    }

    public void setNumeric( boolean numeric ) {
        isNumeric = numeric;
    }

    public int getTotalColumns() {
        return totalColumns;
    }

    public void setTotalColumns( int totalColumns ) {
        this.totalColumns = totalColumns;
    }
}
