package org.jumpmind.analytics;

public class AnswerData {

    private long valueTypeId;

    private int valueType;

    private String pattern;

    private int decimals;

    private String format;

    public AnswerData() {
    }

    public long getValueTypeId() {
        return valueTypeId;
    }

    public void setValueTypeId( long valueTypeId ) {
        this.valueTypeId = valueTypeId;
    }

    public int getValueType() {
        return valueType;
    }

    public void setValueType( int valueType ) {
        this.valueType = valueType;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern( String pattern ) {
        this.pattern = pattern;
    }

    public int getDecimals() {
        return decimals;
    }

    public void setDecimals( int decimals ) {
        this.decimals = decimals;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat( String format ) {
        this.format = format;
    }
}
