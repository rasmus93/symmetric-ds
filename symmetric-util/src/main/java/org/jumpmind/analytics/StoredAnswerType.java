package org.jumpmind.analytics;

public enum StoredAnswerType {
    INT( "Int64" ),
    STRING( "String" ),
    DATE( "Date" ),
    BOOLEAN( "Int8" ),
    FLOAT( "Float64" );

    private String clickHouseType;

    StoredAnswerType( String clickHouseType ) {
        this.clickHouseType = clickHouseType;
    }

    public String getClickHouseType() {
        return clickHouseType;
    }
}
