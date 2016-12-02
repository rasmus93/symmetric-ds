package org.jumpmind.analytics.types;

public enum ColumnType {
    UNKNOWN( 0 ),
    TEXT( 1 ),
    DROPDOWN( 2 ),
    CHECKBOX( 3 ),
    RADIO( 4 );

    private int typeCode;

    ColumnType( int typeCode ) {
        this.typeCode = typeCode;
    }

    public int getTypeCode() {
        return typeCode;
    }

    public static ColumnType getByTypeCode( int typeCode ) {
        for ( ColumnType t : values() ) {
            if ( t.getTypeCode() == typeCode ) {
                return t;
            }
        }
        return UNKNOWN;
    }
}
