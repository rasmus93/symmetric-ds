package org.jumpmind.analytics.types;

public enum AnswerType {
    UNKNOWN( 0 ),
    ATYPE_RADIO( 1 ),
    ATYPE_CHECKBOX( 2 ),
    ATYPE_DROPDOWN( 3 ),
    ATYPE_LISTBOX( 4 ),
    ATYPE_SINGLE_LINE_RADIO( 5 ),
    ATYPE_MULTI_LINE_RADIO( 6 ),
    ATYPE_SINGLE_LINE_CHECKBOX( 7 ),
    ATYPE_MULTI_LINE_CHECKBOX( 8 ),
    ATYPE_SINGLE_LINE( 9 ),
    ATYPE_MULTI_LINE( 10 ),
    ATYPE_HEADING( 11 );

    private int typeCode;

    AnswerType( int typeCode ) {
        this.typeCode = typeCode;
    }

    public int getTypeCode() {
        return typeCode;
    }

    public static AnswerType getByTypeCode( int typeCode ) {
        for ( AnswerType t : values() ) {
            if ( t.getTypeCode() == typeCode ) {
                return t;
            }
        }
        return UNKNOWN;
    }
}
