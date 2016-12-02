package org.jumpmind.analytics.types;

public enum AnswerPatternType {
    STRING( 0 ),
    NUMERIC( 1 ),
    AMOUNT( 2 ),
    PERCENT( 3 ),
    EMAIL( 4 ),
    NUMBER_NATURAL( 5 ),
    DATE( 6 ),
    UNKNOWN( 100 );

    private int typeCode = 100;

    AnswerPatternType( int typeCode ) {
        this.typeCode = typeCode;
    }

    public static AnswerPatternType getByTypeCode( int typeCode ) {
        for ( AnswerPatternType t : values() ) {
            if ( t.getTypeCode() == typeCode ) {
                return t;
            }
        }
        return UNKNOWN;
    }

    public int getTypeCode() {
        return typeCode;
    }
}
