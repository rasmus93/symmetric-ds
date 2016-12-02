package org.jumpmind.analytics.types;

public enum QuestionType {
    UNKNOWN( 0 ),
    PICK_ONE_NO_OTHER( 1 ),
    PICK_ONE_WITH_OTHER( 2 ),
    CHECKALL_NO_OTHER( 3 ),
    CHECKALL_WITH_OTHER( 4 ),
    DROPDOWN( 5 ),
    LISTBOX( 6 ),
    SINGLE_LINE( 7 ),
    MULTI_LINE( 8 ),
    RANK_GRID_DEPENDENT( 9 ),
    COMPARE( 10 ),
    HEADER( 11 ),
    MATRIX_3D( 12 ),
    NUMERIC_ALLOCATION( 13 ),
    PICK_ONE_WITH_COMMENT( 102 ),
    RANK_GRID_INDEPENDENT( 122 ),
    INTERNAL_NOTES( 1000 ),
    REPORT_CROSS_TAB( 2221 ),
    REPORT_SIGNIFICANCE( 2222 ),
    REPORT_TEXTIMAGE( 2223 ),
    REPORT_ADV_CROSS_TAB( 2224 ),
    LOOKUP( 14 ),
    UPLOAD_FILE( 15 ),
    MULTIPLE_LOOKUP( 16 );

    private final int typeCode;

    QuestionType( int typeCode ) {
        this.typeCode = typeCode;
    }

    public int getTypeCode() {
        return typeCode;
    }

    public static QuestionType getByTypeCode( int typeCode ) {
        for ( QuestionType t : values() ) {
            if ( t.getTypeCode() == typeCode ) {
                return t;
            }
        }
        return UNKNOWN;
    }
}
