package org.jumpmind.analytics;

import org.jumpmind.analytics.types.*;

import java.util.HashMap;
import java.util.Map;

public final class AnswerTypeMapping {

    private static final int TOTAL_COLUMNS_FOR_RANK_GRID = 101;
    private static final String LOOKUP_ANSWER_DM_ID = "Object ID";

    private static Map<AnswerType, StoredAnswerType> typeToStore = new HashMap<>();
    private static Map<AnswerPatternType, StoredAnswerType> typeToStoreFromPattern = new HashMap<>();

    static {
        typeToStore.put( AnswerType.ATYPE_CHECKBOX, StoredAnswerType.BOOLEAN );
        typeToStore.put( AnswerType.ATYPE_RADIO, StoredAnswerType.BOOLEAN );
        typeToStore.put( AnswerType.ATYPE_SINGLE_LINE, StoredAnswerType.STRING );
        typeToStore.put( AnswerType.ATYPE_SINGLE_LINE_RADIO, StoredAnswerType.STRING );
        typeToStore.put( AnswerType.ATYPE_SINGLE_LINE_CHECKBOX, StoredAnswerType.STRING );
        typeToStore.put( AnswerType.ATYPE_DROPDOWN, StoredAnswerType.BOOLEAN );
        typeToStore.put( AnswerType.ATYPE_MULTI_LINE_RADIO, StoredAnswerType.BOOLEAN );
        typeToStore.put( AnswerType.ATYPE_MULTI_LINE, StoredAnswerType.STRING );
        typeToStore.put( AnswerType.ATYPE_MULTI_LINE_CHECKBOX, StoredAnswerType.BOOLEAN );
        typeToStore.put( AnswerType.ATYPE_LISTBOX, StoredAnswerType.BOOLEAN );
        typeToStore.put( AnswerType.UNKNOWN, StoredAnswerType.STRING );

        typeToStoreFromPattern.put( AnswerPatternType.AMOUNT, StoredAnswerType.FLOAT );
        typeToStoreFromPattern.put( AnswerPatternType.DATE, StoredAnswerType.DATE );
        typeToStoreFromPattern.put( AnswerPatternType.EMAIL, StoredAnswerType.STRING );
        typeToStoreFromPattern.put( AnswerPatternType.NUMBER_NATURAL, StoredAnswerType.INT );
        typeToStoreFromPattern.put( AnswerPatternType.PERCENT, StoredAnswerType.FLOAT );
        typeToStoreFromPattern.put( AnswerPatternType.NUMERIC, StoredAnswerType.FLOAT );
        typeToStoreFromPattern.put( AnswerPatternType.STRING, StoredAnswerType.STRING );
    }

    private AnswerTypeMapping() {
    }

    public static String getStoreType(
            QuestionData questionData,
            int answerTypeCode,
            AnswerData answerData,
            ColumnData columnData,
            AnswerData columnAnswerData
    ) {
        AnswerType answerType = AnswerType.getByTypeCode( answerTypeCode );
        ColumnType columnType = resolveDbColumnType( DbColumnType.getByTypeCode( columnData.getType() ) );

        switch ( findType( questionData, answerType ) ) {
            case MATRIX_3D:
                switch ( columnType ) {
                    case CHECKBOX:
                    case RADIO:
                        return StoredAnswerType.BOOLEAN.getClickHouseType();
                    case DROPDOWN:
                        return StoredAnswerType.INT.getClickHouseType();
                    case TEXT:
                        AnswerPatternType columnPatternType =
                                AnswerPatternType.getByTypeCode( columnAnswerData.getValueType() );
                        if ( ( columnPatternType.equals( AnswerPatternType.AMOUNT ) ||
                                columnPatternType.equals( AnswerPatternType.NUMERIC ) ) &&
                                columnAnswerData.getDecimals() < 1 ) {
                            return StoredAnswerType.INT.getClickHouseType();
                        }
                        return typeToStoreFromPattern.get( columnPatternType ).getClickHouseType();
                    default:
                        return StoredAnswerType.STRING.getClickHouseType();
                }
            case RANK_GRID_INDEPENDENT:
                return StoredAnswerType.BOOLEAN.getClickHouseType();
            case MULTIPLE_LOOKUP:
            case LOOKUP:
                if ( answerData.getText().equals( LOOKUP_ANSWER_DM_ID ) ) {
                    return StoredAnswerType.INT.getClickHouseType();
                }
            case SINGLE_LINE:
            case NUMERIC_ALLOCATION:
                AnswerPatternType answerPatternType = AnswerPatternType.getByTypeCode( answerData.getValueType() );

                if ( ( answerPatternType.equals( AnswerPatternType.AMOUNT ) ||
                        answerPatternType.equals( AnswerPatternType.NUMERIC ) )
                        && answerData.getDecimals() < 1 ) {
                    return StoredAnswerType.INT.getClickHouseType();
                }
                return typeToStoreFromPattern.get( answerPatternType ).getClickHouseType();
            default:
                return typeToStore.get( answerType ).getClickHouseType();
        }
    }

    private static QuestionType findType( QuestionData questionData, AnswerType answerType ) {
        DbQuestionType type = DbQuestionType.getByTypeCode( questionData.getType() );
        DbQuestionSubType subType = DbQuestionSubType.getByTypeCode( questionData.getSubType() );

        if ( type == DbQuestionType.TYPE_INTERNAL_NOTES ) {
            return QuestionType.INTERNAL_NOTES;
        }
        if ( questionData.isNumeric() ) {
            return QuestionType.NUMERIC_ALLOCATION;
        }
        if ( type == DbQuestionType.TYPE_SIMPLE &&
                questionData.getTotalColumns() == TOTAL_COLUMNS_FOR_RANK_GRID ) {
            return QuestionType.PICK_ONE_WITH_COMMENT;
        }
        switch ( type ) {
            case TYPE_RANK_GRID_DEPENDENT:
                return QuestionType.RANK_GRID_DEPENDENT;
            case TYPE_RANK_GRID_INDEPENDENT:
                return QuestionType.RANK_GRID_INDEPENDENT;
            case TYPE_MATRIX_COMPARE_ONE_AGAINST_ANOTHER:
                return QuestionType.COMPARE;
            case TYPE_3D_MATRIX:
                return QuestionType.MATRIX_3D;
            case TYPE_SIMPLE:
                processSimpleType( subType, answerType );
        }
        return QuestionType.UNKNOWN;
    }

    private static QuestionType processSimpleType(
            DbQuestionSubType subType,
            AnswerType answerType
    ) {
        switch ( subType ) {
            case LOOKUP:
                return QuestionType.LOOKUP;
            case MULTIPLE_LOOKUP:
                return QuestionType.MULTIPLE_LOOKUP;
            case UPLOAD_FILE:
                return QuestionType.UPLOAD_FILE;
            default:
                return findType( answerType );
        }
    }

    private static QuestionType findType( AnswerType answerType ) {
        switch ( answerType ) {
            case ATYPE_RADIO:
                return QuestionType.PICK_ONE_NO_OTHER;
            case ATYPE_CHECKBOX:
                return QuestionType.CHECKALL_NO_OTHER;
            case ATYPE_DROPDOWN:
                return QuestionType.DROPDOWN;
            case ATYPE_LISTBOX:
                return QuestionType.LISTBOX;
            case ATYPE_SINGLE_LINE_RADIO:
            case ATYPE_MULTI_LINE_RADIO:
                return QuestionType.PICK_ONE_WITH_OTHER;
            case ATYPE_SINGLE_LINE_CHECKBOX:
            case ATYPE_MULTI_LINE_CHECKBOX:
                return QuestionType.CHECKALL_WITH_OTHER;
            case ATYPE_SINGLE_LINE:
                return QuestionType.SINGLE_LINE;
            case ATYPE_MULTI_LINE:
                return QuestionType.MULTI_LINE;
            default:
                return QuestionType.UNKNOWN;
        }
    }

    private static ColumnType resolveDbColumnType( DbColumnType dbColumnType ) {
        switch ( dbColumnType ) {
            case EDIT_RADIO:
                return ColumnType.RADIO;
            case EDIT_TEXT:
                return ColumnType.TEXT;
            case EDIT_CHECKBOX:
                return ColumnType.CHECKBOX;
            case EDIT_DROPDOWN:
                return ColumnType.DROPDOWN;
            default:
                throw new IllegalArgumentException( "Can't convert core type to API type: " + dbColumnType );
        }
    }

    private enum DbQuestionType {
        UNKNOWN( 0 ),
        TYPE_SIMPLE( 1 ),
        TYPE_RANK_GRID_DEPENDENT( 2 ),
        TYPE_MATRIX_COMPARE_ONE_AGAINST_ANOTHER( 3 ),
        TYPE_INTERNAL_NOTES( 5 ),
        TYPE_RANK_GRID_INDEPENDENT( 22 ),
        TYPE_3D_MATRIX( 32 ),
        TYPE_TOTAL_SCORE( 33 );

        private final int typeCode;

        DbQuestionType( int typeCode ) {
            this.typeCode = typeCode;
        }

        public int getTypeCode() {
            return typeCode;
        }

        public static DbQuestionType getByTypeCode( int typeCode ) {
            for ( DbQuestionType t : values() ) {
                if ( t.getTypeCode() == typeCode ) {
                    return t;
                }
            }
            return UNKNOWN;
        }
    }

    private enum DbQuestionSubType {
        UNDEFINED( 0 ),
        LOOKUP( 1 ),
        UPLOAD_FILE( 2 ),
        MULTIPLE_LOOKUP( 3 );

        private final int typeCode;

        DbQuestionSubType( int typeCode ) {
            this.typeCode = typeCode;
        }

        public int getTypeCode() {
            return typeCode;
        }

        public static DbQuestionSubType getByTypeCode( int typeCode ) {
            for ( DbQuestionSubType t : values() ) {
                if ( t.getTypeCode() == typeCode ) {
                    return t;
                }
            }
            return UNDEFINED;
        }
    }

    private enum DbColumnType {
        EDIT_TEXT( 0 ),
        EDIT_DROPDOWN( 1 ),
        EDIT_CHECKBOX( 2 ),
        EDIT_RADIO( 3 );

        private final int typeCode;

        DbColumnType( int typeCode ) {
            this.typeCode = typeCode;
        }

        public int getTypeCode() {
            return typeCode;
        }

        public static DbColumnType getByTypeCode( int typeCode ) {
            for ( DbColumnType t : values() ) {
                if ( t.getTypeCode() == typeCode ) {
                    return t;
                }
            }
            throw new IllegalArgumentException( "Unknown column type code: " + typeCode );
        }
    }
}
