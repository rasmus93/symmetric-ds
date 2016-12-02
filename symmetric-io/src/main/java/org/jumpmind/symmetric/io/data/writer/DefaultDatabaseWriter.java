/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.io.data.writer;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.jumpmind.analytics.*;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.*;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.symmetric.io.data.*;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectExpressionKey;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriter extends AbstractDatabaseWriter {

    protected final static Logger log = LoggerFactory.getLogger(DefaultDatabaseWriter.class);


    private static final String ID = "id";
    private static final String RESPONSE_ID = "response_id";
    private static final String USER_ID = "user_id";
    private static final String FORM_ID = "form_id";
    private static final String QUESTION_ID = "question_id";
    private static final String ANSWER_ID = "answer_id";
    private static final String RANK_HEADER_ID = "rank_header_id";
    private static final String ANSWER_GROUP = "answer_group";
    private static final String RESPONSE_LABEL = "response_label";
    private static final String RESPONSE_NUMBER = "response_number";
    private static final String PASSWORD_EMAIL = "password_email";
    private static final String COMPLETED = "completed";
    private static final String CREATED = "created";
    private static final String LAST_SUBMITTED = "last_submitted";
    private static final String DELETED = "deleted";
    private static final String PARTITION_DATE = "partition_date";
    private static final String ANSWER_PREFIX = "a_";
    private static final String COLUMN_PREFIX = "_col_";
    private static final DateTimeFormatter FORMAT = DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss" );

    private static final String SURVEY_TABLE = "tblSurvey";
    private static final String VALUE_TYPE_TABLE = "tblValueType";
    private static final String QUESTION_TABLE = "tblQuestion";
    private static final String ANSWER_TABLE = "tblAnswer";
    private static final String RESPONDENT_TABLE = "tblRespondent";
    private static final String RESPONDENT_LABEL_TABLE = "tblResponseLabel";
    private static final String RANK_HEADER_COLUMN_TABLE = "tblRankGridHeaderColumn";
    private static final String ANSWER_RESULT_TEXT_TABLE = "tblAnswerResultText";
    private static final String ANSWER_RESULT_SET_TABLE = "tblAnswerResultSet";
    private static final String ANSWER_RESULT_3D = "tblAnswerResult3D";

    public static final String CUR_DATA = "DatabaseWriter.CurData";

    protected IDatabasePlatform platform;

    protected ISqlTransaction transaction;

    protected DmlStatement currentDmlStatement;

    protected Object[] currentDmlValues;

    public DefaultDatabaseWriter(IDatabasePlatform platform) {
        this(platform, null, null);
    }

    public DefaultDatabaseWriter(IDatabasePlatform platform, DatabaseWriterSettings settings) {
        this(platform, null, settings);
    }

    public DefaultDatabaseWriter(IDatabasePlatform platform,
            IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings) {
        super(conflictResolver, settings);
        this.platform = platform;
    }

    @Override
    public void open(DataContext context) {
        super.open(context);
        this.transaction = platform.getSqlTemplate().startSqlTransaction();
    }

    @Override
    public boolean start(Table table) {
        this.currentDmlStatement = null;
        boolean process = super.start(table);
        if (process && targetTable != null) {
            allowInsertIntoAutoIncrementColumns(true, this.targetTable);
        }
        return process;
    }

    @Override
    public void end(Table table) {
        super.end(table);
        allowInsertIntoAutoIncrementColumns(false, this.targetTable);
    }

    @Override
    public void end(Batch batch, boolean inError) {
        this.currentDmlStatement = null;
        super.end(batch, inError);
    }

    @Override
    public void close() {
        super.close();
        if (transaction != null) {
            this.transaction.close();
        }
    }

    @Override
    protected void commit(boolean earlyCommit) {
        if (transaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                this.transaction.commit();
                if (!earlyCommit) {
                   notifyFiltersBatchCommitted();
                } else {
                    notifyFiltersEarlyCommit();
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            }

        }
        super.commit(earlyCommit);
    }

    @Override
    protected void rollback() {
        if (transaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                this.transaction.rollback();
                notifyFiltersBatchRolledback();
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            }

        }
        super.rollback();
    }

    @Override
    public void write( CsvData data ) {
        if ( targetTable == null ) {
            targetTable = sourceTable.copy();
        }
        super.write( data );
    }

    @Override
    protected LoadStatus insert(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);

            try {
                if ( targetTable == null ) {
                    targetTable = sourceTable.copy();
                    String tableName = targetTable.getName();
                    String[] values = getRowData( data, CsvData.ROW_DATA );

                    if ( tableName.equalsIgnoreCase( SURVEY_TABLE ) ) {
                        processSurveyTable( values );
                    }
                    if ( tableName.equalsIgnoreCase( VALUE_TYPE_TABLE ) ) {
                        processValueTypeTable( values, 0 );
                    }
                    if ( tableName.equalsIgnoreCase( QUESTION_TABLE ) ) {
                        processQuestionTable( values, 0 );
                    }
                    if ( tableName.equalsIgnoreCase( ANSWER_TABLE ) ) {
                        processAnswerTable( values, "ADD" );
                    }
                    if ( tableName.equalsIgnoreCase( RESPONDENT_TABLE ) ) {
                        insertRespondentTable( values );
                    }
                    if ( tableName.equalsIgnoreCase( RESPONDENT_LABEL_TABLE ) ) {
                        processResponseLabelTable( values );
                    }
                    if ( tableName.equalsIgnoreCase( RANK_HEADER_COLUMN_TABLE ) ) {
                        processRankHeaderColumnTable( values, true, 0 );
                    }

                    if ( tableName.equalsIgnoreCase( ANSWER_RESULT_SET_TABLE ) ) {
                        processAnswerResultSetTable( values, 0 );
                    }
                    if ( tableName.equalsIgnoreCase( ANSWER_RESULT_TEXT_TABLE ) ) {
                        processAnswerResultTextTable( values, 0 );
                    }
                    if ( tableName.equalsIgnoreCase( ANSWER_RESULT_3D ) ) {
                        processAnswerResult3D( values, 0 );
                    }
                    return LoadStatus.SUCCESS;
                }

                if ( requireNewStatement( DmlType.INSERT, data, false, true, null ) ) {
                    this.lastUseConflictDetection = true;
                    this.currentDmlStatement = platform.createDmlStatement(
                            DmlType.INSERT,
                            targetTable,
                            writerSettings.getTextColumnExpression()
                    );
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Preparing dml: " + this.currentDmlStatement.getSql() );
                    }
                    transaction.prepare( this.currentDmlStatement.getSql() );
                }

                Conflict conflict = writerSettings.pickConflict(this.targetTable, batch);
                String[] values = (String[]) ArrayUtils.addAll(getRowData(data, CsvData.ROW_DATA),
                        this.currentDmlStatement.getLookupKeyData(getLookupDataMap(data, conflict)));
                long count = execute(data, values);
                statistics.get(batch).increment(DataWriterStatisticConstants.INSERTCOUNT, count);
                if (count > 0) {
                        return LoadStatus.SUCCESS;
                } else {
                    context.put(CUR_DATA,getCurData(transaction));
                    return LoadStatus.CONFLICT;
                }
            } catch (SqlException ex) {
                if (platform.getSqlTemplate().isUniqueKeyViolation(ex)) {
                    if (!platform.getDatabaseInfo().isRequiresSavePointsInTransaction()) {
                        context.put(CONFLICT_ERROR, ex);
                        context.put(CUR_DATA,getCurData(transaction));
                        return LoadStatus.CONFLICT;
                    } else {
                        log.info("Detected a conflict via an exception, but cannot perform conflict resolution because the database in use requires savepoints");
                        throw ex;
                    }
                } else {
                    throw ex;
                }
            }
        } catch (SqlException ex) {
            logFailureDetails(ex, data, true);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    private void processAnswerResultSetTable( String[] values, int deleted ) {
        ResponseData responseData = new ResponseData();
        String results = "";
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                if ( column.getName().equalsIgnoreCase( "SurveyID" ) ) {
                    responseData.setFormId( Long.parseLong( values[i] ) );
                }
                if ( column.getName().equalsIgnoreCase( "RespondentID" ) ) {
                    responseData.setId( Long.parseLong( values[i] ) );
                }
                if ( column.getName().equalsIgnoreCase( "Results" ) ) {
                    results = values[i];
                }
            }
        }

        Map<String, Object> result = getFormData( responseData.getFormId(), responseData.getId() );
        for ( String currentValue : results.split( " " ) ) {
            String[] parts = currentValue.split( "=" );
            if ( parts.length != 2 ) {
                continue;
            }
            result.put( ANSWER_PREFIX + parts[0].trim(),
                        deleted == 1
                        ? null
                        : parts[1].trim()
            );
        }
        result.put( DELETED, deleted );

        this.currentDmlStatement = platform.createDmlStatement(
                DmlType.INSERT,
                platform.readTableFromDatabase(
                        "",
                        platform.getDefaultSchema(),
                        "form_" + responseData.getFormId() + "_local"
                ),
                writerSettings.getTextColumnExpression()
        );

        transaction.prepare( this.currentDmlStatement.getSql() );
        execute( null,
                 result.values()
                       .stream()
                       .map( v -> v == null
                                  ? null
                                  : v.toString() )
                       .toArray( String[]::new )
        );
    }

    private void processAnswerResultTextTable( String[] values, int deleted ) {
        ResponseData responseData = new ResponseData();
        String results = "";
        String answerId = null;
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                if ( column.getName().equalsIgnoreCase( "SurveyID" ) ) {
                    responseData.setFormId( Long.parseLong( values[i] ) );
                }
                if ( column.getName().equalsIgnoreCase( "RespondentID" ) ) {
                    responseData.setId( Long.parseLong( values[i] ) );
                }
                if ( column.getName().equalsIgnoreCase( "AnswerResult" ) ) {
                    results = values[i];
                }
                if ( column.getName().equalsIgnoreCase( "AnswerId" ) ) {
                    answerId = values[i];
                }
            }
        }
        Map<String, Object> result = getFormData( responseData.getFormId(), responseData.getId() );
        if ( answerId == null || Long.parseLong( answerId ) <= 0L ) {
            final String finalAnswerId = answerId;
            readProperties( results, ( key, value ) -> {
                result.put(
                        ANSWER_PREFIX + String.valueOf( key ),
                        deleted == 1
                        ? null
                        : parseResultTextValue( finalAnswerId, value )
                );
                return null;
            } );
        } else {
            result.put( answerId, parseResultTextValue( answerId, results ) );
        }
        result.put( DELETED, deleted );

        this.currentDmlStatement = platform.createDmlStatement(
                DmlType.INSERT,
                platform.readTableFromDatabase(
                        "",
                        platform.getDefaultSchema(),
                        "form_" + responseData.getFormId() + "_local"
                ),
                writerSettings.getTextColumnExpression()
        );

        transaction.prepare( this.currentDmlStatement.getSql() );
        execute(
                null,
                result.values()
                      .stream()
                      .map( v -> v == null
                                 ? null
                                 : v.toString() )
                      .toArray( String[]::new )
        );
    }

    private String parseResultTextValue( String value, String answerId ) {
        return platform.getSqlTemplate().query(
                "SELECT * FROM answer_data_local FINAL WHERE id = " + answerId,
                row -> {
                    String parsedValue = value;
                    parsedValue = StringUtils.removeStart(
                            parsedValue,
                            row.getString( "prefix" )
                    );
                    parsedValue = StringUtils.removeEnd(
                            parsedValue,
                            row.getString( "suffix" )
                    );
                    return parsedValue;
                }
        ).get( 0 );
    }

    private void processAnswerResult3D( String[] values, int deleted ) {
        ResponseData responseData = new ResponseData();
        String results = "";
        Long answerId = null;
        Long columnId = null;
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                if ( column.getName().equalsIgnoreCase( "SurveyID" ) ) {
                    responseData.setFormId( Long.parseLong( values[i] ) );
                }
                if ( column.getName().equalsIgnoreCase( "RespondentID" ) ) {
                    responseData.setId( Long.parseLong( values[i] ) );
                }
                if ( column.getName().equalsIgnoreCase( "AnswerResult" ) ) {
                    results = values[i];
                }
                if ( column.getName().equalsIgnoreCase( "AnswerId" ) ) {
                    answerId = Long.parseLong( values[i] );
                }
                if ( column.getName().equalsIgnoreCase( "ColumnIndex" ) ) {
                    columnId = Long.parseLong( values[i] );
                }
            }
        }
        Map<String, Object> result = getFormData( responseData.getFormId(), responseData.getId() );
        // TODO parse type for multiply lookup - now supports only matrix type
        if ( ( columnId == null || columnId.equals( -1L ) ) && answerId != null && answerId > 0L ) {
            // Compact by column
            final Long finalAnswerId = answerId;
            readProperties( results, ( key, value ) -> {
                result.put(
                        ANSWER_PREFIX + finalAnswerId + COLUMN_PREFIX + key,
                        deleted == 1
                        ? null
                        : value
                );
                return null;
            } );
        } else if ( answerId == null || answerId.equals( -1L ) ) {
            // Compact by column and answer
            readProperties( results, ( finalAnswerId, columns ) -> {
                readProperties( columns, ( key, value ) -> {
                    result.put(
                            ANSWER_PREFIX + finalAnswerId + COLUMN_PREFIX + key,
                            deleted == 1
                            ? null
                            : value
                    );
                    return null;
                } );
                return null;
            } );
        } else {
            result.put(
                    ANSWER_PREFIX + answerId + COLUMN_PREFIX + columnId,
                    deleted == 1
                    ? null
                    : results
            );
        }
        result.put( DELETED, deleted );
        log.error( "key " + Arrays.toString( result.keySet().toArray() ) );
        log.error( "asgasg " + Arrays.toString( result.values().toArray() ) );

        this.currentDmlStatement = platform.createDmlStatement(
                DmlType.INSERT,
                platform.readTableFromDatabase( "", platform.getDefaultSchema(), "form_" + responseData.getFormId() + "_local" ),
                writerSettings.getTextColumnExpression()
        );

        transaction.prepare( this.currentDmlStatement.getSql() );
        execute( null,
                 result.values()
                       .stream()
                       .map( v -> v == null
                                  ? null
                                  : v.toString() )
                       .toArray( String[]::new )
        );
    }

    private void readProperties( String input, final BiFunction<String, String, Void> func ) {
        try {
            new Properties() {
                @Override
                public synchronized Object put( Object key, Object value ) {
                    func.apply( (String) key, (String) value );
                    return null;
                }
            }.load( new StringReader( input ) );
        } catch ( IOException e ) {
            throw new SqlException( e );
        }
    }

    private void processSurveyTable( String[] values ) {
        String formId = "";
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                if ( column.getName().equalsIgnoreCase( "SurveyID" ) ) {
                    formId = values[i];
                    break;
                }
            }
        }

        //NULL system table
        if ( formId.equals( "-117" ) ) {
            return;
        }
        new SqlScript(
                createTableQuery( "form_" + formId, getColumnNames() ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
        new SqlScript(
                createDistributedTableQuery( "form_" + formId, formId ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
    }

    private void processValueTypeTable( String[] values, int deleted ) {
        AnswerData answerData = new AnswerData();
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                String columnName = column.getName();
                if ( columnName.equalsIgnoreCase( "valueTypeId" ) ) {
                    answerData.setValueTypeId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "valType" ) ) {
                    answerData.setValueType( Integer.parseInt( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "pattern" ) ) {
                    answerData.setPattern( values[i].replaceAll( "'", "\\\\\\\'" ) );
                }
                if ( columnName.equalsIgnoreCase( "decimals" ) ) {
                    answerData.setDecimals( Integer.parseInt( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "format" ) ) {
                    answerData.setFormat( values[i] );
                }
                if ( columnName.equalsIgnoreCase( "preffix" ) ) {
                    answerData.setPrefix( values[i] );
                }
                if ( columnName.equalsIgnoreCase( "suffix" ) ) {
                    answerData.setSuffix( values[i] );
                }
            }
        }
        new SqlScript(
                String.format(
                        "INSERT INTO answer_data_local(value_type_id, value_type, pattern, " +
                                "decimals, format, prefix, suffix, deleted) " +
                                "VALUES (%d, %d, '%s', %d, '%s', '%s', '%s', %d);",
                        answerData.getValueTypeId(),
                        answerData.getValueType(),
                        answerData.getPattern(),
                        answerData.getDecimals(),
                        answerData.getFormat(),
                        answerData.getPrefix(),
                        answerData.getSuffix(),
                        deleted
                ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
    }

    private void processQuestionTable( String[] values, int deleted ) {
        QuestionData questionData = new QuestionData();
        questionData.setRankHeaderId( Long.MIN_VALUE );
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                String columnName = column.getName();
                if ( columnName.equalsIgnoreCase( "SurveyID" ) ) {
                    questionData.setFormId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "QuestionID" ) ) {
                    questionData.setId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "QuestionType" ) ) {
                    questionData.setType( Integer.parseInt( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "questionSubType" ) ) {
                    questionData.setSubType( Integer.parseInt( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "TotalColumns" ) ) {
                    questionData.setTotalColumns( Integer.parseInt( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "RankHeader" ) ) {
                    questionData.setRankHeaderId( Long.parseLong( values[i] ) );
                }
                if ( ( columnName.equalsIgnoreCase( "minimumValue" ) ||
                        columnName.equalsIgnoreCase( "maximumValue" ) ) &&
                        StringUtils.isNotEmpty( values[i] ) ) {
                    questionData.setNumeric( true );
                }
            }
        }

        new SqlScript(
                String.format(
                        "INSERT INTO question_data_local(id, form_id, type, sub_type, numeric, " +
                                "total_columns, rank_header_id, deleted) " +
                                "VALUES (%d, %d, %d, %d, %d, %d, %d, %d);",
                        questionData.getId(),
                        questionData.getFormId(),
                        questionData.getType(),
                        questionData.getSubType(),
                        questionData.isNumeric() ? 1 : 0,
                        questionData.getTotalColumns(),
                        questionData.getRankHeaderId(),
                        deleted
                ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
    }

    private void processAnswerTable( String[] values, String action ) {
        long questionId = 0;
        long answerId = 0;
        int answerType = 0;
        long valueTypeId = 0;
        String answerText = "";
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                String columnName = column.getName().toLowerCase();
                if ( columnName.equalsIgnoreCase( "QuestionID" ) ) {
                    questionId = Long.parseLong( values[i] );
                }
                if ( columnName.equalsIgnoreCase( "AnswerID" ) ) {
                    answerId = Long.parseLong( values[i] );
                }
                if ( columnName.equalsIgnoreCase( "AnswerType" ) ) {
                    answerType = Integer.parseInt( values[i] );
                }
                if ( columnName.equalsIgnoreCase( "ValueTypeId" ) ) {
                    valueTypeId = Long.parseLong( values[i] );
                }
                if ( columnName.equalsIgnoreCase( "AnswerText" ) ) {
                    answerText = values[i];
                }
            }
        }
        new SqlScript(
                String.format(
                        "INSERT INTO question_answer_local (%s, %s) VALUES (%d, %d);",
                        QUESTION_ID,
                        ANSWER_ID,
                        questionId,
                        answerId
                ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();

        List<Long> valueTypes = new ArrayList<>();
        valueTypes.add( valueTypeId );

        QuestionData questionData = platform.getSqlTemplate().query(
                "SELECT * FROM question_data_local FINAL WHERE id = " + questionId,
                this::createQuestionData
        ).get( 0 );
        List<ColumnData> columnData = platform.getSqlTemplate().query(
                "SELECT * FROM column_data_local FINAL WHERE rank_header_id = "
                        + questionData.getRankHeaderId(),
                row -> {
                    ColumnData column = new ColumnData();
                    column.setId( row.getLong( "id" ) );
                    column.setType( row.getInt( "type" ) );
                    column.setValueTypeId( row.getLong( "value_type_id" ) );
                    column.setRankHeaderId( row.getLong( "rank_header_id" ) );
                    column.setPosition( row.getInt( "position" ) );
                    valueTypes.add( column.getValueTypeId() );
                    return column;
                }
        );

        Map<Long, AnswerData> answerData = new HashMap<>();
        platform.getSqlTemplate().query(
                "SELECT * FROM answer_data_local FINAL WHERE value_type_id IN ("
                        + StringUtils.join( valueTypes, "," ) + ")",
                row -> {
                    AnswerData answer = new AnswerData();
                    answer.setValueType( row.getInt( "value_type" ) );
                    answer.setDecimals( row.getInt( "decimals" ) );
                    answerData.put( row.getLong( "value_type_id" ), answer );
                    return null;
                }
        );
        answerData.get( valueTypeId ).setText( answerText );

        if ( columnData.isEmpty() ) {
            processFormColumns(
                    answerData.get( valueTypeId ),
                    null,
                    questionData,
                    action,
                    answerId,
                    answerType,
                    null
            );
        } else {
            for ( ColumnData column : columnData ) {
                processFormColumns(
                        null,
                        answerData.get( column.getValueTypeId() ),
                        questionData,
                        action,
                        answerId,
                        answerType,
                        column
                );
            }
        }
    }

    private QuestionData createQuestionData( Row row ) {
        QuestionData question = new QuestionData();
        question.setId( row.getLong( "id" ) );
        question.setFormId( row.getLong( "form_id" ) );
        question.setType( row.getInt( "type" ) );
        question.setSubType( row.getInt( "sub_type" ) );
        question.setNumeric( row.getBoolean( "numeric" ) );
        question.setTotalColumns( row.getInt( "total_columns" ) );
        question.setRankHeaderId( row.getLong( "rank_header_id" ) );
        return question;
    }

    private void processFormColumns(
            AnswerData answerData,
            AnswerData columnAnswerData,
            QuestionData questionData,
            String action,
            long answerId,
            int answerType,
            ColumnData columnData
    ) {
        StringBuilder query = new StringBuilder();
        query.append( "ALTER TABLE form_%s " )
             .append( action )
             .append( " COLUMN a_" )
             .append( answerId );

        if ( columnData != null ) {
            query.append( COLUMN_PREFIX )
                 .append( columnData.getPosition() );
        }

        if ( !action.equals( "DROP" ) ) {
            query.append( " " )
                 .append( AnswerTypeMapping.getStoreType(
                         questionData,
                         answerType,
                         answerData,
                         columnData,
                         columnAnswerData
                 ) );
        }
        new SqlScript(
                String.format( query.toString(), questionData.getFormId() + "_local" ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
        new SqlScript(
                String.format( query.toString(), questionData.getFormId() ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
    }

    private void insertRespondentTable( String[] values ) {
        ResponseData responseData = getResponseDataFromRespondent(values);

        new SqlScript(
                String.format(
                        "INSERT INTO form_%d_local (%s, %s, %s, %s, %s) VALUES (%d, '%s', '%s', '%s', %d);",
                        responseData.getFormId(),
                        RESPONSE_ID,
                        CREATED,
                        LAST_SUBMITTED,
                        PASSWORD_EMAIL,
                        COMPLETED,
                        responseData.getId(),
                        responseData.getCreated().toString( FORMAT ),
                        responseData.getLastSubmitted().toString( FORMAT ),
                        responseData.getPasswordEmail(),
                        responseData.isCompleted() ? 1 : 0
                ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
        new SqlScript(
                String.format(
                        "INSERT INTO response_data_local (%s, %s, %s) VALUES (%d, %d, %d);",
                        ID,
                        FORM_ID,
                        ANSWER_GROUP,
                        responseData.getId(),
                        responseData.getFormId(),
                        responseData.getAnswerGroup()
                ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
    }

    private void updateRespondentTable( String[] values, int deleted ) {
        ResponseData responseData = getResponseDataFromRespondent( values );

        Map<String, Object> result = getFormData( responseData.getFormId(), responseData.getId() );
        result.put( RESPONSE_ID, responseData.getId() );
        result.put( CREATED, responseData.getCreated().toString( FORMAT ) );
        result.put( LAST_SUBMITTED, responseData.getLastSubmitted().toString( FORMAT ) );
        result.put( PASSWORD_EMAIL, responseData.getPasswordEmail() );
        result.put( DELETED, deleted );
        result.put( COMPLETED, responseData.isCompleted() ? 1 : 0 );
        insertAllDataToForm( responseData.getFormId(), result );

        new SqlScript(
                String.format(
                        "INSERT INTO response_data_local (%s, %s, %s, %s) VALUES (%d, %d, %d, %d);",
                        ID,
                        FORM_ID,
                        ANSWER_GROUP,
                        DELETED,
                        responseData.getId(),
                        responseData.getFormId(),
                        responseData.getAnswerGroup(),
                        deleted
                ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
    }

    private ResponseData getResponseDataFromRespondent( String[] values ) {
        ResponseData responseData = new ResponseData();
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                String columnName = column.getName().toLowerCase();
                if ( columnName.equalsIgnoreCase( "SurveyId" ) ) {
                    responseData.setFormId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "RespondentID" ) ) {
                    responseData.setId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "dateTimeFirstSubmit" ) ) {
                    responseData.setCreated( LocalDateTime.parse( values[i], FORMAT ) );
                }
                if ( columnName.equalsIgnoreCase( "DateTimeSubmit" ) ) {
                    responseData.setLastSubmitted( LocalDateTime.parse( values[i], FORMAT ) );
                }
                if ( columnName.equalsIgnoreCase( "email" ) ) {
                    responseData.setPasswordEmail( values[i] );
                }
                if ( columnName.equalsIgnoreCase( "AnswerGroup" ) ) {
                    responseData.setAnswerGroup( Integer.parseInt( values[i] ) );
                }
            }
        }
        return responseData;
    }

    private void processResponseLabelTable( String[] values ) {
        ResponseData responseData = new ResponseData();
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                String columnName = column.getName().toLowerCase();
                if ( columnName.equalsIgnoreCase( "respondentID" ) ) {
                    responseData.setId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "title" ) ) {
                    responseData.setLabel( values[i] );
                }
            }
        }

        platform.getSqlTemplate().query(
                "SELECT * FROM response_data_local FINAL WHERE id = " + responseData.getId(),
                row -> {
                    responseData.setFormId( row.getLong( FORM_ID ) );
                    return null;
                }
        );

        Map<String, Object> result = getFormData( responseData.getFormId(), responseData.getId() );
        result.put( RESPONSE_LABEL, responseData.getLabel() );

        this.currentDmlStatement = platform.createDmlStatement(
                DmlType.INSERT,
                platform.readTableFromDatabase( "", platform.getDefaultSchema(), "form_" + responseData.getFormId()+"_local" ),
                writerSettings.getTextColumnExpression()
        );

        transaction.prepare( this.currentDmlStatement.getSql() );
        execute( null, result.values().stream().map( Object::toString ).toArray( String[]::new ) );
    }

    private void processRankHeaderColumnTable( String[] values, boolean newPosition, int deleted ) {
        ColumnData columnData = new ColumnData();
        columnData.setValueTypeId( 0 );
        for ( int i = 0; i < targetTable.getColumnCount(); i++ ) {
            Column column = targetTable.getColumn( i );
            if ( column != null && values[i] != null ) {
                String columnName = column.getName();
                if ( columnName.equalsIgnoreCase( "ColumnId" ) ) {
                    columnData.setId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "RankHeaderId" ) ) {
                    columnData.setRankHeaderId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "ValueTypeId" ) ) {
                    columnData.setValueTypeId( Long.parseLong( values[i] ) );
                }
                if ( columnName.equalsIgnoreCase( "EditType" ) ) {
                    columnData.setType( Integer.parseInt( values[i] ) );
                }
            }
        }

        columnData.setPosition( 0 );
        platform.getSqlTemplate().query(
                String.format(
                        "SELECT %s as position FROM column_data_local FINAL WHERE %s = %d",
                        newPosition ? "count()" : "position",
                        newPosition ? RANK_HEADER_ID : ID,
                        newPosition ? columnData.getRankHeaderId() : columnData.getId()
                ),
                row -> {
                    columnData.setPosition( row.getInt( "position" ) );
                    return null;
                }
        );
        if ( !newPosition ) {
            List<QuestionData> questionData = platform.getSqlTemplate().query(
                    "SELECT * FROM question_data_local FINAL WHERE rank_header_id = " +
                            columnData.getRankHeaderId(),
                    this::createQuestionData
            );
            AnswerData answerData = platform.getSqlTemplate().query(
                    "SELECT * FROM answer_data_local FINAL WHERE value_type_id = " +
                            columnData.getValueTypeId(),
                    row -> {
                        AnswerData answer = new AnswerData();
                        answer.setValueType( row.getInt( "value_type" ) );
                        answer.setDecimals( row.getInt( "decimals" ) );
                        return answer;
                    }
            ).get( 0 );

            for ( QuestionData question : questionData ) {
                platform.getSqlTemplate().query(
                        "SELECT * FROM question_answer_local FINAL WHERE question_id = " + question.getId(),
                        row -> row.getLong( "answerId" )
                ).forEach( answerId ->
                                   processFormColumns(
                                           null,
                                           answerData,
                                           question,
                                           deleted == 0
                                           ? "MODIFY"
                                           : "DROP",
                                           answerId,
                                           0,
                                           columnData
                                   ) );
            }
        }

        new SqlScript(
                String.format(
                        "INSERT INTO column_data_local (id, rank_header_id, type, " +
                                "value_type_id, position, deleted) VALUES (%d, %d, %d, %d, %d, %d);",
                        columnData.getId(),
                        columnData.getRankHeaderId(),
                        columnData.getType(),
                        columnData.getValueTypeId(),
                        columnData.getPosition(),
                        deleted
                ),
                platform.getSqlTemplate(), true, true, true, ";", null
        ).execute();
    }

    private Map<String, Object> getFormData( long formId, long responseId ) {
        Map<String, Object> result = new LinkedHashMap<>();
        platform.getSqlTemplate().query(
                String.format(
                        "SELECT * FROM form_%d_local FINAL WHERE %s = %d",
                        formId,
                        RESPONSE_ID,
                        responseId
                ),
                row -> {
                    for ( String key : row.keySet().toArray( new String[0] ) ) {
                        result.put( key, row.get( key ) );
                    }
                    return null;
                }
        );
        return result;
    }

    private void insertAllDataToForm( long formId, Map<String, Object> result ) {
        this.currentDmlStatement = platform.createDmlStatement(
                DmlType.INSERT,
                platform.readTableFromDatabase( "", platform.getDefaultSchema(), "form_" + formId + "_local" ),
                writerSettings.getTextColumnExpression()
        );

        transaction.prepare( this.currentDmlStatement.getSql() );
        execute( null, result.values().stream().map( Object::toString ).toArray( String[]::new ) );
    }

    private String createTableQuery( String tableName, List<String> columns ) {
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s_local (%s) " +
                        " ENGINE = ReplacingMergeTree(%s, %s, 8192);",
                tableName,
                StringUtils.join( columns, ", " ),
                PARTITION_DATE,
                RESPONSE_ID
        );
    }

    private String createDistributedTableQuery( String tableName, String id ) {
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s AS %s_local" +
                        " ENGINE = Distributed(test_cluster, analytics, %s_local, %s);",
                tableName,
                tableName,
                tableName,
                id
        );
    }

    private List<String> getColumnNames() {
        List<String> columns = new ArrayList<>();
        columns.add( String.format( "%s Int64", RESPONSE_ID ) );
        columns.add( String.format( "%s DateTime", CREATED ) );
        columns.add( String.format( "%s DateTime", LAST_SUBMITTED ) );
        columns.add( String.format( "%s String", PASSWORD_EMAIL ) );
        columns.add( String.format( "%s String", RESPONSE_LABEL ) );
        columns.add( String.format( "%s Int64", RESPONSE_NUMBER ) );
        columns.add( String.format( "%s Int64", USER_ID ) );
        columns.add( String.format( "%s UInt8", DELETED ) );
        columns.add( String.format( "%s Date", PARTITION_DATE ) );
        columns.add( String.format( "%s UInt8", COMPLETED ) );
        return columns;
    }

    @Override
    protected LoadStatus delete(CsvData data, boolean useConflictDetection) {
        try {
            if ( targetTable == null ) {
                targetTable = sourceTable.copy();
                String tableName = targetTable.getName();
                String[] values = getRowData( data, CsvData.OLD_DATA );

                if ( tableName.equalsIgnoreCase( VALUE_TYPE_TABLE ) ) {
                    processValueTypeTable( values, 1 );
                }
                if ( tableName.equalsIgnoreCase( QUESTION_TABLE ) ) {
                    processQuestionTable( values, 1 );
                }
                if ( tableName.equalsIgnoreCase( ANSWER_TABLE ) ) {
                    processAnswerTable( values, "DROP" );
                }
                if ( tableName.equalsIgnoreCase( RESPONDENT_TABLE ) ) {
                    updateRespondentTable( values, 1 );
                }
                if ( tableName.equalsIgnoreCase( RANK_HEADER_COLUMN_TABLE ) ) {
                    processRankHeaderColumnTable( values, false, 1 );
                }

                if ( tableName.equalsIgnoreCase( ANSWER_RESULT_SET_TABLE ) ) {
                    processAnswerResultSetTable( values, 1 );
                }
                if ( tableName.equalsIgnoreCase( ANSWER_RESULT_TEXT_TABLE ) ) {
                    processAnswerResultTextTable( values, 1 );
                }
                if ( tableName.equalsIgnoreCase( ANSWER_RESULT_3D ) ) {
                    processAnswerResult3D( values, 1 );
                }
                return LoadStatus.SUCCESS;
            }
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            Conflict conflict = writerSettings.pickConflict(this.targetTable, batch);
            Map<String, String> lookupDataMap = null;
            if (requireNewStatement(DmlType.DELETE, data, useConflictDetection, useConflictDetection,
                    conflict.getDetectType())) {
                this.lastUseConflictDetection = useConflictDetection;
                List<Column> lookupKeys = null;
                if (!useConflictDetection) {
                    lookupKeys = targetTable.getPrimaryKeyColumnsAsList();
                } else {
                    switch (conflict.getDetectType()) {
                        case USE_OLD_DATA:
                            lookupKeys = targetTable.getColumnsAsList();
                            break;
                        case USE_VERSION:
                        case USE_TIMESTAMP:
                            List<Column> lookupColumns = new ArrayList<Column>();
                            Column versionColumn = targetTable.getColumnWithName(conflict
                                    .getDetectExpression());
                            if (versionColumn != null) {
                                lookupColumns.add(versionColumn);
                            } else {
                                log.error(
                                        "Could not find the timestamp/version column with the name {} on table {}.  Defaulting to using primary keys for the lookup.",
                                        conflict.getDetectExpression(), targetTable.getName());
                            }
                            Column[] pks = targetTable.getPrimaryKeyColumns();
                            for (Column column : pks) {
                                // make sure all of the PK keys are in the list
                                // only once and are always at the end of the
                                // list
                                lookupColumns.remove(column);
                                lookupColumns.add(column);
                            }
                            lookupKeys = lookupColumns;
                            break;
                        case USE_PK_DATA:
                        default:
                            lookupKeys = targetTable.getPrimaryKeyColumnsAsList();
                            break;
                    }
                }

                if (lookupKeys == null || lookupKeys.size() == 0) {
                    lookupKeys = targetTable.getColumnsAsList();
                }

                /*List<Column> changedColumnsList = new ArrayList<Column>();
                if (platform.getName().equals( DatabaseNamesConstants.CLICKHOUSE )) {
                    lookupKeys = targetTable.getColumnsAsList();
                    changedColumnsList = targetTable.getColumnsAsList();
                }*/

                int lookupKeyCountBeforeColumnRemoval = lookupKeys.size();

                Iterator<Column> it = lookupKeys.iterator();
                while (it.hasNext()) {
                    Column col = it.next();
                    if ((platform.isLob(col.getMappedTypeCode()) && data.isNoBinaryOldData())
                            || !platform.canColumnBeUsedInWhereClause(col)) {
                        it.remove();
                    }
                }

                if (lookupKeys.size() == 0) {
                    String msg = "There are no keys defined for "
                            + targetTable.getFullyQualifiedTableName()
                            + ".  Cannot build an update statement.  ";
                    if (lookupKeyCountBeforeColumnRemoval > 0) {
                        msg += "The only keys defined are binary and they have been removed.";
                    }
                    throw new IllegalStateException(msg);
                }

                lookupDataMap = getLookupDataMap(data, conflict);

                boolean[] nullKeyValues = new boolean[lookupKeys.size()];
                for (int i = 0; i < lookupKeys.size(); i++) {
                    Column column = lookupKeys.get(i);
                    nullKeyValues[i] = !column.isRequired()
                            && lookupDataMap.get(column.getName()) == null;
                }

                /*this.currentDmlStatement = platform.createDmlStatement(
                        DmlType.DELETE,
                        targetTable.getCatalog(),
                        targetTable.getSchema(),
                        targetTable.getName(),
                        lookupKeys.toArray( new Column[lookupKeys.size()] ),
                        changedColumnsList.toArray(new Column[changedColumnsList.size()]),
                        nullKeyValues,
                        writerSettings.getTextColumnExpression()
                );*/
                this.currentDmlStatement = platform.createDmlStatement(DmlType.DELETE,
                        targetTable.getCatalog(), targetTable.getSchema(), targetTable.getName(),
                        lookupKeys.toArray(new Column[lookupKeys.size()]), null, nullKeyValues, writerSettings.getTextColumnExpression());
                if (log.isDebugEnabled()) {
                    log.debug("Preparing dml: " + this.currentDmlStatement.getSql());
                }
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            try {
                lookupDataMap = lookupDataMap == null ? getLookupDataMap(data, conflict) : lookupDataMap;
                long count = execute(data, this.currentDmlStatement.getLookupKeyData(lookupDataMap));
                statistics.get(batch).increment(DataWriterStatisticConstants.DELETECOUNT, count);
                if (count > 0) {
                        return LoadStatus.SUCCESS;
                } else {
                    context.put(CUR_DATA,null); // since a delete conflicted, there's no row to delete, so no cur data.
                    return LoadStatus.CONFLICT;
                }
            } catch (SqlException ex) {
                if (platform.getSqlTemplate().isUniqueKeyViolation(ex)
                        && !platform.getDatabaseInfo().isRequiresSavePointsInTransaction()) {
                    context.put(CUR_DATA,null); // since a delete conflicted, there's no row to delete, so no cur data.
                    return LoadStatus.CONFLICT;
                } else {
                    throw ex;
                }
            }
        } catch (SqlException ex) {
            logFailureDetails(ex, data, true);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    @Override
    protected LoadStatus update(CsvData data, boolean applyChangesOnly, boolean useConflictDetection) {
        try {
            if ( targetTable == null ) {
                targetTable = sourceTable.copy();
                String tableName = targetTable.getName();
                String[] values = getRowData( data, CsvData.ROW_DATA );

                if ( tableName.equalsIgnoreCase( VALUE_TYPE_TABLE ) ) {
                    processValueTypeTable( values, 0 );
                }
                if ( tableName.equalsIgnoreCase( QUESTION_TABLE ) ) {
                    processQuestionTable( values, 0 );
                }
                if ( tableName.equalsIgnoreCase( ANSWER_TABLE ) ) {
                    processAnswerTable( values, "MODIFY" );
                }
                if ( tableName.equalsIgnoreCase( RESPONDENT_TABLE ) ) {
                    updateRespondentTable( values, 0 );
                }
                if ( tableName.equalsIgnoreCase( RESPONDENT_LABEL_TABLE ) ) {
                    processResponseLabelTable( values );
                }
                if ( tableName.equalsIgnoreCase( RANK_HEADER_COLUMN_TABLE ) ) {
                    processRankHeaderColumnTable( values, false, 0 );
                }

                if ( tableName.equalsIgnoreCase( ANSWER_RESULT_SET_TABLE ) ) {
                    log.error( "ResultSetWHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT???!?!?!?!?!?!" );
                }
                if ( tableName.equalsIgnoreCase( ANSWER_RESULT_TEXT_TABLE ) ) {
                    log.error( "ResulttextWHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT???!?!?!?!?!?!" );
                }
                return LoadStatus.SUCCESS;
            }
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String[] rowData = getRowData(data, CsvData.ROW_DATA);
            String[] oldData = getRowData(data, CsvData.OLD_DATA);
            ArrayList<String> changedColumnNameList = new ArrayList<String>();
            ArrayList<String> changedColumnValueList = new ArrayList<String>();
            ArrayList<Column> changedColumnsList = new ArrayList<Column>();
            for (int i = 0; i < targetTable.getColumnCount(); i++) {
                Column column = targetTable.getColumn(i);
                if (column != null) {
                    if (doesColumnNeedUpdated(i, column, data, rowData, oldData, applyChangesOnly)) {
                        changedColumnNameList.add(column.getName());
                        changedColumnsList.add(column);
                        changedColumnValueList.add(rowData[i]);
                    }
                }
            }

            if (changedColumnNameList.size() > 0) {
                Map<String, String> lookupDataMap = null;
                Conflict conflict = writerSettings.pickConflict(this.targetTable, batch);
                if (requireNewStatement(DmlType.UPDATE, data, applyChangesOnly,
                        useConflictDetection, conflict.getDetectType())) {
                    lastApplyChangesOnly = applyChangesOnly;
                    lastUseConflictDetection = useConflictDetection;
                    List<Column> lookupKeys = null;
                    if (!useConflictDetection) {
                        lookupKeys = targetTable.getPrimaryKeyColumnsAsList();
                    } else {
                        switch (conflict.getDetectType()) {
                            case USE_CHANGED_DATA:
                                ArrayList<Column> lookupColumns = new ArrayList<Column>(
                                        changedColumnsList);
                                Column[] pks = targetTable.getPrimaryKeyColumns();
                                for (Column column : pks) {
                                    // make sure all of the PK keys are in the
                                    // list only once and are always at the end
                                    // of the list
                                    lookupColumns.remove(column);
                                    lookupColumns.add(column);
                                }
                                removeExcludedColumns(conflict, lookupColumns);
                                lookupKeys = lookupColumns;
                                break;
                            case USE_OLD_DATA:
                                lookupKeys = targetTable.getColumnsAsList();
                                break;
                            case USE_VERSION:
                            case USE_TIMESTAMP:
                                lookupColumns = new ArrayList<Column>();
                                Column versionColumn = targetTable.getColumnWithName(conflict
                                        .getDetectExpression());
                                if (versionColumn != null) {
                                    lookupColumns.add(versionColumn);
                                } else {
                                    log.error(
                                            "Could not find the timestamp/version column with the name {} on table {}.  Defaulting to using primary keys for the lookup.",
                                            conflict.getDetectExpression(), targetTable.getName());
                                }
                                pks = targetTable.getPrimaryKeyColumns();
                                for (Column column : pks) {
                                    // make sure all of the PK keys are in the
                                    // list only once and are always at the end
                                    // of the list
                                    lookupColumns.remove(column);
                                    lookupColumns.add(column);
                                }
                                lookupKeys = lookupColumns;
                                break;
                            case USE_PK_DATA:
                            default:
                                lookupKeys = targetTable.getPrimaryKeyColumnsAsList();
                                break;
                        }
                    }

                    if (lookupKeys == null || lookupKeys.size() == 0) {
                        lookupKeys = targetTable.getColumnsAsList();
                    }

                    if (platform.getName().equals( DatabaseNamesConstants.CLICKHOUSE )) {
                        lookupKeys = targetTable.getColumnsAsList();
                        changedColumnsList.clear();
                        changedColumnsList.addAll( targetTable.getColumnsAsList() );
                    }

                    int lookupKeyCountBeforeColumnRemoval = lookupKeys.size();

                    Iterator<Column> it = lookupKeys.iterator();
                    while (it.hasNext()) {
                        Column col = it.next();
                        if ((platform.isLob(col.getMappedTypeCode()) && data.isNoBinaryOldData())
                                || !platform.canColumnBeUsedInWhereClause(col)) {
                            it.remove();
                        }
                    }

                    if (lookupKeys.size() == 0) {
                        String msg = "There are no keys defined for "
                                + targetTable.getFullyQualifiedTableName()
                                + ".  Cannot build an update statement.  ";
                        if (lookupKeyCountBeforeColumnRemoval > 0) {
                            msg += "The only keys defined are binary and they have been removed.";
                        }
                        throw new IllegalStateException(msg);
                    }

                    lookupDataMap = getLookupDataMap(data, conflict);

                    boolean[] nullKeyValues = new boolean[lookupKeys.size()];
                    for (int i = 0; i < lookupKeys.size(); i++) {
                        Column column = lookupKeys.get(i);
                        // the isRequired is a bit of a hack. This nullKeyValues
                        // should really be checking against the object values
                        // because some null values get translated into empty
                        // strings
                        nullKeyValues[i] = !column.isRequired()
                                && lookupDataMap.get(column.getName()) == null;
                    }

                    this.currentDmlStatement = platform.createDmlStatement(DmlType.UPDATE,
                            targetTable.getCatalog(), targetTable.getSchema(),
                            targetTable.getName(),
                            lookupKeys.toArray(new Column[lookupKeys.size()]),
                            changedColumnsList.toArray(new Column[changedColumnsList.size()]),
                            nullKeyValues, writerSettings.getTextColumnExpression());
                    if (log.isDebugEnabled()) {
                        log.debug("Preparing dml: " + this.currentDmlStatement.getSql());
                    }
                    transaction.prepare(this.currentDmlStatement.getSql());

                }

                rowData = (String[]) changedColumnValueList
                        .toArray(new String[changedColumnValueList.size()]);
                lookupDataMap = lookupDataMap == null ? getLookupDataMap(data, conflict) : lookupDataMap;
                if (platform.getName().equals( DatabaseNamesConstants.CLICKHOUSE )) {
                    rowData = getRowData(data, CsvData.ROW_DATA);
                    lookupDataMap = null;
                }
                String[] values = (String[]) ArrayUtils.addAll(rowData,
                        this.currentDmlStatement.getLookupKeyData(lookupDataMap));

                try {
                    long count = execute(data, values);
                    statistics.get(batch)
                            .increment(DataWriterStatisticConstants.UPDATECOUNT, count);
                    if ( count > 0 || platform.getName()
                                              .equals( DatabaseNamesConstants.CLICKHOUSE ) ) {
                        return LoadStatus.SUCCESS;
                    } else {
                        context.put(CUR_DATA,getCurData(transaction));
                        return LoadStatus.CONFLICT;
                    }
                } catch (SqlException ex) {
                    if (platform.getSqlTemplate().isUniqueKeyViolation(ex)
                            && !platform.getDatabaseInfo().isRequiresSavePointsInTransaction()) {
                        context.put(CUR_DATA,getCurData(transaction));
                        return LoadStatus.CONFLICT;
                    } else {
                        throw ex;
                    }
                }
            } else {
                if (log.isDebugEnabled()) {
                   log.debug("Not running update for table {} with pk of {}.  There was no change to apply",
                           targetTable.getFullyQualifiedTableName(), data.getCsvData(CsvData.PK_DATA));
                }
                // There was no change to apply
                return LoadStatus.SUCCESS;
            }
        } catch (SqlException ex) {
            logFailureDetails(ex, data, true);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    @Override
    protected boolean create(CsvData data) {
        String xml = null;
        try {
            transaction.commit();

            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            xml = data.getParsedData(CsvData.ROW_DATA)[0];
            log.info("About to create table using the following definition: {}", xml);
            StringReader reader = new StringReader(xml);
            Database db = DatabaseXmlUtil.read(reader, false);
            if (writerSettings.isCreateTableAlterCaseToMatchDatabaseDefault()) {
                platform.alterCaseToMatchDatabaseDefaultCase(db);
            }

            platform.makePlatformSpecific(db);

            if (writerSettings.isAlterTable()) {
                platform.alterDatabase(db, !writerSettings.isCreateTableFailOnError());
            } else {
                platform.createDatabase(db, writerSettings.isCreateTableDropFirst(), !writerSettings.isCreateTableFailOnError());
            }

            platform.resetCachedTableModel();
            statistics.get(batch).increment(DataWriterStatisticConstants.CREATECOUNT);
            return true;
        } catch (RuntimeException ex) {
            log.error("Failed to alter table using the following xml: {}", xml);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    @Override
    protected boolean sql(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String script = data.getParsedData(CsvData.ROW_DATA)[0];
            List<String> sqlStatements = getSqlStatements(script);
            long count = 0;
            for (String sql : sqlStatements) {

                sql = preprocessSqlStatement(sql);
                transaction.prepare(sql);
                if (log.isDebugEnabled()) {
                    log.debug("About to run: {}", sql);
                }
                count += transaction.prepareAndExecute(sql);
                if (log.isDebugEnabled()) {
                    log.debug("{} rows updated when running: {}", count, sql);
                }
            }
            statistics.get(batch).increment(DataWriterStatisticConstants.SQLCOUNT);
            statistics.get(batch).increment(DataWriterStatisticConstants.SQLROWSAFFECTEDCOUNT,
                    count);
            return true;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    protected boolean requireNewStatement(DmlType currentType, CsvData data,
            boolean applyChangesOnly, boolean useConflictDetection,
            Conflict.DetectConflict detectType) {
        boolean requiresNew = currentDmlStatement == null || lastData == null
                || currentDmlStatement.getDmlType() != currentType
                || lastData.getDataEventType() != data.getDataEventType()
                || lastApplyChangesOnly != applyChangesOnly
                || lastUseConflictDetection != useConflictDetection;
        if (!requiresNew && currentType == DmlType.UPDATE) {
            String currentChanges = Arrays.toString(data.getChangedDataIndicators());
            String lastChanges = Arrays.toString(lastData.getChangedDataIndicators());
            requiresNew = !currentChanges.equals(lastChanges);
        }

        if (!requiresNew) {
            requiresNew |= containsNullLookupKeyDataSinceLastStatement(currentType, data,
                    detectType);
        }

        return requiresNew;
    }

    protected boolean containsNullLookupKeyDataSinceLastStatement(DmlType currentType,
            CsvData data, DetectConflict detectType) {
        boolean foundNullValueChange = false;
        if (currentType == DmlType.UPDATE || currentType == DmlType.DELETE) {
            if (detectType != null
                    && (detectType == DetectConflict.USE_CHANGED_DATA || detectType == DetectConflict.USE_OLD_DATA)) {
                String[] lastOldData = lastData.getParsedData(CsvData.OLD_DATA);
                String[] newOldData = data.getParsedData(CsvData.OLD_DATA);
                if (lastOldData != null && newOldData != null) {
                    for (int i = 0; i < lastOldData.length && i < newOldData.length; i++) {
                        String lastValue = lastOldData[i];
                        String value = newOldData[i];
                        if ((lastValue != null && value == null)
                                || (value != null && lastValue == null)) {
                            foundNullValueChange = true;
                        }
                    }
                }
            } else {
                String[] lastpkData = lastData.getParsedData(CsvData.PK_DATA);
                String[] newpkData = data.getParsedData(CsvData.PK_DATA);
                if (lastpkData != null && newpkData != null) {
                    for (int i = 0; i < lastpkData.length && i < newpkData.length; i++) {
                        String lastValue = lastpkData[i];
                        String value = newpkData[i];
                        if ((lastValue != null && value == null)
                                || (value != null && lastValue == null)) {
                            foundNullValueChange = true;
                        }
                    }
                }
            }
        }
        return foundNullValueChange;
    }

    @Override
    protected void targetTableWasChangedByFilter(Table oldTargetTable) {
        // allow for auto increment columns to be inserted into if appropriate
        if (oldTargetTable!=null) {
            allowInsertIntoAutoIncrementColumns(false, oldTargetTable);
        }
        allowInsertIntoAutoIncrementColumns(true, targetTable);
    }

    private void removeExcludedColumns(Conflict conflict,
            ArrayList<Column> lookupColumns) {
        String excludedString = conflict.getDetectExpressionValue(
                DetectExpressionKey.EXCLUDED_COLUMN_NAMES);
        if (!StringUtils.isBlank(excludedString)) {
            String excludedColumns[] = excludedString.split(",");
            if (excludedColumns.length > 0) {
                Iterator<Column> iter = lookupColumns.iterator();
                while (iter.hasNext()) {
                    Column column = iter.next();
                    for (String excludedColumn : excludedColumns) {
                        if (excludedColumn.trim().equalsIgnoreCase(column.getName())) {
                            iter.remove();
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void logFailureDetails(Throwable e, CsvData data, boolean logLastDmlDetails) {
        StringBuilder failureMessage = new StringBuilder();
        failureMessage.append("Failed to process ");
        failureMessage.append(data.getDataEventType().toString().toLowerCase());
        failureMessage.append(" event in batch ");
        failureMessage.append(batch.getBatchId());
        failureMessage.append(".\n");

        if (logLastDmlDetails && this.currentDmlStatement != null) {
            failureMessage.append("Failed sql was: ");
            failureMessage.append(this.currentDmlStatement.getSql());
            failureMessage.append("\n");
        }

        if (logLastDmlDetails && this.currentDmlValues != null) {
            failureMessage.append("Failed sql parameters: ");
            failureMessage.append(StringUtils.abbreviate(Arrays.toString(currentDmlValues), CsvData.MAX_DATA_SIZE_TO_PRINT_TO_LOG));
            failureMessage.append("\n");
            failureMessage.append("Failed sql parameters types: ");
            failureMessage.append(Arrays.toString(this.currentDmlStatement.getTypes()));
            failureMessage.append("\n");
        }

        if (logLastDmlDetails && e instanceof SqlException && e.getCause() instanceof SQLException) {
            SQLException se = (SQLException) e.getCause();
            failureMessage.append("Failed sql state and code: ").append(se.getSQLState());
            failureMessage.append(" (").append(se.getErrorCode()).append(")");
            failureMessage.append("\n");
        }

        data.writeCsvDataDetails(failureMessage);

        log.info(failureMessage.toString(), e);
    }

    @Override
    protected void bindVariables(Map<String, Object> variables) {
        super.bindVariables(variables);
        ISqlTemplate template = platform.getSqlTemplate();
        Class<?> templateClass = template.getClass();
        if (templateClass.getSimpleName().equals("JdbcSqlTemplate")) {
            try {
                Method method = templateClass.getMethod("getDataSource");
                variables.put("DATASOURCE", method.invoke(template));
            } catch (Exception e) {
                log.warn("Had trouble looking up the datasource used by the sql template", e);
            }
        }
    }

    protected List<String> getSqlStatements(String script) {
        List<String> sqlStatements = new ArrayList<String>();
        SqlScriptReader scriptReader = new SqlScriptReader(new StringReader(script));
        try {
            String sql = scriptReader.readSqlStatement();

            while (sql != null) {
            	if (StringUtils.startsWithIgnoreCase(sql,"delimiter")) {
            		if (log.isDebugEnabled()) {
            			log.debug("Found delimiter line: "+sql);
            		}
            		String delimiter = StringUtils.trimToNull(sql.substring("delimiter".length()));
            		if (delimiter!=null) {
            			scriptReader.setDelimiter(delimiter);
            		}
            	} else {
                    sqlStatements.add(sql);
            	}

                sql = scriptReader.readSqlStatement();
            }
            return sqlStatements;
        } finally {
            IOUtils.closeQuietly(scriptReader);
        }
    }

    protected String preprocessSqlStatement(String sql) {
		sql = FormatUtils.replace("nodeId", batch.getTargetNodeId(), sql);
		if (targetTable != null) {
			sql = FormatUtils.replace("catalogName", quoteString(targetTable.getCatalog()),sql);
			sql = FormatUtils.replace("schemaName", quoteString(targetTable.getSchema()), sql);
			sql = FormatUtils.replace("tableName", quoteString(targetTable.getName()), sql);
		} else if (sourceTable != null){
			sql = FormatUtils.replace("catalogName", quoteString(sourceTable.getCatalog()),sql);
			sql = FormatUtils.replace("schemaName", quoteString(sourceTable.getSchema()), sql);
			sql = FormatUtils.replace("tableName", quoteString(sourceTable.getName()), sql);
		}

		sql = platform.scrubSql(sql);

        sql = FormatUtils.replace("sourceNodeId", (String) context.get("sourceNodeId"), sql);
        sql = FormatUtils.replace("sourceNodeExternalId",
                (String) context.get("sourceNodeExternalId"), sql);
        sql = FormatUtils.replace("sourceNodeGroupId", (String) context.get("sourceNodeGroupId"),
                sql);
        sql = FormatUtils.replace("targetNodeId", (String) context.get("targetNodeId"), sql);
        sql = FormatUtils.replace("targetNodeExternalId",
                (String) context.get("targetNodeExternalId"), sql);
        sql = FormatUtils.replace("targetNodeGroupId", (String) context.get("targetNodeGroupId"),
                sql);

		return sql;
    }

    protected String quoteString(String string) {
    	if (!StringUtils.isEmpty(string)) {
			String quote = platform.getDdlBuilder().isDelimitedIdentifierModeOn() ? platform
					.getDatabaseInfo().getDelimiterToken() : "";
			return String.format("%s%s%s", quote, string, quote);
    	} else {
    		return string;
    	}

    }

    protected boolean doesColumnNeedUpdated(int targetColumnIndex, Column column, CsvData data, String[] rowData, String[] oldData,
            boolean applyChangesOnly) {
        boolean needsUpdated = true;
        if (!platform.getDatabaseInfo().isAutoIncrementUpdateAllowed() && column.isAutoIncrement()) {
            needsUpdated = false;
        } else if (oldData != null && applyChangesOnly) {
            /*
             * Old data isn't captured for some lob fields. When both values are
             * null, then we always have to update because we don't know if the
             * lob field was previously null.
             */
            boolean containsEmptyLobColumn = platform.isLob(column.getMappedTypeCode())
                    && StringUtils.isBlank(oldData[targetColumnIndex]);
            needsUpdated = !StringUtils.equals(rowData[targetColumnIndex], oldData[targetColumnIndex])
                    || data.getParsedData(CsvData.OLD_DATA) == null
                    || containsEmptyLobColumn;
            if (containsEmptyLobColumn) {
                // indicate that we are considering the column to be changed
                data.getChangedDataIndicators()[sourceTable.getColumnIndex(column.getName())] = true;
            }
        } else {
            /*
             * This is in support of creating update statements that don't use
             * the keys in the set portion of the update statement. </p> In
             * oracle (and maybe not only in oracle) if there is no index on
             * child table on FK column and update is performing on PK on master
             * table, table lock is acquired on child table. Table lock is taken
             * not in exclusive mode, but lock contentions is possible.
             */
            needsUpdated = !column.isPrimaryKey()
                    || !StringUtils.equals(rowData[targetColumnIndex], getPkDataFor(data, column));
            /*
             * A primary key change isn't indicated in the change data indicators when there is no old
             * data.  Need to update it manually in that case.
             */
            data.getChangedDataIndicators()[sourceTable.getColumnIndex(column.getName())] = needsUpdated;
        }
        return needsUpdated;
    }

    protected int execute(CsvData data, String[] values) {
        currentDmlValues = platform.getObjectValues(batch.getBinaryEncoding(), values,
                currentDmlStatement.getMetaData(), false, writerSettings.isFitToColumn());
        if (log.isDebugEnabled()) {
            log.debug("Submitting data {} with types {}", Arrays.toString(currentDmlValues),
                    Arrays.toString(this.currentDmlStatement.getTypes()));
        }
        return transaction.addRow(data, currentDmlValues, this.currentDmlStatement.getTypes());
    }

    @Override
    protected Table lookupTableAtTarget(Table sourceTable) {
        String tableNameKey = sourceTable.getTableKey();
        Table table = targetTables.get(tableNameKey);
        if (table == null) {
            table = platform.getTableFromCache(sourceTable.getCatalog(), sourceTable.getSchema(),
                    sourceTable.getName(), false);
            if (table != null) {
                table = table.copyAndFilterColumns(sourceTable.getColumnNames(),
                        sourceTable.getPrimaryKeyColumnNames(),
                        this.writerSettings.isUsePrimaryKeysFromSource());

                Column[] columns = table.getColumns();
                for (Column column : columns) {
                    if (column != null) {
                        int typeCode = column.getMappedTypeCode();
                        if (this.writerSettings.isTreatDateTimeFieldsAsVarchar()
                                && (typeCode == Types.DATE || typeCode == Types.TIME || typeCode == Types.TIMESTAMP)) {
                            column.setMappedTypeCode(Types.VARCHAR);
                        }
                    }
                }

                targetTables.put(tableNameKey, table);
            }
        }
        return table;
    }

    public DmlStatement getCurrentDmlStatement() {
        return currentDmlStatement;
    }

    public ISqlTransaction getTransaction() {
        return transaction;
    }

    public IDatabasePlatform getPlatform() {
        return platform;
    }

    public DatabaseWriterSettings getWriterSettings() {
        return writerSettings;
    }

    protected String getCurData(ISqlTransaction transaction) {
        String curVal = null;
        if (writerSettings.isSaveCurrentValueOnError()) {
            String[] keyNames = Table.getArrayColumns(context.getTable().getPrimaryKeyColumns());
            String[] columnNames = Table.getArrayColumns(context.getTable().getColumns());

            org.jumpmind.db.model.Table targetTable = platform.getTableFromCache(
                    context.getTable().getCatalog(), context.getTable().getSchema(),
                    context.getTable().getName(), false);

            targetTable = targetTable.copyAndFilterColumns(columnNames, keyNames, true);

            String[] data = context.getData().getParsedData(CsvData.OLD_DATA);
            if (data == null) {
                data = context.getData().getParsedData(CsvData.ROW_DATA);
            }

            Column[] columns = targetTable.getColumns();

            Object[] objectValues = platform.getObjectValues(context.getBatch().getBinaryEncoding(), data,
                    columns);

            Map<String, Object> columnDataMap = CollectionUtils
                    .toMap(columnNames, objectValues);

            Column[] pkColumns = targetTable.getPrimaryKeyColumns();
            Object[] args = new Object[pkColumns.length];
            for (int i = 0; i < pkColumns.length; i++) {
                args[i] = columnDataMap.get(pkColumns[i].getName());
            }

            DmlStatement sqlStatement = platform
                    .createDmlStatement(DmlType.SELECT, targetTable, writerSettings.getTextColumnExpression());


            Row row = null;
            List<Row> list =  transaction.query(sqlStatement.getSql(),
                    new ISqlRowMapper<Row>() {
                        public Row mapRow(Row row) {
                            return row;
                        }
            }
                    , args, null);

            if (list != null && list.size() > 0) {
                row=list.get(0);
            }

            if (row != null) {
                String[] existData = platform.getStringValues(context.getBatch().getBinaryEncoding(),
                        columns, row, false, false);
                if (existData != null) {
                    curVal =  CsvUtils.escapeCsvData(existData);
                }
            }
        }
        return curVal;

    }

    @Override
    protected void allowInsertIntoAutoIncrementColumns(boolean value, Table table) {
        DatabaseInfo dbInfo = platform.getDatabaseInfo();
        String quote = dbInfo.getDelimiterToken();
        String catalogSeparator = dbInfo.getCatalogSeparator();
        String schemaSeparator = dbInfo.getSchemaSeparator();
        transaction.allowInsertIntoAutoIncrementColumns(value, table, quote, catalogSeparator, schemaSeparator);
    }

}
