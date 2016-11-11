package org.jumpmind.db.platform.clickhouse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseDmlStatement extends DmlStatement {

    private static final String DATE_COLUMN_NAME = "partition_date";
    private static final String RESPONSE_ID = "response_id";
    private static final String DELETED = "deleted";
    private static final String SUBMIT_DATE = "submit_date";
    private static final String USER_ID = "user_id";

    public ClickHouseDmlStatement(
            DmlType type, String catalogName, String schemaName, String tableName,
            Column[] keysColumns, Column[] columns, boolean[] nullKeyValues,
            DatabaseInfo databaseInfo, boolean useQuotedIdentifiers, String textColumnExpression
    ) {
        super( type, "", "", tableName, keysColumns, columns,
               nullKeyValues, databaseInfo, useQuotedIdentifiers, textColumnExpression
        );
    }

    @Override
    protected String buildUpdateSql( String tableName, Column[] keyColumns, Column[] columns ) {
        if ( tableName.equals( "tblSurvey" ) ) {
            return createTableQuery( "form_%s", getColumnNames() );
        }
        StringBuilder sql = new StringBuilder( "INSERT INTO " + tableName + " (" );
        appendColumns( sql, columns, false );
        sql.append( ") VALUES (" );
        appendColumnParameters( sql, columns );
        sql.append( ")" );
        return sql.toString();
    }

    protected String buildDeleteSql( String tableName, Column[] keyColumns ) {
        StringBuilder sql = new StringBuilder( "INSERT INTO " + tableName + " (" );
        appendColumns( sql, keyColumns, false );
        sql.append( ", deleted) VALUES (" );
        appendColumnParameters( sql, keyColumns );
        sql.append( ", 1)" );
        return sql.toString();
    }

    @Override
    protected String buildInsertSql( String tableName, Column[] keys, Column[] columns ) {
        if ( tableName.equals( "tblSurvey" ) ) {
            return createTableQuery( "form_%s", getColumnNames() )
                    + createDistributedTableQuery( "form_%s" );
        }
        StringBuilder sql = new StringBuilder( "INSERT INTO " + tableName + " (" );
        appendColumns( sql, columns, false );
        sql.append( ") VALUES (" );
        appendColumnParameters( sql, columns );
        sql.append( ")" );
        return sql.toString();
    }

    @Override
    protected String buildSelectSql( String tableName, Column[] keyColumns, Column[] columns ) {
        StringBuilder sql = new StringBuilder( "SELECT " );
        appendColumns( sql, columns, true );
        sql.append( " FROM " )
           .append( tableName )
           .append( " FINAL WHERE " );
        appendColumnsEquals( sql, keyColumns, nullKeyValues, " AND " );
        return sql.toString();
    }

    private List<String> getColumnNames() {
        List<String> columns = new ArrayList<String>();
        columns.add( String.format( "%s Int64", RESPONSE_ID ) );
        columns.add( String.format( "%s Int64", SUBMIT_DATE ) );
        columns.add( String.format( "%s UInt8", DELETED ) );
        columns.add( String.format( "%s Int64", USER_ID ) );
        return columns;
    }

    private String createTableQuery( String tableName, List<String> columns ) {
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s_local (%s, %s Date) " +
                        " ENGINE = ReplacingMergeTree(%s, %s, 8192);",
                tableName,
                StringUtils.join( columns, ", " ),
                DATE_COLUMN_NAME,
                DATE_COLUMN_NAME,
                RESPONSE_ID
        );
    }

    private String createDistributedTableQuery( String tableName ) {
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s AS %s_local" +
                        " ENGINE = Distributed(test_cluster, analytics, %s_local, %s);",
                tableName,
                tableName,
                tableName,
                "%s"
        );
    }
}
