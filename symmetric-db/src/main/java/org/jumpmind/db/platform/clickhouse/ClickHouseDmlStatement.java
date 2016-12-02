package org.jumpmind.db.platform.clickhouse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseDmlStatement extends DmlStatement {

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



}
