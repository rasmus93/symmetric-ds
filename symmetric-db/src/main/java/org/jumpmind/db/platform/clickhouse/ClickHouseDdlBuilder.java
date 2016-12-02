package org.jumpmind.db.platform.clickhouse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.alter.IModelChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IAlterDatabaseInterceptor;

import java.sql.Types;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ClickHouseDdlBuilder extends AbstractDdlBuilder {

    public ClickHouseDdlBuilder() {
        super( DatabaseNamesConstants.CLICKHOUSE );

        databaseInfo.setNullAsDefaultValueRequired( false );
        databaseInfo.setTriggersSupported( false );
        databaseInfo.setIndicesSupported( false );
        databaseInfo.setIndicesEmbedded( false );
        databaseInfo.setForeignKeysSupported( false );
        databaseInfo.setPrimaryKeyEmbedded( false );
        databaseInfo.setDefaultValueUsedForIdentitySpec( false );
        databaseInfo.setDefaultValuesForLongTypesSupported( true );
        databaseInfo.setNonPKIdentityColumnsSupported( false );

        databaseInfo.addNativeTypeMapping( Types.TINYINT, "Int8", Types.TINYINT );
        databaseInfo.addNativeTypeMapping( Types.BIT, "Int8", Types.TINYINT );
        databaseInfo.addNativeTypeMapping( Types.SMALLINT, "Int16", Types.SMALLINT );
        databaseInfo.addNativeTypeMapping( Types.INTEGER, "Int32", Types.INTEGER );
        databaseInfo.addNativeTypeMapping( Types.BIGINT, "Int64", Types.BIGINT );
        //double
        databaseInfo.addNativeTypeMapping( Types.FLOAT, "Float32", Types.FLOAT );
        databaseInfo.addNativeTypeMapping( Types.REAL, "Float32", Types.FLOAT );
        databaseInfo.addNativeTypeMapping( Types.DOUBLE, "Float64", Types.DOUBLE );
        databaseInfo.addNativeTypeMapping( Types.DECIMAL, "Float64", Types.DOUBLE );
        //time
        databaseInfo.addNativeTypeMapping( Types.TIMESTAMP, "DateTime", Types.TIMESTAMP );
        databaseInfo.addNativeTypeMapping( Types.TIME, "DateTime", Types.TIMESTAMP );
        databaseInfo.addNativeTypeMapping( Types.DATE, "Date", Types.DATE );
        //string
        databaseInfo.addNativeTypeMapping( Types.CHAR, "String", Types.VARCHAR );
        databaseInfo.addNativeTypeMapping( Types.VARCHAR, "String", Types.VARCHAR );
        databaseInfo.addNativeTypeMapping( Types.LONGVARCHAR, "String", Types.VARCHAR );
        databaseInfo.addNativeTypeMapping( Types.CLOB, "String", Types.VARCHAR );
        databaseInfo.addNativeTypeMapping( Types.BLOB, "String", Types.VARCHAR );
        //???
        databaseInfo.addNativeTypeMapping( Types.BINARY, "String", Types.VARCHAR );
        databaseInfo.addNativeTypeMapping( Types.VARBINARY, "String", Types.VARCHAR );
        databaseInfo.addNativeTypeMapping( Types.LONGVARBINARY, "String", Types.VARCHAR );


        databaseInfo.setHasSize( Types.CHAR, false );
        databaseInfo.setHasSize( Types.VARCHAR, false );
        databaseInfo.setHasSize( Types.LONGNVARCHAR, false );

        databaseInfo.setHasPrecisionAndScale( Types.INTEGER, false );
        databaseInfo.setHasPrecisionAndScale( Types.BIGINT, false );
        databaseInfo.setHasPrecisionAndScale( Types.FLOAT, false );
        databaseInfo.setHasPrecisionAndScale( Types.DOUBLE, false );

        addEscapedCharSequence( "\\", "\\\\" );
        addEscapedCharSequence( "\"", "\\\"" );
        addEscapedCharSequence( "\'", "\\\'" );
        addEscapedCharSequence( "\b", "\\b" );
        addEscapedCharSequence( "\f", "\\f" );
        addEscapedCharSequence( "\r", "\\r" );
        addEscapedCharSequence( "\n", "\\n" );
        addEscapedCharSequence( "\t", "\\t" );
        addEscapedCharSequence( "\0", "\\0" );
    }

    @Override
    protected void createTable(
            Table table,
            StringBuilder ddl,
            boolean temporary,
            boolean recreate
    ) {
        // lets create any sequences
        for ( Column column : table.getColumns() ) {
            column.setAutoIncrement( false );
        }
        String tableName = getFullyQualifiedTableNameShorten( table );
        /*if (tableName.contains( "sym_channel_" )) {
            log.error( Arrays.toString( Thread.currentThread().getStackTrace() ) );
        }*/
        createLocalTable( table, ddl, tableName, temporary );
        createDistributedTable( table, ddl, tableName, temporary );
    }

    @Override
    protected String getNativeDefaultValue( Column column ) {
        String defaultValue = column.getDefaultValue();
        PlatformColumn platformColumn = column.findPlatformColumn( databaseName );
        if ( platformColumn != null ) {
            defaultValue = platformColumn.getDefaultValue();
        }
        if ( defaultValue.toLowerCase().equals( "current_timestamp" ) ) {
            defaultValue = "0000-00-00 00:00:00";
        }
        return defaultValue;
    }

    private void createLocalTable(
            Table table,
            StringBuilder ddl,
            String tableName,
            boolean temporary
    ) {
        ddl.append( "CREATE TABLE " )
           .append( tableName )
           .append( "_local ( " );
        writeColumns( table, ddl );

        String primaryKey = StringUtils.join( table.getPrimaryKeyColumnNames(), ", " );//
        // .toLowerCase();
        if ( StringUtils.isEmpty( primaryKey ) ) {
            primaryKey = "partition_date";
        }

        //ENGINE = Distributed(test_cluster, analytics, form_331387_local, 331387)
        if ( !temporary ) {
            ddl.append( ", deleted Int8, partition_date Date" );
        }
        ddl.append( ") ENGINE = ReplacingMergeTree (partition_date, " )
           .append( "(" )
           .append( primaryKey )
           .append( ")" )
           .append( ", 8192)" );

        writeTableCreationStmtEnding( table, ddl );
    }

    private void createDistributedTable(
            Table table,
            StringBuilder ddl,
            String tableName,
            boolean temporary
    ) {
        ddl.append( "CREATE TABLE " )
           .append( tableName )
           .append( " ( " );
        writeColumns( table, ddl );

        //ENGINE = Distributed(test_cluster, analytics, form_331387_local, 331387)
        if ( !temporary ) {
            ddl.append( ", deleted Int8, partition_date Date" );
        }
        ddl.append( ") ENGINE = Distributed(test_cluster, analytics, " )
           .append( tableName )
           .append( "_local, 1) " );

        writeTableCreationStmtEnding( table, ddl );
    }

    @Override
    protected void writeColumnNotNullableStmt( StringBuilder ddl ) {
        //ddl.append("NOT NULL");
    }

    @Override
    protected String getNativeType( Column column ) {
        /*String nativeType =  column.getJdbcTypeName();
        if (nativeType == null) {
           nativeType = databaseInfo.getNativeType(column.getMappedTypeCode());
        }
        return nativeType == null ? column.getMappedType() : nativeType;*/
        return super.getNativeType( column );
    }

    @Override
    protected void dropTable(
            Table table,
            StringBuilder ddl,
            boolean temporary,
            boolean recreate
    ) {
        ddl.append( "DROP TABLE IF EXISTS " )
           .append( getFullyQualifiedTableNameShorten( table ) );
        printEndOfStatement( ddl );

        ddl.append( "DROP TABLE IF EXISTS " )
           .append( getFullyQualifiedTableNameShorten( table ) )
           .append( "_local" );
        printEndOfStatement( ddl );
    }

    @Override
    public void writeCopyDataStatement(
            Table sourceTable, Table targetTable,
            LinkedHashMap<Column, Column> columnMap, StringBuilder ddl
    ) {
        ddl.append( "INSERT INTO " );
        ddl.append( getFullyQualifiedTableNameShorten( targetTable ) );
        ddl.append( " (" );
        for ( Iterator<Column> columnIt = columnMap.values().iterator(); columnIt.hasNext(); ) {
            printIdentifier( getColumnName( (Column) columnIt.next() ), ddl );
            if ( columnIt.hasNext() ) {
                ddl.append( "," );
            }
        }
        ddl.append( ") SELECT " );
        for ( Iterator<Map.Entry<Column, Column>> columnsIt = columnMap.entrySet()
                                                                       .iterator(); columnsIt
                      .hasNext(); ) {
            Map.Entry<Column, Column> entry = columnsIt.next();

            writeCastExpression( (Column) entry.getKey(), (Column) entry.getValue(), ddl );
            if ( columnsIt.hasNext() ) {
                ddl.append( "," );
            }
        }
        ddl.append( " FROM " );
        ddl.append( getFullyQualifiedTableNameShorten( sourceTable ) );
        printEndOfStatement( ddl );
    }

    @Override
    protected String getFullyQualifiedTableNameShorten( Table table ) {
        String result = "";
/*
        if ( StringUtils.isNotBlank( table.getCatalog())) {
            result+=getDelimitedIdentifier(table.getCatalog()).concat(databaseInfo.getCatalogSeparator());
        }
        if ( StringUtils.isNotBlank( table.getSchema() ) ) {
            result += getDelimitedIdentifier( table.getSchema() ).concat( databaseInfo.getSchemaSeparator() );
        }
*/
        result += getDelimitedIdentifier( getTableName( table.getName() ) );
        return result;
    }

    protected String getColumnName( Column column ) {
        return shortenName( column.getName(), databaseInfo.getMaxColumnNameLength() );
    }

    @Override
    protected void writeColumnAutoIncrementStmt( Table table, Column column, StringBuilder ddl ) {
        //ddl.append("AUTO_INCREMENT");
    }

}
