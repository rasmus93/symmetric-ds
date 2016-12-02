package org.jumpmind.db.platform.clickhouse;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.substring;

public class ClickHouseDdlReader extends AbstractJdbcDdlReader {

    public ClickHouseDdlReader( IDatabasePlatform platform ) {
        super( platform );
        setDefaultCatalogPattern( null );
        setDefaultSchemaPattern( null );
        setDefaultTablePattern( null );
    }

    @Override
    protected Integer mapUnknownJdbcTypeForColumn( Map<String, Object> values ) {
        String typeName = (String) values.get( "TYPE_NAME" );
        if ( typeName == null ) {
            return super.mapUnknownJdbcTypeForColumn( values );
        }
        if ( typeName.equals( "Int8" ) ) {
            return Types.TINYINT;
        }
        if ( typeName.equals( "Int16" ) ) {
            return Types.SMALLINT;
        }
        if ( typeName.equals( "Int32" ) ) {
            return Types.INTEGER;
        }
        if ( typeName.equals( "Int64" ) ) {
            return Types.BIGINT;
        }
        if ( typeName.equals( "Float32" ) ) {
            return Types.FLOAT;
        }
        if ( typeName.equals( "Float64" ) ) {
            return Types.DOUBLE;
        }
        if ( typeName.equals( "DateTime" ) ) {
            return Types.TIMESTAMP;
        }
        if ( typeName.equals( "Date" ) ) {
            return Types.DATE;
        }
        if ( typeName.equals( "Date" ) ) {
            return Types.DATE;
        }
        if ( typeName.equals( "String" ) ) {
            return Types.VARCHAR;
        }
        return super.mapUnknownJdbcTypeForColumn( values );
    }

    @Override
    protected Table readTable(
            Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values
    ) throws SQLException {
        Table table = super.readTable( connection, metaData, values );
        for ( Column column : table.getColumns() ) {
            column.setAutoIncrement( false );
        }
        return table;
    }

    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn( metaData, values );
        //column.setRequired("NO".equalsIgnoreCase(((String) values.get("IS_NULLABLE")).trim()));
        column.setRequired( false );
        return column;
    }
}
