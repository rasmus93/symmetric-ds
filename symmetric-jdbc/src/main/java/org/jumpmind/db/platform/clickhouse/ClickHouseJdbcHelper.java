package org.jumpmind.db.platform.clickhouse;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.lang.StringUtils;
import org.apache.xpath.operations.Number;
import org.jumpmind.db.sql.SqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClickHouseJdbcHelper {

    private static final Logger log = LoggerFactory.getLogger( ClickHouseJdbcHelper.class );

    private final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat( "yyyy-MM-dd" );

    private final SimpleDateFormat DATE_TIME_FORMATTER = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss" );

    public static List<String> prepareSql(
            Connection connection,
            String sql,
            Object[] args,
            int[] newTypes
    )
            throws SQLException {
        net.sf.jsqlparser.statement.Statement statement;
        try {
            statement = CCJSqlParserUtil.parse( sql );
        } catch ( JSQLParserException e ) {
            throw new SqlException( "Error during parse sql statement", e );
        }
        StringBuilder query = new StringBuilder( "SELECT * FROM " );

        String tableName = "";
        Map<String, Object> columns = new LinkedHashMap<String, Object>();
        if ( sql.startsWith( "update" ) ) {
            Update update = (Update) statement;
            tableName = update.getTables().get( 0 ).toString();
            int passed = 0;
            for ( int i = 0; i < update.getColumns().size(); ++i ) {
                Object value = update.getExpressions().get( i );
                columns.put(
                        update.getColumns().get( i ).getColumnName(),
                        value
                );
                if ( value.toString().equals( "?" ) ) {
                    ++passed;
                }
            }
            query.append( tableName )
                 .append( " FINAL " );

            if ( update.getWhere() != null ) {
                query.append( " WHERE " )
                     .append( processWhere( update.getWhere().toString(), passed, args ) );
            }
        }
        if ( sql.startsWith( "delete" ) ) {
            Delete delete = (Delete) statement;
            tableName = delete.getTable().toString();
            query.append( tableName )
                 .append( " FINAL " );

            if ( delete.getWhere() != null ) {
                query.append( " WHERE " )
                     .append( processWhere( delete.getWhere().toString(), 0, args ) );
            }
        }

        if ( sql.startsWith( "insert" ) ) {
            Insert insert = (Insert) statement;
            tableName = insert.getTable().toString();
            query.append( tableName )
                 .append( " FINAL LIMIT 1" );

            ExpressionList values = (ExpressionList) insert.getItemsList();

            List<Object> newColumns = new ArrayList<Object>();
            List<Object> newValues = new ArrayList<Object>();
            for ( int i = 0; i < insert.getColumns().size(); ++i ) {
                /*columns.put(
                        insert.getColumns().get( i ).getColumnName(),
                        values.getExpressions().get( i )
                );*/
                Object value = values.getExpressions().get( i );
                if ( value == null || value.toString().equals( "NULL" ) ) {
                    continue;
                }
                if ( value.toString().equals( "current_timestamp" ) ) {
                    value = "'" + new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).format( new Date() ) + "'";
                }
                newValues.add( value );
                newColumns.add( insert.getColumns().get( i ).getColumnName() );
            }

            String newInsert = "INSERT INTO " + tableName +
                    " ( " +
                    StringUtils.join( newColumns, ", " ) +
                    ") VALUES (" +
                    StringUtils.join( newValues, ", " ) +
                    ");";
            return Collections.singletonList( newInsert );
        }

        List<String> queries = new ArrayList<String>();
        ResultSet rs;
        try {
            rs = connection.createStatement().executeQuery( query.toString() );
        } catch ( SQLException e ) {
            log.error( "Error while executing query {}", query.toString() );
            return queries;
        }

        while ( rs.next() ) {
            StringBuilder insert = new StringBuilder();
            insert.append( "INSERT INTO " )
                  .append( tableName )
                  .append( " (" );
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            boolean added = addKeyColumns( insert, columns );
            for ( int i = 1; i <= columnCount; i++ ) {
                if ( columns.containsKey( metaData.getColumnName( i ) ) ) {
                    continue;
                }
                if ( added ) {
                    insert.append( ", " );
                }
                added = true;
                insert.append( metaData.getColumnName( i ) );
            }
            insert.append( ") VALUES (" );

            added = addValueColumns( insert, columns, metaData, newTypes );
            for ( int i = 1; i <= columnCount; i++ ) {
                if ( columns.containsKey( metaData.getColumnName( i ) ) ) {
                    continue;
                }
                if ( added ) {
                    insert.append( ", " );
                }
                added = true;
                if ( sql.startsWith( "delete" ) &&
                        metaData.getColumnName( i ).equals( "deleted" ) ) {
                    insert.append( "1" );
                    continue;
                }
                insert.append( getValue( metaData.getColumnTypeName( i ), rs.getObject( i ) ) );
            }
            insert.append( ");" );
            queries.add( insert.toString() );
            //log.error( "CHECK:" + insert );
            insert = new StringBuilder();
        }
        return queries;
    }

    private static String processWhere( String where, int passed, Object[] args ) {
        for ( int i = passed; i < args.length; ++i ) {
            String name = args[i].getClass().getSimpleName();
            if ( !name.contains( "Date" ) && !name.contains( "Time" ) && !name.contains( "String" ) ) {
                where = where.replaceFirst( "\\?", String.valueOf( args[i] ) );
            } else {
                where = where.replaceFirst(
                        "\\?",
                        "'" + String.valueOf( args[i] ) + "'"
                );
            }

        }
        return where;
    }

    private static boolean addKeyColumns(
            StringBuilder insert,
            Map<String, Object> columns
    )
            throws SQLException {
        boolean added = false;
        for ( Map.Entry<String, Object> column : columns.entrySet() ) {
            if ( column.getValue() == null
                    || column.getValue().toString().equals( "NULL" ) ) {
                continue;
            }
            if ( added ) {
                insert.append( ", " );
            }
            added = true;
            insert.append( column.getKey() );
        }
        return added;
    }

    private static boolean addValueColumns(
            StringBuilder insert,
            Map<String, Object> columns,
            ResultSetMetaData metaData,
            int[] newTypes
    ) throws SQLException {
        Map<String, String> orderColumns = new HashMap<String, String>();
        Map<String, Integer> columnTypes = new HashMap<String, Integer>();
        for ( int i = 1; i <= metaData.getColumnCount(); i++ ) {
            String columnName = metaData.getColumnName( i );
            if ( !columns.containsKey( columnName ) ) {
                continue;
            }
            orderColumns.put( columnName, metaData.getColumnTypeName( i ) );
            columnTypes.put( columnName, metaData.getColumnType( i ) );
        }

        int j = 0;
        boolean added = false;
        for ( Map.Entry<String, Object> column : columns.entrySet() ) {
            if ( column.getValue() == null
                    || column.getValue().toString().equals( "NULL" ) ) {
                continue;
            }
            if ( added ) {
                insert.append( ", " );
            }
            added = true;
            Object value = getValue( orderColumns.get( column.getKey() ), column.getValue() );
            if ( value.toString().equals( "?" ) ) {
                newTypes[j++] = columnTypes.get( column.getKey() );
            }
            insert.append( value );
        }
        return added;
    }

    private static Object getValue( String type, Object value ) {
        String stringValue = value.toString();
        if ( stringValue.equals( "?" ) ) {
            return value;
        }
        if ( type.equals( "Date" ) ) {
            value = "'" + new SimpleDateFormat( "yyyy-MM-dd" ).format( value ) + "'";
        }
        if ( type.equals( "DateTime" ) ) {
            if ( value.toString().equals( "current_timestamp" ) ) {
                value = "'" + new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).format( new Date() ) + "'";
            } else {
                value = "'" + new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).format( value ) + "'";
            }
        }
        if ( type.equals( "String" ) &&
                (!stringValue.startsWith( "'" ) || !stringValue.endsWith( "'" ))) {
            value = "'" + value + "'";
        }
        return value;
    }

    public static Object[] getNewArgs( String sql, Object[] args ) {
        int wherePosition = sql.toLowerCase().indexOf( "where" );
        if ( wherePosition != -1 ) {
            return Arrays.copyOf(
                    args,
                    StringUtils.countMatches( sql.substring( 0, wherePosition ), "?" )
            );
        }
        return args;
    }

}
