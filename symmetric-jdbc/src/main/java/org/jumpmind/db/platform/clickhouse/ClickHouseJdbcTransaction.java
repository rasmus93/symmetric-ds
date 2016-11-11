package org.jumpmind.db.platform.clickhouse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class ClickHouseJdbcTransaction extends JdbcSqlTransaction {

    public ClickHouseJdbcTransaction( JdbcSqlTemplate jdbcSqlTemplate ) {
        super( jdbcSqlTemplate );
    }

    @Override
    public int prepareAndExecute(final String sql, final Object[] args, final int[] types) {
        return executeCallback(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                PreparedStatement stmt = null;
                ResultSet rs = null;
                try {
                    int[] newTypes = new int[args.length];
                    List<String> queries = ClickHouseJdbcHelper.prepareSql( con, sql, args, newTypes );
                    Object[] newArgs = ClickHouseJdbcHelper.getNewArgs( sql, args );

                    for ( String query : queries ) {
                        stmt = con.prepareStatement(query);
                        jdbcSqlTemplate.setValues(
                                stmt, newArgs, types, jdbcSqlTemplate.getLobHandler().getDefaultHandler()
                        );
                        long startTime = System.currentTimeMillis();
                        stmt.execute();
                        long endTime = System.currentTimeMillis();
                        logSqlBuilder.logSql(log, query, newArgs, types, (endTime-startTime));
                    }

                    return stmt.getUpdateCount();
                } catch (SQLException e) {
                    throw logSqlBuilder.logSqlAfterException(log, sql, args, e);
                } finally {
                    JdbcSqlTemplate.close(rs);
                    JdbcSqlTemplate.close(stmt);
                }

            }
        });
    }

    @Override
    public int prepareAndExecute( final String sql, final Object... args ) {
        return executeCallback( new IConnectionCallback<Integer>() {
            public Integer execute( Connection con ) throws SQLException {
                int[] types = new int[args.length];
                List<String> queries = ClickHouseJdbcHelper.prepareSql( con, sql, args, types );
                Object[] newArgs = ClickHouseJdbcHelper.getNewArgs( sql, args );

                int updated = 0;
                for ( String query : queries ) {
                    updated += executeQuery( con, query, newArgs );
                }
                return 1;
            }
        } );
    }

    private int executeQuery( Connection con, String sql, final Object... args )
            throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            if ( StringUtils.isEmpty( sql ) ) {
                return 0;
            }
            stmt = con.prepareStatement( sql );
            if ( args != null && args.length > 0 ) {
                jdbcSqlTemplate.setValues( stmt, args );
            }
            long startTime = System.currentTimeMillis();
            boolean hasResults = stmt.execute();
            long endTime = System.currentTimeMillis();
            logSqlBuilder.logSql( log, sql, args, null, ( endTime - startTime ) );
            if ( hasResults ) {
                rs = stmt.getResultSet();
                while ( rs.next() ) {
                }
            }
            return stmt.getUpdateCount();
        } catch ( SQLException e ) {
            throw logSqlBuilder.logSqlAfterException( log, sql, args, e );
        } finally {
            JdbcSqlTemplate.close( rs );
            JdbcSqlTemplate.close( stmt );
        }
    }

}
