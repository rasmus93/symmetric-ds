package org.jumpmind.db.platform.clickhouse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.*;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.support.lob.LobHandler;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isNotBlank;

public class ClickHouseJdbcSqlTemplate extends JdbcSqlTemplate {

    private final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat( "yyyy-MM-dd" );

    private final SimpleDateFormat DATE_TIME_FORMATTER = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss" );

    public ClickHouseJdbcSqlTemplate(
            DataSource dataSource,
            SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler,
            DatabaseInfo databaseInfo
    ) {
        super( dataSource, settings, lobHandler, databaseInfo );
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public ISqlTransaction startSqlTransaction() {
        return new ClickHouseJdbcTransaction( this );
    }

    @Override
    public void setValues(
            PreparedStatement ps, Object[] args, int[] argTypes,
            LobHandler lobHandler
    ) throws SQLException {

        for ( int i = 0; i < args.length; i++ ) {
            Object arg = args[i];
            int argType = argTypes != null && argTypes.length >= i
                          ? argTypes[i]
                          : SqlTypeValue.TYPE_UNKNOWN;
            if ( arg == null ) {
                if ( argType == Types.TINYINT || argType == Types.BIT || argType == Types.SMALLINT ||
                        argType == Types.INTEGER || argType == Types.BIGINT ) {
                    args[i] = 0;
                }
                if ( argType == Types.FLOAT || argType == Types.DOUBLE || argType == Types.DECIMAL ) {
                    args[i] = 0.0;
                }
                if ( argType == Types.TIMESTAMP || argType == Types.TIME ) {
                    args[i] = "0000-00-00 00:00:00";
                }
                if ( argType == Types.DATE ) {
                    args[i] = "0000-00-00";
                }
                if ( argType == Types.CHAR || argType == Types.VARCHAR || argType == Types.CLOB ||
                        argType == Types.BLOB || argType == Types.LONGVARCHAR ) {
                    args[i] = "";
                }
                if ( argType == Types.BINARY || argType == Types.LONGVARBINARY
                        || argType == Types.VARBINARY ) {
                    args[i] = "";
                }
            } else {
                if ( argType == Types.DATE ) {
                    args[i] = DATE_FORMATTER.format( arg );
                }
                if ( argType == Types.TIMESTAMP || argType == Types.TIME ) {
                    if ( arg.toString().equals( "current_timestamp" ) ) {
                        args[i] = DATE_TIME_FORMATTER.format( new Date() );
                    } else {
                        args[i] = DATE_TIME_FORMATTER.format( arg );
                    }
                }
            }
        }
        super.setValues( ps, args, argTypes, lobHandler );
    }

    @Override
    public int update(
            final boolean autoCommit,
            final boolean failOnError,
            final boolean failOnDrops,
            final boolean failOnSequenceCreate,
            final int commitRate,
            final ISqlResultsListener resultsListener,
            final ISqlStatementSource source
    ) {
        return execute( new IConnectionCallback<Integer>() {
            @SuppressWarnings( "resource" )
            public Integer execute( Connection con ) throws SQLException {
                int totalUpdateCount = 0;
                boolean oldAutoCommitSetting = con.getAutoCommit();
                Statement stmt = null;
                try {
                    con.setAutoCommit( autoCommit );
                    stmt = con.createStatement();
                    int statementCount = 0;
                    for ( String statement = source.readSqlStatement(); statement != null; statement = source
                            .readSqlStatement() ) {
                        if ( isNotBlank( statement ) ) {
                            try {
                                long startTime = System.currentTimeMillis();
                                boolean hasResults = stmt.execute( statement );
                                long endTime = System.currentTimeMillis();
                                logSqlBuilder.logSql(
                                        log,
                                        statement,
                                        null,
                                        null,
                                        ( endTime - startTime )
                                );

                                int updateCount = stmt.getUpdateCount();
                                totalUpdateCount += updateCount;
                                int rowsRetrieved = 0;
                                if ( hasResults ) {
                                    ResultSet rs = null;
                                    try {
                                        rs = stmt.getResultSet();
                                        while ( rs.next() ) {
                                            rowsRetrieved++;
                                        }
                                    } finally {
                                        close( rs );
                                    }
                                }
                                if ( resultsListener != null ) {
                                    resultsListener.sqlApplied(
                                            statement,
                                            updateCount,
                                            rowsRetrieved,
                                            statementCount
                                    );
                                }
                                statementCount++;
                                if ( statementCount % commitRate == 0 && !autoCommit ) {
                                    con.commit();
                                }
                            } catch ( SQLException ex ) {
                                boolean isDrop = statement.toLowerCase()
                                                          .trim()
                                                          .startsWith( "drop" );
                                boolean isSequenceCreate = statement.toLowerCase()
                                                                    .trim()
                                                                    .startsWith( "create sequence" );
                                if ( resultsListener != null ) {
                                    resultsListener.sqlErrored(
                                            statement,
                                            translate( statement, ex ),
                                            statementCount,
                                            isDrop,
                                            isSequenceCreate
                                    );
                                }

                                if ( ( isDrop && !failOnDrops ) || ( isSequenceCreate && !failOnSequenceCreate ) ) {
                                    log.debug(
                                            "{}.  Failed to execute: {}",
                                            ex.getMessage(),
                                            statement
                                    );
                                } else {
                                    log.warn(
                                            "{}.  Failed to execute: {}",
                                            ex.getMessage(),
                                            statement
                                    );
                                    if ( failOnError ) {
                                        throw ex;
                                    }
                                }
                            }
                        }
                    }

                    if ( !autoCommit ) {
                        con.commit();
                    }
                    return totalUpdateCount;
                } catch ( SQLException ex ) {
                    if ( !autoCommit ) {
                        con.rollback();
                    }
                    throw ex;
                } finally {
                    close( stmt );
                    if ( !con.isClosed() ) {
                        con.setAutoCommit( oldAutoCommitSetting );
                    }
                }
            }
        } );
    }

    @Override
    public int update( final String sql, final Object[] args, final int[] types ) {
        return execute( new IConnectionCallback<Integer>() {
            public Integer execute( Connection con ) throws SQLException {
                int[] newTypes = new int[args.length];
                List<String> queries = ClickHouseJdbcHelper.prepareSql( con, sql, args, newTypes );
                Object[] newArgs = ClickHouseJdbcHelper.getNewArgs( sql, args );

                int updated = 0;
                for ( String query : queries ) {
                    if ( types != null || sql.startsWith( "insert" ) ) {
                        updated += executeQuery( con, query, newArgs, types );
                    } else {
                        updated += executeQuery( con, query, newArgs, newTypes );
                    }
                }
                return 1;
            }
        } );
    }

    @Override
    public void setValues( PreparedStatement ps, Object[] args ) throws SQLException {
        if ( args != null ) {
            for ( int i = 0; i < args.length; i++ ) {
                Object arg = args[i];
                if ( arg == null || arg.toString().equals( "NULL" ) ) {
                    arg = "";
                }
                try {
                    doSetValue( ps, i + 1, arg );
                } catch ( SQLException ex ) {
                    log.warn( "Parameter arg '{}' caused exception: {}", arg, ex.getMessage() );
                    throw ex;
                }
            }
        }
    }

    @Override
    public boolean isStoresUpperCaseIdentifiers() {
        return false;
    }


    private Integer executeQuery(
            Connection con,
            final String sql,
            final Object[] args,
            final int[] types
    )
            throws SQLException {
        if ( StringUtils.isEmpty( sql ) ) {
            return 0;
        }
        if ( args == null ) {
            Statement stmt = null;
            try {
                stmt = con.createStatement();
                stmt.setQueryTimeout( settings.getQueryTimeout() );

                long startTime = System.currentTimeMillis();
                stmt.execute( sql );
                long endTime = System.currentTimeMillis();
                logSqlBuilder.logSql( log, sql, args, types, ( endTime - startTime ) );

                return stmt.getUpdateCount();
            } catch ( SQLException e ) {
                throw logSqlBuilder.logSqlAfterException( log, sql, args, e );
            } finally {
                close( stmt );
            }
        } else {
            PreparedStatement ps = null;
            try {
                ps = con.prepareStatement( sql );
                ps.setQueryTimeout( settings.getQueryTimeout() );
                if ( types != null ) {
                    setValues( ps, args, types, getLobHandler()
                            .getDefaultHandler() );
                } else {
                    setValues( ps, args );
                }

                long startTime = System.currentTimeMillis();
                ps.execute();
                long endTime = System.currentTimeMillis();
                logSqlBuilder.logSql( log, sql, args, types, ( endTime - startTime ) );

                return ps.getUpdateCount();
            } catch ( SQLException e ) {
                throw logSqlBuilder.logSqlAfterException( log, sql, args, e );
            } finally {
                close( ps );
            }
        }
    }
}
