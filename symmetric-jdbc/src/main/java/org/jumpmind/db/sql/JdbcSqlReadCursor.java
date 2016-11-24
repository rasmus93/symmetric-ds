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
package org.jumpmind.db.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSqlReadCursor<T> implements ISqlReadCursor<T> {
    
    static final Logger log = LoggerFactory.getLogger(JdbcSqlReadCursor.class);
    
    protected Connection c;

    protected ResultSet rs;

    protected Statement st;

    protected boolean autoCommitFlag;

    protected ISqlRowMapper<T> mapper;

    protected JdbcSqlTemplate sqlTemplate;

    protected int rowNumber;
    
    protected int originalIsolationLevel;
    
    protected ResultSetMetaData rsMetaData = null;
    
    protected int rsColumnCount;

    protected IConnectionHandler connectionHandler;
    
    public JdbcSqlReadCursor() {
    }
    
    public JdbcSqlReadCursor(JdbcSqlTemplate sqlTemplate, ISqlRowMapper<T> mapper, String sql,
            Object[] values, int[] types) {
        this(sqlTemplate, mapper, sql, values, types, null);
    }

    public JdbcSqlReadCursor(JdbcSqlTemplate sqlTemplate, ISqlRowMapper<T> mapper, String sql,
            Object[] values, int[] types, IConnectionHandler connectionHandler) {
        this.sqlTemplate = sqlTemplate;
        this.mapper = mapper;
        this.connectionHandler = connectionHandler;
        
        try {
            c = sqlTemplate.getDataSource().getConnection();
            if (this.connectionHandler != null) {
                this.connectionHandler.before(c);
            }
        	originalIsolationLevel = c.getTransactionIsolation();            
            autoCommitFlag = c.getAutoCommit();
            if (c.getTransactionIsolation() != sqlTemplate.getIsolationLevel()) {
            	c.setTransactionIsolation(sqlTemplate.getIsolationLevel());
            }
            if (sqlTemplate.isRequiresAutoCommitFalseToSetFetchSize()) {
                c.setAutoCommit(false);
            }

            try {
                if (values != null) {
                    PreparedStatement pstmt;
                    if (c.getMetaData().getDatabaseProductName().equals( "ClickHouse" )) {
                        pstmt = c.prepareStatement( parseClickhouseSql( sql ) );
                    } else {
                        pstmt = c.prepareStatement(
                                sql,
                                sqlTemplate.getSettings().getResultSetType(),
                                ResultSet.CONCUR_READ_ONLY
                        );
                    }
                    sqlTemplate.setValues(pstmt, values, types, sqlTemplate.getLobHandler()
                            .getDefaultHandler());
                    st = pstmt;                    
                    st.setQueryTimeout(sqlTemplate.getSettings().getQueryTimeout());
                    st.setFetchSize(sqlTemplate.getSettings().getFetchSize());
                    rs = pstmt.executeQuery();

                } else {
                    st = c.createStatement(sqlTemplate.getSettings().getResultSetType(),
                            ResultSet.CONCUR_READ_ONLY);
                    st.setQueryTimeout(sqlTemplate.getSettings().getQueryTimeout());
                    st.setFetchSize(sqlTemplate.getSettings().getFetchSize());
                    rs = st.executeQuery(sql);
                }
            } catch (SQLException e) {
                /*
                 * The Xerial SQLite JDBC driver throws an exception if a query
                 * returns an empty set This gets around that
                 */
                if (e.getMessage() == null
                        || !e.getMessage().equalsIgnoreCase("query does not return results")) {
                    throw e;
                }
            }
            SqlUtils.addSqlReadCursor(this);
            
        } catch (SQLException ex) {
            close();
            throw sqlTemplate.translate("Failed to execute sql: " + sql, ex);
        } catch (Throwable ex) {
            close();
            throw sqlTemplate.translate("Failed to execute sql: " + sql, ex);
        }
    }

    private String parseClickhouseSql( String sql ) {
        sql = sql.replaceAll( "[A-Za-z]*\\.", "" );
        if ( sql.contains( "join" ) ) {
            Select select;
            try {
                select = (Select) CCJSqlParserUtil.parse( sql );
            } catch ( JSQLParserException e ) {
                throw new SqlException( "Error during parse sql statement", e );
            }
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                            /*List<String> selectItems = new ArrayList<String>();
                            for ( Object item : plainSelect.getSelectItems().toArray() ) {
                                String value = item.toString();
                                if ( value.contains( "." ) ) {
                                    selectItems.add( value.substring( value.indexOf( '.' ) + 1 ) );
                                } else {
                                    selectItems.add( value );
                                }
                            }*/

            StringBuilder query = new StringBuilder();
            for ( int i = 0; i < plainSelect.getJoins().size(); ++i ) {
                query.append( " SELECT * " );
                if ( i == 0 ) {
                    query.append( ", " )
                         .append( StringUtils.join(
                                 plainSelect.getSelectItems().toArray(), "," ) );
                }
                query.append( " FROM ( " );
            }
            query.append( "SELECT * FROM " )
                 .append( plainSelect.getFromItem().toString() )
                 .append( " FINAL ) " );
            //List<String> using = new ArrayList<String>();
            int i = plainSelect.getJoins().size();
            for ( Join join : plainSelect.getJoins() ) {
                query.append( " ALL " );
                if ( join.isLeft() ||
                        ( !join.isLeft() && !join.isInner() ) ) {
                    query.append( " LEFT " );
                }
                if ( join.isInner() ) {
                    query.append( " INNER " );
                }
                if ( join.isOuter() ) {
                    query.append( " OUTER " );
                }

                String joinItem = join.getOnExpression().toString();
                EqualsTo equalsTo = (EqualsTo) join.getOnExpression();

                                /*if ( joinItem.contains( "=" ) ) {
                                    joinItem = joinItem.substring(0, joinItem.indexOf( '=' ) );
                                }*/
                query.append( " JOIN ( SELECT *, " )
                     .append( equalsTo.getRightExpression() )
                     .append( " AS " )
                     .append( equalsTo.getLeftExpression() )
                     .append( " FROM " )
                     .append( join.getRightItem() )
                     .append( " FINAL ) " );
                query.append( " USING " )
                     .append( equalsTo.getLeftExpression() );
                if ( i-- != 1 ) {
                    query.append( ") " );
                }
            }
                            /*query.append( " USING " )
                                 .append( StringUtils.join( using, "," ) );*/
            if ( plainSelect.getWhere() != null ) {
                query.append( " WHERE " )
                     .append( plainSelect.getWhere().toString() );
            }
            if ( plainSelect.getOrderByElements() != null ) {
                query.append( " ORDER BY " )
                     .append( StringUtils.join(
                             plainSelect.getOrderByElements().toArray(), "," ) );
            }
            //log.error( "Query: " + query.toString());
            sql = query.toString();
            //SqlUtils.addSqlReadCursor(this);
            //return;
        }
        sql = sql.replaceAll( "inactive_time is null", "inactive_time=\'0000-00-00 00:00:00\'" );
        sql = sql.replaceAll( "is null", "='\'\'" );
        return sql;
    }

    public T next() {
        try {
            while (rs!=null && rs.next()) {
                if (rsMetaData == null) {
                    rsMetaData = rs.getMetaData();
                    rsColumnCount = rsMetaData.getColumnCount();
                }
                
                Row row = getMapForRow(rs, rsMetaData, rsColumnCount, sqlTemplate.getSettings().isReadStringsAsBytes());
                T value = mapper.mapRow(row);
                if (value != null) {
                    return value;
                }
            } 
            return null;
        } catch (SQLException ex) {
            throw sqlTemplate.translate(ex);
        }
    }

    protected static Row getMapForRow(ResultSet rs, ResultSetMetaData argResultSetMetaData, 
            int columnCount, boolean readStringsAsBytes) throws SQLException {
        Row mapOfColValues = new Row(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            String key = JdbcSqlTemplate.lookupColumnName(argResultSetMetaData, i);
            Object obj = JdbcSqlTemplate.getResultSetValue(rs, argResultSetMetaData, i, readStringsAsBytes);
            mapOfColValues.put(key, obj);
        }
        return mapOfColValues;
    }

	public void close() {
	    if (this.connectionHandler != null) {
	        this.connectionHandler.after(c);
	    }
		JdbcSqlTemplate.close(rs);
		JdbcSqlTemplate.close(st);
		JdbcSqlTemplate.close(autoCommitFlag, originalIsolationLevel, c);
		SqlUtils.removeSqlReadCursor(this);
	}
}
