package org.jumpmind.db.platform.clickhouse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.platform.mysql.MySqlJdbcSqlTemplate;
import org.jumpmind.db.platform.voltdb.VoltDbDdlBuilder;
import org.jumpmind.db.sql.SqlTemplateSettings;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Types;

public class ClickHouseDatabasePlatform extends AbstractJdbcDatabasePlatform {

    public static final String JDBC_DRIVER = " ru.yandex.clickhouse.ClickHouseDriver";

    public static final String JDBC_SUBPROTOCOL = "clickhouse";

    public ClickHouseDatabasePlatform(
            DataSource dataSource,
            SqlTemplateSettings settings
    ) {
        super( dataSource, settings );
        getDatabaseInfo().setRequiresAutoCommitForDdl( true );
        getDatabaseInfo().setDelimiterToken( "" );
        getDatabaseInfo().setDelimitedIdentifiersSupported( false );
        getDatabaseInfo().setTriggersSupported( false );
        getDatabaseInfo().setForeignKeysSupported( false );
        getDatabaseInfo().setHasPrecisionAndScale( Types.DECIMAL, false );
        getDatabaseInfo().setHasPrecisionAndScale( Types.FLOAT, false );
    }

    @Override
    public Database readDatabaseFromXml(
            InputStream is,
            boolean alterCaseToMatchDatabaseDefaultCase
    ) {
        Database database = super.readDatabaseFromXml( is, alterCaseToMatchDatabaseDefaultCase );
        for ( Table table : database.getTables() ) {
            for ( Column column : table.getColumns() ) {
                column.setAutoIncrement( false );
            }
        }
        return database;
    }

    @Override
    protected IDdlBuilder createDdlBuilder() {
        return new ClickHouseDdlBuilder();
    }

    @Override
    protected IDdlReader createDdlReader() {
        return new ClickHouseDdlReader( this );
    }

    @Override
    protected ClickHouseJdbcSqlTemplate createSqlTemplate() {
        return new ClickHouseJdbcSqlTemplate( dataSource, settings, null, getDatabaseInfo() );
    }

    @Override
    public String getName() {
        return DatabaseNamesConstants.CLICKHOUSE;
    }

    @Override
    public String getDefaultSchema() {
        return "analytics";
    }

    @Override
    public String getDefaultCatalog() {
        return "";
    }
}
