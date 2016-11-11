package org.jumpmind.symmetric.db.clickhouse;

import org.jumpmind.db.alter.IModelChange;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;

import java.util.List;

public class ClickHouseSymmetricDialect extends AbstractSymmetricDialect {

    public ClickHouseSymmetricDialect(
            IParameterService parameterService,
            IDatabasePlatform platform
    ) {
        super( parameterService, platform );
        this.triggerTemplate = new ClickHouseTriggerTemplate( this );
        this.supportsSubselectsInDelete = false;
        this.supportsSubselectsInUpdate = false;
    }

    @Override
    public void cleanDatabase() {
    }

    @Override
    public void dropRequiredDatabaseObjects() {
    }

    @Override
    public void createRequiredDatabaseObjects() {
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    @Override
    public void disableSyncTriggers( ISqlTransaction transaction, String nodeId ) {
        // VoltDB doesn't support triggers currently.
    }

    @Override
    public void enableSyncTriggers( ISqlTransaction transaction ) {
        // VoltDB doesn't support triggers currently.
    }

    @Override
    public void removeTrigger(
            StringBuilder sqlBuffer, String catalogName, String schemaName,
            String triggerName, String tableName
    ) {

    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.db.ISymmetricDialect#getSyncTriggersExpression()
     */
    @Override
    public String getSyncTriggersExpression() {
        return null;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(
            String catalogName,
            String schema,
            String tableName,
            String triggerName
    ) {
        return false;
    }
}
