package org.jumpmind.symmetric.db.clickhouse;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

import java.util.HashMap;

public class ClickHouseTriggerTemplate extends AbstractTriggerTemplate {

    public ClickHouseTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"')|| '\"' end " ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then 0 else '\"'||cast($(tableAlias).\"$(columnName)\" as varchar(50))||'\"' end " ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '0000-00-00 00:00:00' else '\"'|| cast($(tableAlias).\"$(columnName)\" as varchar) || '\"' end " ;
        //clobColumnTemplate = stringColumnTemplate;
        //blobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"'||replace(replace(sym_BASE64_ENCODE($(tableAlias).\"$(columnName)\"),'\\','\\\\'),'\"','\\\"')||'\"' end " ;
        booleanColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then 0 when $(tableAlias).\"$(columnName)\" then '\"1\"' else '\"0\"' end " ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "" ;
        oldTriggerValue = "" ;
        sqlTemplates = new HashMap<String,String>();

        sqlTemplates.put("insertTriggerTemplate" , "");
        sqlTemplates.put("updateTriggerTemplate" , "");
        sqlTemplates.put("deleteTriggerTemplate" , "");
        sqlTemplates.put("initialLoadSqlTemplate" ,
                         "select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}
