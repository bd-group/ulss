/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.handler;

import cn.ac.ict.ncic.util.dao.DaoPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ulss.se2db.commons.RuntimeEnv;
import ulss.se2db.metastore.TableSe2DBRule;

/**
 *
 * @author alexmu
 */
public class TableSe2DBHandler {

    TableSe2DBRule tableSe2DBRule = null;
    List<SE2DBWorker> se2DBWorkerSet = new ArrayList<SE2DBWorker>();
    ExecutorService threadPool = Executors.newFixedThreadPool(100);
    private Schema docSchema = null;
    private DatumReader<GenericRecord> docReader = null;
    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(TableSe2DBHandler.class.getName());
    }

    private TableSe2DBHandler(TableSe2DBRule pTableSe2DBRule) {
        tableSe2DBRule = pTableSe2DBRule;
    }

    public static TableSe2DBHandler getTableSe2DBHandler(TableSe2DBRule pTableSe2DBRule) {
        TableSe2DBHandler tableSe2DBHandler = new TableSe2DBHandler(pTableSe2DBRule);

        ResultSet rs = null;
        try {
            String sql = "select dataschema.schema_name as schema_name,dataschema.schema_content as schema_content from dataschema,dataschema_mq where dataschema_mq.mq='" + tableSe2DBHandler.getMQName() + "' and dataschema.schema_name=dataschema_mq.schema_name";
            logger.info(sql);
            rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
            if (rs.next()) {
                String docSchemaName = rs.getString("schema_name");
                if (docSchemaName == null || docSchemaName.isEmpty()) {
                    throw new Exception("no schema_name found");
                }
                String docSchemaContent = rs.getString("schema_content");
                if (docSchemaContent == null || docSchemaContent.isEmpty()) {
                    throw new Exception("no schema content found for " + docSchemaName);
                }
                docSchemaContent = docSchemaContent.toLowerCase();
                logger.info("docSchemaName:" + docSchemaName + ",dcoSchemaContent:" + docSchemaContent.replaceAll("\\n", ""));
                Protocol protocol = Protocol.parse(docSchemaContent);
                tableSe2DBHandler.docSchema = protocol.getType(docSchemaName);
                tableSe2DBHandler.docReader = new GenericDatumReader<GenericRecord>(tableSe2DBHandler.docSchema);
            } else {
                logger.error("no schema content corresponding to this mq " + tableSe2DBHandler.getMQName() + " is found in metastore");
                tableSe2DBHandler = null;
            }
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
            tableSe2DBHandler = null;
        } finally {
            Connection tmpConn = null;
            try {
                tmpConn = rs.getStatement().getConnection();
            } catch (Exception ex) {
            }
            try {
                rs.close();
            } catch (Exception ex) {
            }
            try {
                tmpConn.close();
            } catch (Exception ex) {
            }
        }
        tableSe2DBHandler.appendSE2DBTask(1);
        return tableSe2DBHandler;
    }

    private void appendSE2DBTask(int pTaskNum) {
        for (int i = 0; i < pTaskNum; i++) {
            SE2DBWorker se2DBWorker = null;
            try {
                se2DBWorker = SE2DBWorkerFactory.getSE2DBWroker(tableSe2DBRule.getDbType(), this);
                threadPool.execute(se2DBWorker);
            } catch (Exception ex) {
                logger.error("execute serializing to db " + tableSe2DBRule.getDbType() + " is failed for " + ex.getMessage(), ex);
            }
        }
    }

    private void stopSE2DBTask() {
        for (SE2DBWorker se2DBWorker : se2DBWorkerSet) {
            se2DBWorker.stop();
        }
    }

    public void act2TableSe2DBRuleChange() {
        synchronized (this) {
            TableSe2DBRule tmpTableSe2DBRule = RuntimeEnv.metaStoreManger.getTableSe2DBRule(getRuleName());
            if (tmpTableSe2DBRule == null/*the persistance rule of the table has been deleted*/) {
                //stop persistance operation firstly
                stopSE2DBTask();
            } else {
                if (!tmpTableSe2DBRule.getMqName().equals(tableSe2DBRule.getMqName())/*mq has been changed to another*/) {
                    //1.stop persistance operation firstly
                    stopSE2DBTask();
                    //2.fixme
                }
                //if....fixme
            }
        }
    }

    public TableSe2DBRule getTableSe2DBRule() {
        return tableSe2DBRule;
    }

    public String getMQName() {
        return tableSe2DBRule.getMqName();
    }

    public String getTableName() {
        return tableSe2DBRule.getTableName();
    }

    public Schema getDocSchema() {
        return docSchema;
    }
    
    public int getBatchSize(){
        return tableSe2DBRule.getBatchSize();
    }

    public DatumReader<GenericRecord> getDocReader() {
        return docReader;
    }

    public String getRuleName() {
        return tableSe2DBRule.getRuleName();
    }
    
    
}
