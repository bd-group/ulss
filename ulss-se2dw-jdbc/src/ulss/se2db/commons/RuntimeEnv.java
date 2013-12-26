/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.commons;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.ict.ncic.util.dao.util.ClusterInfoOP;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ulss.se2db.config.Configuration;
import ulss.se2db.metastore.MetaStoreManager;

/**
 *
 * @author AlexMu
 */
public class RuntimeEnv {

    private static Configuration conf = null;
    public static final String METADB_CLUSTER = "metaStoreDB";
    public static final String ZK_CLUSTER = "zkCluster";
    private static Map<String, Object> dynamicParams = new HashMap<String, Object>();
    public static MetaStoreManager metaStoreManger = null;
    public static Schema docsSchema = null;
    public static DatumReader<GenericRecord> docsReader = null;
    //logger
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RuntimeEnv.class.getName());
    }

    public static boolean initialize(Configuration pConf) {

        if (pConf == null) {
            logger.error("configuration object is null");
            return false;
        }

        conf = pConf;

        String zkClusterConnectString = conf.getString(RuntimeEnv.ZK_CLUSTER, "");
        if (zkClusterConnectString.isEmpty()) {
            logger.error("parameter zkCluster does not exist or is not defined");
            return false;
        }
        
        addParam(ZK_CLUSTER, zkClusterConnectString);

        metaStoreManger = MetaStoreManager.getMetaStoreManger(pConf);
        if (metaStoreManger == null) {
            return false;
        }

        String metadbCluster = conf.getString(METADB_CLUSTER, "");
        if (metadbCluster.isEmpty()) {
            logger.error("parameter dbcluster does not exist or is not defined");
            return false;
        }

        try {
            DaoPool.putDao(ClusterInfoOP.getDBClusters(metadbCluster));//need check
        } catch (Exception ex) {
            logger.error("init dao is failed for " + ex);
            return false;
        }

        ResultSet rs = null;
        try {
            rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery("select schema_name,schema_content from dataschema where dataschema.schema_name='docs'");
            if (rs.next()) {
                String docsSchemaName = rs.getString("schema_name");
                String docsSchemaContent = rs.getString("schema_content");
                logger.info("docSchemaName:" + docsSchemaName + ",dcoSchemaContent:" + docsSchemaContent.replaceAll("\\n", ""));
                Protocol protocol = Protocol.parse(docsSchemaContent);
                docsSchema = protocol.getType(docsSchemaName);
                docsReader = new GenericDatumReader<GenericRecord>(docsSchema);
            } else {
                logger.error("no schema content corresponding to docs is found in metastore");
                return false;
            }
        } catch (Exception ex) {
            logger.error("init docs reader is failed for " + ex);
            return false;
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

        return true;
    }

    public static void dumpEnvironment() {
        conf.dumpConfiguration();
    }

    public static void addParam(String pParamName, Object pValue) {
        synchronized (dynamicParams) {
            dynamicParams.put(pParamName, pValue);
        }
    }

    public static Object getParam(String pParamName) {
        return dynamicParams.get(pParamName);
    }
}
