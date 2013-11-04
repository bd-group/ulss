/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.datadispatch.commons;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.ict.ncic.util.dao.util.ClusterInfoOP;
import cn.ac.iie.ulss.datadispatch.config.Configuration;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public class RuntimeEnv {

    private static Configuration conf = null;
    private static final String DB_CLUSTERS = "dbClusters";
    public static final String METADB_CLUSTER = "metaStoreDB";
    public static final String ZK_CLUSTER = "zkCluster";
    public static final String LOAD_CLUSTER = "loadCluster";
    private static Map<String, Object> dynamicParams = new HashMap<String, Object>();
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

        String zkClusterConnectString = conf.getString("zkCluster", "");
        if (zkClusterConnectString.isEmpty()) {
            logger.error("parameter zkCluster does not exist or is not defined");
            return false;
        }

        dynamicParams.put(ZK_CLUSTER, zkClusterConnectString);


        String dbCluster = conf.getString(DB_CLUSTERS, "");
        if (dbCluster.isEmpty()) {
            logger.error("parameter dbcluster does not exist or is not defined");
            return false;
        }

        try {
            DaoPool.putDao(ClusterInfoOP.getDBClusters(dbCluster));//need check
        } catch (Exception ex) {
            logger.error("init dao is failed for " + ex);
            return false;
        }

        String loadClusterString = conf.getString("loadCluster", "");
        if (loadClusterString.isEmpty()) {
            logger.error("parameter loadCluster does not exist or is not defined");
            return false;
        }

        dynamicParams.put(LOAD_CLUSTER, loadClusterString);

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
