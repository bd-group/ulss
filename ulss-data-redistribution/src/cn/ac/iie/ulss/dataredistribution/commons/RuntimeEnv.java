package cn.ac.iie.ulss.dataredistribution.commons;

import cn.ac.iie.ulss.dataredistribution.config.Configuration;
import cn.ac.iie.ulss.dataredistribution.config.ConfigurationException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class RuntimeEnv {

    public static final String DB_CLUSTER = "dbCluster";
    public static final String ZK_CLUSTER = "zkCluster";
//    public static final String BUFFER_POOL_SIZE = "bufferPoolSize";
    public static final String DATA_POOL_SIZE = "dataPoolSize";
    public static final String SEND_POOL_SIZE = "sendPoolSize";
//    public static final String WRITE_TO_FILE_THREAD = "writeToFileThread";
    public static final String TRANSMIT_THREAD = "transmitThread";
    public static final String DATASENDER_THREAD = "datasenderThread";
    public static final String DATASENDER_LIMITTIME = "datasenderLimitTime";
    public static final String SEND_THREAD_POOL_SIZE = "sendThreadPoolSize";
    public static final String ACTIVE_THREAD_COUNT = "activeThreadCount";
    public static final String METASTORE_CLIENT = "metaStoreClient";
    public static final String DATA_DIR = "dataDir";
    public static final String METASTORE_CLIENT_POOL_SIZE = "metaStoreClientPoolSize";
    public static final String METASTORE_ZK_CLUSTER = "metaStoreZkCluster";
    public static final String REGION = "region";
    public static final String GROUP = "group";
    private static Map<String, Object> dynamicParams = new HashMap<String, Object>();
    private static Configuration conf = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RuntimeEnv.class.getName());
    }

    /**
     *
     * initialize the RuntimeEnv from the configuration file
     */
    public static boolean initialize(Configuration pConf) throws ConfigurationException {
        if (pConf == null) {
            logger.error("configuration object is null");
            return false;
        }

        conf = pConf;

        String dbCluster = conf.getString(DB_CLUSTER, "");
        if (dbCluster.isEmpty()) {
            logger.error("parameter dbCluster does not exist or is not defined");
            return false;
        }
        logger.info("get dbCluster " + dbCluster);
        dynamicParams.put(DB_CLUSTER, dbCluster);

        String zkCluster = conf.getString(ZK_CLUSTER, "");
        if (zkCluster.isEmpty()) {
            logger.error("parameter zkCluster does not exist or is not defined");
            return false;
        }
        logger.info("get zkCluster " + zkCluster);
        dynamicParams.put(ZK_CLUSTER, zkCluster);

//        int bufferPoolSize = conf.getInt(BUFFER_POOL_SIZE, 100);
//        if (bufferPoolSize <= 0) {
//            logger.error("parameter bufferPoolSize is a wrong number");
//            return false;
//        }
//        logger.info("get bufferPoolSize " + bufferPoolSize);
//        dynamicParams.put(BUFFER_POOL_SIZE, bufferPoolSize);

        int dataPoolSize = conf.getInt(DATA_POOL_SIZE, 1000);
        if (dataPoolSize <= 0) {
            logger.error("parameter dataPoolSize is a wrong number");
            return false;
        }
        logger.info("get dataPoolSize " + dataPoolSize);
        dynamicParams.put(DATA_POOL_SIZE, dataPoolSize);

        int sendPoolSize = conf.getInt(SEND_POOL_SIZE, 5000);
        if (sendPoolSize <= 0) {
            logger.error("parameter sendPoolSize is a wrong number");
            return false;
        }
        logger.info("get sendPoolSize " + sendPoolSize);
        dynamicParams.put(SEND_POOL_SIZE, sendPoolSize);

//        int writeToFileThread = conf.getInt(WRITE_TO_FILE_THREAD, 5);
//        if (writeToFileThread <= 0) {
//            logger.error("parameter writeToFileThread is a wrong number");
//            return false;
//        }
//        logger.info("get writeToFileThread " + writeToFileThread);
//        dynamicParams.put(WRITE_TO_FILE_THREAD, writeToFileThread);

        int transmitThread = conf.getInt(TRANSMIT_THREAD, 5);
        if (transmitThread <= 0) {
            logger.error("parameter transmitThread is a wrong number");
            return false;
        }
        logger.info("get transmitThread " + transmitThread);
        dynamicParams.put(TRANSMIT_THREAD, transmitThread);

        int datasenderThread = conf.getInt(DATASENDER_THREAD, 2);
        if (datasenderThread <= 0) {
            logger.error("parameter datasenderThread is a wrong number");
            return false;
        }
        logger.info("get datasenderThread " + datasenderThread);
        dynamicParams.put(DATASENDER_THREAD, datasenderThread);

        int datasenderLimitTime = conf.getInt(DATASENDER_LIMITTIME, 1000);
        if (datasenderLimitTime <= 0) {
            logger.error("parameter datasenderLimitTime is a wrong number");
            return false;
        }
        logger.info("get datasenderLimitTime " + datasenderLimitTime);
        dynamicParams.put(DATASENDER_LIMITTIME, datasenderLimitTime);

        int sendThreadPoolSize = conf.getInt(SEND_THREAD_POOL_SIZE, 20);
        if (sendThreadPoolSize <= 0) {
            logger.error("parameter sendThreadPoolSize is a wrong number");
            return false;
        }
        logger.info("get sendThreadPoolSize " + sendThreadPoolSize);
        dynamicParams.put(SEND_THREAD_POOL_SIZE, sendThreadPoolSize);

        int activeThreadCount = conf.getInt(ACTIVE_THREAD_COUNT, 0);
        if (activeThreadCount < 0) {
            logger.error("parameter activeThreadCount is a wrong number");
            return false;
        }
        logger.info("get activeThreadCount " + activeThreadCount);
        dynamicParams.put(ACTIVE_THREAD_COUNT, activeThreadCount);

        String metaStoreClient = conf.getString(METASTORE_CLIENT, "");
        if (metaStoreClient.isEmpty()) {
            logger.error("parameter metaStoreClient does not exist or is not defined");
            return false;
        }
        logger.info("get metaStoreClient " + metaStoreClient);
        dynamicParams.put(METASTORE_CLIENT, metaStoreClient);

        String dataDir = conf.getString(DATA_DIR, "");
        if (dataDir.isEmpty()) {
            logger.error("parameter dataDir does not exist or is not defined");
            return false;
        }
        logger.info("get dataDir " + dataDir);
        dynamicParams.put(DATA_DIR, dataDir);

        int metaStoreClientPoolSize = conf.getInt(METASTORE_CLIENT_POOL_SIZE, 20);
        if (metaStoreClientPoolSize <= 0) {
            logger.error("parameter metaStoreClientPoolSize is a wrong number");
            return false;
        }
        logger.info("get metaStoreClientPoolSize " + metaStoreClientPoolSize);
        dynamicParams.put(METASTORE_CLIENT_POOL_SIZE, metaStoreClientPoolSize);

        String metaStoreZkCluster = conf.getString(METASTORE_ZK_CLUSTER, "");
        if (metaStoreZkCluster.isEmpty()) {
            logger.error("parameter metaStoreZkCluster does not exist or is not defined");
            return false;
        }
        logger.info("get metaStoreZkCluster " + metaStoreZkCluster);
        dynamicParams.put(METASTORE_ZK_CLUSTER, metaStoreZkCluster);

        String region = conf.getString(REGION, "");
        if (region.isEmpty()) {
            logger.error("parameter region does not exist or is not defined");
            return false;
        }
        logger.info("get region " + region);
        dynamicParams.put(REGION, region);

        String group = conf.getString(GROUP, "");
        if (group.isEmpty()) {
            logger.error("parameter group does not exist or is not defined");
            return false;
        }
        logger.info("get group " + group);
        dynamicParams.put(GROUP, group);

        return true;
    }

    public static void dumpEnvironment() {
        conf.dumpConfiguration();
    }

    /**
     *
     * add the param to the RuntimeEnv
     */
    public synchronized static void addParam(String pParamName, Object pValue) {
        dynamicParams.put(pParamName, pValue);
    }

    /**
     *
     * get the param to the RuntimeEnv
     */
    public static Object getParam(String pParamName) {
        return dynamicParams.get(pParamName);
    }
}
