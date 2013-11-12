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
    public static final String BUFFER_POOL_SIZE = "bufferPoolSize";
    public static final String DATA_POOL_SIZE = "dataPoolSize";
    public static final String SEND_POOL_SIZE = "sendPoolSize";
    public static final String SEND_TIME_OUT = "sendTimeout";
    public static final String WRITE_TO_FILE_THREAD = "writeToFileThread";
    public static final String TRANSMIT_THREAD = "transmitThread";
    public static final String SEND_THREAD_POOL_SIZE = "sendThreadPoolSize";
    public static final String ACCEPT_TIMEOUT = "acceptTimeout";
    public static final String ACTIVE_THREAD_COUNT = "activeThreadCount";
    public static final String INTERVAL_SIZE = "intervalSize";
    public static final String METASTORE_CLIENT_STRING = "metaStoreClientString";
    public static final String DATA_DIR = "dataDir";
    public static final String METASTORE_CLIENT_POOL_SIZE = "metaStoreClientPoolSize";
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

        String dbCluster = conf.getString("dbCluster", "");
        if (dbCluster.isEmpty()) {
            logger.error("parameter dbCluster does not exist or is not defined");
            return false;
        }
        dynamicParams.put(DB_CLUSTER, dbCluster);

        String zkClusterConnectString = conf.getString("zkCluster", "");
        if (zkClusterConnectString.isEmpty()) {
            logger.error("parameter zkCluster does not exist or is not defined");
            return false;
        }

        dynamicParams.put(ZK_CLUSTER, zkClusterConnectString);

        int bufferPoolSize = conf.getInt("bufferPoolSize", 50);
        if (bufferPoolSize <= 0) {
            logger.error("parameter bufferPoolSize is a wrong number");
            return false;
        }
        dynamicParams.put(BUFFER_POOL_SIZE, bufferPoolSize);

        int dataPoolSize = conf.getInt("dataPoolSize", 1000);
        if (dataPoolSize <= 0) {
            logger.error("parameter dataPoolSize is a wrong number");
            return false;
        }
        dynamicParams.put(DATA_POOL_SIZE, dataPoolSize);

        int sendPoolSize = conf.getInt("sendPoolSize", 5000);
        if (sendPoolSize <= 0) {
            logger.error("parameter sendPoolSize is a wrong number");
            return false;
        }
        dynamicParams.put(SEND_POOL_SIZE, sendPoolSize);

        int sendTimeout = conf.getInt("sendTimeout", 5000);
        if (sendTimeout <= 0) {
            logger.error("parameter sendTimeout is a wrong number");
            return false;
        }
        dynamicParams.put(SEND_TIME_OUT, sendTimeout);

        int writeToFileThread = conf.getInt("writeToFileThread", 10);
        if (writeToFileThread <= 0) {
            logger.error("parameter writeToFileThread is a wrong number");
            return false;
        }
        dynamicParams.put(WRITE_TO_FILE_THREAD, writeToFileThread);

        int transmitThread = conf.getInt("transmitThread", 10);
        if (transmitThread <= 0) {
            logger.error("parameter transmitThread is a wrong number");
            return false;
        }
        dynamicParams.put(TRANSMIT_THREAD, transmitThread);

        int sendThreadPoolSize = conf.getInt("sendThreadPoolSize", 100);
        if (sendThreadPoolSize <= 0) {
            logger.error("parameter sendThreadPoolSize is a wrong number");
            return false;
        }
        dynamicParams.put(SEND_THREAD_POOL_SIZE, sendThreadPoolSize);

        int acceptTimeout = conf.getInt("acceptTimeout", 30000);
        if (acceptTimeout <= 0) {
            logger.error("parameter acceptTimeout is a wrong number");
            return false;
        }
        dynamicParams.put(ACCEPT_TIMEOUT, acceptTimeout);

        int activeThreadCount = conf.getInt("activeThreadCount", 0);
        if (activeThreadCount < 0) {
            logger.error("parameter activeThreadCount is a wrong number");
            return false;
        }
        dynamicParams.put(ACTIVE_THREAD_COUNT, activeThreadCount);

        int intervalSize = conf.getInt("intervalSize", 10);
        if (intervalSize < 0) {
            logger.error("parameter intervalSize is a wrong number");
            return false;
        }
        dynamicParams.put(INTERVAL_SIZE, intervalSize);

        String metaStoreClient = conf.getString("metaStoreClient", "");
        if (metaStoreClient.isEmpty()) {
            logger.error("parameter metaStoreClient does not exist or is not defined");
            return false;
        }

        dynamicParams.put(METASTORE_CLIENT_STRING, metaStoreClient);

        String dataDir = conf.getString("dataDir", "");
        if (dataDir.isEmpty() || dataDir == "") {
            logger.error("parameter dataDir does not exist or is not defined");
            return false;
        }

        dynamicParams.put(DATA_DIR, dataDir);

        int metaStoreClientPoolSize = conf.getInt("metaStoreClientPoolSize", 20);
        if (metaStoreClientPoolSize <= 0) {
            logger.error("parameter metaStoreClientPoolSize is a wrong number");
            return false;
        }
        dynamicParams.put(METASTORE_CLIENT_POOL_SIZE, metaStoreClientPoolSize);

        return true;
    }

    public static void dumpEnvironment() {
        conf.dumpConfiguration();
    }

    /**
     *
     * add the param to the RuntimeEnv
     */
    public static void addParam(String pParamName, Object pValue) {
        synchronized (dynamicParams) {
            dynamicParams.put(pParamName, pValue);
        }
    }

    /**
     *
     * get the param to the RuntimeEnv
     */
    public static Object getParam(String pParamName) {
        return dynamicParams.get(pParamName);
    }
}
