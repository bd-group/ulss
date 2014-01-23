package cn.ac.iie.ulss.statistics.commons;



import cn.ac.iie.ulss.statistics.config.Configuration;
import cn.ac.iie.ulss.statistics.config.ConfigurationException;
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
    public static final String DATA_DIR = "dataDir";
    public static final String PRINT_TIME = "printtime";
    public static final String STATISTICS_TIME = "statisticstime";
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

        String dataDir = conf.getString("dataDir", "");
        if (dataDir.isEmpty() || dataDir == "") {
            logger.error("parameter dataDir does not exist or is not defined");
            return false;
        }

        dynamicParams.put(DATA_DIR, dataDir);
        
        int printtime = conf.getInt("printtime", 0);
        if (printtime == 0) {
            logger.error("parameter printtime does not exist or is not defined");
            return false;
        }

        dynamicParams.put(PRINT_TIME, printtime);
        
        int statisticstime = conf.getInt("statisticstime", 0);
        if (statisticstime == 0) {
            logger.error("parameter statisticstime does not exist or is not defined");
            return false;
        }

        dynamicParams.put(STATISTICS_TIME, statisticstime);

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
