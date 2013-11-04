/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.commons;

import cn.ac.ncic.impdatabg.conf.Configuration;
import cn.ac.ncic.impdatabg.conf.ConfigurationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public class RuntimeEnv {

    public static final int DIR_NUM = 4;
    public static Configuration conf = null;
    //parameters related with source file
    public static List<DirGroup> dirGroupLst = null;
    public static String lineEndSymbol;
    public static String fileEncoding;
    public static String fileNamePrefix;
    public static int fileDisposeWorkerNum;
    //parameters related with db
    public static int batchSize;
    public static final String DB_CLUSTER = "dbcluster";
    //parameters for message process
    public static boolean messageProcessEnable;
    public static final String ZK_CLUSTER = "zkCluster";
    //parameters added dynamicly
    private static Map<String, Object> dynamicParams = new HashMap<String, Object>();
    //logger
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RuntimeEnv.class.getName());
    }

    private static List getDirGroups(String pdirGroups) {
        List<DirGroup> lDirGroupLst = null;

        if (pdirGroups == null || pdirGroups.equals("")) {
            logger.error("the value of parameter dirGroups is null.");
            return null;
        }

        String[] dirGroupItems = pdirGroups.split(";");

        if (dirGroupItems.length == 0) {
            logger.error("the format of parameter dirGroups is wrong.");
            return null;
        }

        lDirGroupLst = new ArrayList<DirGroup>();

        for (String dirGroupItem : dirGroupItems) {
            String[] dirItems = dirGroupItem.split(":");
            if (dirItems.length == DIR_NUM) {
                lDirGroupLst.add(new DirGroup(dirItems[0], dirItems[1], dirItems[2], dirItems[3]));
            } else {
                lDirGroupLst.clear();
                lDirGroupLst = null;
                logger.error("item number of dirGroup is wrong with " + dirItems.length + " and expected is " + DIR_NUM);
                break;
            }
        }
        return lDirGroupLst;
    }

    public static boolean initialize(Configuration pConf) {

        if (pConf == null) {
            logger.error("configuration object is null");
            return false;
        }

        conf = pConf;

        String dirGroups = conf.getString("dirGroups", "");
        dirGroupLst = dirGroups.isEmpty() ? null : getDirGroups(dirGroups);
        if (dirGroupLst == null) {
            logger.error("getting dir groups is failed with " + dirGroups);
            return false;
        }

        lineEndSymbol = conf.getString("lineEndSymbol", "");

        fileEncoding = conf.getString("fileEncoding", "");
        if (fileEncoding.isEmpty()) {
            logger.error("parameter fileEncoding does not exist or is not defined");
            return false;
        }

        fileNamePrefix = conf.getString("fileNamePrefix", "");

        try {
            fileDisposeWorkerNum = conf.getInt("fileDisposeWorkerNum", 10);
            batchSize = conf.getInt("batchSize", 1000);
            messageProcessEnable = conf.getBoolean("messageProcessEnable", false);
        } catch (ConfigurationException confex) {
            logger.error(confex.getMessage());
            return false;
        }

        String zkClusterConnectString = conf.getString("zkCluster", "");
        if (zkClusterConnectString.isEmpty()) {
            logger.error("parameter zkCluster does not exist or is not defined");
            return false;
        }

        dynamicParams.put(ZK_CLUSTER, zkClusterConnectString);

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
