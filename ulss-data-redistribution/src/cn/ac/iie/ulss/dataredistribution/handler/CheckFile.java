/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class CheckFile {

    private static ConcurrentHashMap<String, Object[]> valueToFile = (ConcurrentHashMap<String, Object[]>) RuntimeEnv.getParam(GlobalVariables.VALUE_TO_FILE);
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(CheckFile.class.getName());
    }

    static synchronized void check(long fid) {
        logger.info("get the wrong file " + fid);
        String tmp = null;
        for (String t : valueToFile.keySet()) {
            Object[] o = valueToFile.get(t);
            if ((Long) o[1] == fid) {
                tmp = t;
                break;
            }
        }
        if (tmp != null) {
            valueToFile.remove(tmp);
            logger.info("valuetofile remove the file " + fid + " for the key " + tmp);
        }
    }
}
