/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.data.statistics;

import cn.ac.iie.ulss.statistics.commons.GlobalVariables;
import cn.ac.iie.ulss.statistics.commons.RuntimeEnv;
import cn.ac.iie.ulss.statistics.dao.SimpleDaoImpl;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class GetMQFromDB {

    static Logger logger = null;
    private static SimpleDaoImpl simpleDao;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GetMQFromDB.class.getName());
    }
    
    public static void get() {
        Map<String, String> mqToTime = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_TIME);

        String dbCluster = (String) RuntimeEnv.getParam(RuntimeEnv.DB_CLUSTER);
        simpleDao = SimpleDaoImpl.getDaoInstance(dbCluster);
        logger.info("getting mqToTime from oracledb...");
        String sql = "select MQ,TIMENAME from DATA_STATISTICS";
        List<List<String>> rs = simpleDao.queryForList(sql);
        for (List<String> r1 : rs) {
            mqToTime.put(r1.get(0), r1.get(1).toLowerCase());
        }
    }
}
