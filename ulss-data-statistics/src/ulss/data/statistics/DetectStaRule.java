/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.data.statistics;

import cn.ac.iie.ulss.statistics.commons.GlobalVariables;
import cn.ac.iie.ulss.statistics.commons.RuntimeEnv;
import cn.ac.iie.ulss.statistics.dao.SimpleDaoImpl;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
class DetectStaRule implements Runnable {

    static Logger logger = null;
    private static SimpleDaoImpl simpleDao;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DetectStaRule.class.getName());
    }

    @Override
    public void run() {
        HashMap<String, HashMap<String, AtomicLong[]>> MQToCount = (HashMap<String, HashMap<String, AtomicLong[]>>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_COUNT);
        String zkUrl = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);

        while (true) {
            try {
                Thread.sleep(60000);
            } catch (InterruptedException ex) {
                //donothing
            }

            logger.info("detect new rule");

            Map<String, String> mqToTime = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_TIME);
            Map<String, String> newMqToTime = new HashMap<String, String>();
            String[] group = ((String) RuntimeEnv.getParam(RuntimeEnv.GROUP)).split("\\|");
            List<String> groups = Arrays.asList(group);

            String dbCluster = (String) RuntimeEnv.getParam(RuntimeEnv.DB_CLUSTER);
            simpleDao = SimpleDaoImpl.getDaoInstance(dbCluster);
            logger.info("getting mqToTime from oracledb...");
            String sql = "select MQ,TIMENAME,GROUP_S from DATA_STATISTICS";
            List<List<String>> rs = simpleDao.queryForList(sql);
            for (List<String> r1 : rs) {
                if (groups.contains(r1.get(2))) {
                    newMqToTime.put(r1.get(0), r1.get(1).toLowerCase());
                }
            }
            
            Map<String, String> updateMqToTime = new HashMap<String, String>();
            for (String mq : newMqToTime.keySet()) {
                if ( !mqToTime.containsKey(mq)) {
                    updateMqToTime.put(mq, newMqToTime.get(mq));
                }
            }

            for (String mq : updateMqToTime.keySet()) {
                synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_COUNT)) {
                    logger.info("start statistics for " + mq);
                    HashMap<String, AtomicLong[]> timeToCount = new HashMap<String, AtomicLong[]>();
                    MQToCount.put(mq, timeToCount);
                    String time = updateMqToTime.get(mq);
                    mqToTime.put(mq, time);
                    DataAccepterThread dat = new DataAccepterThread(zkUrl, mq, time, timeToCount);
                    Thread tdat = new Thread(dat);
                    tdat.start();
                }
            }
        }
    }
}
