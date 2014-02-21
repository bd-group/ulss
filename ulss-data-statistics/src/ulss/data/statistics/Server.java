/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.data.statistics;

import cn.ac.iie.ulss.statistics.commons.GlobalVariables;
import cn.ac.iie.ulss.statistics.commons.RuntimeEnv;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class Server {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(Server.class.getName());
    }

    public static void run() {
        HashMap<String, HashMap<String, AtomicLong[]>> MQToCount = (HashMap<String, HashMap<String, AtomicLong[]>>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_COUNT);
        Map<String, String> mqToTime = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_TIME);
        String zkUrl = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);
        
        for (String mq : mqToTime.keySet()) {
            synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_COUNT)) {
                logger.info("start statistics for " + mq);
                HashMap<String, AtomicLong[]> timeToCount = new HashMap<String, AtomicLong[]>();
                MQToCount.put(mq, timeToCount);
                String time = mqToTime.get(mq);
                DataAccepterThread dat = new DataAccepterThread(zkUrl, mq, time, timeToCount);
                Thread tdat = new Thread(dat);
                tdat.start();
            }
        }
    }
}
