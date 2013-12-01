/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.data.statistics;

import cn.ac.iie.ulss.statistics.commons.GlobalVariables;
import cn.ac.iie.ulss.statistics.commons.RuntimeEnv;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class PrintCount implements Runnable {

    public SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(PrintCount.class.getName());
    }

    @Override
    public void run() {
        int printtime = (Integer) RuntimeEnv.getParam(RuntimeEnv.PRINT_TIME);
        while (true) {
            try {
                Thread.sleep(60000 * printtime);
            } catch (InterruptedException ex) {
                logger.error(ex, ex);
            }

            logger.info("print the count ");
            String dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);

            File out = new File(dataDir);
            File f = null;
            if (!out.exists() && !out.isDirectory()) {
                out.mkdirs();
                logger.info("create the directory " + dataDir);
            }

            f = new File(dataDir + "/data_statistics.st");

            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(f, true);
            } catch (FileNotFoundException ex) {
                logger.error(ex, ex);
                continue;
            }

            BufferedOutputStream bos = new BufferedOutputStream(fos);
            HashMap<String, HashMap<String, AtomicLong[]>> MQToCount = (HashMap<String, HashMap<String, AtomicLong[]>>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_COUNT);
            Map<String, String> mqToTime = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_TIME);
            Date d = new Date();
            String dtime = dateFormat.format(d);
            int statisticstime = (Integer) RuntimeEnv.getParam(RuntimeEnv.STATISTICS_TIME);
            try {
                bos.write("------------".getBytes());
                bos.write(("this is counted in the " + dtime).getBytes());
                bos.write("------------".getBytes());
                for (String mq : mqToTime.keySet()) {
                    HashMap<String, AtomicLong[]> timeToCount = MQToCount.get(mq);
                    bos.write('\n');
                    d.setMinutes(0);
                    d.setSeconds(0);
                    for (int i = 0; i < statisticstime; i++) {
                        String tmptime = dateFormat.format(d);
                        AtomicLong[] al = timeToCount.get(tmptime);
                        if (al != null) {
                            bos.write((dtime + " " + mq + " " + tmptime + " count : " + al[0]).getBytes());
                        } else {
                            bos.write((dtime + " " + mq + " " + tmptime + " count : 0").getBytes());
                        }
                        d.setHours(d.getHours() - 1);
                        bos.write('\n');
                    }

                    Date dh = new Date();
                    dh.setMinutes(0);
                    dh.setSeconds(0);
                    int day = statisticstime / 24;
                    for (int i = 0; i <= day; i++) {
                        long dcount = 0L;
                        for (int j = 0; j < 24; j++) {
                            dh.setHours(j);
                            String tmpd = dateFormat.format(dh);
                            AtomicLong[] al = timeToCount.get(tmpd);
                            if (al != null) {
                                dcount += al[0].longValue();
                            }
                        }
                        dh.setHours(0);
                        String tmpd = dateFormat.format(dh);
                        bos.write((dtime + " " + mq + " " + tmpd + " count : " + dcount).getBytes());
                        bos.write('\n');
                        dh.setDate(dh.getDate() - 1);
                    }
                }
                bos.write("------------------------------------------------------------------".getBytes());
                bos.write('\n');
                bos.write('\n');
            } catch (IOException ex) {
                logger.error(ex, ex);
            }

            try {
                bos.flush();
                bos.close();
            } catch (IOException ex) {
                logger.error(ex, ex);
            }

        }
    }
}
