/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.data.statistics;

import cn.ac.iie.ulss.statistics.commons.GlobalVariables;
import cn.ac.iie.ulss.statistics.commons.RuntimeEnv;
import cn.ac.iie.ulss.statistics.config.Configuration;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.exception.MetaClientException;
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
public class UlssDataStatistics {

    static final String CONFIGURATIONFILENAME = "data_statistics_config.properties";
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(UlssDataStatistics.class.getName());
    }

    /**
     *
     * main function
     */
    public static void main(String[] arg) {
        logger.info("intializing data_redistribution client...");
        try {
            init();
            run();
        } catch (Exception ex) {
            logger.info(ex);
        }
    }

    /**
     *
     * init the Environment and the Global Variables
     */
    private static void init() throws Exception {
        logger.info("getting configuration from configuration file " + CONFIGURATIONFILENAME);
        Configuration conf = Configuration.getConfiguration(CONFIGURATIONFILENAME);
        if (conf == null) {
            throw new Exception("reading " + CONFIGURATIONFILENAME + " is failed.");
        }

        logger.info("initializng runtaime enviroment...");
        if (!RuntimeEnv.initialize(conf)) {
            throw new Exception("initializng runtime enviroment is failed");
        }
        logger.info("initialize runtime enviroment successfully");

        logger.info("initializng Global Variables...");
        GlobalVariables.initialize();
        logger.info("initialize Global Variables  successfully");
    }

    /**
     *
     * run the main server
     */
    private static void run() throws FileNotFoundException, IOException, InterruptedException {
        GetSchemaFromDB.getSchemaFromDB();
        GetMQFromDB.get();
        Server.run();
        logger.info("starting data statistics client...");

        PrintCount pc = new PrintCount();
        Thread tpc = new Thread(pc);
        tpc.start();

        DetectStaRule dsr = new DetectStaRule();
        Thread tdsr = new Thread(dsr);
        tdsr.start();

        doShutDownWork();
    }

    private static void doShutDownWork() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                Map<String, MessageConsumer> MQToConsumer = (Map<String, MessageConsumer>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_CONSUMER);
                for (String mq : MQToConsumer.keySet()) {
                    MessageConsumer mqc = MQToConsumer.get(mq);
                    while (true) {
                        try {
                            mqc.shutdown();
                            logger.info("shutdown the metaq consumer for " + mq);
                            break;
                        } catch (MetaClientException ex) {
                            logger.error("cann't shutdown the metaq consumer for " + mq + ex, ex);
                        }
                    }
                }

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    //donothing
                }

                String dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);
                File out = new File(dataDir);

                logger.info("print the count ");

                if (!out.exists() && !out.isDirectory()) {
                    out.mkdirs();
                    logger.info("create the directory " + dataDir);
                }

                File f = new File(dataDir + "/data_statistics.st");

                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(f, true);
                } catch (FileNotFoundException ex) {
                    logger.error(ex, ex);
                }

                BufferedOutputStream bos = new BufferedOutputStream(fos);
                HashMap<String, HashMap<String, AtomicLong[]>> MQToCount = (HashMap<String, HashMap<String, AtomicLong[]>>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_COUNT);
                Map<String, String> mqToTime = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_TIME);
                Date d = new Date();
                String dtime = dateFormat.format(d);
                int statisticstime = (Integer) RuntimeEnv.getParam(RuntimeEnv.STATISTICS_TIME);
                try {
                    synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_COUNT)) {
                        bos.write("------------".getBytes());
                        bos.write(("this is counted in the " + dtime).getBytes());
                        bos.write("------------".getBytes());
                        for (String mq : mqToTime.keySet()) {
                            HashMap<String, AtomicLong[]> timeToCount = MQToCount.get(mq);
                            bos.write('\n');
                            d = new Date();
                            d.setMinutes(0);
                            d.setSeconds(0);
                            for (int i = 0; i < statisticstime; i++) {
                                String tmptime = dateFormat.format(d);
                                AtomicLong[] al = timeToCount.get(tmptime);
                                if (al != null) {
                                    bos.write((dtime + " " + mq + " " + tmptime + " hour count : " + al[0]).getBytes());
                                } else {
                                    bos.write((dtime + " " + mq + " " + tmptime + " hour count : 0").getBytes());
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
                                bos.write((dtime + " " + mq + " " + tmpd + " day count : " + dcount).getBytes());
                                bos.write('\n');
                                dh.setDate(dh.getDate() - 1);
                            }
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

                Date date = new Date();
                String fb = format.format(date) + "_" + f.getName();

                if (f.exists()) {
                    f.renameTo(new File(dataDir + "/" + fb));
                }
            }
        });
    }
}
