package cn.ac.iie.ulss.dataredistribution.server;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.config.Configuration;
import cn.ac.iie.ulss.dataredistribution.handler.GetRuleFromDB;
import cn.ac.iie.ulss.dataredistribution.tools.GetSchemaFromDB;
import cn.ac.iie.ulss.dataredistribution.tools.GetTBNameFromDB;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.exception.MetaClientException;
import java.io.File;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author evan yang
 */
public class StartDataRedistributionServer {

    static final String CONFIGURATIONFILENAME = "data_redistribution.properties";
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd:HH");
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(StartDataRedistributionServer.class.getName());
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
        logger.info("starting data transmit client...");
        GetSchemaFromDB.getSchemaFromDB();
        GetTBNameFromDB.getTBNameFromDB();
        GetRuleFromDB gr = new GetRuleFromDB();
        gr.start();

        doShutDownWork();
    }

    private static void doShutDownWork() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                Map<String, MessageConsumer> topicToConsumer = (Map<String, MessageConsumer>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_CONSUMER);
                for (String t : topicToConsumer.keySet()) {
                    MessageConsumer mc = topicToConsumer.get(t);
                    while (true) {
                        try {
                            mc.shutdown();
                            logger.info("shutdown the metaq consumer for " + t);
                            break;
                        } catch (MetaClientException ex) {
                            logger.error("cann't shutdown the metaq consumer for " + t + ex, ex);
                        }
                    }
                }

                String dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);

                File out = new File(dataDir + "backup");
                synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_DIR)) {
                    if (!out.exists() && !out.isDirectory()) {
                        out.mkdirs();
                        logger.info("create the directory " + dataDir + "backup");
                    }
                }

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    //donothing
                }

                for (String t : topicToConsumer.keySet()) {
                    File fbk = new File(dataDir + "backup/" + t + ".bk");
                    while (true) {
                        if (fbk.exists()) {
                            logger.info("the data for topic " + t + " has not been processed completely");
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException ex) {
                                //donothing
                            }
                            continue;
                        } else {
                            logger.info("the data for topic " + t + " has been processed completely");
                        }
                        break;
                    }
                }

                Date date = new Date();
                String time = dateFormat.format(date);
                ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
                for (String topic : topicToAcceptCount.keySet()) {
                    logger.info(time + " this hour accept " + topicToAcceptCount.get(topic) + " messages from the topic " + topic);
                }
                ConcurrentHashMap<String, AtomicLong> ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
                for (String rule : ruleToCount.keySet()) {
                    logger.info(time + " this hour send " + ruleToCount.get(rule) + " messages for " + rule);
                }
            }
        });
    }
}
