package cn.ac.iie.ulss.dataredistribution.server;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.config.Configuration;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.handler.CountThread;
import cn.ac.iie.ulss.dataredistribution.handler.CreateFileThread;
import cn.ac.iie.ulss.dataredistribution.handler.DataAccepter;
import cn.ac.iie.ulss.dataredistribution.handler.GetErrorFile;
import cn.ac.iie.ulss.dataredistribution.handler.GetMessageFromMetaStore;
import cn.ac.iie.ulss.dataredistribution.handler.GetRuleFromDB;
import cn.ac.iie.ulss.dataredistribution.handler.PrintEnvironment;
import cn.ac.iie.ulss.dataredistribution.handler.TopicThread;
import cn.ac.iie.ulss.dataredistribution.tools.DataProducer;
import cn.ac.iie.ulss.dataredistribution.tools.GetSchemaFromDB;
import cn.ac.iie.ulss.dataredistribution.tools.GetTBNameFromDB;
import cn.ac.iie.ulss.dataredistribution.tools.HttpConnectionManager;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.File;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 *
 * @author evan yang
 */
public class StartDataRedistributionServer {

    static final String CONFIGURATIONFILENAME = "data_redistribution.properties";
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd:HH");
    static int handlertype = 0;
    static int filecount = 0;
    static Map<String, ConcurrentLinkedQueue[]> topicToDataPool = null;
    static Map<String, AtomicLong> topicToPackage = null;
    static ConcurrentHashMap<String, ArrayList<Rule>> topicToRules = null;
    static DataAccepter accepter = null;
    static DataProducer producer = null;
    static Integer datapoolcount = null;
    static Integer sendThreadPoolSize = null;
    static Map<String, HttpClient> topicToHttpclient = null;
    static ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = null;
    static Map<String, ArrayList<RNode>> topicToNodes = null;
    static Map<String, Map<String, AtomicLong>> ruleToThreadPoolSize = null;
    static ArrayList<Rule> ruleSet = null;
    static ConcurrentHashMap<String, AtomicLong> ruleToCount = null;
    static ConcurrentHashMap<String, AtomicLong> ruleToFilterCount = null;
    static String timefilter = null;
    static Map<RNode, Rule> nodeToRule = null;
    static Logger logger = null;
    static GetErrorFile errorfileconsumer = null;

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
        handlertype = (Integer) RuntimeEnv.getParam(RuntimeEnv.HDNDLER_TYPE);
        filecount = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATA_POOL_COUNT);
        topicToDataPool = (Map<String, ConcurrentLinkedQueue[]>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_DATAPOOL);
        topicToPackage = (Map<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_PACKAGE);
        topicToRules = (ConcurrentHashMap<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES);
        accepter = (DataAccepter) RuntimeEnv.getParam(GlobalVariables.CONSUMER);
        producer = (DataProducer) RuntimeEnv.getParam(GlobalVariables.PRODUCER);
        errorfileconsumer = (GetErrorFile) RuntimeEnv.getParam(GlobalVariables.ERRORFILECONSUMER);
        datapoolcount = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATA_POOL_COUNT);
        sendThreadPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_THREAD_POOL_SIZE);
        topicToHttpclient = (Map<String, HttpClient>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_HTTPCLIENT);
        topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
        topicToNodes = (Map<String, ArrayList<RNode>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_NODES);
        ruleToThreadPoolSize = (Map<String, Map<String, AtomicLong>>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_THREADPOOLSIZE);
        ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
        ruleToFilterCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_FILTERCOUNT);
        timefilter = (String) RuntimeEnv.getParam(RuntimeEnv.TIME_FILTER);
        nodeToRule = (Map<RNode, Rule>) RuntimeEnv.getParam(GlobalVariables.NODE_TO_RULE);
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
        gr.getRules();

        initTopic();

        producer.init();
        accepter.init();
        errorfileconsumer.init();

        MessageTransferStation.init(topicToRules); // initialise the message transfer station 

        for (String topickey : topicToRules.keySet()) {
            TopicThread topicthread = new TopicThread(topickey); // starting the transmit server by topic 
            Thread tt = new Thread(topicthread);
            tt.setName("TopicThread-" + topickey);
            tt.start();
        }

        accepter.pullDataFromQ();
        errorfileconsumer.pullDataFromQ();

        GetMessageFromMetaStore gmfms = new GetMessageFromMetaStore(); // getting the change message from the metastore
        Thread tgmfms = new Thread(gmfms);
        tgmfms.setName("GetMessageFromMetaStore");
        tgmfms.start();

//        DetectTransmitRule dtr = new DetectTransmitRule();
//        Thread tdtr = new Thread(dtr);
//        tdtr.setName("DetectTransmitRule");
//        tdtr.start();

        CountThread act = new CountThread();
        Thread tact = new Thread(act);
        tact.setName("AcceptCountThread");
        tact.start();

        PrintEnvironment pe = new PrintEnvironment();
        Thread tpe = new Thread(pe);
        tpe.setName("PrintEnvironment");
        tpe.start();

        doShutDownWork();
    }

    private static void initTopic() {
        for (String topic : topicToRules.keySet()) {
            ConcurrentLinkedQueue[] dataPool = new ConcurrentLinkedQueue[datapoolcount];
            for (int i = 0; i < dataPool.length; i++) {
                ConcurrentLinkedQueue clq = new ConcurrentLinkedQueue();
                dataPool[i] = clq;
            }
            topicToDataPool.put(topic, dataPool);

            AtomicLong pac = new AtomicLong(0);
            topicToPackage.put(topic, pac);

            HttpClient httpclient = HttpConnectionManager.getHttpClient(sendThreadPoolSize);
            topicToHttpclient.put(topic, httpclient);

            AtomicLong acceptCount = new AtomicLong(0);
            topicToAcceptCount.put(topic, acceptCount);

            ArrayList<RNode> alr = new ArrayList<RNode>();
            topicToNodes.put(topic, alr);

            Map<String, AtomicLong> topicToThreadSize = new HashMap<String, AtomicLong>();
            ruleToThreadPoolSize.put(topic, topicToThreadSize);

            ruleSet = (((ConcurrentHashMap<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES)).get(topic));
            for (Rule rule : ruleSet) {
                AtomicLong rulecount = new AtomicLong(0);
                ruleToCount.put(rule.getTopic() + rule.getServiceName(), rulecount);
                if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3) {
                    AtomicLong filterCount = new AtomicLong(0);
                    ruleToFilterCount.put(rule.getTopic() + rule.getServiceName() + rule.getFilters(), filterCount);
                } else {
                    AtomicLong filterCount1 = new AtomicLong(0);
                    ruleToFilterCount.put(rule.getTopic() + rule.getServiceName() + "timeparsewrong", filterCount1);
                    AtomicLong filterCount2 = new AtomicLong(0);
                    ruleToFilterCount.put(rule.getTopic() + rule.getServiceName() + timefilter, filterCount2);
                }
                AtomicLong threadSize = new AtomicLong(0);
                topicToThreadSize.put(rule.getServiceName(), threadSize);

                ArrayList nodeurls = rule.getNodeUrls();
                for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                    RNode node = (RNode) itit.next();
                    nodeToRule.put(node, rule);
                    alr.add(node);
                }

                if (rule.getType() == 4) {
                    CreateFileThread cft = new CreateFileThread(rule);
                    Thread tcft = new Thread(cft);
                    tcft.setName("CreateFileThread-" + rule.getTopic() + rule.getServiceName());
                    tcft.start();
                }
            }
        }
    }

    private static void doShutDownWork() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                accepter.shutdown();

                if (handlertype == 1) {
                    String dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);

                    File out = new File(dataDir + "backup");
                    synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_DIR)) {
                        if (!out.exists() && !out.isDirectory()) {
                            out.mkdirs();
                            logger.info("create the directory " + dataDir + "backup");
                        }
                    }

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        //donothing
                    }

                    for (String t : topicToRules.keySet()) {
                        for (int i = 0; i < filecount; i++) {
                            File fbk = new File(dataDir + "backup/" + t + i + ".bk");
                            while (true) {
                                if (fbk.exists()) {
                                    logger.info("the data for topic " + t + i + " has not been processed completely");
                                    try {
                                        Thread.sleep(2000);
                                    } catch (Exception ex) {
                                        //donothing
                                    }
                                    continue;
                                } else {
                                    logger.info("the data for topic " + t + i + " has been processed completely");
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    while (true) {
                        for (String topic : topicToDataPool.keySet()) {
                            ConcurrentLinkedQueue[] dataPool = topicToDataPool.get(topic);
                            for (int i = 0; i < dataPool.length; i++) {
                                if (!dataPool[i].isEmpty()) {
                                    try {
                                        Thread.sleep(2000);
                                    } catch (InterruptedException ex) {
                                        //java.util.logging.Logger.getLogger(StartDataRedistributionServer.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                    continue;
                                }
                            }
                        }
                        break;
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        //java.util.logging.Logger.getLogger(StartDataRedistributionServer.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    while (true) {
                        for (String topic : topicToPackage.keySet()) {
                            AtomicLong al = topicToPackage.get(topic);
                            if (al.get() > 0) {
                                try {
                                    Thread.sleep(2000);
                                } catch (InterruptedException ex) {
                                    //java.util.logging.Logger.getLogger(StartDataRedistributionServer.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                continue;
                            }
                        }
                        break;
                    }

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        //java.util.logging.Logger.getLogger(StartDataRedistributionServer.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    while (true) {
                        ConcurrentHashMap<Rule, ConcurrentLinkedQueue> UnvalidDataStore = (ConcurrentHashMap<Rule, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.UNVALID_DATA_STORE);
                        ConcurrentHashMap<String, ConcurrentLinkedQueue> uselessDataStore = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.USELESS_DATA_STORE);
                        for (String topic : uselessDataStore.keySet()) {
                            ConcurrentLinkedQueue clq = uselessDataStore.get(topic);
                            if (clq != null) {
                                if (!clq.isEmpty()) {
                                    logger.info("the queue of uselessDataStore for " + topic + " is not empty");
                                    continue;
                                }
                            }
                        }
                        for (Rule r : UnvalidDataStore.keySet()) {
                            ConcurrentLinkedQueue clq2 = UnvalidDataStore.get(r);
                            if (clq2 != null) {
                                if (!clq2.isEmpty()) {
                                    logger.info("the queue of unvalidDataStore for " + r.getTopic() + " " + r.getServiceName() + " is not empty");
                                    continue;
                                }
                            }
                        }
                        break;
                    }

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        //java.util.logging.Logger.getLogger(StartDataRedistributionServer.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    while (true) {
                        for (String topic : ruleToThreadPoolSize.keySet()) {
                            for (String service : ruleToThreadPoolSize.get(topic).keySet()) {
                                AtomicLong ruleToSize = ruleToThreadPoolSize.get(topic).get(service);
                                if (ruleToSize.longValue() > 0) {
                                    logger.info("");
                                    continue;
                                }
                            }
                        }
                        break;
                    }
                }

                errorfileconsumer.shutdown();
                producer.shutdown();
                Map<String, CloseableHttpClient> TopicToHttpclient = (Map<String, CloseableHttpClient>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_HTTPCLIENT);
                for (String s : TopicToHttpclient.keySet()) {
                    CloseableHttpClient client = TopicToHttpclient.get(s);
                    try {
                        client.close();
                    } catch (Exception ex) {
                        logger.error(ex, ex);
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
