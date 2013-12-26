package cn.ac.iie.ulss.dataredistribution.commons;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.handler.HandlerDetectNodeThread;
import cn.ac.iie.ulss.dataredistribution.handler.SendStrandedDataThread;
import cn.ac.iie.ulss.dataredistribution.handler.TransmitStrandedDataThread;
import cn.ac.iie.ulss.dataredistribution.tools.MetaStoreClientPool;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.client.HttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class GlobalVariables {

    public static final String SYN_VALUE_TO_FILE = "synValueToFile";
    public static final String SYN_MESSAGETRANSFERSTATION = "synMessageTransferStation";
    public static final String SYN_STORE_STRANDEDDATA = "synStoreStrandedData";
    public static final String SYN_STORE_USELESSDATA = "synStoreUselessData";
    public static final String SYN_STORE_UNVALIDDATA = "synStoreUnvalidData";
    public static final String SYN_DIR = "synDir";
    public static final String SYN_METASTORE_CLIENT = "synMetaStoreClient";
    public static final String SYN_DETECT_NODE = "synDetectNode";
    public static final String TOPIC_TO_ACCEPTCOUNT = "topicToAcceptCount";
    public static final String RULE_TO_COUNT = "ruleToCount";
    public static final String METASTORE_CLIENT_POOL = "metaStoreClientPool";
    public static final String TOPIC_TO_RULES = "topicToRules";
    public static final String VALUE_TO_FILE = "valueToFile";
    public static final String DETECT_NODELIST = "detectNodeList";
    public static final String DETECT_NODE = "detectNode";
    public static final String STRANDED_DATA_TRANSMIT = "strandedDataTransmit";
    public static final String STRANDED_DATA_SEND = "strandedDataSend";
    public static final String STRANDED_DATA_STORE = "strandedDataStore";
    public static final String USELESS_DATA_STORE = "uselessDataStore";
    public static final String UNVALID_DATA_STORE = "unvalidDataStore";
    public static final String TOPIC_TO_SEND_THREADPOOL = "topicToSendThreadPool";
    public static final String TOPIC_TO_SCHEMACONTENT = "topicToSchemaContent";
    public static final String TOPIC_TO_SCHEMANAME = "topicToSchemaName";
    public static final String DOCS_SCHEMA_CONTENT = "docsSchemaContent";
    public static final String TOPIC_TO_TBNAME = "topicToTBName";
    public static final String META_TO_TOPIC = "metaToTopic";
    public static final String NODE_TO_RULE = "nodeToRule";
    public static final String DOCS = "docs";
    public static final String DOC_SET = "doc_set";
    public static final String DOC_SCHEMA_NAME = "doc_schema_name";
    public static final String SIGN = "sign";
    public static final String TRANSMITRULE = "transmitrule";
    public static final String TOPIC_TO_SYN_COUNT = "TopicToSynCount";
    public static final String TOPIC_TO_HTTPCLIENT = "TopicToHttpclient";
    public static final String TOPIC_TO_NODES = "topicToNodes";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GlobalVariables.class.getName());
    }

    /**
     *
     * init the Global Variables
     */
    public static void initialize() {

        byte[] synValueToFile = new byte[0];
        RuntimeEnv.addParam(SYN_VALUE_TO_FILE, synValueToFile);

        byte[] synMessageTransferStation = new byte[0];
        RuntimeEnv.addParam(SYN_MESSAGETRANSFERSTATION, synMessageTransferStation);

        byte[] synStoreStrandedData = new byte[0];
        RuntimeEnv.addParam(SYN_STORE_STRANDEDDATA, synStoreStrandedData);

        byte[] synStoreUselssData = new byte[0];
        RuntimeEnv.addParam(SYN_STORE_USELESSDATA, synStoreUselssData);

        byte[] synStoreUnvalidData = new byte[0];
        RuntimeEnv.addParam(SYN_STORE_UNVALIDDATA, synStoreUnvalidData);

        byte[] synDir = new byte[0];
        RuntimeEnv.addParam(SYN_DIR, synDir);

        byte[] synMetaStoreClient = new byte[0];
        RuntimeEnv.addParam(SYN_METASTORE_CLIENT, synMetaStoreClient);

        byte[] synDetectNode = new byte[0];
        RuntimeEnv.addParam(SYN_DETECT_NODE, synDetectNode);

        logger.info("setting the topicToAcceptCount to the Global Variables");
        ConcurrentHashMap<String, AtomicLong[]> topicToAcceptCount = new ConcurrentHashMap<String, AtomicLong[]>();
        RuntimeEnv.addParam(TOPIC_TO_ACCEPTCOUNT, topicToAcceptCount);

        logger.info("setting the ruleToCount to the Global Variables");
        ConcurrentHashMap<String, AtomicLong> ruleToCount = new ConcurrentHashMap<String, AtomicLong>();
        RuntimeEnv.addParam(RULE_TO_COUNT, ruleToCount);

        logger.info("setting the MetaStoreClientPool to the Global Variables");
        MetaStoreClientPool mscp = null;
        RuntimeEnv.addParam(METASTORE_CLIENT_POOL, mscp);

        logger.info("setting the topicToRules to the Global Variables");
        Map<String, ArrayList<Rule>> topicToRules = new ConcurrentHashMap<String, ArrayList<Rule>>();
        RuntimeEnv.addParam(TOPIC_TO_RULES, topicToRules);

        logger.info("setting the valueToFile to the Global Variables");
        ConcurrentHashMap<String, Object[]> valueToFile = new ConcurrentHashMap<String, Object[]>();
        RuntimeEnv.addParam(VALUE_TO_FILE, valueToFile);

        logger.info("setting the detectNodeList and detectNode to the Global Variables");
        ConcurrentLinkedQueue<Object[]> detectNodeList = new ConcurrentLinkedQueue<Object[]>();
        RuntimeEnv.addParam(DETECT_NODELIST, detectNodeList);
        ArrayList<RNode> detectNode = new ArrayList<RNode>();
        RuntimeEnv.addParam(DETECT_NODE, detectNode);
        HandlerDetectNodeThread hd = new HandlerDetectNodeThread(detectNodeList);
        Thread thd = new Thread(hd);
        thd.setName("HandlerDetectNodeThread");
        thd.start();

        logger.info("setting the strandedDataTransmit for retransmit to the Global Variables");
        ConcurrentLinkedQueue<Object[]> strandedDataTransmit = new ConcurrentLinkedQueue<Object[]>();
        RuntimeEnv.addParam(STRANDED_DATA_TRANSMIT, strandedDataTransmit);
        TransmitStrandedDataThread tsdt = new TransmitStrandedDataThread(strandedDataTransmit);
        Thread ttsdt = new Thread(tsdt);
        ttsdt.setName("TransmitStrandedDataThread");
        ttsdt.start();

        logger.info("setting the strandedDataSend for send to the Global Variables");
        ConcurrentHashMap<Map<Rule, byte[]>, Object[]> strandedDataSend = new ConcurrentHashMap<Map<Rule, byte[]>, Object[]>();
        RuntimeEnv.addParam(STRANDED_DATA_SEND, strandedDataSend);
        SendStrandedDataThread ss = new SendStrandedDataThread(strandedDataSend);
        Thread tss = new Thread(ss);
        tss.setName("SendStrandedDataThread");
        tss.start();

        logger.info("setting the strandedDataStore for store to the Global Variables");
        ConcurrentHashMap<Rule, ConcurrentLinkedQueue> strandedDataStore = new ConcurrentHashMap<Rule, ConcurrentLinkedQueue>();
        RuntimeEnv.addParam(STRANDED_DATA_STORE, strandedDataStore);

        logger.info("setting the uselessDataStore for store to the Global Variables");
        ConcurrentHashMap<String, ConcurrentLinkedQueue> uselessDataStore = new ConcurrentHashMap<String, ConcurrentLinkedQueue>();
        RuntimeEnv.addParam(USELESS_DATA_STORE, uselessDataStore);

        logger.info("setting the unvalidDataStore for store to the Global Variables");
        ConcurrentHashMap<Rule, ConcurrentLinkedQueue> unvalidDataStore = new ConcurrentHashMap<Rule, ConcurrentLinkedQueue>();
        RuntimeEnv.addParam(UNVALID_DATA_STORE, unvalidDataStore);

        logger.info("setting the topicToSendThreadPool to the Global Variables");
        Map<String, ThreadGroup> topicToSendThreadPool = new HashMap<String, ThreadGroup>();
        RuntimeEnv.addParam(TOPIC_TO_SEND_THREADPOOL, topicToSendThreadPool);

        logger.info("setting the topicToSchemaContent to the Global Variables");
        Map<String, String> topicToSchemaContent = new HashMap<String, String>();
        RuntimeEnv.addParam(TOPIC_TO_SCHEMACONTENT, topicToSchemaContent);

        logger.info("setting the topicToSchemaName to the Global Variables");
        Map<String, String> topicToSchemaName = new HashMap<String, String>();
        RuntimeEnv.addParam(TOPIC_TO_SCHEMANAME, topicToSchemaName);

        logger.info("setting the topicToTBName to the Global Variables");
        Map<String, String> topicToTBName = new HashMap<String, String>();
        RuntimeEnv.addParam(TOPIC_TO_TBNAME, topicToTBName);

        logger.info("setting the metaToTopic to the Global Variables");
        Map<String, String> metaToTopic = new HashMap<String, String>();
        RuntimeEnv.addParam(META_TO_TOPIC, metaToTopic);

        logger.info("setting the nodeToRule to the Global Variables");
        Map<RNode, Rule> nodeToRule = new HashMap<RNode, Rule>();
        RuntimeEnv.addParam(NODE_TO_RULE, nodeToRule);

        logger.info("setting the transmitrule to the Global Variables");
        ArrayList<String> transmitrule = new ArrayList<String>();
        RuntimeEnv.addParam(TRANSMITRULE, transmitrule);

        logger.info("setting the TopicToSynCount to the Global Variables");
        Map<String, byte[]> TopicToSynCount = new HashMap<String, byte[]>();
        RuntimeEnv.addParam(TOPIC_TO_SYN_COUNT, TopicToSynCount);
        
        logger.info("setting the TopicToHttpclient to the Global Variables");
        Map<String, HttpClient> TopicToHttpclient = new HashMap<String, HttpClient>();
        RuntimeEnv.addParam(TOPIC_TO_HTTPCLIENT, TopicToHttpclient);
        
        logger.info("setting the topicToNodes to the Global Variables");
        Map<String, ArrayList<RNode>> topicToNodes = new HashMap<String, ArrayList<RNode>>();
        RuntimeEnv.addParam(TOPIC_TO_NODES, topicToNodes);
    }
}
