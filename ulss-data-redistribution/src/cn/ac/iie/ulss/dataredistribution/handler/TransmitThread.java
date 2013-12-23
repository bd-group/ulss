package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.MD5NodeLocator;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.NodeLocator;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class TransmitThread implements Runnable {

    ArrayList<Rule> ruleSet = null;
    public final String allChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    String topic = null;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Schema msgSchema = null;
//    Schema docsSchema = null;
//    DatumReader<GenericRecord> docsreader = null;
    DatumReader<GenericRecord> msgreader = null;
//    ByteArrayInputStream docsin = null;
//    BinaryDecoder docsdecoder = null;
//    GenericRecord docsGr = null;
    GenericArray msgSet = null;
    Iterator<ByteBuffer> msgitor = null;
    ConcurrentLinkedQueue dataPool = null;
    MD5NodeLocator nodelocator = null;
    String reader = null;
//    String docsSchemaContent = null;
    String msgSchemaContent = null;
    String msgSchemaName = null;
//    Protocol protocoldocs = null;
    Protocol protocolMsg = null;
    Map<RNode, Object> sendRows = null;
    Integer sendPoolSize = 0;
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(TransmitThread.class.getName());
    }

    public TransmitThread(ConcurrentLinkedQueue dataPool, ArrayList<Rule> ruleSet, String topic) {
        this.dataPool = dataPool;
        this.ruleSet = ruleSet;
        this.topic = topic;
    }

    @Override
    public void run() {
        init();

        while (true) {
            if (!dataPool.isEmpty()) {
                try {
                    logger.info("begining the dataSplit and send the message from " + topic + " to the transfer station ");
                    dataSplitAndSend();
                } catch (Exception ex) {
                    logger.error(ex, ex);
                    logger.error("a transmit thread for " + topic + " is dead ");
                    return;
                }
            } else {
                logger.info("dataPool for the topic " + topic + " is empty !");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    logger.info(ex, ex);
                }
            }
        }
    }

    /**
     *
     * init the environment
     */
    private void init() {
        sendPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_POOL_SIZE);
        msgSchemaContent = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMACONTENT)).get(topic);
//        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        msgSchemaName = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMANAME)).get(topic);
//        protocoldocs = Protocol.parse(docsSchemaContent);
//        docsSchema = protocoldocs.getType(GlobalVariables.DOCS);
//        docsreader = new GenericDatumReader<GenericRecord>(docsSchema);
        protocolMsg = Protocol.parse(msgSchemaContent);
        sendRows = MessageTransferStation.getMessageTransferStation();
    }

    /**
     *
     * Split and send the message to the transfer station
     */
    public void dataSplitAndSend() throws InterruptedException, Exception {
        logger.debug("begining the dataSplit  message from " + topic + " and send to the transfer station ");
        byte[] data = null;
        while ((data = (byte[]) dataPool.poll()) != null) {
            msgSchema = protocolMsg.getType(msgSchemaName);
            msgreader = new GenericDatumReader<GenericRecord>(msgSchema);

            ByteArrayInputStream msgbis = new ByteArrayInputStream(data);
            BinaryDecoder msgbd = new DecoderFactory().binaryDecoder(msgbis, null);
            GenericRecord msgRecord = null;
            try {
                msgRecord = msgreader.read(null, msgbd);
            } catch (IOException ex) {
                logger.info(" split the one data from the topic " + topic + " in the dataPool wrong " + ex, ex);
                storeUselessData(topic, data);
                continue;
            }
            for (Rule rule : ruleSet) {
                if (rule.getType() == 0) {
                    sendToType0(rule, data);
                } else if (rule.getType() == 1) {
                    sendToType1(rule, data, msgRecord);
                } else if (rule.getType() == 2) {
                    sendToType2(rule, data, msgRecord);
                } else if (rule.getType() == 3) {
                    sendToType3(rule, data, msgRecord);
                } else if (rule.getType() == 4) {
                    sendToType4(rule, data, msgRecord);
                } else {
                    logger.info("one rule is wrong because it's type is not 01234");
                }
            }
        }
    }

    /**
     *
     * send message whose type is 0
     */
    private void sendToType0(Rule rule, byte[] data) {
        NodeLocator n0 = rule.getNodelocator();
        String randomstring = generateString(10);
        if (n0.getNodesNum() > 0) {
            RNode node = n0.getPrimary(randomstring);
            ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) sendRows.get(node);
            clq.offer(data);
        } else {
            storeStrandedData(rule, data);
        }
    }

    /**
     *
     * send message whose type is 1
     */
    private void sendToType1(Rule rule, byte[] data, GenericRecord record) {
        String[] keywords = (rule.getKeywords()).split("\\;");
        StringBuilder sb = new StringBuilder();
        for (String s : keywords) {
            if (record.get(s.toLowerCase()) == null) {
                sb.append("");
            } else {
                sb.append((record.get(s.toLowerCase())).toString());
            }
        }

        NodeLocator n = rule.getNodelocator();
        if (n.getNodesNum() > 0) {
            RNode node = n.getPrimary(sb.toString());
            ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) sendRows.get(node);
            clq.offer(data);
        } else {
            storeStrandedData(rule, data);
        }
    }

    /**
     *
     * send message whose type is 2
     */
    private void sendToType2(Rule rule, byte[] data, GenericRecord record) {
        String f = rule.getFilters();
        if (isTrue(f, record)) {
            NodeLocator n = rule.getNodelocator();
            if (n.getNodesNum() > 0) {
                String randomstring = generateString(10);
                RNode node = n.getPrimary(randomstring);
                ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) sendRows.get(node);
                clq.offer(data);
            } else {
                storeStrandedData(rule, data);
            }
        }
    }

    /**
     *
     * send message whose type is 3
     */
    private void sendToType3(Rule rule, byte[] data, GenericRecord record) {
        String[] keywords = (rule.getKeywords()).split("\\;");
        String f = rule.getFilters();
        if (isTrue(f, record)) {
            NodeLocator n3 = rule.getNodelocator();
            if (n3.getNodesNum() > 0) {
                StringBuilder sb = new StringBuilder();
                for (String ss : keywords) {
                    if (record.get(ss.toLowerCase()) == null) {
                        sb.append("");
                    } else {
                        sb.append((record.get(ss.toLowerCase())).toString());
                    }
                }
                RNode node = n3.getPrimary(sb.toString());
                ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) sendRows.get(node);
                clq.offer(data);
            } else {
                storeStrandedData(rule, data);
            }
        }
    }

    /**
     *
     * send message whose type is 4
     */
    private void sendToType4(Rule rule, byte[] data, GenericRecord record) {
        String[] pt = rule.getPartType().split("\\|");
        if (pt.length == 7) {
            String[] keywords = (rule.getKeywords()).split("\\|");
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < keywords.length; i++) {
                if (record.get(keywords[i]) == null) {
                    sb.append("");
                } else {
                    sb.append((record.get(keywords[i])).toString());
                }
            }
            NodeLocator n4 = rule.getNodelocator();
            RNode node = n4.getPrimary(sb.toString());
            ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) sendRows.get(node);
            String keyinterval = null;
            String keytime = keywords[0].toLowerCase();
            try {
                Long time = (Long) record.get(keytime);
                String unit = (rule.getPartType().split("\\|"))[3];
                String interval = (rule.getPartType().split("\\|"))[4];
                keyinterval = getKeyInterval(time, unit, interval);
            } catch (Exception e) {
                storeUnvalidData(rule, data);
                logger.error(e, e);
            }

//            boolean vb = false;
//            try {
//                vb = isValid(keyinterval);
//            } catch (Exception ex) {
//                logger.error(ex, ex);
//            }
//
//            if (!vb) {
//                logger.info("the time in the topic " + topic + " is out-of-date!");
//            }
            
            synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_MESSAGETRANSFERSTATION)) {
                ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) chm.get(keyinterval);
                if (clq != null) {
                    clq.offer(data);
                } else {
                    logger.info("create a abq for " + topic + " " + rule.getServiceName() + " " + keyinterval + " " + node.getName());
                    GetFileFromMetaStore gffm = new GetFileFromMetaStore(keyinterval , node , rule);
                    gffm.getFileForInverval();
                    int datasendertsize = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATASENDER_THREAD);
                    clq = new ConcurrentLinkedQueue();
                    clq.offer(data);
                    chm.put(keyinterval, clq);
                    for (int i = 0; i < datasendertsize; i++) {
                        DataSenderThread dst = new DataSenderThread(clq, node , rule, keyinterval);
                        Thread tdst = new Thread(dst);
                        tdst.setName("DataSenderThread-" + topic + "-" + node.getName() + "-" + keyinterval);
                        tdst.start();
                    }
                }
            }
        } else {
            logger.error("partitioninfo is wrong!!!");
        }
    }

    /**
     *
     * generate the random string
     */
    public String generateString(int length) {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(allChar.charAt(random.nextInt(allChar.length())));
        }
        return sb.toString();
    }

    /**
     *
     * judge the string is valid or not , return the true if is valid , or
     * return false
     */
    private boolean isTrue(String s, GenericRecord dxxRecord) {

        if ((!s.contains("|")) && (!s.contains("&"))) {
            if (!s.contains("=")) {
                logger.error("the rule's fileter is wrong");
                return false;
            } else {
                String[] ss = s.split("\\=");
                String key = (dxxRecord.get(ss[0].toLowerCase())).toString();
                if (key == null ? ss[1] == null : key.equals(ss[1])) {
                    return true;
                } else {
                    return false;
                }
            }
        } else if (s.contains("|")) {
            String[] sr = s.split("\\|");
            for (int i = 0; i < sr.length; i++) {
                if (isTrue(sr[i], dxxRecord)) {
                    return true;
                }
            }
            return false;
        } else if (s.contains("&")) {
            String[] sc = s.split("\\&");
            for (int i = 0; i < sc.length; i++) {
                if (!isTrue(sc[i], dxxRecord)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     *
     * place the startded data to the strandedDataStore
     */
    private void storeStrandedData(Rule rule, byte[] data) {
        ConcurrentHashMap<Rule, ConcurrentLinkedQueue> strandedDataStore = (ConcurrentHashMap<Rule, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.STRANDED_DATA_STORE);
        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_STORE_STRANDEDDATA)) {
            if (strandedDataStore.containsKey(rule)) {
                ConcurrentLinkedQueue clq = strandedDataStore.get(rule);
                clq.offer(data);
            } else {
                ConcurrentLinkedQueue sdQueue = new ConcurrentLinkedQueue();
                sdQueue.offer(data);
                strandedDataStore.put(rule, sdQueue);
                StoreStrandedDataThread sdt = new StoreStrandedDataThread(sdQueue, rule);
                Thread tsdt = new Thread(sdt);
                tsdt.setName("StoreStrandedDataThread-" + rule.getTopic() + "-" + rule.getServiceName());
                tsdt.start();
                logger.info("start a StoreStrandedDataThread for " + topic);
            }
        }
    }

    /**
     *
     * place the useless data to the uselessDataStore
     */
    private void storeUselessData(String topic, byte[] data) {
        ConcurrentHashMap<String, ConcurrentLinkedQueue> uselessDataStore = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.USELESS_DATA_STORE);
        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_STORE_USELESSDATA)) {
            if (uselessDataStore.containsKey(topic)) {
                ConcurrentLinkedQueue clq = uselessDataStore.get(topic);
                clq.offer(data);
            } else {
                ConcurrentLinkedQueue sdQueue = new ConcurrentLinkedQueue();
                sdQueue.offer(data);
                uselessDataStore.put(topic, sdQueue);
                StoreUselessDataThread sudt = new StoreUselessDataThread(sdQueue, topic);
                Thread tsudt = new Thread(sudt);
                tsudt.setName("StoreUselessDataThread-" + topic);
                tsudt.start();
                logger.info("start a StoreUselessDataThread for " + topic);
            }
        }
    }

    /**
     *
     * place the unvalid data to the unvalidDataStore
     */
    private void storeUnvalidData(Rule rule, byte[] data) {
        ConcurrentHashMap<Rule, ConcurrentLinkedQueue> UnvalidDataStore = (ConcurrentHashMap<Rule, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.UNVALID_DATA_STORE);
        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_STORE_UNVALIDDATA)) {
            if (UnvalidDataStore.containsKey(rule)) {
                ConcurrentLinkedQueue clq = UnvalidDataStore.get(rule);
                clq.offer(data);
            } else {
                ConcurrentLinkedQueue sdQueue = new ConcurrentLinkedQueue();
                sdQueue.offer(data);
                UnvalidDataStore.put(rule, sdQueue);
                StoreUnvalidDataThread sudt = new StoreUnvalidDataThread(sdQueue, rule);
                Thread tsudt = new Thread(sudt);
                tsudt.setName("StoreUnvalidDataThread-" + rule.getTopic() + "-" + rule.getServiceName());
                tsudt.start();
                logger.info("start a StoreUnvalidDataStoreDataThread for " + topic);
            }
        }
    }

    /**
     *
     * test the keyinterval is valid or not
     */
    public Boolean isValid(String keyinterval) throws ParseException {
        if (keyinterval == null) {
            return false;
        }

        String st = keyinterval.split("\\|")[0];
        String et = keyinterval.split("\\|")[1];
        Date stime = dateFormat.parse(st);
        Date etime = dateFormat.parse(et);

        Calendar ca = Calendar.getInstance();
        ca.set(5, ca.get(5) - 30);
        Date nowtime = ca.getTime();
        if (nowtime.after(stime)) {
            return false;
        }

        ca.set(5, ca.get(5) + 37);
        nowtime = ca.getTime();
        if (nowtime.before(etime)) {
            return false;
        }
        return true;
    }

    /**
     *
     * get the keyinterval by time ,unit and interval
     */
    private String getKeyInterval(Long time, String unit, String interval) {
        String st = null;
        String et = null;

        Date dtime = new Date();
        dtime.setTime(time * 1000);
        String keyinterval = null;

        if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位
            dtime.setSeconds(0);
            dtime.setMinutes(dtime.getMinutes() - (dtime.getMinutes() % Integer.parseInt(interval)));
            st = dateFormat.format(dtime);
            int zt = dtime.getMinutes() + Integer.parseInt(interval);
            if (zt >= 60) {
                zt = 60;
            }
            dtime.setMinutes(zt);
            et = dateFormat.format(dtime);
            keyinterval = st + "|" + et;
        } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
            dtime.setSeconds(0);
            dtime.setMinutes(0);
            dtime.setHours(dtime.getHours() - (dtime.getHours() % Integer.parseInt(interval)));
            st = dateFormat.format(dtime);
            int zt = dtime.getHours() + Integer.parseInt(interval);
            if (zt >= 24) {
                zt = 24;
            }
            dtime.setHours(zt);
            et = dateFormat.format(dtime);
            keyinterval = st + "|" + et;
        } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
            dtime.setSeconds(0);
            dtime.setMinutes(0);
            dtime.setHours(0);
            dtime.setDate(dtime.getDate() - (dtime.getDate() % Integer.parseInt(interval)));
            st = dateFormat.format(dtime);
            dtime.setDate(dtime.getDate() + Integer.parseInt(interval));
            et = dateFormat.format(dtime);
            keyinterval = st + "|" + et;
        } else {
            logger.error("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
        }
        return keyinterval;
    }
}