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
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
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
    Schema docsSchema = null;
    DatumReader<GenericRecord> docsreader = null;
    DatumReader<GenericRecord> msgreader = null;
    ByteArrayInputStream docsin = null;
    BinaryDecoder docsdecoder = null;
    GenericRecord docsGr = null;
    GenericArray msgSet = null;
    Iterator<ByteBuffer> msgitor = null;
    ArrayBlockingQueue dataPool = null;
    MD5NodeLocator nodelocator;
    String reader = null;
    static org.apache.log4j.Logger logger = null;
    String docsSchemaContent = null;
    String msgSchemaContent = null;
    String msgSchemaName = null;
    Map<RNode, Object> sendRows = null;
    static Integer sendPoolSize = 1000;
    static long limit = 5000;
    static final byte[] li = new byte[0];

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(TransmitThread.class.getName());
    }

    public TransmitThread(ArrayBlockingQueue dataPool, ArrayList<Rule> ruleSet, String topic) {
        this.dataPool = dataPool;
        this.ruleSet = ruleSet;
        this.topic = topic;
    }

    @Override
    public void run() {
        sendPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_POOL_SIZE);
        msgSchemaContent = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMACONTENT)).get(topic);
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        msgSchemaName = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMANAME)).get(topic);

        Protocol protocoldocs = Protocol.parse(docsSchemaContent);
        docsSchema = protocoldocs.getType(GlobalVariables.DOCS);
        docsreader = new GenericDatumReader<GenericRecord>(docsSchema);

        while (true) {
            if (!dataPool.isEmpty()) {
                try {
                    dataSplitAndSent();
                } catch (Exception ex) {
                    logger.error(ex, ex);
                    return;
                }
            } else {
                logger.debug("dataPool for the topic " + topic + " is empty !");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    logger.info(ex, ex);
                }
            }
        }
    }

    /**
     *
     * Split and send the message to the transfer station
     */
    public void dataSplitAndSent() throws InterruptedException, Exception {
        logger.info("begining the dataSplit and send the message from " + topic + " to the transfer station ");
        byte[] data = null;
        while ((data = (byte[]) dataPool.poll(1000, TimeUnit.MILLISECONDS)) != null) {
            docsin = new ByteArrayInputStream(data);
            docsdecoder = DecoderFactory.get().binaryDecoder(docsin, null);
            try {
                docsGr = docsreader.read(null, docsdecoder);
            } catch (IOException ex) {
                logger.info((new Date()) + " split the data package from the topic " + topic + " in the dataPool wrong " + ex, ex);
                storeUselessData(topic, data);
                continue;
            }
            msgSet = (GenericData.Array<GenericRecord>) docsGr.get(GlobalVariables.DOC_SET);
            msgitor = msgSet.iterator();
            sendRows = MessageTransferStation.getMessageTransferStation();
            Protocol protocolMsg = Protocol.parse(msgSchemaContent);
            msgSchema = protocolMsg.getType(msgSchemaName);
            msgreader = new GenericDatumReader<GenericRecord>(msgSchema);

            addCount(); // print the accept count 

            while (msgitor.hasNext()) {
                byte[] onedata = ((ByteBuffer) msgitor.next()).array();
                ByteArrayInputStream msgbis = new ByteArrayInputStream(onedata);
                BinaryDecoder msgbd = new DecoderFactory().binaryDecoder(msgbis, null);
                GenericRecord dxxRecord;
                try {
                    dxxRecord = msgreader.read(null, msgbd);
                } catch (IOException ex) {
                    logger.info((new Date()) + " split the one data from the topic " + topic + " in the dataPool wrong " + ex, ex);
                    storeUselessData(topic, onedata);
                    continue;
                }
                for (Rule rule : ruleSet) {
                    while (true) {
                        String flag = null;
                        Map<Rule, String> ruleToControl = (Map<Rule, String>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_CONTROL);
                        flag = ruleToControl.get(rule);
                        if (flag.equals("start")) {
                            if (rule.getType() == 0) {
                                NodeLocator n0 = rule.getNodelocator();
                                String randomstring = generateString(10);
                                if (n0.getNodesNum() > 0) {
                                    RNode node = n0.getPrimary(randomstring);
                                    if (sendRows.containsKey(node)) {
                                        ArrayBlockingQueue abq = (ArrayBlockingQueue) sendRows.get(node);
                                        abq.put(onedata);
                                        break;
                                    } else {
                                        throw new Exception("there is no this node in " + topic + " " + rule.getServiceName());
                                    }
                                } else {
                                    //logger.info("there is no accept node for the service " + rule.getServiceName());
                                    storeStrandedData(rule, onedata);
                                    break;
                                }
                            } else if (rule.getType() == 1) {
                                String[] keywords = (rule.getKeywords()).split("\\;");
                                StringBuilder sb = new StringBuilder();
                                for (String s : keywords) {
                                    if (dxxRecord.get(s.toLowerCase()) == null) {
                                        sb.append("");
                                    } else {
                                        sb.append((dxxRecord.get(s.toLowerCase())).toString());
                                    }
                                }

                                NodeLocator n1 = rule.getNodelocator();
                                if (n1.getNodesNum() > 0) {
                                    RNode node = n1.getPrimary(sb.toString());
                                    //System.out.println(topic + " " + sb +" " + node.getName());
                                    if (sendRows.containsKey(node)) {
                                        ArrayBlockingQueue abq = (ArrayBlockingQueue) sendRows.get(node);
                                        abq.put(onedata);
                                        break;
                                    } else {
                                        throw new Exception("there is no this node in " + topic + " " + rule.getServiceName());
                                    }
                                } else {
                                    //logger.info("there is no accept node for the service " + rule.getServiceName());
                                    storeStrandedData(rule, onedata);
                                    break;
                                }
                            } else if (rule.getType() == 2) {
                                String f = rule.getFilters();
                                String[] ss = f.split("\\=");
                                if (isTrue(f, dxxRecord)) {
                                    NodeLocator n2 = rule.getNodelocator();
                                    if (n2.getNodesNum() > 0) {
                                        String randomstring = generateString(10);
                                        RNode node = n2.getPrimary(randomstring);
                                        if (sendRows.containsKey(node)) {
                                            ArrayBlockingQueue abq = (ArrayBlockingQueue) sendRows.get(node);
                                            abq.put(onedata);
                                            break;
                                        } else {
                                            throw new Exception("there is no this node in " + topic + " " + rule.getServiceName());
                                        }
                                    } else {
                                        //logger.info("there is no accept node for the service " + rule.getServiceName());
                                        storeStrandedData(rule, onedata);
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            } else if (rule.getType() == 3) {
                                String[] keywords = (rule.getKeywords()).split("\\;");
                                String f = rule.getFilters();
                                if (isTrue(f, dxxRecord)) {
                                    NodeLocator n3 = rule.getNodelocator();
                                    if (n3.getNodesNum() > 0) {
                                        StringBuilder sb = new StringBuilder();
                                        for (String ss : keywords) {
                                            if (dxxRecord.get(ss.toLowerCase()) == null) {
                                                sb.append("");
                                            } else {
                                                sb.append((dxxRecord.get(ss.toLowerCase())).toString());
                                            }
                                        }
                                        RNode node = n3.getPrimary(sb.toString());
                                        if (sendRows.containsKey(node)) {
                                            ArrayBlockingQueue abq = (ArrayBlockingQueue) sendRows.get(node);
                                            abq.put(onedata);
                                            break;
                                        } else {
                                            throw new Exception("there is no this node in " + topic + " " + rule.getServiceName());
                                        }
                                    } else {
                                        //logger.info("there is no accept node for the service " + rule.getServiceName());
                                        storeStrandedData(rule, onedata);
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            } else if (rule.getType() == 4) {
                                while (true) {
                                    String[] keywords = (rule.getKeywords()).split("\\|");
                                    StringBuilder sb = new StringBuilder();
                                    for (int i = 1; i < keywords.length; i++) {
                                        if (dxxRecord.get(keywords[i]) == null) {
                                            sb.append("");
                                        } else {
                                            sb.append((dxxRecord.get(keywords[i])).toString());
                                        }
                                    }
                                    NodeLocator n4 = rule.getNodelocator();
                                    RNode node = n4.getPrimary(sb.toString());
                                    if (sendRows.containsKey(node)) {
                                        ConcurrentHashMap<String, ArrayBlockingQueue> chm = (ConcurrentHashMap<String, ArrayBlockingQueue>) sendRows.get(node);
                                        String keyinterval = null;
                                        String keytime = (rule.getKeywords().split("\\|"))[0].toLowerCase();
                                        try {
                                            Long time = (Long) dxxRecord.get(keytime);
                                            String unit = (rule.getPartType().split("\\|"))[3];
                                            String interval = (rule.getPartType().split("\\|"))[4];
                                            keyinterval = getKeyInterval(time, unit, interval);
                                        } catch (Exception e) {
                                            storeUnvalidData(rule, onedata);
                                            break;
                                        }

                                        boolean vb = false;
                                        try {
                                            vb = isValid(keyinterval);
                                        } catch (Exception ex) {
                                            logger.error(ex, ex);
                                        }

                                        if (vb) {
                                            synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_MESSAGETRANSFERSTATION)) {
                                                if (!chm.containsKey(keyinterval)) {
                                                    ArrayBlockingQueue abq = new ArrayBlockingQueue(2 * sendPoolSize);
                                                    chm.put(keyinterval, abq);
                                                    logger.debug("the ConcurrentHashMap for " + node.getName() + " for the keyinterval " + keyinterval + " is created");
                                                    abq.put(onedata);
                                                    ThreadGroup sendThreadPool = ((Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL)).get(topic);
                                                    DataSenderThread dst = new DataSenderThread(abq, sendPoolSize, node, rule.getTopic(), rule.getServiceName(), sendThreadPool, rule, keyinterval,rule.getPartType() , rule.getKeywords());
                                                    Thread tdst = new Thread(dst);
                                                    tdst.start();
                                                } else {
                                                    ArrayBlockingQueue abq = (ArrayBlockingQueue) chm.get(keyinterval);
                                                    abq.put(onedata);
                                                }
                                            }
                                        } else {
                                            storeUnvalidData(rule, onedata);
                                        }
                                        break;
                                    } else {
                                        throw new Exception("there is no this node in " + topic + " " + rule.getServiceName());
                                    }
                                }
                                break;
                            } else {
                                throw new Exception("the rule is wrong");
                            }
                        } else {
                            logger.info("waitting for the updateNodeThread in " + topic);
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ex) {
                                logger.info("ex,ex");
                            }
                        }
                    }
                }
            }
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
    private void storeStrandedData(Rule rule, byte[] onedata) {
        ConcurrentHashMap<Rule, ArrayBlockingQueue> strandedDataStore = (ConcurrentHashMap<Rule, ArrayBlockingQueue>) RuntimeEnv.getParam(GlobalVariables.STRANDED_DATA_STORE);
        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_STORE_STRANDEDDATA)) {
            if (strandedDataStore.containsKey(rule)) {
                ArrayBlockingQueue sdQueue = strandedDataStore.get(rule);
                try {
                    sdQueue.put(onedata);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            } else {
                ArrayBlockingQueue sdQueue = new ArrayBlockingQueue(5000);
                strandedDataStore.put(rule, sdQueue);
                StoreStrandedDataThread sdt = new StoreStrandedDataThread(sdQueue, rule);
                Thread tsdt = new Thread(sdt);
                tsdt.start();
                try {
                    sdQueue.put(onedata);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        }
    }

    /**
     *
     * place the useless data to the uselessDataStore
     */
    private void storeUselessData(String topic, byte[] onedata) {
        ConcurrentHashMap<String, ArrayBlockingQueue> uselessDataStore = (ConcurrentHashMap<String, ArrayBlockingQueue>) RuntimeEnv.getParam(GlobalVariables.USELESS_DATA_STORE);
        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_STORE_USELESSDATA)) {
            if (uselessDataStore.containsKey(topic)) {
                ArrayBlockingQueue sdQueue = uselessDataStore.get(topic);
                try {
                    sdQueue.put(onedata);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            } else {
                ArrayBlockingQueue sdQueue = new ArrayBlockingQueue(5000);
                uselessDataStore.put(topic, sdQueue);
                StoreUselessDataThread sudt = new StoreUselessDataThread(sdQueue, topic);
                Thread tsudt = new Thread(sudt);
                tsudt.start();
                try {
                    sdQueue.put(onedata);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        }
    }

    /**
     *
     * place the unvalid data to the unvalidDataStore
     */
    private void storeUnvalidData(Rule rule, byte[] onedata) {
        ConcurrentHashMap<Rule, ArrayBlockingQueue> UnvalidDataStore = (ConcurrentHashMap<Rule, ArrayBlockingQueue>) RuntimeEnv.getParam(GlobalVariables.UNVALID_DATA_STORE);
        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_STORE_UNVALIDDATA)) {
            if (UnvalidDataStore.containsKey(rule)) {
                ArrayBlockingQueue sdQueue = UnvalidDataStore.get(rule);
                try {
                    sdQueue.put(onedata);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            } else {
                ArrayBlockingQueue sdQueue = new ArrayBlockingQueue(5000);
                UnvalidDataStore.put(rule, sdQueue);
                StoreUnvalidDataThread sotdsdt = new StoreUnvalidDataThread(sdQueue, rule);
                Thread tsotdsdt = new Thread(sotdsdt);
                tsotdsdt.start();
                try {
                    sdQueue.put(onedata);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
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
        Date nowtime = new Date();

        nowtime.setMonth(stime.getMonth() - 1);
        if (nowtime.after(stime)) {
            return false;
        }
        nowtime = new Date();
        nowtime.setDate(etime.getDate() + 7);
        if (nowtime.before(etime)) {
            return false;
        }

        return true;
    }

    /**
     *
     * Split and send the message to the transfer station
     */
    private void addCount() {
        synchronized (li) {
            Date dm = new Date();
            ConcurrentHashMap<String, AtomicLong[]> topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong[]>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
            AtomicLong[] al = topicToAcceptCount.get(topic);
            long ac = al[0].addAndGet(msgSet.size());
            if (ac >= al[1].longValue()) {
                logger.info(dm + " " + ac + " accept messages from " + topic + " successfully");
                al[1].addAndGet(limit);
            }
        }
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