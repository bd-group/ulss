/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.NodeLocator;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
public class TransmitStrandedDataThread implements Runnable {

    ConcurrentLinkedQueue<Object[]> strandedDataTransmit = null;
    public final String allChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    Map<RNode, Object> sendRows = null;
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(TransmitStrandedDataThread.class.getName());
    }

    public TransmitStrandedDataThread(ConcurrentLinkedQueue<Object[]> strandedDataTransmit) {
        this.strandedDataTransmit = strandedDataTransmit;
    }

    @Override
    public void run() {
        while (true) {
            Object[] o = strandedDataTransmit.poll();
            if (o == null) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            } else {
                Rule r = (Rule) o[0];
                byte[] sendData = (byte[]) o[1];
                try {
                    dataSplitAndSent(r, sendData);
                } catch (Exception ex) {
                    logger.error(ex, ex);
                    return;
                }
            }
        }
    }

    /**
     *
     * dataSplit and send the stranded message to the transfer station
     */
    private void dataSplitAndSent(Rule rule, byte[] sendData) throws InterruptedException {
        //logger.info("begining the dataSplit and send the stranded message from " + r.getTopic() + " to the transfer station ");
        String msgSchemaContent = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMACONTENT)).get(rule.getTopic());
        String docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        String msgSchemaName = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMANAME)).get(rule.getTopic());
        Protocol protocoldocs = Protocol.parse(docsSchemaContent);
        Schema docsSchema = protocoldocs.getType(GlobalVariables.DOCS);
        DatumReader<GenericRecord> docsreader = new GenericDatumReader<GenericRecord>(docsSchema);
        ByteArrayInputStream docsin = new ByteArrayInputStream(sendData);
        BinaryDecoder docsdecoder = DecoderFactory.get().binaryDecoder(docsin, null);

        GenericRecord docsGr = null;

        try {
            docsGr = docsreader.read(null, docsdecoder);
        } catch (IOException ex) {
            logger.info((new Date()) + " split the data package from the topic " + rule.getTopic() + " in the dataPool wrong " + ex, ex);
            storeUselessData(rule.getTopic(), sendData);
            return;
        }

        GenericArray msgSet = (GenericData.Array<GenericRecord>) docsGr.get(GlobalVariables.DOC_SET);
        Iterator<ByteBuffer> msgitor = msgSet.iterator();
        Protocol protocolMsg = Protocol.parse(msgSchemaContent);
        Schema msgSchema = protocolMsg.getType(msgSchemaName);
        DatumReader<GenericRecord> msgreader = new GenericDatumReader<GenericRecord>(msgSchema);
        sendRows = MessageTransferStation.getMessageTransferStation();
        while (msgitor.hasNext()) {
            byte[] onedata = ((ByteBuffer) msgitor.next()).array();
            ByteArrayInputStream msgbis = new ByteArrayInputStream(onedata);
            BinaryDecoder msgbd = new DecoderFactory().binaryDecoder(msgbis, null);
            GenericRecord msgRecord = null;

            try {
                msgRecord = msgreader.read(null, msgbd);
            } catch (IOException ex) {
                logger.info((new Date()) + " split the one data from the topic " + rule.getTopic() + " in the dataPool wrong " + ex, ex);
                storeUselessData(rule.getTopic(), onedata);
                continue;
            }
            if (rule.getType() == 0) {
                sendToType0(rule, onedata);
            } else if (rule.getType() == 1) {
                sendToType1(rule, onedata, msgRecord);
            } else if (rule.getType() == 2) {
                sendToType2(rule, onedata, msgRecord);
            } else if (rule.getType() == 3) {
                sendToType3(rule, onedata, msgRecord);
            } else {
                logger.info("one rule is wrong because it's type is not 0123");
            }
        }
    }

    /**
     *
     * send message whose type is 0
     */
    private void sendToType0(Rule rule, byte[] data) {
        String randomstring = generateString(10);
        while (true) {
            NodeLocator n0 = rule.getNodelocator();
            if (n0.getNodesNum() > 0) {
                RNode node = n0.getPrimary(randomstring);
                ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) sendRows.get(node);
                clq.offer(data);
                break;
            } else {
//                storeStrandedData(rule, data);
                logger.error("There is no node for the " + rule.getTopic() + " " + rule.getServiceName());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    //do nothing
                }
            }
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

        while (true) {
            NodeLocator n = rule.getNodelocator();
            if (n.getNodesNum() > 0) {
                RNode node = n.getPrimary(sb.toString());
                ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) sendRows.get(node);
                clq.offer(data);
                break;
            } else {
//                storeStrandedData(rule, data);
                logger.error("There is no node for the " + rule.getTopic() + " " + rule.getServiceName());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    //do nothing
                }
            }
        }

    }

    /**
     *
     * send message whose type is 2
     */
    private void sendToType2(Rule rule, byte[] data, GenericRecord record) {
        String f = rule.getFilters();
        if (isTrue(f, record)) {
            while (true) {
                NodeLocator n = rule.getNodelocator();
                if (n.getNodesNum() > 0) {
                    String randomstring = generateString(10);
                    RNode node = n.getPrimary(randomstring);
                    ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) sendRows.get(node);
                    clq.offer(data);
                    break;
                } else {
//                storeStrandedData(rule, data);
                    logger.error("There is no node for the " + rule.getTopic() + " " + rule.getServiceName());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        //do nothing
                    }
                }
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
            while (true) {
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
                    break;
                } else {
//                    storeStrandedData(rule, data);
                    logger.error("There is no node for the " + rule.getTopic() + " " + rule.getServiceName());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        //do nothing
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
                String key = (dxxRecord.get(ss[0])).toString();
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
                tsdt.start();
                logger.info("start a StoreStrandedDataThread for " + rule.getTopic());
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
                tsudt.start();
                logger.info("start a StoreUselessDataThread for " + topic);
            }
        }
    }
}
