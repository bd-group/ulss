/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class DataSenderThread implements Runnable {

    ConcurrentLinkedQueue sendQueue = null;
    Integer sendPoolSize = 0;
    String topic = null;
    String msgSchemaContent = null;
    String docsSchemaContent = null;
    String msgSchemaName = null;
    RNode node = null;
    String serviceName = null;
    String keyinterval = null;
    Map<String, ThreadGroup> topicToSendThreadPool = null;
    ThreadGroup sendThreadPool = null;
    ConcurrentHashMap<String, Object[]> valueToFile = null;
    int datasenderLimitTime = 0;
    int sendThreadPoolSize = 0;
    Map<String, AtomicLong> topicToPackage = null;
    Rule rule = null;
    int count = 0;
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DataSenderThread.class.getName());
    }

    public DataSenderThread(ConcurrentLinkedQueue abq, RNode node, Rule rule, String keyinterval) {
        this.sendQueue = abq;
        this.node = node;
        this.rule = rule;
        this.keyinterval = keyinterval;
        this.topic = rule.getTopic();
        this.serviceName = rule.getServiceName();
    }

    @Override
    public void run() {
        sendPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_POOL_SIZE);
        msgSchemaContent = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMACONTENT)).get(topic);
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        msgSchemaName = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMANAME)).get(topic);
        sendThreadPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_THREAD_POOL_SIZE);
        datasenderLimitTime = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATASENDER_LIMITTIME);
        valueToFile = (ConcurrentHashMap<String, Object[]>) RuntimeEnv.getParam(GlobalVariables.VALUE_TO_FILE);
        topicToSendThreadPool = (Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL);
        sendThreadPool = topicToSendThreadPool.get(rule.getTopic());
        topicToPackage = (Map<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_PACKAGE);
//        AtomicLong packagecount = topicToPackage.get(topic);
        int timenum = 0;

        while (true) {
            String sendIP = "";
            Long f_id = 0L;
            String road = "";
            byte[] sendData;
            if (!sendQueue.isEmpty()) {
//                packagecount.incrementAndGet();
                timenum = 0;
                sendData = pack(sendQueue);
                if (sendData != null) {
                    if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3) {
                        if (node.getName() == null) {
                            sendIP = "";
                        } else {
                            sendIP = node.getName();
                        }
                    } else if (rule.getType() == 4) {
                        Object[] obj = valueToFile.get(topic + keyinterval + node.getName());
                        if (obj != null) {
                            if (obj[0] == null) {
                                sendIP = "";
                                logger.error("the sendIP is null");
                            } else {
                                sendIP = (String) obj[0];
                            }

                            if (obj[1] == null) {
                                f_id = 0L;
                            } else {
                                f_id = (Long) obj[1];
                            }

                            if (obj[2] == null) {
                                road = "";
                                logger.error("the road is null");
                            } else {
                                road = (String) obj[2];
                            }
                        } else {
                            logger.info("the sendip and road and f_id for " + topic + " " + serviceName + " " + keyinterval + " " + node.getName() + " is null");
                        }
                    }

                    SendToServiceThread sendT = new SendToServiceThread(sendData, node, rule, sendIP, keyinterval, f_id, road, count);
                    while (sendThreadPool.activeCount() >= sendThreadPoolSize) {
                        logger.debug("the sendThreadPool for " + topic + " " + keyinterval + " " + node.getName() + " is full...");
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ex) {
                            logger.error(ex, ex);
                        }
                    }
                    logger.debug("the sendThreadPool for " + topic + " " + serviceName + " " + keyinterval + " " + node.getName() + " is not full and start a sendtoservicethread");
                    Thread t = new Thread(sendThreadPool, sendT);
                    t.setName("SendToServiceThread-" + topic + "-" + serviceName + "-" + node.getName() + "-" + keyinterval);
                    t.start();
                }
//                packagecount.decrementAndGet();
            } else {
                logger.debug("the sendQueue for " + topic + " " + keyinterval + " " + node.getName() + " is empty");
                if (rule.getType() == 4) {
                    timenum++;
                    if (timenum >= datasenderLimitTime / 2) {
                        Map<RNode, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();
                        ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) messageTransferStation.get(node);
                        if (chm == null) {
                            break;
                        }

                        String is = keyinterval;
                        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_MESSAGETRANSFERSTATION)) {
                            chm.remove(is);
                        }
                        break;
                    }
                }

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        }
        logger.info("the ConcurrentLinkedQueue for for " + topic + " " + serviceName + " " + keyinterval + " " + node.getName() + " is removed");
    }

    /**
     *
     * package the data to a message
     */
    byte[] pack(ConcurrentLinkedQueue clq) {
        Protocol protocoldocs = Protocol.parse(docsSchemaContent);
        Schema docs = protocoldocs.getType(GlobalVariables.DOCS);
        GenericRecord docsRecord = new GenericData.Record(docs);
        docsRecord.put(GlobalVariables.DOC_SCHEMA_NAME, msgSchemaName);
        GenericArray docSet = new GenericData.Array<GenericRecord>((sendPoolSize), docs.getField(GlobalVariables.DOC_SET).schema());
        count = 0;
        int count2 = 0;
        while (count2 < sendPoolSize) {
            count2++;
            byte[] data = (byte[]) clq.poll();
            if (data != null) {
                docSet.add(ByteBuffer.wrap(data));
                count++;
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {
                    //do nothing
                }
            }
        }

        if (count <= 0) {
            return null;
        }

        docsRecord.put(GlobalVariables.SIGN, "evan");
        docsRecord.put(GlobalVariables.DOC_SET, docSet);

        DatumWriter<GenericRecord> docsWriter = new GenericDatumWriter<GenericRecord>(docs);
        ByteArrayOutputStream docsbaos = new ByteArrayOutputStream();
        BinaryEncoder docsbe = new EncoderFactory().binaryEncoder(docsbaos, null);
        try {
            docsWriter.write(docsRecord, docsbe);
            docsbe.flush();
        } catch (IOException ex) {
            logger.error(ex);
        }

        return docsbaos.toByteArray();
    }
}