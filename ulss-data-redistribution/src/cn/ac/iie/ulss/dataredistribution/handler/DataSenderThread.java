/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.DataProducer;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayOutputStream;
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
    Map<String, Integer> ruleToSendPoolSize = null;
    int sendPoolSize = 2000;
    String topic = null;
    String msgSchemaContent = null;
    String docsSchemaContent = null;
    String msgSchemaName = null;
    RNode node = null;
    String serviceName = null;
    String keyinterval = null;
    ConcurrentHashMap<String, Object[]> valueToFile = null;
    int datasenderLimitTime = 0;
    int sendThreadPoolSize = 0;
    Map<String, AtomicLong> topicToPackage = null;
    Map<String, ConcurrentLinkedQueue[]> topicToDataPool = null;
    ConcurrentLinkedQueue[] datapool = null;
    Rule rule = null;
    int count = 0;
    long stime = 0L;
    long packagetimelimit = 0L;
    Map<String, Map<String, AtomicLong>> ruleToThreadPoolSize = null;
    Protocol protocoldocs = null;
    Schema docs = null;
    DataProducer producer = (DataProducer) RuntimeEnv.getParam(GlobalVariables.PRODUCER);
    org.apache.log4j.Logger logger = null;

    {
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
        ruleToSendPoolSize = (Map<String, Integer>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_SENDPOOLSIZE);
        if (ruleToSendPoolSize.get(topic + serviceName) != null) {
            sendPoolSize = ruleToSendPoolSize.get(topic + serviceName);
        }
        msgSchemaContent = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMACONTENT)).get(topic);
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        msgSchemaName = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMANAME)).get(topic);
        sendThreadPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_THREAD_POOL_SIZE);
        datasenderLimitTime = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATASENDER_LIMITTIME);
        valueToFile = (ConcurrentHashMap<String, Object[]>) RuntimeEnv.getParam(GlobalVariables.VALUE_TO_FILE);
        topicToPackage = (Map<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_PACKAGE);
        packagetimelimit = (Integer) RuntimeEnv.getParam(RuntimeEnv.PACKAGE_TIMELIMIT) * 1000;
        topicToDataPool = (Map<String, ConcurrentLinkedQueue[]>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_DATAPOOL);
        datapool = topicToDataPool.get(topic);
        AtomicLong packagecount = topicToPackage.get(topic);
        ruleToThreadPoolSize = (Map<String, Map<String, AtomicLong>>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_THREADPOOLSIZE);
        AtomicLong ruleToSize = ruleToThreadPoolSize.get(rule.getTopic()).get(rule.getServiceName());
        protocoldocs = Protocol.parse(docsSchemaContent);
        docs = protocoldocs.getType(GlobalVariables.DOCS);

        int timenum = 0;
        byte[] sendData = null;
        while (true) {
            String sendIP = "";
            Long f_id = 0L;
            String road = "";
            if (!sendQueue.isEmpty()) {
                timenum = 0;
                if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3) {
                    if (node.getName() == null) {
                        sendIP = "";
                    } else {
                        sendIP = node.getName();
                    }
                    packagecount.incrementAndGet();
                    sendData = pack(sendQueue, 0, "", "", 0);
                    packagecount.decrementAndGet();
                    if (sendData == null) {
                        continue;
                    }
                    SendToServiceThread sendT = new SendToServiceThread(sendData, node, rule, sendIP, count);
                    while (ruleToSize.longValue() >= sendThreadPoolSize) {
                        logger.debug("the sendThreadPool for " + topic + " " + keyinterval + " " + node.getName() + " is full...");
                        try {
                            Thread.sleep(200);
                        } catch (Exception ex) {
                        }
                    }
                    logger.debug("the sendThreadPool for " + topic + " " + serviceName + " " + keyinterval + " " + node.getName() + " is not full and start a sendtoservicethread");
                    Thread t = new Thread(sendT);
                    t.setName("SendToServiceThread-" + topic + "-" + serviceName + "-" + node.getName() + "-" + keyinterval);
                    t.start();
                    ruleToSize.incrementAndGet();
                } else if (rule.getType() == 4) {
                    while (true) {
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

                            if (sendIP.equals("") || f_id == 0L || road.equals("")) {
                                logger.info("the sendip or road or f_id for " + topic + " " + serviceName + " " + keyinterval + " " + node.getName() + " is null");
                                GetFileFromMetaStore gffm = new GetFileFromMetaStore(keyinterval, node, rule);
                                gffm.getFileForInverval();
                            } else {
                                break;
                            }
                        } else {
                            logger.info("the sendip or road or f_id for " + topic + " " + serviceName + " " + keyinterval + " " + node.getName() + " is null");
                            GetFileFromMetaStore gffm = new GetFileFromMetaStore(keyinterval, node, rule);
                            gffm.getFileForInverval();
                        }
                    }

                    packagecount.incrementAndGet();
                    sendData = pack(sendQueue, f_id, road, sendIP, 1);
                    packagecount.decrementAndGet();
                    if (sendData == null) {
                        continue;
                    }
                    producer.send(topic, serviceName, sendIP, count, keyinterval, sendData);
                    ruleToSize.incrementAndGet();
                }
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
                        synchronized (chm) {
                            chm.remove(is);
                            valueToFile.remove(topic + keyinterval + node.getName());
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
        logger.info("the ConcurrentLinkedQueue for " + topic + " " + serviceName + " " + keyinterval + " " + node.getName() + " is removed");
    }

    /**
     *
     * package the data to a message
     */
    public byte[] pack(ConcurrentLinkedQueue clq, long fid, String road, String sendIP, int type) {
        GenericRecord docsRecord = new GenericData.Record(docs);
        docsRecord.put(GlobalVariables.DOC_SCHEMA_NAME, msgSchemaName);
        GenericArray docSet = new GenericData.Array<GenericRecord>((sendPoolSize), docs.getField(GlobalVariables.DOC_SET).schema());
        count = 0;
        stime = System.currentTimeMillis();
        byte[] data = null;
        while (count < sendPoolSize) {
            data = (byte[]) clq.poll();
            if (data != null) {
                docSet.add(ByteBuffer.wrap(data));
                count++;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    //
                }
                if (clq.isEmpty() && datapoolisEmpty()) {
                    break;
                } else if ((System.currentTimeMillis() - stime) >= packagetimelimit) {
                    break;
                }
            }
        }

        if (count <= 0) {
            return null;
        }

        if (type == 0) {
            docsRecord.put(GlobalVariables.SIGN, "evan");
        } else {
            docsRecord.put(GlobalVariables.SIGN, fid + "|" + road + "|" + sendIP + "|" + topic);
        }
        docsRecord.put(GlobalVariables.DOC_SET, docSet);

        DatumWriter<GenericRecord> docsWriter = new GenericDatumWriter<GenericRecord>(docs);
        ByteArrayOutputStream docsbaos = new ByteArrayOutputStream();
        BinaryEncoder docsbe = new EncoderFactory().binaryEncoder(docsbaos, null);
        try {
            docsWriter.write(docsRecord, docsbe);
            docsbe.flush();
        } catch (Exception ex) {
            logger.error(ex);
        }

        return docsbaos.toByteArray();
    }

    private boolean datapoolisEmpty() {
        for (int i = 0; i < datapool.length; i++) {
            if (!datapool[i].isEmpty()) {
                return false;
            }
        }
        return true;
    }
}