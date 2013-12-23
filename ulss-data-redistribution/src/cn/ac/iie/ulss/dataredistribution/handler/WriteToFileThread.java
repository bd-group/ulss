/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import com.taobao.metamorphosis.Message;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class WriteToFileThread implements Runnable {

    public String topic = null;
    public LinkedBlockingQueue bufferPool = null;
    public ConcurrentLinkedQueue dataPool = null;
    public File fsmit = null;
    String docsSchemaContent = null;
    Integer dataPoolSize = 0;
    Integer writeToFileThread = 0;
    Integer activeThreadCount = 0;
    Map<String, ThreadGroup> topicToSendThreadPool = null;
    ThreadGroup sendThreadPool = null;
    Protocol protocol = null;
    Schema docsschema = null;
    DatumReader<GenericRecord> docsreader = null;
    DatumWriter<GenericRecord> write = null;
    DataFileWriter<GenericRecord> dataFileWriter = null;
    DatumReader<GenericRecord> dxreader = null;
    ByteArrayInputStream docsin = null;
    BinaryDecoder docsdecoder = null;
    GenericRecord docsGr = null;
    GenericArray msgSet = null;
    Iterator<ByteBuffer> msgitor = null;
    String dataDir = null;
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = null;
    AtomicLong acceptCount = null;
    Map<String, ArrayList<RNode>> topicToNodes = (Map<String, ArrayList<RNode>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_NODES);
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(WriteToFileThread.class.getName());
    }

    public WriteToFileThread(LinkedBlockingQueue bufferPool, File fsmit, ConcurrentLinkedQueue dataPool, String topic) {
        this.bufferPool = bufferPool;
        this.fsmit = fsmit;
        this.dataPool = dataPool;
        this.topic = topic;
    }

    @Override
    public void run() {
        init();
        File out = new File(dataDir + "backup");
        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_DIR)) {
            if (!out.exists() && !out.isDirectory()) {
                out.mkdirs();
                logger.info("create the directory " + dataDir + "backup");
            }
        }

        while (true) {
            logger.info(topic + " dataPool's size is " + dataPool.size() + " and the activeSendThreadCount's size is " + sendThreadPool.activeCount()
                    + " and the nodeNums of the MessageTransferStation is " + MessageTransferStation.getMessageTransferStation().size() + " and the num of queue in the "
                    + "MessageTransferStation is " + numOfQueues());
            if (dataPool.isEmpty() && sendThreadPool.activeCount() <= activeThreadCount && isEmpty()) {
                String f = fsmit.getName();
                Date d = new Date();
                String fb = format.format(d) + "_" + f + ".old";

                if (fsmit.exists()) {
                    logger.info("rename the file " + fsmit.getName());
                    fsmit.renameTo(new File(dataDir + "backup/" + fb));
                }
                byte[] msg = null;
                if (!bufferPool.isEmpty()) {
                    fsmit = new File(dataDir + "backup/" + f);
                    logger.info("create the file " + fsmit.getName());
                    try {
                        dataFileWriter.create(docsschema, fsmit);
                    } catch (Exception ex) {
                        logger.error("can not create the file " + fsmit.getName() + ex, ex);
                        return;
                    }

                    long count = 0;
                    int count2 = 0;
                    while (count < (dataPoolSize / writeToFileThread)) {
                        Message message = (Message) bufferPool.poll();
                        if (message != null) {
                            count2 = 0;
                            msg = message.getData();
                            ByteArrayInputStream dxin = new ByteArrayInputStream(msg);
                            BinaryDecoder dxdecoder = DecoderFactory.get().binaryDecoder(dxin, null);
                            GenericRecord dxr = null;;
                            try {
                                dxr = dxreader.read(null, dxdecoder);
                            } catch (IOException ex) {
                                logger.info(" the schema of the data is wrong, can not be writen into the file " + fsmit.getName());
                                try {
                                    dataFileWriter.flush();
                                    dataFileWriter.close();
                                } catch (IOException ex1) {
                                    logger.error(ex1, ex1);
                                }

                                return;
                            }

                            if (dxr != null) {
                                while (true) {
                                    try {
                                        dataFileWriter.append(dxr);
                                        break;
                                    } catch (IOException ex) {
                                        logger.error("IOException when append to the file " + fsmit.getName() + ex, ex);
                                    }
                                }
                            }

                            count += addcount(msg);
                        } else {
                            count2++;
                            logger.info("bufferPool'size is 0");
                            if (count2 > 30) {
                                break;
                            }
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException ex) {
                                logger.error(ex, ex);
                            }
                        }
                    }
                    try {
                        dataFileWriter.flush();
                        dataFileWriter.close();
                    } catch (IOException ex1) {
                        logger.error(ex1, ex1);
                    }
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        logger.error(ex, ex);
                    }
                }
            } else {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        }
    }

    private void init() {
        dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);
        dataPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATA_POOL_SIZE);
        writeToFileThread = (Integer) RuntimeEnv.getParam(RuntimeEnv.WRITE_TO_FILE_THREAD);
        activeThreadCount = (Integer) RuntimeEnv.getParam(RuntimeEnv.ACTIVE_THREAD_COUNT);
        topicToSendThreadPool = (Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL);
        sendThreadPool = topicToSendThreadPool.get(topic);
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        protocol = Protocol.parse(docsSchemaContent);
        docsschema = protocol.getType(GlobalVariables.DOCS);
        docsreader = new GenericDatumReader<GenericRecord>(docsschema);
        write = new GenericDatumWriter<GenericRecord>(docsschema);
        dataFileWriter = new DataFileWriter<GenericRecord>(write);
        dxreader = new GenericDatumReader<GenericRecord>(docsschema);
        topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
        acceptCount = topicToAcceptCount.get(topic);
    }

    private long addcount(byte[] msg) {
        docsin = new ByteArrayInputStream(msg);
        docsdecoder = DecoderFactory.get().binaryDecoder(docsin, null);
        try {
            docsGr = docsreader.read(null, docsdecoder);
        } catch (IOException ex) {
            logger.info(" split the data package from the topic " + topic + " in the dataPool wrong " + ex, ex);
            storeUselessData(topic, msg);
        }
        msgSet = (GenericData.Array<GenericRecord>) docsGr.get(GlobalVariables.DOC_SET);
        msgitor = msgSet.iterator();
        while (msgitor.hasNext()) {
            byte[] onedata = ((ByteBuffer) msgitor.next()).array();
            dataPool.offer(onedata);
        }

        acceptCount.addAndGet(msgSet.size());
        return msgSet.size();
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

    private synchronized boolean isEmpty() {
        Map<RNode, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();
        ArrayList<RNode> alr = topicToNodes.get(topic);
        for (RNode n : alr) {
            if (messageTransferStation.containsKey(n)) {
                if (n.getType() == 4) {
                    ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) messageTransferStation.get(n);
                    if (chm != null) {
                        for (ConcurrentLinkedQueue clq : chm.values()) {
                            if (!clq.isEmpty()) {
                                logger.info("the messageTransferStation for " + topic + " is not empty!");
                                return false;
                            }
                        }
                    } else {
                        logger.info("the chm for " + n + " is null");
                    }
                } else {
                    ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) messageTransferStation.get(n);
                    if (clq != null) {
                        if (!clq.isEmpty()) {
                            logger.info("the messageTransferStation for " + topic + " is not empty!");
                            return false;
                        }
                    } else {
                        logger.info("the clq for " + n + " is null");
                    }
                }
            } else {
                alr.remove(n);
            }
        }
        logger.info("the messageTransferStation for " + topic + " is empty!");
        return true;
    }

    private int numOfQueues() {
        int count = 0;
        Map<RNode, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();
        for (RNode n : messageTransferStation.keySet()) {
            if (n.getType() == 4) {
                ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) messageTransferStation.get(n);
                count += chm.size();
            } else {
                count++;
            }
        }
        return count;
    }
}
