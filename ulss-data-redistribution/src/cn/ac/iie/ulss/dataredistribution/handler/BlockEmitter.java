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
import java.util.concurrent.atomic.AtomicBoolean;
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
 * @author evan
 */
public class BlockEmitter {

    ConcurrentLinkedQueue dataPool = null;
    String topic = null;
    String docsSchemaContent = null;
    Integer dataPoolSize = 0;
    String dataDir = null;
//    Integer writeToFileThread = 0;
    Integer activeThreadCount = 0;
    Map<String, ThreadGroup> topicToSendThreadPool = null;
    ThreadGroup sendThreadPool = null;
    Protocol protocol = null;
    Schema docsschema = null;
    DatumReader<GenericRecord> docsreader = null;
    DatumWriter<GenericRecord> write = null;
    DataFileWriter<GenericRecord> dataFileWriter = null;
    DatumReader<GenericRecord> dxreader = null;
    ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = null;
    AtomicLong acceptCount = null;
    AtomicLong count = new AtomicLong(0);
    Map<String, ArrayList<RNode>> topicToNodes = null;
    File fsmit = null;
    ByteArrayInputStream docsin = null;
    BinaryDecoder docsdecoder = null;
    GenericRecord docsGr = null;
    GenericArray msgSet = null;
    Iterator<ByteBuffer> msgitor = null;
    AtomicBoolean changeFile = new AtomicBoolean(false);
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    AtomicLong time = new AtomicLong(0);
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(BlockEmitter.class.getName());
    }

    public BlockEmitter(ConcurrentLinkedQueue dataPool, String topic) {
        this.dataPool = dataPool;
        this.topic = topic;
    }

    public void init() {
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        dataPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATA_POOL_SIZE);
        dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);
//        writeToFileThread = (Integer) RuntimeEnv.getParam(RuntimeEnv.WRITE_TO_FILE_THREAD);
        activeThreadCount = (Integer) RuntimeEnv.getParam(RuntimeEnv.ACTIVE_THREAD_COUNT);
        topicToSendThreadPool = (Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL);
        sendThreadPool = topicToSendThreadPool.get(topic);
        protocol = Protocol.parse(docsSchemaContent);
        docsschema = protocol.getType(GlobalVariables.DOCS);
        docsreader = new GenericDatumReader<GenericRecord>(docsschema);
        write = new GenericDatumWriter<GenericRecord>(docsschema);
        dataFileWriter = new DataFileWriter<GenericRecord>(write);
        dxreader = new GenericDatumReader<GenericRecord>(docsschema);
        topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
        acceptCount = topicToAcceptCount.get(topic);
        topicToNodes = (Map<String, ArrayList<RNode>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_NODES);

        File out = new File(dataDir + "backup");
        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_DIR)) {
            if (!out.exists() && !out.isDirectory()) {
                out.mkdirs();
                logger.info("create the directory " + dataDir + "backup");
            }
        }
        fsmit = new File(dataDir + "backup/" + topic + ".bk");
    }

    public synchronized void emit(Message message, String commander, long sendtime) {
        if ("data".equals(commander)) {
            time.set(sendtime);
            if (changeFile.compareAndSet(false, true)) {
                checkEnvironment();
            }
            byte[] msg = null;
            if (message != null) {
                msg = message.getData();
                ByteArrayInputStream dxin = new ByteArrayInputStream(msg);
                BinaryDecoder dxdecoder = DecoderFactory.get().binaryDecoder(dxin, null);
                GenericRecord dxr = null;;
                try {
                    dxr = dxreader.read(null, dxdecoder);
                } catch (Exception ex) {
                    logger.info(" the schema of the data is wrong, can not be writen into the file " + fsmit.getName());
                    storeUselessData(topic, msg);
                    return;
                }

                if (dxr != null) {
                    while (true) {
                        try {
                            dataFileWriter.append(dxr);
                            break;
                        } catch (Exception ex) {
                            logger.error("Exception when append to the file " + fsmit.getName() + ex, ex);
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException ex1) {
                                //donothing
                            }
                        }
                    }
                }
                count.addAndGet(addcount(msg));
                if (count.get() >= dataPoolSize) {
                    changeFile.compareAndSet(true, false);
                    try {
                        dataFileWriter.flush();
                        dataFileWriter.close();
                    } catch (Exception ex1) {
                        logger.error(ex1, ex1);
                    }
                }
            } else {
                logger.info("one message is null for " + topic);
            }
        } else if ("timeout".equals(commander)) {
            try {
                dataFileWriter.flush();
                dataFileWriter.close();
            } catch (Exception ex1) {
                //logger.error(ex1, ex1);
            }
            changeFile.compareAndSet(true, false);
            count.set(0);
            String f = fsmit.getName();
            if (fsmit.exists()) {
                Date d = new Date();
                String fb = format.format(d) + "_" + f + ".old";
                logger.info("rename the file " + fsmit.getName() + " to " + fb);
                fsmit.renameTo(new File(dataDir + "backup/" + fb));
            }
            fsmit = new File(dataDir + "backup/" + f);
        }
    }

    private void checkEnvironment() {
        while (!dataPool.isEmpty() || sendThreadPool.activeCount() > activeThreadCount || !isEmpty()) {
            logger.info(topic + " dataPool's size is " + dataPool.size() + " and the activeSendThreadCount's size is " + sendThreadPool.activeCount()
                    + " and the nodeNums of the MessageTransferStation is " + MessageTransferStation.getMessageTransferStation().size() + " and the num of queue in the "
                    + "MessageTransferStation is " + numOfQueues());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                //donothing
            }
        }

        logger.info(topic + " dataPool's size is " + dataPool.size() + " and the activeSendThreadCount's size is " + sendThreadPool.activeCount()
                + " and the nodeNums of the MessageTransferStation is " + MessageTransferStation.getMessageTransferStation().size() + " and the num of queue in the "
                + "MessageTransferStation is " + numOfQueues());

        
        count.set(0);
        String f = fsmit.getName();
        if (fsmit.exists()) {
            Date d = new Date();
            String fb = format.format(d) + "_" + f + ".old";
            logger.info("rename the file " + fsmit.getName() + " to " + fb);
            fsmit.renameTo(new File(dataDir + "backup/" + fb));
        }

        fsmit = new File(dataDir + "backup/" + f);
        logger.info("create the file " + fsmit.getName());
        try {
            dataFileWriter.create(docsschema, fsmit);
        } catch (Exception ex) {
            logger.error("can not create the file " + fsmit.getName() + ex, ex);
            return;
        }
    }

    private boolean isEmpty() {
        Map<RNode, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();
        ArrayList<RNode> alr = topicToNodes.get(topic);
        synchronized (alr) {
            Iterator it = alr.iterator();
            while (it.hasNext()) {
                RNode n = (RNode) it.next();
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
                    it.remove();
                }
            }

            logger.info("the messageTransferStation for " + topic + " is empty!");

            Map<String, ArrayList<Rule>> topicToRules = (Map<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES);
            ArrayList<Rule> rules = topicToRules.get(topic);
            for (Rule n : rules) {
                if (n.getNodeUrls().isEmpty()) {
                    return false;
                }
            }

            logger.info("every topic's rule has node");
        }

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

    public long getTime() {
        return time.get();
    }
    
    public long getCount(){
        return count.get();
    }
}
