/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayInputStream;
import java.io.File;
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

    long getmsg = 0;
    long size = 0;
    int handlertype = 0;
    int filenumber = 0;
    ConcurrentLinkedQueue[] dataPool = null;
    String topic = null;
    String docsSchemaContent = null;
    Map<String, Integer> topicToAcceptPoolSize = null;
    int dataPoolSize = 20000;
    String dataDir = null;
    Integer activeThreadCount = 0;
    Long activePackageLimit = 0L;
    Protocol protocol = null;
    Schema docsschema = null;
    DatumReader<GenericRecord> docsreader = null;
    DatumWriter<GenericRecord> write = null;
    DataFileWriter<GenericRecord> dataFileWriter = null;
    DatumReader<GenericRecord> dxreader = null;
    ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = null;
    AtomicLong acceptCount = null;
    AtomicLong count = null;
    AtomicLong version = null;
    AtomicLong time = null;
    long localversion = 0;
    Map<String, ArrayList<RNode>> topicToNodes = null;
    File fsmit = null;
    ByteArrayInputStream docsin = null;
    BinaryDecoder docsdecoder = null;
    GenericRecord docsGr = null;
    GenericArray msgSet = null;
    Iterator<ByteBuffer> msgitor = null;
    AtomicBoolean changeFile = new AtomicBoolean(false);
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd:HH");
    Map<String, AtomicLong> topicToPackage = null;
    AtomicLong packagecount = new AtomicLong(0);
    Map<String, Map<String, AtomicLong>> ruleToThreadPoolSize = null;
    Map<String, AtomicLong> serviceToThreadSize = null;
    ConcurrentLinkedQueue<Object[]> strandedDataTransmit = null;
    byte[] onedata = null;
    long stime = System.currentTimeMillis();
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(BlockEmitter.class.getName());
    }

    public BlockEmitter(ConcurrentLinkedQueue[] dataPool, String topic, int filenumber, int handlertype, AtomicLong count, AtomicLong version, AtomicLong time) {
        this.dataPool = dataPool;
        this.topic = topic;
        this.filenumber = filenumber;
        this.handlertype = handlertype;
        this.count = count;
        this.version = version;
        this.localversion = version.longValue();
        this.time = time;
    }

    public void init() {
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        topicToAcceptPoolSize = (Map<String, Integer>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTPOOLSIZE);
        dataPoolSize = topicToAcceptPoolSize.get(topic);
        dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);
        protocol = Protocol.parse(docsSchemaContent);
        docsschema = protocol.getType(GlobalVariables.DOCS);
        docsreader = new GenericDatumReader<GenericRecord>(docsschema);
        write = new GenericDatumWriter<GenericRecord>(docsschema);
        dataFileWriter = new DataFileWriter<GenericRecord>(write);
        dxreader = new GenericDatumReader<GenericRecord>(docsschema);
        topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
        acceptCount = topicToAcceptCount.get(topic);
        topicToNodes = (Map<String, ArrayList<RNode>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_NODES);
        topicToPackage = (Map<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_PACKAGE);
        packagecount = topicToPackage.get(topic);
        ruleToThreadPoolSize = (Map<String, Map<String, AtomicLong>>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_THREADPOOLSIZE);
        serviceToThreadSize = ruleToThreadPoolSize.get(topic);
        activeThreadCount = (Integer) RuntimeEnv.getParam(RuntimeEnv.ACTIVE_THREAD_COUNT);
        strandedDataTransmit = (ConcurrentLinkedQueue<Object[]>) RuntimeEnv.getParam(GlobalVariables.STRANDED_DATA_TRANSMIT);

        if (handlertype == 1) {
            File out = new File(dataDir + "backup");
            synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_DIR)) {
                if (!out.exists() && !out.isDirectory()) {
                    out.mkdirs();
                    logger.info("create the directory " + dataDir + "backup");
                }
            }
            fsmit = new File(dataDir + "backup/" + topic + filenumber + ".bk");
        }
    }

    public synchronized void emit(byte[] message, String commander, long sendtime) {
        if (this.handlertype == 0) {
            emit0(message, commander, sendtime);
        } else if (this.handlertype == 1) {
            emit1(message, commander, sendtime);
        } else {
            emit2(message, sendtime);
        }
    }

    private synchronized void emit0(byte[] message, String commander, long sendtime) {
        if ("data".equals(commander)) {
            time.set(sendtime);
            if (changeFile.compareAndSet(false, true)) {
                checkEnvironment0();
                stime = System.currentTimeMillis();
            }
            if (message != null) {
                getmsg = addCount(message);
                size = count.addAndGet(getmsg);
                if (size >= dataPoolSize || localversion < version.longValue()) {
                    Date date = new Date();
                    String time = dateFormat.format(date);
                    logger.info(time + " " + topic + " accept " + size + " messages from metaq use " + (System.currentTimeMillis() - stime) + " ms " + filenumber);
                    changeFile.compareAndSet(true, false);
                }
            } else {
                logger.info("one message is null for " + topic);
            }
        } else if ("timeout".equals(commander)) {
            changeFile.compareAndSet(true, false);
        }
    }

    private synchronized void emit1(byte[] message, String commander, long sendtime) {
        if ("data".equals(commander)) {
            time.set(sendtime);
            if (changeFile.compareAndSet(false, true)) {
                checkEnvironment1();
                stime = System.currentTimeMillis();
            }
            if (message != null) {
                ByteArrayInputStream dxin = new ByteArrayInputStream(message);
                BinaryDecoder dxdecoder = DecoderFactory.get().binaryDecoder(dxin, null);
                GenericRecord dxr = null;
                try {
                    dxr = dxreader.read(null, dxdecoder);
                } catch (Exception ex) {
                    logger.info(" the schema of the data is wrong, can not be writen into the file " + fsmit.getName() + ex, ex);
                    storeUselessData(topic, message);
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
                                Thread.sleep(5000);
                            } catch (Exception ex1) {
                                //donothing
                            }
                        }
                    }
                }
                getmsg = addCount(message);
                size = count.addAndGet(getmsg);
                if (size >= dataPoolSize || localversion < version.longValue()) {
                    Date date = new Date();
                    String time = dateFormat.format(date);
                    logger.info(time + " " + topic + " accept " + size + " messages from metaq use " + (System.currentTimeMillis() - stime) + " ms " + filenumber);
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

            String f = fsmit.getName();
            if (fsmit.exists()) {
                Date d = new Date();
                String fb = format.format(d) + "_" + f + ".old";
                logger.info("rename the file " + fsmit.getName() + " to " + fb);
                fsmit.renameTo(new File(dataDir + "backup/" + fb));
            }
            fsmit = new File(dataDir + "backup/" + f);
            changeFile.compareAndSet(true, false);
        }
    }

    private synchronized void emit2(byte[] message, long sendtime) {
        if (changeFile.compareAndSet(false, true)) {
            checkEnvironment2();
        }
        if (message != null) {
            addCount(message);
            if (dataPoolCount() >= dataPoolSize || localversion < version.longValue()) {
                changeFile.compareAndSet(true, false);
            }
        } else {
            logger.info("one message is null for " + topic);
        }
    }

    private synchronized void checkEnvironment0() {
        synchronized (version) {
            if (localversion < version.longValue()) {
                localversion = version.longValue();
            } else {
                int i = 0;
                while (!dataPoolIsEmpty() || packagecount.get() > activePackageLimit || !sendThreadIsFull() || hasDataToWrite() || !strandedDataTransmit.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (Exception ex) {
                        //donothing
                    }
                    i++;
                    if (i % 40 == 0) {
                        logger.info("check the environment for " + topic + " " + filenumber);
                    }
                }

                Date date = new Date();
                String time = dateFormat.format(date);
                logger.info(time + " handler " + count + " messages from metaq use " + (System.currentTimeMillis() - stime) + " ms " + filenumber);
                count.set(0);
                version.incrementAndGet();
                localversion = version.longValue();
            }
        }
    }

    private synchronized void checkEnvironment1() {
        synchronized (version) {
            if (localversion < version.longValue()) {
                localversion = version.longValue();
            } else {
                int i = 0;
                while (!dataPoolIsEmpty() || packagecount.get() > activePackageLimit || !sendThreadIsFull() || hasDataToWrite() || !strandedDataTransmit.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (Exception ex) {
                        //donothing
                    }
                    i++;
                    if (i % 40 == 0) {
                        logger.info("check the environment for " + topic + " " + filenumber);
                    }
                }

                Date date = new Date();
                String time = dateFormat.format(date);
                logger.info(time + " handler " + count + " messages from metaq use " + (System.currentTimeMillis() - stime) + " ms " + filenumber);
                count.set(0);
                version.incrementAndGet();
                localversion = version.longValue();
            }
        }

        String f = fsmit.getName();
        if (fsmit.exists()) {
            Date d = new Date();
            String fb = format.format(d) + "_" + f + ".old";
            logger.info("rename the file " + fsmit.getName() + " to " + fb);
            fsmit.renameTo(new File(dataDir + "backup/" + fb));
        }

        fsmit = new File(dataDir + "backup/" + f);
        logger.info("create the file " + fsmit.getName());
        while (true) {
            try {
                dataFileWriter.create(docsschema, fsmit);
                break;
            } catch (Exception ex) {
                logger.error("can not create the file " + fsmit.getName() + ex, ex);
                try {
                    Thread.sleep(5000);
                } catch (Exception ex1) {
                }
                if (fsmit.exists()) {
                    Date d = new Date();
                    String fb = format.format(d) + "_" + f + ".old";
                    logger.info("rename the file " + fsmit.getName() + " to " + fb);
                    fsmit.renameTo(new File(dataDir + "backup/" + fb));
                }
                fsmit = new File(dataDir + "backup/" + f);
            }
        }
    }

    private synchronized void checkEnvironment2() {
        synchronized (version) {
            if (localversion < version.longValue()) {
                localversion = version.longValue();
            } else {
                int i = 0;
                while (dataPoolCount() > dataPoolSize / 2 || !sendThreadIsFull() || hasDataToWrite() || !strandedDataTransmit.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (Exception ex) {
                        //donothing
                    }
                    i++;
                    if (i % 40 == 0) {
                        logger.info("check the environment for " + topic + " " + filenumber);
                    }
                }
                version.incrementAndGet();
                localversion = version.longValue();
            }
        }
    }

    private boolean hasDataToWrite() {
        ConcurrentHashMap<Rule, ConcurrentLinkedQueue> UnvalidDataStore = (ConcurrentHashMap<Rule, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.UNVALID_DATA_STORE);
        ConcurrentHashMap<String, ConcurrentLinkedQueue> uselessDataStore = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.USELESS_DATA_STORE);
        ConcurrentLinkedQueue clq = uselessDataStore.get(topic);
        if (clq != null) {
            if (!clq.isEmpty()) {
                logger.info("the queue of uselessDataStore for " + topic + " is not empty");
                return true;
            }
        }
        for (Rule r : UnvalidDataStore.keySet()) {
            if (r.getTopic().equals(topic)) {
                ConcurrentLinkedQueue clq2 = UnvalidDataStore.get(r);
                if (clq2 != null) {
                    if (!clq2.isEmpty()) {
                        logger.info("the queue of unvalidDataStore for " + topic + " " + r.getServiceName() + " is not empty");
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private long addCount(byte[] msg) {
        docsin = new ByteArrayInputStream(msg);
        docsdecoder = DecoderFactory.get().binaryDecoder(docsin, null);
        try {
            docsGr = docsreader.read(null, docsdecoder);
        } catch (Exception ex) {
            logger.info("split the data package from the topic " + topic + " in the dataPool wrong " + ex, ex);
            storeUselessData(topic, msg);
        }
        msgSet = (GenericData.Array<GenericRecord>) docsGr.get(GlobalVariables.DOC_SET);
        msgitor = msgSet.iterator();
        while (msgitor.hasNext()) {
            onedata = ((ByteBuffer) msgitor.next()).array();
            dataPool[filenumber].offer(onedata);
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

    private boolean dataPoolIsEmpty() {
        for (int i = 0; i < dataPool.length; i++) {
            if (!dataPool[i].isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private long dataPoolCount() {
        long count = 0;
        for (int i = 0; i < dataPool.length; i++) {
            count += dataPool[i].size();
        }
        return count;
    }

    private boolean sendThreadIsFull() {
        for (String str : serviceToThreadSize.keySet()) {
            if (serviceToThreadSize.get(str).longValue() >= activeThreadCount) {
                return false;
            }
        }
        return true;
    }
}
