/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.worker;

import cn.ac.iie.ulss.struct.BusiRecordNode;
import cn.ac.iie.ulss.struct.CDRRecordNode;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

public class BusiMatchworker implements Runnable {
    //要能处理群发情况下的问题（发送一样，接收不一样，这是没有问题的，发送+接收就是一个完整的键值）
    //足够灵活，合理设置参数设置不同状况

    public static Logger log = Logger.getLogger(BusiMatchworker.class.getName());
    /*
     */
    public String region;
    public String schemaName;
    public String schemanameInstance;
    public String resultSchemaName;
    /*
     */
    public int smallWindowSize;
    public int smalWindowNum;
    public int updateInterval;
    public int timeoutDeal;
    public String[] accurateJoinAttributes;
    public String fuzzyJoinAttribute;
    public String matchFuzzyJoinAttribute;
    public long maxDeviation;
    public String[] resultOwnAttributes;
    public String[] resultOtherAttributes;
    public ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> topMatchMap;
    public ConcurrentHashMap<Integer, LinkedBlockingQueue<BusiRecordNode>> inbuffer;
    public LinkedBlockingQueue<GenericRecord> outBuf;
    /*
     */
    AtomicBoolean isBegin;
    public Lock readLock;
    final public Object lk;
    /*
     */
    public AtomicInteger httpReceiveBufferIndex;
    public AtomicInteger clearBufferIndex;
    public AtomicInteger workBufferIndex;
    /*
     */
    boolean isMain;

    BusiMatchworker(String reg, String flowName, String resName, int smWinSize, int winNum,
            String[] acJoinAttributes, String fJoinAttribute, String mfJoinAttribute, long maxDevi,
            String[] resOwnAtttis, String[] resOtherAtttis, int timeOutDeal,
            ConcurrentHashMap<Integer, LinkedBlockingQueue<BusiRecordNode>> inbuf,
            ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> topMtMap,
            LinkedBlockingQueue<GenericRecord> bufOut, Lock rLock, Object lock, AtomicInteger httpReceiveBufferIdx, AtomicInteger clearBufferIdx, AtomicInteger workBufferIdx) {

        this.region = reg;
        this.schemaName = flowName;
        this.schemanameInstance = this.region.toLowerCase() + "." + this.schemaName;

        this.resultSchemaName = resName;
        this.smallWindowSize = smWinSize;
        this.smalWindowNum = winNum;
        this.updateInterval = smallWindowSize;
        this.accurateJoinAttributes = acJoinAttributes;
        this.fuzzyJoinAttribute = fJoinAttribute;
        this.matchFuzzyJoinAttribute = mfJoinAttribute;
        this.maxDeviation = maxDevi;

        this.resultOwnAttributes = resOwnAtttis;
        this.resultOtherAttributes = resOtherAtttis;
        this.timeoutDeal = timeOutDeal;
        this.inbuffer = inbuf;
        this.topMatchMap = topMtMap;
        this.isBegin = new AtomicBoolean(false);
        this.readLock = rLock;
        this.outBuf = bufOut;
        this.lk = lock;
        /*
         */
        this.httpReceiveBufferIndex = httpReceiveBufferIdx;
        this.clearBufferIndex = clearBufferIdx;
        this.workBufferIndex = workBufferIdx;
    }

    @Override
    public void run() {
        HashMap<String, Method> methodMap = new HashMap<String, Method>();
        try {
            for (Field f : CDRRecordNode.class.getFields()) {
                Method m = CDRRecordNode.class.getMethod("get" + f.getName());
                methodMap.put("get" + f.getName(), m);
            }
        } catch (Exception e) {
            log.error(e, e);
            return;
        }

        int recordFieldIdx = 0;
        int arrayIndex = 0;
        long hashcode = 0l;
        long bucket = 0;
        long bufferSize = 0;
        long remainCap = 0;
        long data_timestamp = 0;
        long cdr_timestamp = 0;

        long timestamp_gab = 0;
        int resultIndex = 0;

        Object tmpObj = null;
        String strKey = null;
        StringBuilder sb = new StringBuilder(100);

        int logPrintCount = 0;
        BusiRecordNode tmpBusiRecord = null;
        List<CDRRecordNode> CDRRecords = null;
        CDRRecordNode tmpCdrRecord = null;
        CDRRecordNode okCdrRecord = null;


        int innerbufferSize = 150000;
        LinkedBlockingQueue<BusiRecordNode> buf = new LinkedBlockingQueue<BusiRecordNode>(innerbufferSize);
        CDRRecordNode[] tmpArray = new CDRRecordNode[Matcher.maxStorePositionPerNumber];
        ConcurrentHashMap<String, List<CDRRecordNode>> currentLevel2Map = null;
        Schema resSchema = Matcher.schemaname2Schema.get(this.resultSchemaName);
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }

            logPrintCount++;
            for (LinkedBlockingQueue<BusiRecordNode> abq : this.inbuffer.values()) {
                bufferSize += abq.size();
                remainCap += abq.remainingCapacity();
            }
            //if (logPrintCount % 20 == 0) {
            log.info("now the data number in buffer is " + bufferSize + " and remaining capacity is " + remainCap);
            //}
            bufferSize = 0;
            remainCap = 0;


            AtomicLong num = Matcher.schemanameInstance2MatchOKTotal.get(this.schemanameInstance);

            int okNum = 0;
            int totalMatchNum = 0;
            long bgMatchTime = System.currentTimeMillis();

            this.inbuffer.get(this.workBufferIndex.get()).drainTo(buf, innerbufferSize);
            totalMatchNum += buf.size();
            Iterator<BusiRecordNode> it = buf.iterator();
            while (it.hasNext()) {
                tmpBusiRecord = it.next();
                if (tmpBusiRecord != null && tmpBusiRecord.state == 0) {
                    for (String ss : accurateJoinAttributes) {
                        tmpObj = tmpBusiRecord.genRecord.get(ss);
                        if (tmpObj != null) {
                            sb.append(tmpObj);
                        } else {
                            log.debug("the field " + ss + "  is null");
                        }
                    }
                    strKey = sb.toString();
                    sb = sb.delete(0, sb.length());
                    if ("".equals(strKey)) {
                        log.warn("the key is blank string ");
                        continue;
                    }
                    hashcode = strKey.hashCode();
                    if (hashcode < 0) {
                        hashcode = -hashcode;
                    }
                    bucket = hashcode % Matcher.topMapSize;
                    currentLevel2Map = topMatchMap.get(bucket);
                    if ((CDRRecords = currentLevel2Map.get(strKey)) != null) {
                        tmpArray = CDRRecords.toArray(tmpArray);
                    }

                    okCdrRecord = null;
                    tmpCdrRecord = null;
                    cdr_timestamp = 0;
                    timestamp_gab = Long.MAX_VALUE;
                    if (tmpArray.length > 0) {
                        data_timestamp = (Long) tmpBusiRecord.getGenericRecord().get(this.fuzzyJoinAttribute);//得到业务数据自己的时间戳
                        for (arrayIndex = 0; arrayIndex < tmpArray.length; arrayIndex++) {
                            tmpCdrRecord = tmpArray[arrayIndex];
                            if (tmpCdrRecord == null) {
                                continue;
                            }
                            if (Math.abs(tmpCdrRecord.c_timestamp - data_timestamp) <= timestamp_gab) {
                                timestamp_gab = Math.abs(tmpCdrRecord.c_timestamp - data_timestamp);
                                resultIndex = arrayIndex;
                                tmpCdrRecord = tmpArray[resultIndex];
                                okCdrRecord = tmpCdrRecord;
                                cdr_timestamp = (Long) okCdrRecord.c_timestamp;
                            }
                        }
                        if (okCdrRecord != null) {
                            GenericRecord record = new GenericData.Record(resSchema);
                            for (recordFieldIdx = 0; recordFieldIdx < this.resultOwnAttributes.length; recordFieldIdx++) {
                                record.put(resultOwnAttributes[recordFieldIdx], tmpBusiRecord.genRecord.get(resultOwnAttributes[recordFieldIdx].toLowerCase()));
                            }
                            for (recordFieldIdx = 0; recordFieldIdx < this.resultOtherAttributes.length; recordFieldIdx++) {
                                try {
                                    record.put(resultOwnAttributes.length + recordFieldIdx, methodMap.get("get" + resultOtherAttributes[recordFieldIdx].toLowerCase()).invoke(okCdrRecord));
                                } catch (Exception ex) {
                                    log.error(ex, ex);
                                }
                            }
                            tmpBusiRecord.state = 1;
                            try {
                                this.outBuf.put(record);
                            } catch (Exception ex) {
                                log.error(ex, ex);
                            }
                            it.remove();
                            okNum++;
                        } else {
                            GenericRecord record = new GenericData.Record(resSchema);
                            for (int i = 0; i < this.resultOwnAttributes.length; i++) {
                                record.put(resultOwnAttributes[i], tmpBusiRecord.genRecord.get(resultOwnAttributes[i]));
                            }
                            try {
                                this.outBuf.put(record);
                            } catch (Exception ex) {
                                log.error(ex, ex);
                            }
                            it.remove();
                        }
                    } else {
                        log.warn("the tmp array size is 0 !");
                        GenericRecord record = new GenericData.Record(resSchema);
                        for (int i = 0; i < this.resultOwnAttributes.length; i++) {
                            record.put(resultOwnAttributes[i], tmpBusiRecord.genRecord.get(resultOwnAttributes[i]));
                        }
                        try {
                            this.outBuf.put(record);
                        } catch (Exception ex) {
                            log.error(ex, ex);
                        }
                        it.remove();
                    }
                    for (arrayIndex = 0; arrayIndex < tmpArray.length; arrayIndex++) {
                        tmpArray[arrayIndex] = null;
                    }
                }
            }
            if (logPrintCount % 20 == 0) {
                log.info("now do math opteraion use " + (System.currentTimeMillis() - bgMatchTime) + " ms for " + totalMatchNum);
            }

            num.addAndGet(okNum);
            this.isBegin.set(false);

            if (logPrintCount % 20 == 0) {
                log.info("clear sliding window ok，now for dataflow " + this.schemanameInstance + " match successful num is " + num.get());
                logPrintCount = 0;
            }
        }
    }
}