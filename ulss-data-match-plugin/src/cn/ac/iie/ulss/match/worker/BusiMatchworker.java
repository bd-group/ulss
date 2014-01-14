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
        long s1 = 0;
        long s2 = 0;
        Object tmpObj = null;
        String strKey = null;
        StringBuilder sb = new StringBuilder(100);

        BusiRecordNode tmpBusiRecord = null;
        List<CDRRecordNode> CDRRecords = null;
        CDRRecordNode tmpCdrRecord = null;
        List<LinkedBlockingQueue<BusiRecordNode>> buffers = new ArrayList<LinkedBlockingQueue<BusiRecordNode>>();
        Schema resSchema = Matcher.schemaname2Schema.get(this.resultSchemaName); //比对结果的schema
        CDRRecordNode[] tmpArray = new CDRRecordNode[Matcher.maxStorePositionPerNumber];
        ConcurrentHashMap<String, List<CDRRecordNode>> currentLevel2Map = null;
        while (true) {
            if (!this.isBegin.get()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                }
                continue;
            } else {
                for (LinkedBlockingQueue<BusiRecordNode> abq : this.inbuffer.values()) {
                    bufferSize += abq.size();
                    remainCap += abq.remainingCapacity();
                }
                log.info("now the data number in buffer is " + bufferSize + " and remaining capacity is " + remainCap);
                bufferSize = 0;
                remainCap = 0;

                LinkedBlockingQueue<BusiRecordNode> cleanWorkBuf = this.inbuffer.get(this.clearBufferIndex.get());
                LinkedBlockingQueue<BusiRecordNode> matchWorkBuf = this.inbuffer.get(this.workBufferIndex.get());
                buffers.add(matchWorkBuf);
                buffers.add(cleanWorkBuf);

                log.info("now begin do the match for dataflow " + this.schemanameInstance);
                AtomicLong num = Matcher.schemanameInstance2MatchOKTotal.get(this.schemanameInstance);

                int okNum = 0;
                int totalMatchNum = 0;
                long bgMatchTime = System.currentTimeMillis();
                for (LinkedBlockingQueue<BusiRecordNode> buf : buffers) {
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
                                continue;
                            }

                            hashcode = strKey.hashCode();
                            if (hashcode < 0) {
                                hashcode = -hashcode;
                            }
                            bucket = hashcode % Matcher.topMapSize;
                            currentLevel2Map = topMatchMap.get(bucket);

                            //synchronized (currentLevel2Map) {
                            if ((CDRRecords = currentLevel2Map.get(strKey)) != null) {
                                tmpArray = CDRRecords.toArray(tmpArray);
                                //okNum++;
                            }
                            //}
                            for (arrayIndex = 0; arrayIndex < tmpArray.length; arrayIndex++) {
                                tmpCdrRecord = tmpArray[arrayIndex];
                                if (tmpCdrRecord == null) {
                                    continue;
                                }
                                s1 = (Long) tmpBusiRecord.getGenericRecord().get(this.fuzzyJoinAttribute);//得到自己的时间戳
                                s2 = (Long) tmpCdrRecord.c_timestamp;
                                if (Math.abs(s1 - s2) <= this.maxDeviation) {   //判断是否比对上
                                    GenericRecord record = new GenericData.Record(resSchema);
                                    for (recordFieldIdx = 0; recordFieldIdx < this.resultOwnAttributes.length; recordFieldIdx++) {
                                        record.put(resultOwnAttributes[recordFieldIdx], tmpBusiRecord.genRecord.get(resultOwnAttributes[recordFieldIdx].toLowerCase()));
                                    }
                                    for (recordFieldIdx = 0; recordFieldIdx < this.resultOtherAttributes.length; recordFieldIdx++) {
                                        try {
                                            //log.debug("the first field is " + record + "---" + record.get(0) + ", and now will set " + (resultOwnAttributes.length + i) + "get" + resultOtherAttributes[i].toLowerCase() + " " + tmpCdrRecord);
                                            record.put(resultOwnAttributes.length + recordFieldIdx, methodMap.get("get" + resultOtherAttributes[recordFieldIdx].toLowerCase()).invoke(tmpCdrRecord));
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
                                    //log.debug(record + " <------> " + tmpCdrRecord);
                                    okNum++;
                                    break;
                                }
                            }
                            for (arrayIndex = 0; arrayIndex < tmpArray.length; arrayIndex++) {
                                tmpArray[arrayIndex] = null;
                            }
                        }
                    }
                }
                log.info("now do math opteraion use " + (System.currentTimeMillis() - bgMatchTime) + " ms for " + totalMatchNum);

                buffers.clear();
                num.addAndGet(okNum);

                log.info("now the match operation is over，begin clear the sliding window buffer for " + this.schemanameInstance);
                this.simpleClearWindow();

                this.isBegin.set(false);
                log.info("clear sliding window ok，now for dataflow " + this.schemanameInstance + " match successful num is " + num.get());
            }
        }
    }

    public void simpleClearWindow() {
        try {
            Schema resultSchema = Matcher.schemaname2Schema.get(this.resultSchemaName);
            LinkedBlockingQueue<BusiRecordNode> buf = this.inbuffer.get(this.clearBufferIndex.get());
            while (!buf.isEmpty()) {
                BusiRecordNode tmpBusiRecord = buf.poll();
                if (tmpBusiRecord != null && tmpBusiRecord.state == 0) { 
                    GenericRecord record = new GenericData.Record(resultSchema);
                    for (int i = 0; i < this.resultOwnAttributes.length; i++) {
                        record.put(resultOwnAttributes[i], tmpBusiRecord.genRecord.get(resultOwnAttributes[i]));
                    }
                    this.outBuf.put(record);
                }
            }
            log.info("after clear the window，the size of output buffer is " + this.outBuf.size());
            buf.clear();
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
}
