/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.worker;

import cn.ac.iie.ulss.struct.BusiRecordNode;
import cn.ac.iie.ulss.struct.CDRRecordNode;
import cn.ac.iie.ulss.util.SimpleHash;
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
        long bucket = 0;
        long bufferSize = 0;
        long remainCap = 0;
        long s1 = 0;
        long s2 = 0;
        Object tmpObj = null;
        String strKey = null;
        CDRRecordNode tmpCdrRecord = null;
        BusiRecordNode tmpBusiRecord = null;
        List<CDRRecordNode> CDRRecords = null;
        List<LinkedBlockingQueue<BusiRecordNode>> buffers = new ArrayList<LinkedBlockingQueue<BusiRecordNode>>();
        Schema resSchema = Matcher.schemaname2Schema.get(this.resultSchemaName); //比对结果的schema
        CDRRecordNode[] tmpArray = new CDRRecordNode[2];
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
                for (LinkedBlockingQueue<BusiRecordNode> buf : buffers) {
                    Iterator<BusiRecordNode> it = buf.iterator();
                    while (it.hasNext()) {
                        tmpBusiRecord = it.next();
                        if (tmpBusiRecord != null && tmpBusiRecord.state == 0) {
                            strKey = "";
                            for (String s : accurateJoinAttributes) {
                                tmpObj = tmpBusiRecord.genRecord.get(s);
                                if (tmpObj != null) {
                                    strKey += tmpObj.toString();
                                } else {
                                    log.debug("the field " + s + "  is null");
                                }
                            }
                            bucket = SimpleHash.getSimpleHash(strKey) % Matcher.topMapSize;

                            synchronized (this.lk) {
                                if ((CDRRecords = topMatchMap.get(bucket).get(strKey)) != null) {
                                    tmpArray = CDRRecords.toArray(tmpArray);
                                }
                            }

                            if (tmpArray.length > 0) {
                                for (int j = 0; j < tmpArray.length; j++) {
                                    tmpCdrRecord = tmpArray[j];
                                    if (tmpCdrRecord == null) {
                                        continue;
                                    }
                                    s1 = (Long) tmpBusiRecord.getGenericRecord().get(this.fuzzyJoinAttribute);//得到自己的时间戳
                                    s2 = (Long) tmpCdrRecord.c_timestamp;
                                    if (Math.abs(s1 - s2) <= this.maxDeviation) {   //判断是否比对上
                                        GenericRecord record = new GenericData.Record(resSchema);
                                        for (int i = 0; i < this.resultOwnAttributes.length; i++) {
                                            record.put(resultOwnAttributes[i], tmpBusiRecord.genRecord.get(resultOwnAttributes[i].toLowerCase()));
                                        }
                                        for (int i = 0; i < this.resultOtherAttributes.length; i++) {
                                            try {
                                                record.put(resultOwnAttributes.length + i, methodMap.get("get" + resultOtherAttributes[i].toLowerCase()).invoke(tmpCdrRecord));
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

                                        log.debug(record + " <------> " + tmpCdrRecord);

                                        okNum++;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

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
        } catch (InterruptedException ex) {
            log.error(ex, ex);
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
}
//s2 = (Long) tmpCdrRecord.getGenericRecord().get(this.matchFuzzyJoinAttribute);//得到比对成功的数据
//if (Math.abs(s1 - s2) <= this.maxDeviation) {   //判断是否比对上
//GenericRecord tmRec = tmpCdrRecord.genRecord;
//record.put(resultOtherAttributes[i], tmRec.get(resultOtherAttributes[i].toLowerCase()));
//record.put(resultOwnAttributes.length + i, tmRec.get(resultOtherAttributes[i].toLowerCase()));