/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.iie.match.handler;

import cn.iie.struct.RecordNode;
import cn.iie.util.SimpleMD5;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

public class MatchWorker implements Runnable {
    //要能处理群发情况下的问题（发送一样，接收不一样，这是没有问题的，发送+接收就是一个完整的键值）
    //要能根据发送源填充前端所在省字段
    //足够灵活，合理设置参数设置不同状况

    public static Logger log = Logger.getLogger(MatchWorker.class.getName());
    public String schemaName;     //数据流的名称，如Dx，Fs_Xl，还有中间结果也可以说是一个数据流
    public String resultSchemaName;
    public int smallWindowSize;     //本数据流的单个小滑動窗口大小，单位是毫秒
    public int smalWindowNum;       //本数据流单个小滑動窗口的个数
    public int updateInterval;      //滑动窗口更新间隔，单位是毫秒，实际是与smallWindowSize一样的
    /**/
    public int timeoutDeal;   //0代表直接丢弃，1代表当成元组输出
    public String[] accurateJoinAttributes;  //本数据流精确比对需要的需要使用的连接属性字段
    public String fuzzyJoinAttribute;  //本数据流模糊比对需要的需要使用的连接属性字段
    public String matchFuzzyJoinAttribute;
    public long maxDeviation;  //两个流之间的时间戳相差在多少以内才能匹配上（单位是毫秒ms），两边数据的时间戳不能保证完全一致，有误差
    /**
     * 结果中要用到的字段
     */
    public String[] resOwnAttributes;
    public String[] resOtherAttributes;
    /**/
    public ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMap;
    public ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMatchMap;
    /**/
    public LinkedBlockingQueue<RecordNode> inbuf;         //输入缓冲区,使用同一个对象创建多个线程，各个线程共同消费此缓冲区
    public LinkedBlockingQueue<GenericRecord> outBuf;    //输出缓冲区
    /**/
    AtomicBoolean isBegin;
    AtomicBoolean isCleaned;
    public Lock readLock;
    final public Object lk;

    MatchWorker(String flowName, String resName, int smWinSize, int winNum,
            String[] acJoinAttributes, String fJoinAttribute, String mfJoinAttribute, long maxDevi,
            String[] resOwnAtttis, String[] resOtherAtttis, int timeOutDeal,
            ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMap, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMtMap,
            LinkedBlockingQueue<RecordNode> b1, LinkedBlockingQueue<GenericRecord> b2, AtomicBoolean isClean, Lock rLock, Object l) {

        this.schemaName = flowName;
        this.resultSchemaName = resName;
        this.smallWindowSize = smWinSize;
        this.smalWindowNum = winNum;
        this.updateInterval = smallWindowSize;

        this.accurateJoinAttributes = acJoinAttributes;
        this.fuzzyJoinAttribute = fJoinAttribute;
        this.matchFuzzyJoinAttribute = mfJoinAttribute;

        this.maxDeviation = maxDevi;
        /**/
        this.resOwnAttributes = resOwnAtttis;
        this.resOtherAttributes = resOtherAtttis;

        this.timeoutDeal = timeOutDeal;

        this.topMap = topMap;
        this.topMatchMap = topMtMap;

        this.isBegin = new AtomicBoolean(false);
        this.isCleaned = isClean;

        this.readLock = rLock;

        this.inbuf = b1;
        this.outBuf = b2;

        this.lk = l;
    }

    @Override
    public void run() {
        long s1 = 0;
        long s2 = 0;

        long tsmp = 0;
        String strKey = "";
        RecordNode tmpRec;  //程序内部使用的临时recnode
        RecordNode tmp;
        List<RecordNode> resultRecordList;  //结果的record

        Object tmpObj = null;
        List<RecordNode> tmpBuf = new ArrayList<RecordNode>();
        Schema resSchema = Matcher.schemaname2Schema.get(this.resultSchemaName); //结果的schema
        while (true) {
            if (!this.isBegin.get()) { //实际上可以wait notify的方式将所有的线程唤起，而不是使用循环检测的方法
                try {
                    Thread.sleep(5);
                } catch (InterruptedException ex) {
                }
                continue;
            } else {
                isCleaned.set(false);
                tsmp = System.currentTimeMillis();  //这里是有问题的，不能如此简单处理
                //log.info("now do the match, the sliding window map size is " + this.map.size() + " and the match sliding window map size is " + this.matchMap.size());
                readLock.lock();
                try {
                    while (!this.inbuf.isEmpty()) {
                        tmpRec = this.inbuf.poll();
                        if (tmpRec != null) {
                            tmpBuf.add(tmpRec);
                            if (tmpBuf.size() >= this.smallWindowSize * Matcher.maxProcessNumPerMillis || this.inbuf.isEmpty()) {
                                break;
                            }
                        }
                    }
                } finally {
                    readLock.unlock();
                }

                log.info("now begin do the match ...");
                for (RecordNode rec : tmpBuf) {
                    strKey = "";
                    for (String s : accurateJoinAttributes) {
                        tmpObj = rec.genRecord.get(s);
                        if (tmpObj != null) {    //如果得到字段的值不是null则处理
                            strKey += tmpObj.toString();
                        } else {
                            log.debug("the field " + s + "  is null");
                        }
                    }

                    long bucket = SimpleMD5.getSimpleHash(strKey) % Matcher.topMapSize;
                    synchronized (this.lk) {
                        resultRecordList = topMatchMap.get(bucket).get(strKey);
                        if (resultRecordList != null && resultRecordList.size() > 0) {
                            for (int j = 0; j < resultRecordList.size(); j++) {
                                tmp = resultRecordList.get(j);
                                if (tmp.getState() == 0) {
                                    s1 = (Long) rec.getGenericRecord().get(this.fuzzyJoinAttribute);//得到自己的时间戳
                                    s2 = (Long) tmp.getGenericRecord().get(this.matchFuzzyJoinAttribute);//得到比对成功的数据
                                    if (Math.abs(s1 - s2) <= this.maxDeviation) {
                                        GenericRecord tmRec = tmp.genRecord;
                                        tmp.state = 1;
                                        GenericRecord record = new GenericData.Record(resSchema);
                                        for (int i = 0; i < this.resOwnAttributes.length; i++) {
                                            record.put(resOwnAttributes[i], rec.genRecord.get(resOwnAttributes[i].toLowerCase()));
                                        }
                                        for (int i = 0; i < this.resOtherAttributes.length; i++) {
                                            record.put(resOtherAttributes[i], tmRec.get(resOtherAttributes[i].toLowerCase()));
                                        }
                                        log.debug("get one ok record " + record);
                                        try {
                                            this.outBuf.put(record);
                                        } catch (InterruptedException ex) {
                                        }
                                        break;
                                    }
                                }
                            }
                        } else {
                            List<RecordNode> ownList = topMap.get(bucket).get(strKey);
                            if (ownList == null) {
                                ownList = new ArrayList<RecordNode>();
                                ownList.add(rec);
                                log.debug("the size for map " + strKey + " is 1,this is a new key ");
                                topMap.get(bucket).put(strKey, ownList);
                            } else {
                                ownList.add(rec);
                                log.debug("the size for map " + strKey + " is " + ownList.size());
                            }
                        }
                    }
                }
                tmpBuf.clear();
                log.info("now the match operation is over ");
            }

            synchronized (lk) {
                if (!isCleaned.get()) {
                    log.info("now begin clear the sliding window map ...");
                    this.simpleClearMap(tsmp);
                    log.info("clear sliding window ok,the out buffer size is " + this.outBuf.size());
                    isCleaned.set(true);
                }
            }
            this.isBegin.set(false);   //状态重置为初始状态
        }
    }

    public void simpleClearMap(long cur) {
        Schema resSchema = Matcher.schemaname2Schema.get(this.resultSchemaName);
        RecordNode t;
        String key = "";
        List<RecordNode> tmp = null;
        try {
            for (long j = 0; j < Matcher.topMapSize; j++) {
                ConcurrentHashMap<String, List<RecordNode>> map = this.topMap.get(j);
                Iterator it = map.keySet().iterator();
                while (it.hasNext()) {
                    key = (String) it.next();
                    tmp = map.get(key);
                    Iterator iter = tmp.iterator();
                    while (iter.hasNext()) {
                        t = (RecordNode) iter.next();
                        if (t.arriveTimestamp <= cur - this.smalWindowNum * this.smallWindowSize) {
                            if ((t.state == 0)) {
                                if (this.timeoutDeal == 1) { //代表数据过期后还要输出
                                    GenericRecord record = new GenericData.Record(resSchema);
                                    for (int i = 0; i < this.resOwnAttributes.length; i++) {
                                        record.put(resOwnAttributes[i], t.getGenericRecord().get(resOwnAttributes[i]));
                                    }
                                    this.outBuf.put(record);
                                } else if (this.timeoutDeal == 0) { //代表数据过期后直接丢掉
                                    //this.map.remove(k);
                                }
                            }
                            iter.remove();
                        }
                    }
                    if (tmp.isEmpty()) {
                        it.remove();
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, e);
        }
    }
}