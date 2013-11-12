/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.iie.match.handler;

import cn.iie.struct.RecordNode;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

public class MatchControler implements Runnable {
    //要能处理群发情况下的问题（发送一样，接收不一样，这是没有问题的，发送+接收就是一个完整的键值）
    //要能根据发送源填充前端所在省字段
    //足够灵活，合理设置参数设置不同状况

    public static Logger log = Logger.getLogger(MatchControler.class.getName());
    public String schemaName;     //数据流的名称，如Dx，Fs_Xl，还有中间结果也可以说是一个数据流
    public String matchSchemaName;    //应该和那个数据流进行比对
    public String resultSchemaName;
    public int smallWindowSize;     //本数据流的单个小滑動窗口大小，单位是毫秒
    public int smalWindowNum;       //本数据流单个小滑動窗口的个数
    public int updateInterval;      //滑动窗口更新间隔，单位是毫秒，实际是与smallWindowSize一样的
    /*
     */
    public int timeoutDeal;   //0代表直接丢弃，1代表当成元组输出
    public String[] accurateJoinAttributes;  //本数据流精确比对需要的需要使用的连接属性字段
    public String fuzzyJoinAttribute;  //本数据流模糊比对需要的需要使用的连接属性字段
    public String matchFuzzyJoinAttribute;  //本数据流模糊比对需要的需要使用的连接属性字段
    public long maxDeviation;  //两个流之间的时间戳相差在多少以内才能匹配上（单位是毫秒ms），两边数据的时间戳不能保证完全一致，有误差
    /*
     * 结果中要用到的字段
     */
    public String[] resOwnAttributes;
    public String[] resOtherAttributes;
    /*
     */
    ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>>(1024 * 32, 0.8f);
    ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMtMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>>(1024 * 32, 0.8f);
    //public ConcurrentHashMap<String, List<RecordNode>> map;  //key的值是发送号码+"_"+接收号码+"_"+内容,如果处理群发的情况也能按照此结构进行
    //public ConcurrentHashMap<String, List<RecordNode>> matchMap; //和哪个map进行比对,现在只支持和一个数据源进行比对，不支持和多个进行比对
     /*
     */
    public LinkedBlockingQueue<RecordNode> inbuf;          //输入缓冲区,使用同一个对象创建多个线程，各个线程共同消费此缓冲区
    public LinkedBlockingQueue<GenericRecord> outBuf;    //输出缓冲区
    public final Object otherLock;
    public AtomicBoolean isShouldNew;
    public Lock readLock;

    MatchControler(String flowName, String mFlowName, String resName, int smWinSize, int winNum,
            String[] acJoinAttributes, String fJoinAttribute, String mfJoinAttribute, long maxDevi,
            String[] resOwnAtttis, String[] resOtherAtttis,
            int timeOutDeal, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMap, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMtMap,
            Object oLock, Lock rLock) {

        this.schemaName = flowName;
        this.matchSchemaName = mFlowName;
        this.resultSchemaName = resName;
        this.smallWindowSize = smWinSize;
        this.smalWindowNum = winNum;

        this.updateInterval = smallWindowSize;

        this.accurateJoinAttributes = acJoinAttributes;
        this.fuzzyJoinAttribute = fJoinAttribute;
        this.matchFuzzyJoinAttribute = mfJoinAttribute;
        this.maxDeviation = maxDevi;
        /*
         *
         */
        this.resOwnAttributes = resOwnAtttis;
        this.resOtherAttributes = resOtherAtttis;

        this.timeoutDeal = timeOutDeal;

        this.topMap = topMap;
        this.topMtMap = topMtMap;

        this.otherLock = oLock;
        this.readLock = rLock;

        this.inbuf = new LinkedBlockingQueue<RecordNode>(Matcher.inbufferSize);
        this.outBuf = new LinkedBlockingQueue<GenericRecord>(Matcher.outbufferSize);
        this.isShouldNew = new AtomicBoolean(true);
    }

    @Override
    public void run() {
        MatchWorker[] mWorkers = new MatchWorker[Matcher.matchThreadNum];
        Thread[] threads = new Thread[Matcher.matchThreadNum];
        final Object lk = new Object();
        AtomicBoolean isCleaned = new AtomicBoolean(false);
        for (int i = 0; i < Matcher.matchThreadNum; i++) {
            MatchWorker m = new MatchWorker(this.schemaName, this.resultSchemaName, this.smallWindowSize, this.smalWindowNum,
                    this.accurateJoinAttributes, this.fuzzyJoinAttribute, this.matchFuzzyJoinAttribute, this.maxDeviation,
                    this.resOwnAttributes, this.resOtherAttributes, this.timeoutDeal,
                    this.topMap, this.topMtMap, this.inbuf, this.outBuf, isCleaned, this.readLock, lk);
            mWorkers[i] = m;
            threads[i] = new Thread(m);

            Matcher.schema2Matchworker.put(this.schemaName.toLowerCase(), m);
            log.debug("put " + this.schemaName.toLowerCase() + " " + m + " to map");
        }
        for (int i = 0; i < Matcher.matchThreadNum; i++) {
            threads[i].setName(this.schemaName + "-" + i);
            threads[i].start();
        }
        long newTsmp = 0;
        long oldTsmp = 0;
        newTsmp = System.currentTimeMillis();
        oldTsmp = newTsmp;

        int clearCount = 0;

        while (true) {
            newTsmp = System.currentTimeMillis();
            if (newTsmp - oldTsmp < updateInterval) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                }
                continue;
            } else {
                oldTsmp = newTsmp;
                /*
                 *用于同步多个流的匹配，一个流正在进行探测，那么另一个流就不能继进行相同的探测过程
                 */
                synchronized (this.otherLock) {
                    for (int i = 0; i < mWorkers.length; i++) {
                        mWorkers[i].isBegin.set(true);
                    }
                    /*
                     * 当前数据流对应的所有的匹配线程结束时才会释放锁，
                     * 允许另一个流的匹配线程启动进行匹配
                     */
                    this.ensureIsEnd(mWorkers);
                }
                clearCount++;
                if (clearCount >= smalWindowNum) {
                    clearCount = 0;
                    this.isShouldNew.set(true);
                }
                log.debug("the size of out buffer is " + this.outBuf.size());
            }
        }
    }

    public void ensureIsEnd(MatchWorker[] mws) {
        while (true) {
            boolean isEnd = true;
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
            }
            for (int i = 0; i < mws.length; i++) {
                if (mws[i].isBegin.get() == true) {
                    isEnd = false;
                    break;
                }
            }
            if (isEnd) {
                break;
            }
        }
    }
}