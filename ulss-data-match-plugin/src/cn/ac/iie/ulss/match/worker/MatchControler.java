/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.worker;

import cn.ac.iie.ulss.struct.BusiRecordNode;
import cn.ac.iie.ulss.struct.CDRRecordNode;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

public class MatchControler implements Runnable {

    public static Logger log = Logger.getLogger(MatchControler.class.getName());
    /*
     */
    public String region;
    /*
     */
    public String schemaName;
    public String matchSchemaName;     //应该和那个数据流进行比对
    public String resultSchemaName;
    public int smallWindowSize;       //本数据流的单个小滑動窗口大小，单位是毫秒
    public int smalWindowNum;         //本数据流单个小滑動窗口的个数
    /**/
    public int inputbufferSize;
    public int outputbufferSize;
    public int matchThreadNumber;
    /**/
    public int updateInterval;        //滑动窗口更新间隔，单位是毫秒，实际是与smallWindowSize一样的
    public int timeoutDeal;            //0代表直接丢弃，1代表当成元组输出
    public String[] accurateJoinAttributes;    //本数据流精确比对需要的需要使用的连接属性字段
    public String fuzzyJoinAttribute;     //本数据流模糊比对需要的需要使用的连接属性字段
    public String matchFuzzyJoinAttribute;     //本数据流模糊比对需要的需要使用的连接属性字段
    public long maxDeviation;     //两个流之间的时间戳相差在多少以内才能匹配上（单位是毫秒ms），两边数据的时间戳不能保证完全一致，有误差
    /*
     * 结果中要用到的字段
     */
    public String[] resultOwnAttributes;
    public String[] resultOtherAttributes;
    /*
     */
    public ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> topMtMap;  //和哪个map进行比对,现在只支持和一个数据源进行比对，不支持和多个进行比对
    public LinkedBlockingQueue<GenericRecord> outBuf;
    /*
     */
    public AtomicBoolean isShouldNew;
    public AtomicInteger httpReceiveBufferIndex;
    public AtomicInteger clearBufferIndex;
    public AtomicInteger workBufferIndex;
    public BusiMatchworker[] matchWorkers;
    public Lock readLock;

    MatchControler(String reg,
            String flowName, String mFlowName, String resName, int smWinSize, int winNum, int bufSize, int outbuffersize, int matchThreadNum,
            String[] acJoinAttributes, String fJoinAttribute, String mfJoinAttribute, long maxDevi,
            String[] resOwnAtttis, String[] resOtherAtttis,
            int timeOutDeal, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> topMtMap,
            Lock rLock) {
        this.region = reg;
        this.schemaName = flowName;
        this.matchSchemaName = mFlowName;
        this.resultSchemaName = resName;
        this.smallWindowSize = smWinSize;
        this.smalWindowNum = winNum;

        this.inputbufferSize = bufSize;
        this.outputbufferSize = outbuffersize;
        this.matchThreadNumber = matchThreadNum;

        this.updateInterval = smallWindowSize;

        this.accurateJoinAttributes = acJoinAttributes;
        this.fuzzyJoinAttribute = fJoinAttribute;
        this.matchFuzzyJoinAttribute = mfJoinAttribute;
        this.maxDeviation = maxDevi;

        this.resultOwnAttributes = resOwnAtttis;
        this.resultOtherAttributes = resOtherAtttis;
        this.timeoutDeal = timeOutDeal;
        this.topMtMap = topMtMap;
        this.readLock = rLock;
        this.outBuf = new LinkedBlockingQueue<GenericRecord>(this.outputbufferSize);

        this.isShouldNew = new AtomicBoolean(true);

        this.httpReceiveBufferIndex = new AtomicInteger(0);
        this.clearBufferIndex = new AtomicInteger(1);
        this.workBufferIndex = new AtomicInteger(this.smalWindowNum - 1);

        this.matchWorkers = new BusiMatchworker[this.matchThreadNumber];
    }

    public void init() {
        final Object lock = new Object();
        for (int i = 0; i < this.matchThreadNumber; i++) {
            ConcurrentHashMap<Integer, LinkedBlockingQueue<BusiRecordNode>> httpInbuf = new ConcurrentHashMap<Integer, LinkedBlockingQueue<BusiRecordNode>>();
            for (int j = 0; j < this.smalWindowNum; j++) {
                httpInbuf.put(j, new LinkedBlockingQueue<BusiRecordNode>(this.inputbufferSize / (this.smalWindowNum * this.matchThreadNumber)));
            }
            BusiMatchworker m = new BusiMatchworker(this.region, this.schemaName, this.resultSchemaName, this.smallWindowSize, this.smalWindowNum,
                    this.accurateJoinAttributes, this.fuzzyJoinAttribute, this.matchFuzzyJoinAttribute, this.maxDeviation,
                    this.resultOwnAttributes, this.resultOtherAttributes, this.timeoutDeal,
                    httpInbuf, this.topMtMap, this.outBuf, this.readLock, lock, this.httpReceiveBufferIndex, this.clearBufferIndex, this.workBufferIndex);
            this.matchWorkers[i] = m;
        }
        log.debug("the small sliding window size for " + this.schemaName + " is " + this.smallWindowSize);
    }

    @Override
    public void run() {
        Thread[] threads = new Thread[this.matchThreadNumber];
        for (int i = 0; i < this.matchThreadNumber; i++) {
            threads[i] = new Thread(matchWorkers[i]);
        }
        for (int i = 0; i < this.matchThreadNumber; i++) {
            threads[i].setName(this.region + "." + this.schemaName + "-" + i);
            threads[i].start();
        }

        long newTsmp = 0;
        long oldTsmp = 0;
        newTsmp = System.currentTimeMillis();
        oldTsmp = newTsmp;
        int clearCount = 0;
        int index;
        while (true) {
            newTsmp = System.currentTimeMillis();
            if (newTsmp - oldTsmp < this.updateInterval) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                }
                continue;
            } else {
                oldTsmp = newTsmp;
                //10个窗口，从: 0 开始接收，1 开始清理，9 开始探测
                index = this.httpReceiveBufferIndex.get() + 1;
                this.httpReceiveBufferIndex.set((index) % this.smalWindowNum);
                this.clearBufferIndex.set((this.httpReceiveBufferIndex.get() + 1) % this.smalWindowNum);
                this.workBufferIndex.set((this.httpReceiveBufferIndex.get() + this.smalWindowNum - 1) % this.smalWindowNum);

                log.info(this.schemaName + "：the buffer index for receive,clean,work is -> " + this.httpReceiveBufferIndex + " " + this.clearBufferIndex + " " + this.workBufferIndex);
                for (int i = 0; i < matchWorkers.length; i++) {
                    matchWorkers[i].isBegin.set(true);
                }
                this.ensureIsEnd(matchWorkers);

                clearCount++;
                if (clearCount >= smalWindowNum) {
                    clearCount = 0;
                    this.isShouldNew.set(true);
                }
            }
        }
    }

    public void ensureIsEnd(BusiMatchworker[] mws) {
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