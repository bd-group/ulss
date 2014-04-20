/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.worker;

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
import org.apache.log4j.Logger;

public class CDRUpdater implements Runnable {

    public static Logger log = Logger.getLogger(CDRUpdater.class.getName());
    /*
     */
    public String region;
    /*
     */
    public String schemaName;
    public int maxStorePosNum;
    public long maxStoreTime;            //一条位置信息最多在内存内驻留多长时间
    public long cleanInterval;           //间隔多长时间进行一次位置信息的清除
    public int updateInterval;           //使用毫秒,缓冲多长时间数据然后更新cdr map，进行更新内部的位置信息（更新位置信息的时候其实已经在清除了老的位置信息了cleanInterval有没有必要）
    public int timeoutDeal;              //过期后如何处理，0 丢掉，1代表继续使用
    public String[] accurateJoinAttributes;  //使用那几个字段进行连接，也就是作为map的key
    public String fuzzyJoinAttribute;
    public String matchFuzzyJoinAttribute;
    public long maxDeviation;            //对于时间字段使用模糊匹配时最大允许的误差是多少
    public ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> topMap;
    AtomicBoolean isBegin;
    AtomicBoolean isMain;
    final public Object lk;
    public LinkedBlockingQueue<CDRRecordNode> inbuf; //cdr记录的输入缓冲区
    public AtomicBoolean isDumping = null;

    CDRUpdater(String reg, String flowName, int smWinSize, int winNum,
            String[] acJoinAttributes,
            int timeOutDeal, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> tp, LinkedBlockingQueue<CDRRecordNode> inbuf,
            AtomicBoolean isMain, Object lock) {
        this.region = reg;
        this.schemaName = flowName;
        this.updateInterval = smWinSize;  //使用毫秒
        this.accurateJoinAttributes = acJoinAttributes;
        this.timeoutDeal = timeOutDeal;
        this.topMap = tp;
        this.isBegin = new AtomicBoolean(false);
        this.isMain = isMain;
        this.inbuf = inbuf;

        this.lk = lock;
        this.maxStorePosNum = Matcher.maxStorePositionPerNumber;

        this.isDumping = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        long bg = 0;
        long hashcode = 0l;
        long bucket = 0;
        long updateCount = 0;
        long cdrKeyCount = 0;
        //StringBuffer sb = new StringBuffer();
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

        long old = 0;
        long cur = 0;
        String strKey = "";
        CDRRecordNode tmpRec;
        List<CDRRecordNode> posRecords;
        List<CDRRecordNode> innerBuf = new ArrayList<CDRRecordNode>();
        ConcurrentHashMap<String, List<CDRRecordNode>> currentLevel2Map = null;
        old = cur = System.currentTimeMillis();
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
            cur = System.currentTimeMillis();
            if (cur - old < this.updateInterval) {
                continue;
            } else {
                updateCount++;
                old = cur;

                log.info("before begin do the cdr map update and compression,the cdr in buffer size and cap is  " + this.inbuf.size() + " " + this.inbuf.remainingCapacity());
                try {
                    while (!this.inbuf.isEmpty()) {
                        tmpRec = this.inbuf.poll();
                        if (tmpRec != null) {
                            innerBuf.add(tmpRec);
                            if (innerBuf.size() >= this.updateInterval * Matcher.maxProcessCdrPerMillis || this.inbuf.isEmpty()) {
                                log.info("get data from the buffer and put to the inner buffer ok " + innerBuf.size() + " " + this.inbuf.isEmpty());
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error(e, e);
                }
                log.info("now do the cdr map update and compression,the cdr in buffer size and cap is  " + this.inbuf.size() + " " + this.inbuf.remainingCapacity());

                bg = System.currentTimeMillis();
                for (CDRRecordNode cdrRec : innerBuf) {
                    strKey = "";
                    for (String s : accurateJoinAttributes) {
                        try {
                            strKey += methodMap.get("get" + s).invoke(cdrRec);
                        } catch (Exception ex) {
                            log.error(ex, ex);
                        }
                    }
                    hashcode = strKey.hashCode();
                    if (hashcode < 0) {
                        hashcode = -hashcode;
                    }
                    bucket = hashcode % Matcher.topMapSize;
                    currentLevel2Map = topMap.get(bucket);

                    synchronized (currentLevel2Map) {
                        posRecords = currentLevel2Map.get(strKey);
                        if (posRecords != null && posRecords.size() > 0) {   //开始更新对应的cdr的位置信息，update或者insert操作
                            updatePos(cdrRec, posRecords);
                        } else { //为对应的key插入一条位置信息 只有insert操作
                            if (posRecords == null) {
                                posRecords = new ArrayList<CDRRecordNode>();
                                posRecords.add(cdrRec);
                                topMap.get(bucket).put(strKey, posRecords);
                            } else {
                                posRecords.add(cdrRec);
                            }
                        }
                    }
                }
                for (long i = 0; i < topMap.size(); i++) {
                    cdrKeyCount += topMap.get(i).size();
                }
                log.info("there is " + cdrKeyCount + " number in the cdr map now ");
                cdrKeyCount = 0;
                innerBuf.clear();
                log.info("now the cdr map update and compression is over, use " + (System.currentTimeMillis() - bg) + " milliseconds ");

                if (isMain.get()) {
                    //非更新cdrmap固定次数后就将数据dump出来
                    if ((this.isDumping.get())) {
                        //if ((updateCount % (Matcher.posDumpIntervalMiliSeconds / this.updateInterval) == 0) || (this.isDumping.get())) {
                        log.info("now will dump the position information map " + (this.isDumping.get() == true ? "" : ",it is manually dump"));
                        RecoverPos.dump(this.topMap, Matcher.positionDumpPath, this.region);
                        this.isDumping.set(false);
                    }
                    //this.simpleClearCDR(updateCount);
                }
            }
        }
    }

    private void updatePos(CDRRecordNode latest, List<CDRRecordNode> positions) {
        boolean isNewPosition = true;

        for (CDRRecordNode old : positions) {
            if (latest.c_lac == old.c_lac) {
                if (latest.c_ci == old.c_ci) {
                    //发现latest不是最新的位置信息，要将老的cdr位置信息记录的 timestamp字段 、tmpUpdateTime 字段更新一下即可,
                    //arrive time 就是（这条位置信息到阿达系统的时间，一旦到达就不会更新，除非lac ci 变化，记录被剔除）
                    //而timestamp   tmpUpdateTime 变化：lac  ci  不变，但是有新的cdr记录到来就将其更新一下
                    old.c_timestamp = latest.c_timestamp;
                    old.updateTime = System.currentTimeMillis();
                    isNewPosition = false;
                    return;
                }
            }
        }

        if (isNewPosition) {  //如果是新位置信息
            CDRRecordNode tobeReplaced = positions.get(0);
            long tmpUpdateTime = tobeReplaced.updateTime;
            if (positions.size() < maxStorePosNum) { //如果是新位置并且位置信息小于能存储的最大位置信息,则直接插入
                positions.add(latest);
            } else {
                for (CDRRecordNode cur : positions) { //如果是新位置信息并且超过了最大能存储的位置信息则选择内存中驻留时间最长的替换掉
                    if (cur.updateTime <= tmpUpdateTime) {
                        tobeReplaced = cur;
                        tmpUpdateTime = cur.updateTime;
                    }
                }
                tobeReplaced.c_usernum = latest.c_usernum;
                tobeReplaced.c_imsi = latest.c_imsi;
                tobeReplaced.c_imei = latest.c_imei;
                tobeReplaced.c_spcode = latest.c_spcode;
                tobeReplaced.c_ascode = latest.c_ascode;
                tobeReplaced.c_lac = latest.c_lac;
                tobeReplaced.c_ci = latest.c_ci;
                tobeReplaced.c_rac = latest.c_rac;
                tobeReplaced.c_areacode = latest.c_areacode;
                tobeReplaced.c_homecode = latest.c_homecode;
                tobeReplaced.c_timestamp = latest.c_timestamp;
                tobeReplaced.arriveTime = latest.arriveTime;
                tobeReplaced.updateTime = System.currentTimeMillis();
            }
        }
    }

    public void simpleClearCDR(long sleepTime) {
        String key = "";
        List<CDRRecordNode> tmp = null;
        ConcurrentHashMap<String, List<CDRRecordNode>> map = null;
        if (sleepTime % (Matcher.maxPosStoreMiliSeconds / this.updateInterval) == 0) {
            log.info("now begin clear the cdr position map,will remove some zombie numbers ... ");
            try {
                Iterator iter = topMap.values().iterator();
                boolean shouldDelete = true;
                while (iter.hasNext()) {
                    map = (ConcurrentHashMap<String, List<CDRRecordNode>>) iter.next();
                    Iterator it = map.keySet().iterator();
                    while (it.hasNext()) {
                        key = (String) it.next();
                        tmp = map.get(key);
                        shouldDelete = true;
                        for (CDRRecordNode tm : tmp) {
                            if ((System.currentTimeMillis() - tm.updateTime) < Matcher.maxPosStoreMiliSeconds) {
                                shouldDelete = false;
                                break;
                            }
                        }

                        if (shouldDelete) {
                            log.info("will remove one number: " + key + " from the cdr position map,because it has not been updated for very long time");
                            it.remove();
                        }

                    }
                }
            } catch (Exception e) {
                log.error(e, e);
            }
            log.info("now begin clear the cdr map ok ");
        }
    }
}