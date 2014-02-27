/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.datastatics.DataEmiter;
import cn.ac.iie.ulss.indexer.metastore.MetastoreWrapper;
import cn.ac.iie.ulss.struct.BloomFileWriter;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import iie.metastore.MetaStoreClient;
import iie.mm.client.ClientAPI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;

public class CreateIndex implements Runnable {

    public static Logger log = Logger.getLogger(CreateIndex.class.getName());
    public String zkUrl;
    public String dbName;
    public String tbName;
    public String timeLable;
    public Long file_id;
    public DataSourceConfig dc;
    public AtomicBoolean isEnd;
    public AtomicBoolean isAllOver;
    public AtomicBoolean isShouldNewRaw;
    public ArrayBlockingQueue inbuffer;
    public List<WriteIndexFile> wList;
    public List<Thread> wThList;
    public MetaStoreClient msCli;
    public ClientAPI ca;
    public LuceneFileWriter normalLucWriter;
    public BloomFileWriter normalBloWriter;
    final Object lk;
    private IndexWriter iw = null;
    private List<SFile> Listsf = null; //实际上有且只有一个文件
    AtomicBoolean isDiskBad;

    public CreateIndex(DataSourceConfig dsc, ClientAPI capi, ArrayBlockingQueue buf, LuceneFileWriter norLucWriter, long f_id, Object l) {
        dc = dsc;
        zkUrl = dsc.getZkUrl();
        dbName = dsc.getDbName();
        tbName = dsc.getTbName();
        timeLable = dsc.getTimeLable();
        isEnd = new AtomicBoolean(false);
        isAllOver = new AtomicBoolean(false);
        isShouldNewRaw = new AtomicBoolean(true);
        wList = new ArrayList<WriteIndexFile>();
        wThList = new ArrayList<Thread>();
        ca = capi;
        normalLucWriter = norLucWriter;
        inbuffer = buf;
        file_id = f_id;
        lk = l;
        iw = this.normalLucWriter.getWriterMap().get(0);
        Listsf = this.normalLucWriter.getSfMap().get(0);
        isDiskBad = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        {
            Thread[] wT = new Thread[Indexer.writeIndexThreadNum];
            for (int j = 0; j < Indexer.writeIndexThreadNum; j++) {
                WriteIndexFile wf = new WriteIndexFile(this.dc, inbuffer, this.ca, normalLucWriter, normalBloWriter, this.isDiskBad);
                Thread t = new Thread(wf);
                t.setName(tbName + "_" + "w" + "_" + j + "_" + file_id);
                wT[j] = t;
                wThList.add(t);
                wList.add(wf);
            }
            for (int j = 0; j < Indexer.writeIndexThreadNum; j++) {
                wT[j].start();
            }
            log.info("init threads for " + tbName + " ok");
        }

        Date currentTime = new Date();
        Date commitTime = new Date();
        boolean isOver = false;
        boolean isWaitingShutSignal = true;
        int bufferEmptyCount = 0;
        int currentDocNum = 0;
        long begin = System.currentTimeMillis();

        while (true) {
            if (isOver) {
                log.info("the createIndex thread stop");
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
            }
            if (this.inbuffer.isEmpty()) {
                bufferEmptyCount++;
            } else {
                bufferEmptyCount = 0;
            }
            if (!Indexer.isShouldExit.get()) {
                if (bufferEmptyCount >= 100 * Indexer.maxDelaySeconds) { //连续循环检测1200次发现数据都是空的，表明在这段时间内（300秒）都没有数据发送过来，那么文件就自动关闭
                    isEnd.set(true);
                }
            } else {
                if (isWaitingShutSignal) {
                    bufferEmptyCount = 0;
                    isWaitingShutSignal = false;
                }
                if (bufferEmptyCount >= 100 * Indexer.closeDelay.get()) {
                    isEnd.set(true);
                }
            }

            currentTime = new Date();
            if (currentTime.getTime() - commitTime.getTime() >= Indexer.commitgab * 1000) {
                try {
                    commitTime.setTime(currentTime.getTime());
                    log.info("now do the commit for " + this.dbName + "." + this.tbName);
                    currentDocNum = iw.maxDoc();
                    begin = System.currentTimeMillis();
                    iw.commit();
                    log.info("done commit for " + this.dbName + "." + this.tbName + ",increase doc num is " + (iw.maxDoc() - currentDocNum) + ",use time is " + (System.currentTimeMillis() - begin) + " ms ");
                    /*
                     * fix me
                     */
                    GenericRecord tmpRecord = DataEmiter.getStaticsRecords(this.tbName, this.dbName, (iw.maxDoc() - currentDocNum), "normal", "", "DP_persi_commit");
                    DataEmiter.emit(tmpRecord);

                    isShouldNewRaw.set(true); //设置成创建新的缓存raw文件
                } catch (CorruptIndexException ex) {
                    log.error(ex, ex);
                } catch (IOException ex) {
                    log.error(ex, ex);
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            }

            if (!isEnd.get()) {
                continue;
            } else {  //如果该结束了，就应该通知读和写线程，停止后然做优化,这里不对isEnd进行加锁
                log.info("set the createIndex thread stop done");
                boolean wAliveFlag = true;
                for (int i = 0; i < wList.size(); i++) {
                    log.info("set the write thread " + wThList.get(i).getName() + " stop");
                    wList.get(i).setIsEnd(true);
                }
                while (true) {
                    wAliveFlag = false;
                    for (int i = 0; i < wThList.size(); i++) {
                        if (wThList.get(i).isAlive()) {
                            wAliveFlag = true;
                            log.info("wirte thread " + wThList.get(i).getName() + " is alive");
                            break;
                        }
                    }
                    if (!wAliveFlag) { //写线程没有存活了就break
                        log.info("all write threads stop");
                        isOver = true;
                        break;
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                    }
                }
            }
        }

        long file_legth = 0l;
        long doc_num = 0l;
        log.info("the normal index writer begin to commit,the last time,wait ...");
        try {
            log.info("the normal index writer " + iw.toString() + " commit the last time and merge");
            begin = System.currentTimeMillis();
            iw.commit();
            iw.forceMerge(1);
            /*fix me */
            GenericRecord tmpRecord = DataEmiter.getStaticsRecords(this.tbName, this.dbName, (iw.maxDoc() - currentDocNum), "normal", "", "DP_persi_commit");
            DataEmiter.emit(tmpRecord);

            log.info("index writer close file,at last doc num in is " + (doc_num = iw.maxDoc()) + ",use time is " + (System.currentTimeMillis() - begin) + " ms ");
            iw.close(true);
            try {
                String[] ss = iw.getDirectory().listAll();
                if (ss != null) {
                    for (int i = 0; i < ss.length; i++) {
                        file_legth += iw.getDirectory().fileLength(ss[i]);
                    }
                }
            } catch (Exception e) {
                log.error(e, e);
            }
        } catch (Exception e) {
            log.error(e, e);
        }

        if (MetastoreWrapper.makeSureCloseFile(Listsf.get(0), doc_num, file_legth)) {
            MetastoreWrapper.retryCloseUnclosedFile(Indexer.unclosedFilePath);
        } else {
            MetastoreWrapper.writeFileInfo(Indexer.unclosedFilePath, file_id, doc_num, file_legth);//将未关闭的文件写入单独的文件中
            log.warn("close current file:" + this.file_id + " " + doc_num + " " + file_legth + "fail,there is no need to close the unclosed file");
        }
        /*
         *
         ****** ---- 打印正在在写入的文件的信息，以便线上分析----
         *
         */
        String tb = "";
        int number = 0;
        HashMap<String, Integer> tb2FileMap = new HashMap<String, Integer>();
        for (CreateIndex c : HttpDataHandler.id2Createindex.values()) {
            tb = c.dbName + "." + c.tbName;
            if (!tb2FileMap.containsKey(tb)) {
                number = 1;
            } else {
                number = tb2FileMap.get(tb) + 1;
            }
            tb2FileMap.put(tb, number);
        }
        log.info("close lucene sfile done，now write file number is " + HttpDataHandler.id2Createindex.size() + "," + tb2FileMap.toString());
        /*
         */
        isAllOver.set(true);
    }

    public void setIsEnd(boolean isEnd) {
        this.isEnd.set(isEnd);
    }

    public static void main(String[] args) {
        ZkClient zc = new ZkClient("192.168.120.221:3181");
        String path = "/dfsaafs/dsfa";
        try {
            if (!zc.exists(path)) {
                try {
                    zc.createPersistent(path, true);
                } catch (final ZkNodeExistsException e) {
                    e.printStackTrace();
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception ex) {
        }
    }
}