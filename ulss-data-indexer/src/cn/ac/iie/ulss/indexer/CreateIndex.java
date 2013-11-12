/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer;

import cn.ac.iie.ulss.metastore.MetastoreWrapper;
import cn.ac.iie.ulss.struct.BloomFileWriter;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import iie.metastore.MetaStoreClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;

public class CreateIndex implements Runnable {

    public static Logger log = Logger.getLogger(CreateIndex.class.getName());
    String zkUrl;
    String dbName;
    String tbName;
    String timeLable;
    Long file_id;
    DataSourceConfig dc;
    AtomicBoolean isEnd;
    AtomicBoolean isAllOver;
    AtomicBoolean isShouldNewRaw;
    ArrayBlockingQueue inbuffer;
    List<WriteIndexFile> wList;
    List<Thread> wThList;
    MetaStoreClient msCli;
    LuceneFileWriter normalLucWriter;
    BloomFileWriter normalBloWriter;
    final Object lk;
    final Object clientLock;  //用于协调各个线程对client进行操作的锁
    private IndexWriter iw = null;
    private List<SFile> Listsf = null;
    private String location;

    public CreateIndex(DataSourceConfig dsc, MetaStoreClient msc, ArrayBlockingQueue buf, LuceneFileWriter norLucWriter, long f_id, Object l, Object clientLk) {
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
        msCli = msc;
        normalLucWriter = norLucWriter;
        inbuffer = buf;
        file_id = f_id;
        lk = l;
        clientLock = clientLk;
        iw = this.normalLucWriter.getWriterMap().get(0);
        Listsf = this.normalLucWriter.getSfMap().get(0);
    }

    @Override
    public void run() {
        {
            Thread[] wT = new Thread[Indexer.writeIndexThreadNum];
            for (int j = 0; j < Indexer.writeIndexThreadNum; j++) {
                WriteIndexFile wf = new WriteIndexFile(this.dc, inbuffer, this.msCli, normalLucWriter, normalBloWriter, this.clientLock);
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


        Date curTime = new Date();
        Date commitTime = new Date();
        boolean isOver = false;

        int noDataCount = 0;
        while (true) {
            if (isOver) {
                log.info("the createIndex thread stop");
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException ex) {
            }
            if (this.inbuffer.isEmpty()) {
                noDataCount++;
            } else {
                noDataCount = 0;
            }
            if (noDataCount / 2 >= 4 * Indexer.maxDelaySeconds) { //连续循环检测1200次发现数据都是空的，表明在这段时间内（300秒）都没有数据发送过来，那么文件就自动关闭
                isEnd.set(true);
            }

            curTime = new Date();
            if (curTime.getTime() - commitTime.getTime() >= Indexer.commitgab * 1000) {
                try {
                    commitTime.setTime(curTime.getTime());
                    log.info("now do the commit for " + this.dbName + "." + this.tbName);
                    iw.commit();
                    log.info("now done the commit for " + this.dbName + "." + this.tbName);
                    synchronized (this.lk) {
                        HttpDataHandler.closeAvroFile(HttpDataHandler.id2cachefile.get(file_id));
                        HttpDataHandler.id2cachefile.remove(file_id);
                        isShouldNewRaw.set(true); //设置成创建新的文件
                    }
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
            iw.commit();
            iw.forceMerge(1);
            log.info("the normal index writer close file,in the end doc num in this index file is " + (doc_num = iw.maxDoc()));
            iw.close(true);
            synchronized (this.lk) {
                HttpDataHandler.closeAvroFile(HttpDataHandler.id2cachefile.get(file_id));
                HttpDataHandler.id2cachefile.remove(file_id);
            }
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

        synchronized (this.lk) {
            if (MetastoreWrapper.makeSureCloseFile(Listsf, doc_num, file_legth)) {
                MetastoreWrapper.retryCloseUnclosedFile(Indexer.unclosedFilePath);
            } else {
                MetastoreWrapper.writeFileInfo(Indexer.unclosedFilePath, file_id, doc_num, file_legth);//将未关闭的文件写入单独的文件中
                log.warn("close current file:" + this.file_id + " fail,there is no need to close the unclosed file");
            }
        }
        log.info("close normal lucene sfile done");
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
/*
 *
 *
 synchronized (this.lk) {
 try {
 for (SFile sf : Listsf) {
 HttpDataHandler.id2Createindex.remove(sf.getFid());
 }
 for (SFile sf : Listsf) {
 log.info("close normal sfile " + sf.toString());  //HttpDataHandler.closedIdMap.put(file_id, file_id);
 sf.setRecord_nr(doc_num);
 sf.setLength(file_legth);
 log.info("after update,the close normal sfile is : " + sf.toString());
 msCli.client.close_file(sf);
 }
 } catch (FileOperationException e) {
 log.warn("when close file get warn： " + e, e);
 } catch (TException e) {
 log.error(e, e);
 MetaStoreClient cli = CreateIndex.makeSureGetCli();
 if (cli != null) {
 Indexer.cli = cli;
 } else {
 log.error("when get the metastore cli get fetal error !");
 }
 } catch (Exception e) {
 log.error(e, e);
 }
 CreateIndex.retryCloseBadFile();
 }
 *
 *
 */