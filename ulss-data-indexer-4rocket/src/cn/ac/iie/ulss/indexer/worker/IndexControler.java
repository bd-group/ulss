/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.datastatics.DataEmiter;
import cn.ac.iie.ulss.indexer.metastore.MetastoreWrapper;
import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.struct.PoolWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;

public class IndexControler implements Runnable {

    public static Logger log = Logger.getLogger(IndexControler.class.getName());
    public String dbName;
    public String tbName;
    public String timeLable;
    public Long file_id;
    public DataSourceConfig dc;
    public AtomicBoolean isEnd;
    public AtomicBoolean isAllOver;
    public AtomicBoolean isShouldNewRaw;
    public ArrayBlockingQueue inbuffer;
    public LuceneFileWriter normalLucWriter;
    private IndexWriter iw = null;
    private List<SFile> Listsf = null; //实际上有且只有一个文件
    AtomicBoolean isDiskBad;
    AtomicBoolean isFileStatusBad;
    /*
     */
    AtomicLong willWriteNum;
    AtomicLong failWriteNum;
    /*
     * 
     */
    ExecutorService threadPool;

    public IndexControler(DataSourceConfig dsc, ArrayBlockingQueue buf, LuceneFileWriter norLucWriter, long f_id) {
        dc = dsc;
        dbName = dsc.getDbName();
        tbName = dsc.getTbName();
        timeLable = dsc.getTimeLable();
        isEnd = new AtomicBoolean(false);
        isAllOver = new AtomicBoolean(false);
        isShouldNewRaw = new AtomicBoolean(true);
        normalLucWriter = norLucWriter;
        inbuffer = buf;
        file_id = f_id;
        iw = this.normalLucWriter.getWriterMap().get(0);
        /*
         * fix me
         */
        if (!GlobalParas.isTestMode) {
            Listsf = this.normalLucWriter.getSfMap().get(0);
        }
        isDiskBad = new AtomicBoolean(false);
        isFileStatusBad = new AtomicBoolean(false);
        /*
         */
        willWriteNum = new AtomicLong(0);
        failWriteNum = new AtomicLong(0);
    }

    @Override
    public void run() {
        int tone_up = 2;
        int sleepMilis = 40;
        int printGab = 1000;
        int printCount = 0;

        ArrayBlockingQueue<PoolWriter> writerPool = new ArrayBlockingQueue<PoolWriter>(GlobalParas.luceneWriterMaxParallelNum * tone_up);
        ArrayList<Future> executeResultPool = new ArrayList<Future>(GlobalParas.luceneWriterMaxParallelNum * tone_up);
        Iterator<Future> it = null;
        Future tmpFuture = null;
        Date currentTime = new Date();
        Date commitTime = new Date();

        int currentParallelNum = 0;
        boolean isOver = false;
        boolean isWaitingShutSignal = true;
        int bufferEmptyCount = 0;
        long currentDocNum = 0;
        long oldDocNum = 0;
        long lastWriteTime = System.currentTimeMillis();
        long tmp = System.currentTimeMillis();
        long begin = System.currentTimeMillis();

        for (int i = 0; i < GlobalParas.luceneWriterMaxParallelNum * tone_up; i++) {
            PoolWriter pw = new PoolWriter();
            pw.dataWriter = new DataWriter();
            pw.result = null;
            try {
                writerPool.put(pw);
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }

        GenericRecord tmpGenericRecord = null;
        GenericArray docSet = null;
        int bufferSizeInfact = 0;
        DataWriter dataWriter = null;
        PoolWriter pw = null;
        Future future = null;
        while (true) {
            if (isOver) {
                log.info("the index control thread stop done");
                break;
            }
            try {
                Thread.sleep(sleepMilis);
            } catch (InterruptedException ex) {
            }
            if (this.inbuffer.isEmpty()) {
                bufferEmptyCount++;
            } else {
                bufferEmptyCount = 0;
            }

            if (!GlobalParas.isShouldExit.get()) {
                if (bufferEmptyCount >= (1000 / sleepMilis) * GlobalParas.maxDelaySeconds) { //连续循环检测1200次发现数据都是空的，表明在这段时间内（300秒）都没有数据发送过来，那么文件就自动关闭
                    isEnd.set(true);
                }
            } else {
                if (isWaitingShutSignal) {
                    bufferEmptyCount = 0;
                    isWaitingShutSignal = false;
                    log.info("receive the kill signal,will do something ...");
                }
                if (bufferEmptyCount >= (1000 / sleepMilis) * GlobalParas.closeDelay.get()) {
                    isEnd.set(true);
                    log.info("the file will close,will set the write thread close");
                }
            }

            it = executeResultPool.iterator();
            while (it.hasNext()) {   /*得到并发数，控制单个文件消耗的并发数，否则会造成单个文件消耗过多的线程，indexerwriter的最高效率的并发数为8，超过此并发数就无法提高效率，浪费资源 */
                tmpFuture = it.next();
                if (tmpFuture.isDone()) {
                    it.remove();
                }
            }
            currentParallelNum = executeResultPool.size();

            printCount++;
            if (printCount % (printGab / sleepMilis) == 0) {
                if (currentParallelNum >= 4) {
                    log.info("before dispatch new task,the parallel num is " + currentParallelNum + ",the data pool size is " + this.inbuffer.size());
                }
            }

            //if ((!this.inbuffer.isEmpty() || (System.currentTimeMillis() - lastWriteTime >= 1000 * 2)) && currentParallelNum < Indexer.luceneWriterMaxParallelNum) {  //并发数不超过限制，并且输入缓冲区中不为空
            if (!this.inbuffer.isEmpty() && currentParallelNum < GlobalParas.luceneWriterMaxParallelNum) {  //并发数不超过限制，并且输入缓冲区中不为空
                bufferEmptyCount = 0;
                try {
                    pw = null;
                    future = null;
                    pw = writerPool.take();
                    future = pw.result;
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
                if (future == null || (future != null && future.isDone())) {
                    dataWriter = pw.dataWriter;
                    if (dataWriter != null) {
                        dataWriter.dbName = this.dbName;
                        dataWriter.tableName = this.tbName;
                        dataWriter.file_id = this.file_id;
                        dataWriter.isDiskBad = this.isDiskBad;
                        dataWriter.willwriteDocNum = this.willWriteNum;
                        dataWriter.failwriteDocNum = this.failWriteNum;
                        dataWriter.writer = this.normalLucWriter.getWriterMap().get(0);

                        bufferSizeInfact = 0;
                        tmp = System.currentTimeMillis();
                        while (true) {
                            //bufferSizeInfact = 0;
                            try {
                                tmpGenericRecord = (GenericRecord) this.inbuffer.poll(50, TimeUnit.MILLISECONDS);
                            } catch (Exception ex) {
                                log.error(ex, ex);
                            }
                            if (tmpGenericRecord != null) {
                                dataWriter.innerDataBuf.add(tmpGenericRecord);
                                bufferSizeInfact += ((GenericData.Array<GenericRecord>) tmpGenericRecord.get(GlobalParas.docs_set)).size();
                            }
                            if (bufferSizeInfact >= GlobalParas.writeMinNumOnce || (System.currentTimeMillis() - tmp) >= 1000 * GlobalParas.submitTimeout) {
                                log.info("will submit one task ...");
                                Future f = threadPool.submit(dataWriter);
                                pw.result = f;
                                executeResultPool.add(f);
                                currentParallelNum++;
                                lastWriteTime = System.currentTimeMillis();
                                break;
                            }
                        }
                    } else {
                        log.warn("null command,it is wrong ...");
                    }
                }
                try {
                    writerPool.put(pw);
                } catch (Exception e) {
                    log.error(e, e);
                }
            }
            if (printCount % (printGab / sleepMilis) == 0) {
                if (currentParallelNum >= 4) {
                    log.info("after dispatch new task,the parallel num is " + currentParallelNum + ",the data pool size is " + this.inbuffer.size());
                }
                printCount = 0;
            }

            currentTime = new Date();
            if (currentTime.getTime() - commitTime.getTime() >= GlobalParas.commitgab * 1000) {
                try {
                    commitTime.setTime(currentTime.getTime());
                    log.info("now do the commit for " + this.dbName + "." + this.tbName);
                    begin = System.currentTimeMillis();
                    iw.commit();

                    /*每次commit成功后就把isDiskBad设置成false,commit不成功时会抛异常,this.isDiskBad.set(false)就不会执行
                     * 相当于为磁盘的写入增加了一种定时检测、恢复机可以使文件写入从异常状态中恢复
                     */
                    this.isDiskBad.set(false);

                    currentDocNum = iw.maxDoc();
                    log.info("done commit for " + this.dbName + "." + this.tbName + ",increase doc num is " + (currentDocNum - oldDocNum) + ",use " + (System.currentTimeMillis() - begin) + " ms ");
                    oldDocNum = currentDocNum;

                    /* fix me
                     if (!GlobalParas.isTestMode) {
                     this.willWriteNum.set(0l);
                     GenericRecord tmpRecord = DataEmiter.getStaticsRecords(this.tbName, this.dbName, (currentDocNum - oldDocNum), "normal", "", "DP", "out");
                     DataEmiter.emit(tmpRecord);
                     }*/

                    /*fix me */
                    //isShouldNewRaw.set(true); //设置成创建新的缓存raw文件
                } catch (CorruptIndexException ex) {
                    log.error(ex, ex);
                    /* fix me
                     if (!GlobalParas.isTestMode) {
                     long failnum = this.willWriteNum.get();
                     this.willWriteNum.set(0l);
                     GenericRecord tmpRecord = DataEmiter.getStaticsRecords(this.tbName, this.dbName, failnum, "abnormal", ex.toString(), "DP", "out");
                     DataEmiter.emit(tmpRecord);
                     }*/
                } catch (IOException ex) {
                    log.error(ex, ex);
                    /*  fix me
                     if (!GlobalParas.isTestMode) {
                     long failnum = this.willWriteNum.get();
                     this.willWriteNum.set(0l);
                     GenericRecord tmpRecord = DataEmiter.getStaticsRecords(this.tbName, this.dbName, failnum, "abnormal", ex.toString(), "DP", "out");
                     DataEmiter.emit(tmpRecord);
                     }*/
                } catch (Exception ex) {
                    log.error(ex, ex);
                    /* fix me
                     if (!GlobalParas.isTestMode) {
                     long failnum = this.willWriteNum.get();
                     this.willWriteNum.set(0l);
                     GenericRecord tmpRecord = DataEmiter.getStaticsRecords(this.tbName, this.dbName, failnum, "abnormal", ex.toString(), "DP", "out");
                     DataEmiter.emit(tmpRecord);
                     }*/
                }
            }
            if (!isEnd.get()) {
                continue;
            } else {
                log.info("will clear the data and flush to the disk,then commit,close ... ");
                isOver = true;
            }
        }

        long file_legth = 0l;
        long doc_num = 0l;
        log.info("the normal index writer begin to commit,the last time,wait ...");
        try {
            log.info("the normal index writer " + iw.toString() + " commit the last time and merge");
            begin = System.currentTimeMillis();
            iw.commit();
            doc_num = iw.maxDoc();
            log.info("at last indexwriter commit,doc num in is " + doc_num + ",use " + (System.currentTimeMillis() - begin) + " ms ");

            iw.forceMerge(1);
            log.info("at last indexwriter forceMerge,doc num in is " + (doc_num = iw.maxDoc()) + ",use " + (System.currentTimeMillis() - begin) + " ms ");

            /* fix me 
             if (!GlobalParas.isTestMode) {
             this.willWriteNum.set(0l);
             GenericRecord tmpRecord = DataEmiter.getStaticsRecords(this.tbName, this.dbName, (doc_num - oldDocNum), "normal", "", "DP", "out");
             DataEmiter.emit(tmpRecord);
             }*/
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
            log.info("indexwriter commit,merge,close lucene all done,at last doc num in is " + (doc_num = iw.maxDoc()) + ",use time in total is " + (System.currentTimeMillis() - begin) + " ms ");
            iw.close();
        } catch (Exception e) {
            /* fix me 
             if (!GlobalParas.isTestMode) {
             long failnum = this.willWriteNum.get();
             this.willWriteNum.set(0l);
             GenericRecord tmpRecord = DataEmiter.getStaticsRecords(this.tbName, this.dbName, failnum, "abnormal", e.toString(), "DP", "out");
             DataEmiter.emit(tmpRecord);
             }*/
            log.error(e, e);
        }

        if (!GlobalParas.isTestMode) {
            if (isFileStatusBad.get()) {
                Listsf.get(0).setLoad_status(1);
            }
            if (MetastoreWrapper.closeFile(Listsf.get(0), doc_num, file_legth)) {
                MetastoreWrapper.retryCloseUnclosedFile(GlobalParas.unclosedFilePath);
            } else {
                MetastoreWrapper.writeFileInfo(GlobalParas.unclosedFilePath, file_id, doc_num, file_legth);//将未关闭的文件写入单独的文件中
                log.warn("close current file:" + this.file_id + " " + doc_num + " " + file_legth + "fail,there is no need to close the unclosed file");
            }
        }
        /*
         * ---- 打印正在在写入的文件的信息，以便线上分析 ----
         */
        String tb = "";
        int number = 0;
        HashMap<String, Integer> tb2FileMap = new HashMap<String, Integer>();
        for (IndexControler c : GlobalParas.id2Createindex.values()) {
            tb = c.dbName + "." + c.tbName;
            if (!tb2FileMap.containsKey(tb)) {
                number = 1;
            } else {
                number = tb2FileMap.get(tb) + 1;
            }
            tb2FileMap.put(tb, number);
        }
        log.info("close lucene sfile done，now write file number is " + GlobalParas.id2Createindex.size() + "," + tb2FileMap.toString());
        /*
         */
        isAllOver.set(true);
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
//
//
//
//if (this.inbuffer.size() >= Indexer.writeInnerpoolSize || System.currentTimeMillis() - lastWriteTime >= 1000 * 3) {
//if (!this.inbuffer.isEmpty() && currentParallelNum < Indexer.luceneWriterMaxParallelNum) {  //并发数不超过限制，并且输入缓冲区中不为空
/*
 * 
 this.inbuffer.drainTo(dataWriter.innerDataBuf, Indexer.writeInnerpoolBatchDrainSize);
 for (GenericRecord ms : dataWriter.innerDataBuf) {
 docSet = (GenericData.Array<GenericRecord>) ms.get(Indexer.docs_set);
 bufferSizeInfact += docSet.size();
 docSet = null;
 * 
 * 
 }
 */
