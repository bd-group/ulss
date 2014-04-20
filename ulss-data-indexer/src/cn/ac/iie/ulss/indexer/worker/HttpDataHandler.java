/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.datastatics.DataEmiter;
import cn.ac.iie.ulss.indexer.metastore.MetastoreWrapper;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.utiltools.FileFilter;
import cn.ac.iie.ulss.utiltools.LuceneWriterUtil;
import devmap.DevMap;
import iie.metastore.MetaStoreClient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.thrift.TException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class HttpDataHandler extends AbstractHandler {

    public static Logger log = Logger.getLogger(HttpDataHandler.class.getName());
    private static SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static Schema docsSchema = null;
    public static DatumReader<GenericRecord> docsReader = null;
    private static AtomicLong receiveIntotal = new AtomicLong(0);
    public static AtomicLong activethreadNum = new AtomicLong(0);
    public static ConcurrentHashMap<Long, IndexControler> id2Createindex = new ConcurrentHashMap<Long, IndexControler>();  //file_id到createIndex的映射
    public static ConcurrentHashMap<Long, WriteRawfile> id2WriteRawfile = new ConcurrentHashMap<Long, WriteRawfile>();  //file_id到createIndex的映射
    public static final Object o = new Object();

    @Override
    public void handle(String string, Request rqst, HttpServletRequest hsr, HttpServletResponse hsResonse) {
        HttpDataHandler.activethreadNum.incrementAndGet();
        String reqID = String.valueOf(System.nanoTime());
        long bg = System.currentTimeMillis();
        long bgbg = bg;
        try {
            rqst.setHandled(true);
            String action = rqst.getHeader("cmd");
            log.info("handle http request " + action + ",request id:" + reqID);
            /*
             * createfile 必须同步，在网络环境中，前端可能因为种种原因导致重复发送一个createfile指令，
             * 如果同时对一个lucene文件进行操作会出问题，create lucene物理文件的频率较低，同步代价可以接受;
             * 但如果磁盘异常，导致物理文件无法创建，需要另外处理
             */
            if (action.startsWith("createfile")) {
                long file_id = 0;
                String fullPath = "";
                synchronized (HttpDataHandler.o) {
                    if (Indexer.isTestMode) {
                        file_id = Long.parseLong(action.split("[|]")[1]);
                        fullPath = action.split("[|]")[2];
                        IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath);
                        if (FSWriter != null) {
                            FSWriter.commit();
                            FSWriter.close();
                            FSWriter = null;
                        } else {
                            log.error("the test lucene file is null,why ??");
                        }

                        try {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("0\nbzs_s\nulss_s");
                        } catch (Exception e) {
                            log.error(e, e);
                        }

                    } else {
                        file_id = Long.parseLong(action.split("[|]")[1]);
                        String filelocation = action.split("[|]")[2];
                        String devId = action.split("[|]")[3];
                        DevMap dm = new DevMap();
                        try {
                            fullPath = dm.getPath(devId, filelocation);
                        } catch (Exception e) {
                            try {
                                hsResonse.setStatus(HttpServletResponse.SC_OK);
                                hsResonse.getWriter().println("-1\n" + " get path fail for file_id:" + file_id + " " + e);
                            } catch (Exception ex) {
                                log.error(ex, ex);
                            }
                            log.error(e, e);
                            return;
                        }

                        StringBuilder sb = new StringBuilder();
                        SFile sf = MetastoreWrapper.getFile(file_id, sb);
                        if (sf == null) {
                            String message = sb.toString();
                            try {
                                hsResonse.setStatus(HttpServletResponse.SC_OK);
                                hsResonse.getWriter().println("-1\n" + "get one new file from metastore error for " + message + ",file_id" + file_id);
                                log.error("get one new sfile from metastore error for " + message + ",file_id " + file_id + ",request id:" + reqID);
                            } catch (Exception ex) {
                                log.error(ex, ex);
                            }
                            return;
                        }
                        log.info("now will init file，new file id will be file_id " + file_id + ",request id:" + reqID + ",now the increate state file num is " + id2Createindex.size() + ",file ids are " + id2Createindex.keySet());
                        bg = System.currentTimeMillis();
                        try {
                            IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath);
                            if (FSWriter != null) {
                                FSWriter.commit();
                                FSWriter.close();
                            } else {
                                /* FSWriter==null
                                 * 情况一：此createfile指令不是第一次收到，磁盘上已经初始化了相关file_id对应的lucene文件
                                 * 情况二：磁盘只读或者爆满，此时不能返回0，只能返回-1，以下的处理方式存在问题
                                 */
                                try {
                                    log.warn("get null IndexWriter for " + "request id:" + reqID + "file_id:" + file_id + " " + fullPath + ",maybe it has been created");
                                    hsResonse.setStatus(HttpServletResponse.SC_OK);
                                    hsResonse.getWriter().println("-1\n" + "get null IndexWriter for " + "request id:" + reqID + "file_id:" + file_id + " " + fullPath);
                                } catch (Exception e) {
                                    log.error(e, e);
                                }
                                return;
                            }
                        } catch (Exception e) {
                            log.error(e, e);
                        }
                        log.info("init lucene file use " + (System.currentTimeMillis() - bg) + " ms for request id:" + reqID + " <---> " + action);
                        sf.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
                        sb = new StringBuilder();
                        if (MetastoreWrapper.onlineFileLocation(sf, sb) == false) {
                            try {
                                hsResonse.setStatus(HttpServletResponse.SC_OK);
                                hsResonse.getWriter().println("-1\n" + "online operation error for: " + sb.toString() + ",file_id " + file_id);
                                log.error("online operation error for: " + sb.toString() + ",request id " + reqID + ",file_id " + file_id);
                            } catch (Exception ex) {
                                log.error(ex, ex);
                            }
                            return;
                        }
                        try {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("0\nbzs_s\nulss_s");
                        } catch (Exception e) {
                            log.error(e, e);
                        }
                        log.info("now init new file " + file_id + " and the physical lucene file is ok");
                    }
                }
                log.info("create file use " + (System.currentTimeMillis() - bgbg) + " ms for request id:" + reqID + " <---> " + action);
            } else {
                long file_id = 0;
                String filelocation = null;
                String devId = null;
                bg = System.currentTimeMillis();

                if (Indexer.isTestMode) {
                    synchronized (HttpDataHandler.o) {
                        file_id = Long.parseLong(action.split("[|]")[0]);
                        if (!HttpDataHandler.id2Createindex.containsKey(file_id)) {
                            //file_id = Long.parseLong(action.split("[|]")[0]);
                            String fullPath = action.split("[|]")[1];
                            DataSourceConfig ds = new DataSourceConfig("sichuan", "t_ybrz", "fortest");
                            ConcurrentHashMap<Integer, IndexWriter> hashWriterMap = new ConcurrentHashMap<Integer, IndexWriter>();
                            IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, false);
                            FSWriter.close();
                            FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, false);
                            hashWriterMap.put(0, FSWriter);
                            LuceneFileWriter writer = new LuceneFileWriter(hashWriterMap, null);
                            ArrayBlockingQueue buf = new ArrayBlockingQueue(Indexer.readbufSize);
                            IndexControler c = new IndexControler(ds, buf, writer, file_id);
                            c.threadPool = Workerpool.hlwrzpool;
                            Thread ct = new Thread(c);
                            ct.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                            ct.start();
                            HttpDataHandler.id2Createindex.put(file_id, c);
                            try {
                                hsResonse.setStatus(HttpServletResponse.SC_OK);
                                hsResonse.getWriter().println("0\nbzs_s\nulss_s");
                            } catch (Exception e) {
                                log.error(e, e);
                            }
                        }
                    }
                } else {
                    file_id = Long.parseLong(action.split("[|]")[0]);
                    filelocation = action.split("[|]")[1];
                    devId = action.split("[|]")[2];
                    if (HttpDataHandler.id2Createindex.containsKey(file_id)) {
                        //log.warn("receive one command that create file twice --> " + action);
                    } else {
                        synchronized (HttpDataHandler.o) {
                            try { /*不包括这个file_id的话证明对应的create线程已经结束或者还没有开始，那么就要进行创建*/
                                if (!HttpDataHandler.id2Createindex.containsKey(file_id)) {  //与CreateIndex中的HttpDataHandler.key2Createindex.remove(sf.getFid()) 语句同步?
                                    List<SFile> luceneFileList = new ArrayList<SFile>();
                                    log.info("will get one exist file,the file id;current thread info is " + file_id + ";" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId());
                                    StringBuilder sb = new StringBuilder();
                                    SFile sf = MetastoreWrapper.getFile(file_id, sb);
                                    String message = "";
                                    if (sf == null) {
                                        log.info("get one file fail,the file is null,and the file id is " + file_id);
                                        message = sb.toString();
                                        hsResonse.setStatus(HttpServletResponse.SC_OK);
                                        hsResonse.getWriter().println("-1\n" + "get file error for " + message + ",and the file_id is " + file_id);
                                        return;
                                    }
                                    if (sf.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
                                        log.error("get the not in create status file_id " + file_id + "，will resonse the client ");
                                        hsResonse.setStatus(HttpServletResponse.SC_OK);
                                        hsResonse.getWriter().println("-2\n" + "the file for file id " + file_id + " has been closed,may be your buffer has not been cleaned over ?");
                                        return;
                                    }
                                    log.info("get one exist file done,the file is " + sf.toString());
                                    String db = sf.getDbName();
                                    String tb = sf.getTableName();
                                    luceneFileList.add(sf);
                                    if ("".equalsIgnoreCase(filelocation) || filelocation == null) {
                                        log.error("get file needed to be read error for no available file location,and the file_id is " + file_id);
                                        hsResonse.setStatus(HttpServletResponse.SC_OK);
                                        hsResonse.getWriter().println("-1\n" + "get file needed to be read error for no available file location,and the file_id is " + file_id); //一旦发现不能创建文件，就要退出并且回复，使其重新选择
                                        return;
                                    }
                                    DevMap dm = new DevMap();
                                    String fullPath = dm.getPath(devId, filelocation);
                                    log.info("will get the lucene file from disk,the fullpath is " + fullPath);

                                    String stTime = "";
                                    String edTime = "";
                                    for (int j = 0; j < 2; j++) {
                                        long time = Long.parseLong(sf.getValues().get(j).getValue().trim());
                                        if (j == 0) {
                                            stTime = String.valueOf(time);
                                        } else {
                                            edTime = String.valueOf(time);
                                        }
                                    }
                                    log.info("now set the time flag for file " + file_id + "to " + stTime + "_" + edTime);
                                    DataSourceConfig ds = new DataSourceConfig(db, tb, stTime);
                                    ConcurrentHashMap<Integer, IndexWriter> hashWriterMap = new ConcurrentHashMap<Integer, IndexWriter>();
                                    ConcurrentHashMap<Integer, List<SFile>> luceneSFMap = new ConcurrentHashMap<Integer, List<SFile>>();

                                    IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, false);
                                    FSWriter.close();
                                    FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, false);

                                    log.info("get one lucene physicical file ok ->" + file_id + ";" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId());

                                    hashWriterMap.put(0, FSWriter);
                                    luceneSFMap.put(0, luceneFileList);
                                    LuceneFileWriter writer = new LuceneFileWriter(hashWriterMap, luceneSFMap);

                                    ArrayBlockingQueue buf = new ArrayBlockingQueue(Indexer.readbufSize);
                                    IndexControler c = new IndexControler(ds, buf, writer, file_id);
                                    //fix me
                                    c.threadPool = Workerpool.hlwrzpool;
                                    Thread ct = new Thread(c);
                                    ct.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                                    ct.start();
                                    HttpDataHandler.id2Createindex.put(file_id, c);
                                    /*
                                     * write raw file thread
                                     */
                                    ArrayBlockingQueue writebuf = new ArrayBlockingQueue(Indexer.readbufSize);
                                    WriteRawfile wr = new WriteRawfile(c, Indexer.cachePath, filelocation, devId, tb, file_id, writebuf);
                                    Thread wrt = new Thread(wr);
                                    wrt.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                                    wrt.start();
                                    HttpDataHandler.id2WriteRawfile.put(file_id, wr);

                                    log.info("apply one file from metastore use  " + (System.currentTimeMillis() - bg) + " ms for " + reqID + " <---> " + action);
                                }
                            } catch (Exception e) {
                                log.error(e, e);
                                try {
                                    hsResonse.setStatus(HttpServletResponse.SC_OK);
                                    hsResonse.getWriter().println("-1\n" + " new lucene file is failed for " + e.getMessage()); //一旦发现不能创建文件，就要退出并且回复，使其重新选择
                                } catch (Exception ex) {
                                    log.error(ex, ex);
                                }
                                return;
                            }
                        }
                    }
                }

                ServletInputStream servletInputStream = null;
                ByteArrayOutputStream out = null;
                try {
                    servletInputStream = rqst.getInputStream();
                    out = new ByteArrayOutputStream();
                    int i = 0;
                    byte[] b = new byte[4096];
                    while ((i = servletInputStream.read(b, 0, 4096)) > 0) {
                        out.write(b, 0, i);
                    }
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
                byte[] req = out.toByteArray();
                log.info("now recv data length " + req.length + ",src host is " + rqst.getRemoteHost() + ":" + rqst.getRemotePort() + ",request id:" + reqID);

                ByteArrayInputStream docsbis = new ByteArrayInputStream(req);
                BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
                GenericRecord docsRecord = new GenericData.Record(docsSchema);

                bg = System.currentTimeMillis();
                String schemaName = "";
                GenericData.Array<GenericRecord> tmp = null;
                IndexControler createIdx = null;
                try {
                    docsReader.read(docsRecord, docsbd);
                    tmp = (GenericData.Array<GenericRecord>) docsRecord.get(Indexer.docs_set);
                    //receiveIntotal.addAndGet(tmp.size());
                    //log.info("receive in total " + reqID + " " + action + " " + receiveIntotal.get() + ",this time receive " + tmp.size());
                    schemaName = docsRecord.get(Indexer.docs_schema_name).toString().toLowerCase();
                    /*
                     * 往缓冲区加数据的时候，如果正好处于线程要结束的时刻
                     *（假定createIndex线程，中检测数据长时间为不到达的机制在某些情况下是运转不正常的
                     * 那么下面这个步骤就有可能出问题，如何解决这个问题呢，要进行同步
                     */
                    createIdx = id2Createindex.get(file_id);
                    if (createIdx == null) {
                        try {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("-2\n" + "warning:the file has been closed for file_id " + file_id);
                        } catch (Exception ex) {
                            log.error(ex, ex);
                        }
                        log.error("the file has been closed for request id " + reqID + " <---> " + action);
                        return;
                    } else if (!id2Createindex.get(file_id).isEnd.get()) {
                        if (id2Createindex.get(file_id).isDiskBad.get()) {
                            try {
                                hsResonse.setStatus(HttpServletResponse.SC_OK);   //就是createindex线程已经将状态置为空了
                                hsResonse.getWriter().println("-1\n" + "error:the disk is bad,can not continue write ");
                            } catch (Exception ex) {
                                log.error(ex, ex);
                            }
                            log.error("the file request id " + reqID + " <---> " + action + " bad for disk error");
                            //fix me
                            //if (!Indexer.isTestMode) {
                            GenericRecord tmpRecord = DataEmiter.getStaticsRecords(schemaName, createIdx.dbName, tmp.size(), "abnormal", "disk error", "DP", "in");
                            DataEmiter.emit(tmpRecord);
                            //}

                        } else {
                            //synchronized (createIdx) {
                            HttpDataHandler.id2Createindex.get(file_id).inbuffer.put(docsRecord);//将数据写入对象的缓冲区中
                            //HttpDataHandler.id2WriteRawfile.get(file_id).buffer.put(docsRecord);//将数据写入对象的缓冲区中
                            //}
                            //if (!Indexer.isTestMode) {
                            GenericRecord tmpRecord = DataEmiter.getStaticsRecords(schemaName, createIdx.dbName, tmp.size(), "normal", "", "DP", "in");
                            DataEmiter.emit(tmpRecord);
                            //}
                        }
                    } else {
                        try {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);   //就是createindex线程已经将状态置为空了
                            hsResonse.getWriter().println("-2\n" + "warning:the file has been closed ！");
                            log.error("warning:the file has been closed,request id " + reqID + " <---> " + action);
                        } catch (Exception ex) {
                            log.error(ex, ex);
                        }
                    }
                    log.info("put data to buffer use  " + (System.currentTimeMillis() - bg) + " ms for request id " + reqID + " <---> " + action);
                    /*
                     * write rawfile
                     */
                } catch (Exception ex) {
                    try {
                        hsResonse.setStatus(HttpServletResponse.SC_OK);
                        hsResonse.getWriter().println("-1\n" + " parse the docs is failed for " + ex.getMessage());
                    } catch (Exception ex1) {
                        log.error(ex1, ex1);
                    }
                    log.error("parse the docs is failed for " + ex, ex);

                    //if (!Indexer.isTestMode) {
                    GenericRecord tmpRecord = DataEmiter.getStaticsRecords(schemaName, createIdx.dbName, tmp.size(), "abnormal", ex.toString(), "DP", "in");
                    DataEmiter.emit(tmpRecord);
                    //}
                }
            }
            log.info("handle the http request use time in total is " + (System.currentTimeMillis() - bgbg) + " ms for " + " request id:" + reqID + " <---> " + action);
        } catch (Exception e) {
            log.error(e, e);
        }
        /*
         */
        HttpDataHandler.activethreadNum.decrementAndGet();
    }

    public static HttpDataHandler getDataHandler() {
        HttpDataHandler dataHandler = new HttpDataHandler();
        try {
            Protocol protocol = Protocol.parse(Indexer.schemaname2schemaContent.get(Indexer.docs));
            HttpDataHandler.docsSchema = protocol.getType(Indexer.docs);
            HttpDataHandler.docsReader = new GenericDatumReader<GenericRecord>(HttpDataHandler.docsSchema);
        } catch (Exception ex) {
            log.error("constructing data dispath handler is failed docs for " + ex, ex);
            dataHandler = null;
        }

        if (docsSchema == null) {
            log.error("schema docs is not found in metadb,please check metadb to ensure that docs schema exist");
            dataHandler = null;
        }

        try {
            int countC = 0;
            MetaStoreClient msc = Indexer.clientPool.getClient();
            countC = HttpDataHandler.getBadData(Indexer.cachePath, msc);
            Indexer.clientPool.realseOneClient(msc);
            log.info("get unresolved data from the disk for index，get number is " + countC);
        } catch (Exception ex) {
            log.error("when get unresolved data error occurs: " + ex, ex);
        }
        return dataHandler;
    }

    public static int getBadData(String rawDataPath, MetaStoreClient cli) throws IOException, FileOperationException, MetaException, TException, InterruptedException {
        File file = new File(rawDataPath);
        String[] nameList = file.list(new FileFilter(".*\\.ori"));
        int countc = 0;
        if (nameList == null) {
            log.info("get ori file number is 0");
            return -1;
        }
        log.info("get ori file number is " + nameList.length);

        for (int i = 0; i < nameList.length; i++) {
            try {
                Long file_id = Long.parseLong(nameList[i].split("[+]")[0]);//file_id + "_" + devId + "_" + pathInfo + "_" + schemaName + "_" + System.currentTimeMillis() / 1000 + ".ori";
                String devId = nameList[i].split("[+]")[1];
                String filePath = nameList[i].split("[+]")[2];
                filePath = "/" + filePath.replace('-', '/');

                StringBuilder sb = new StringBuilder();
                SFile sf = MetastoreWrapper.getFile(file_id, sb);

                log.info("now do the recover operation for " + sf.toString());
                String db = sf.getDbName();
                String tb = sf.getTableName();
                DevMap dm = new DevMap();
                String fullFilePath = dm.getPath(devId, filePath);
                if (!MetastoreWrapper.reopenFile(file_id)) {
                    continue;
                }

                StringBuilder sbb = new StringBuilder();
                SFile recoverSf = MetastoreWrapper.getFile(file_id, sbb);
                log.info("after reopen the file is " + recoverSf.toString());

                List<SFile> luceneFileList = new ArrayList<SFile>();
                luceneFileList.add(recoverSf);

                String stTime = "";
                String edTime = "";
                for (int j = 0; j < 2; j++) {
                    long time = Long.parseLong(sf.getValues().get(j).getValue().trim());
                    if (j == 0) {
                        stTime = String.valueOf(time);
                    } else {
                        edTime = String.valueOf(time);
                    }
                }
                log.info("now set the time flag for file " + file_id + " to " + stTime + "_" + edTime);
                DataSourceConfig ds = new DataSourceConfig(db, tb, stTime);
                ConcurrentHashMap<Integer, IndexWriter> hashWriterMap = new ConcurrentHashMap<Integer, IndexWriter>();
                ConcurrentHashMap<Integer, List<SFile>> luceneSFMap = new ConcurrentHashMap<Integer, List<SFile>>();
                IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullFilePath, false);
                if (FSWriter == null) {
                    log.warn("when get the lucene file writer,get null for " + file_id + " and the full path is " + fullFilePath);
                    continue;
                }
                hashWriterMap.put(0, FSWriter);
                luceneSFMap.put(0, luceneFileList);
                LuceneFileWriter writer = new LuceneFileWriter(hashWriterMap, luceneSFMap);

                ArrayBlockingQueue buf = new ArrayBlockingQueue(Indexer.readbufSize);
                IndexControler c = new IndexControler(ds, buf, writer, file_id);
                c.threadPool = Workerpool.hlwrzpool;
                Thread ct = new Thread(c);
                ct.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                ct.start();
                HttpDataHandler.id2Createindex.put(file_id, c);

                for (IndexWriter iw : writer.getWriterMap().values()) {
                    try {
                        iw.commit();
                    } catch (IOException ex) {
                        log.error(ex, ex);
                    }
                }
                String rawFileFullName = rawDataPath + "/" + nameList[i];
                log.info("will use file " + rawFileFullName + " to recover the data ...");
                File f = new File(rawFileFullName);
                DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(f, docsReader);
                if (dataFileReader != null) {
                    while (dataFileReader.hasNext()) {
                        GenericRecord docsRecord = dataFileReader.next();
                        c.inbuffer.put(docsRecord);
                    }
                }
                log.info("now will erase the cache raw file information for " + rawFileFullName);
                f.renameTo(new File(rawFileFullName + ".good")); //最后要rename
                countc++;
            } catch (Exception e) {
                log.error(e, e);
            }
        }
        return countc;
    }

    public static String getPathDirs(String filelocation) {
        String[] ss = filelocation.split("[/]");
        String pathInfo = "";
        for (int j = 0; j < ss.length; j++) {
            if (ss[j] != null && !"".equalsIgnoreCase(ss[j])) {
                pathInfo = pathInfo + ss[j] + "-";
            }
        }
        pathInfo = pathInfo.substring(0, pathInfo.length() - 1);
        return pathInfo;
    }

    public static void main(String[] args) throws IOException {
        IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter("56031379", false);
        if (FSWriter == null) {
            System.out.println(" null ");
        }

        long file_legth = 0;
        String[] ss = FSWriter.getDirectory().listAll();
        if (ss != null) {
            for (int i = 0; i < ss.length; i++) {
                file_legth += FSWriter.getDirectory().fileLength(ss[i]);
            }
        }
        System.out.println("the file legth is " + file_legth);

        //IndexReader reader = DirectoryReader.open(FSDirectory.open());
        //IndexSearcher searcher = new IndexSearcher(reader);
        //FSWriter.close();
        //FSWriter = MetastoreUtil.getSimpleLuceneWriter("asd");
        //FSWriter.close();
        //FSWriter.commit();
        //FSWriter.close();
    }
}

/*
 *
 * * * write raw file
 *
 */
//                        bg = System.currentTimeMillis();
//                        try {
//                            if (HttpDataHandler.id2Createindex.get(file_id).isShouldNewRaw.get()) {
//                                HttpDataHandler.id2Createindex.get(file_id).isShouldNewRaw.set(false);
//                                String pathInfo = getPathDirs(filelocation);
//                                cacheFileName = Indexer.cachePath + "/" + file_id + "+" + devId + "+" + pathInfo + "+" + schemaName + "_" + System.currentTimeMillis() / 1000 + ".ori";
//                                log.info("new one cache data file " + cacheFileName);
//                                HttpDataHandler.id2cachefile.put(file_id, cacheFileName);//（put 操作就是更改file_id对应的文件名)
//                                HttpDataHandler.writeAvroFile(docsRecord, cacheFileName, true);
//                            } else {
//                                cacheFileName = HttpDataHandler.id2cachefile.get(file_id);
//                                //HttpDataHandler.writeAvroFile(docsRecord, cacheFileName, false);
//                            }
//                            hsResonse.getWriter().println("0\nbzs_s\nulss_s");
//                        } catch (Exception ex) {
//                            log.error("write the cache file is failed for " + ex, ex);
//                            hsResonse.setStatus(HttpServletResponse.SC_OK);
//                            hsResonse.getWriter().println("-1\n" + " parse one record is failed for " + ex.getMessage());
//                        }
//                        log.info("write raw file use " + (System.currentTimeMillis() - bg) + " ms for " + reqID + " <---> " + action);

