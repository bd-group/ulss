/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer;

import cn.ac.iie.ulss.metastore.MetastoreWrapper;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.util.FileFilter;
import cn.ac.iie.ulss.util.LuceneWriterUtil;
import devmap.DevMap;
import iie.metastore.MetaStoreClient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
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
    public static long receiveCount = 0l;
    private static Schema docsSchema = null;
    private static DatumReader<GenericRecord> docsReader = null;
    private static AtomicLong receiveIntotal = new AtomicLong(0);
    private static SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /*
     */
    public static ConcurrentHashMap<Long, String> id2cachefile = new ConcurrentHashMap<Long, String>();
    public static ConcurrentHashMap<String, AtomicBoolean> schema2isnew = new ConcurrentHashMap<String, AtomicBoolean>();
    public static ConcurrentHashMap<Long, CreateIndex> id2Createindex = new ConcurrentHashMap<Long, CreateIndex>();  //file_id到createIndex的映射
    public static ConcurrentHashMap<Long, Long> closedIdMap = new ConcurrentHashMap<Long, Long>();
    public static final Object o = new Object();
    public static final Object clientLock = new Object();

    @Override
    public void handle(String string, Request rqst, HttpServletRequest hsr, HttpServletResponse hsResonse) {
        try {
            rqst.setHandled(true);
            String action = rqst.getHeader("cmd");
            log.info("handle the http request " + action);

            if (action.startsWith("createfile")) {
                synchronized (HttpDataHandler.o) {
                    long file_id = Long.parseLong(action.split("[|]")[1]);
                    String filelocation = action.split("[|]")[2];
                    String devId = action.split("[|]")[3];
                    DevMap dm = new DevMap();
                    String fullPath = "";
                    try {
                        fullPath = dm.getPath(devId, filelocation);
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                    StringBuilder sb = new StringBuilder();
                    SFile sf = MetastoreWrapper.makeSureGetFile(file_id, sb);
                    if (sf == null) {
                        String message = sb.toString();
                        try {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("-1\n" + "get one new file error for " + message + ",and the file_id is " + file_id);
                        } catch (IOException ex) {
                            log.error(ex, ex);
                        }
                        return;
                    }
                    log.info("now will init file，and the new file id will be " + file_id + ",now the increate state file num is " + id2Createindex.size() + ",the file ids are " + id2Createindex.keySet());
                    try {
                        IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, true);
                        if (FSWriter != null) {
                            FSWriter.commit();
                            FSWriter.close();
                        } else { //就是新的文件又被创建了一次，此时直接返回，而不是再次创建(磁盘上已经有了相关file_id对应的lucene文件)
                            log.warn("get null lucene IndexWriter for " + file_id + " " + fullPath + ",maybe it has been created");
                            try {
                                hsResonse.setStatus(HttpServletResponse.SC_OK);
                                hsResonse.getWriter().println("0\nbzs_s\nulss_s");
                            } catch (Exception e) {
                                log.error(e, e);
                            }
                            return;
                        }
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                    log.info("init lucene file for " + file_id + " ok,the file location size for it is " + sf.getLocations().size());
                    sf.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
                    sb = new StringBuilder();
                    if (MetastoreWrapper.makeSureOnlineFileLocation(sf, sb) == false) {
                        try {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("-1\n" + "online operation error for: " + sb.toString() + ",and the file_id is " + file_id);
                        } catch (IOException ex1) {
                            log.error(ex1, ex1);
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

            } else {
                long file_id = Long.parseLong(action.split("[|]")[0]);
                String filelocation = action.split("[|]")[1];
                String devId = action.split("[|]")[2];
                String cacheFileName = "";
                synchronized (HttpDataHandler.o) {
                    try {                                                            /*不包括这个file_id的话证明对应的create线程已经结束或者还没有开始，那么就要进行创建*/
                        if (!HttpDataHandler.id2Createindex.containsKey(file_id)) {  //与CreateIndex中的HttpDataHandler.key2Createindex.remove(sf.getFid()) 语句同步?
                            List<SFile> luceneFileList = new ArrayList<SFile>();
                            log.info("will get one exist file,the file id;current thread info is " + file_id + ";" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId());
                            StringBuilder sb = new StringBuilder();
                            SFile sf = MetastoreWrapper.makeSureGetFile(file_id, sb);
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
                                long time = Long.parseLong(sf.getValues().get(j).getValue().trim()) * 1000;
                                Date d = new Date();
                                d.setTime(time);
                                if (j == 0) {
                                    stTime = secondFormat.format(d);
                                } else {
                                    edTime = secondFormat.format(d);
                                }
                            }
                            log.info("now set the time flag for file " + file_id + "to " + stTime + "_" + edTime);
                            DataSourceConfig ds = new DataSourceConfig(Indexer.zkUrl, db, tb, stTime + "_" + edTime, "metaQtopic", null);
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
                            CreateIndex c = new CreateIndex(ds, Indexer.cli, Indexer.ClientAPIMap.get(ds.getDbName()), buf, writer, file_id, HttpDataHandler.o, HttpDataHandler.clientLock);
                            Thread ct = new Thread(c);
                            ct.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                            ct.start();

                            id2Createindex.put(file_id, c);

                        } else {
                        }
                    } catch (Exception e) {
                        log.error(e, e);
                        hsResonse.setStatus(HttpServletResponse.SC_OK);
                        try {
                            hsResonse.getWriter().println("-1\n" + " new lucene file is failed for " + e.getMessage()); //一旦发现不能创建文件，就要退出并且回复，使其重新选择
                        } catch (IOException ex) {
                            log.error(ex, ex);
                        }
                        return;
                    }
                }

                ServletInputStream servletInputStream = null;
                try {
                    servletInputStream = rqst.getInputStream();
                } catch (IOException ex) {
                    log.error(ex, ex);
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] b = new byte[1024];
                int i = 0;
                try {
                    while ((i = servletInputStream.read(b, 0, 1024)) > 0) {
                        out.write(b, 0, i);
                    }
                } catch (IOException ex) {
                    log.error(ex, ex);
                }
                byte[] req = out.toByteArray();
                log.info("now recv data length " + req.length + ",and the host is " + rqst.getRemoteHost() + ":" + rqst.getRemotePort() + ",request conten type:" + rqst.getContentType());

                ByteArrayInputStream docsbis = new ByteArrayInputStream(req);
                BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
                GenericRecord docsRecord = new GenericData.Record(docsSchema);
                try {
                    docsReader.read(docsRecord, docsbd);
                    receiveIntotal.addAndGet(((GenericData.Array<GenericRecord>) docsRecord.get(Indexer.docs_set_name)).size());
                    log.info("the data receive in total is " + receiveIntotal.get());
                    String schemaName = docsRecord.get("doc_schema_name").toString().toLowerCase();
                    log.info("now the receive data name is " + schemaName);
                    synchronized (HttpDataHandler.o) {               /* 往缓冲区加数据的时候，如果正好处于线程要结束的时刻（假定createIndex线程，中检测数据长时间为不到达的机制在某些情况下是运转不正常的 那么下面这个步骤就有可能出问题，如何解决这个问题呢，要进行同步*/
                        if (id2Createindex.get(file_id) == null) {   //就是被createindex线程给remove掉了
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("-2\n" + "warning:the file has been closed ！");
                            log.error("the file has been closed for file_id " + file_id);
                            return;
                        } else if (!id2Createindex.get(file_id).isEnd.get()) {
                            id2Createindex.get(file_id).inbuffer.put(docsRecord);//将数据写入对象的缓冲区中
                        } else {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);   //就是createindex线程已经将状态置为空了
                            hsResonse.getWriter().println("-2\n" + "warning:the file has been closed ！");
                        }
                        try {
                            if (HttpDataHandler.id2Createindex.get(file_id).isShouldNewRaw.get()) {
                                HttpDataHandler.id2Createindex.get(file_id).isShouldNewRaw.set(false);
                                String pathInfo = getPathDirs(filelocation);
                                cacheFileName = Indexer.cachePath + "/" + file_id + "+" + devId + "+" + pathInfo + "+" + schemaName + "_" + System.currentTimeMillis() / 1000 + ".ori";
                                log.info("new one cache data file " + cacheFileName);
                                HttpDataHandler.id2cachefile.put(file_id, cacheFileName);//（put 操作就是更改file_id对应的文件名)
                                HttpDataHandler.writeAvroFile(docsRecord, cacheFileName, true);
                            } else {
                                cacheFileName = HttpDataHandler.id2cachefile.get(file_id);
                                HttpDataHandler.writeAvroFile(docsRecord, cacheFileName, false);
                            }
                            hsResonse.getWriter().println("0\nbzs_s\nulss_s");
                        } catch (Exception ex) {
                            log.error("write the cache file is failed for " + ex, ex);
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("-1\n" + " parse one record is failed for " + ex.getMessage());
                        }
                    }
                } catch (Exception ex) {
                    log.error("parse the docs is failed for " + ex, ex);
                    hsResonse.setStatus(HttpServletResponse.SC_OK);
                    try {
                        hsResonse.getWriter().println("-1\n" + " parse the docs is failed for " + ex.getMessage());
                    } catch (IOException ex1) {
                        log.error(ex1, ex1);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, e);
        }
    }

    public static HttpDataHandler getDataHandler() {
        String schemaName = "";
        String schemaContent = "";
        HttpDataHandler dataHandler = new HttpDataHandler();
//        try {
//             Indexer.cli.cl = new MetaStoreClient(Indexer.metaStoreCientUrl, Indexer.metaStoreCientPort);
//        } catch (Exception e) {
//            log.error(e, e);
//            System.exit(0);
//        }
        try {
            List<List<String>> allSchema = Indexer.imd.getAllSchema();
            for (List<String> list : allSchema) {
                schemaName = list.get(0).toLowerCase();
                schemaContent = list.get(1).toLowerCase();
                if (schemaName.equals("docs")) {
                    Protocol protocol = Protocol.parse(schemaContent);
                    HttpDataHandler.docsSchema = protocol.getType("docs");
                    HttpDataHandler.docsReader = new GenericDatumReader<GenericRecord>(HttpDataHandler.docsSchema);
                }
            }
        } catch (Exception ex) {
            log.error("constructing data dispath handler is failed: " + schemaName + " for " + ex, ex);
            dataHandler = null;
        }
        log.info("init all data schemas done ");

        if (docsSchema == null) {
            log.error("schema docs is not found in metadb,please check metadb to ensure that docs schema exist");
            dataHandler = null;
        }
        try {
            int countC = 0;
            countC = HttpDataHandler.getBadData(Indexer.cachePath, Indexer.cli);
            log.info("get unresolved data from the disk for index，get number is " + countC);
        } catch (Exception ex) {
            log.error("when get unresolved data error occurs: " + ex, ex);
        }
        return dataHandler;
    }

    /*
     */
    public static void writeAvroFile(GenericRecord gr, String fname, boolean isNew) throws IOException {
        File f = new File(fname);
        DatumWriter<GenericRecord> write = new GenericDatumWriter<GenericRecord>(HttpDataHandler.docsSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(write);
        if (isNew) {
            dataFileWriter = dataFileWriter.create(HttpDataHandler.docsSchema, f);
        } else {
            dataFileWriter = dataFileWriter.appendTo(f);
        }
        dataFileWriter.append(gr);
        dataFileWriter.flush();
    }

    public static void closeAvroFile(String fname) throws IOException {
        DatumWriter<GenericRecord> write = new GenericDatumWriter<GenericRecord>(HttpDataHandler.docsSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(write);
        if (fname != null && !"".equalsIgnoreCase(fname)) {
            File f = new File(fname);
            if (f.exists()) {
                dataFileWriter = dataFileWriter.appendTo(f);
                dataFileWriter.flush();
                dataFileWriter.close();
                File nf = new File(fname + ".good");
                f.renameTo(nf);
            }
        }
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
            Long file_id = Long.parseLong(nameList[i].split("[+]")[0]);//file_id + "_" + devId + "_" + pathInfo + "_" + schemaName + "_" + System.currentTimeMillis() / 1000 + ".ori";
            String devId = nameList[i].split("[+]")[1];
            String filePath = nameList[i].split("[+]")[2];
            filePath = "/" + filePath.replace('-', '/');

            StringBuilder sb = new StringBuilder();
            SFile sf = MetastoreWrapper.makeSureGetFile(file_id, sb);

            log.info("now do the recover operation for " + sf.toString());
            String db = sf.getDbName();
            String tb = sf.getTableName();
            DevMap dm = new DevMap();
            String fullFilePath = dm.getPath(devId, filePath);
            if (!MetastoreWrapper.makeSureReopenFile(file_id)) {
                return -1;
            }

            StringBuilder sbb = new StringBuilder();
            SFile recoverSf = MetastoreWrapper.makeSureGetFile(file_id, sbb);
            log.info("after reopen the file is " + recoverSf.toString());

            List<SFile> luceneFileList = new ArrayList<SFile>();
            luceneFileList.add(recoverSf);

            String stTime = "";
            String edTime = "";
            for (int j = 0; j < 2; j++) {
                long time = Long.parseLong(sf.getValues().get(j).getValue().trim()) * 1000;
                Date d = new Date();
                d.setTime(time);
                if (j == 0) {
                    stTime = secondFormat.format(d);
                } else {
                    edTime = secondFormat.format(d);
                }
            }
            log.info("now set the time flag for file " + file_id + " to " + stTime + "_" + edTime);
            DataSourceConfig ds = new DataSourceConfig(Indexer.zkUrl, db, tb, stTime + "_" + edTime, "", null);
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
            CreateIndex c = new CreateIndex(ds, Indexer.cli, Indexer.ClientAPIMap.get(ds.getDbName()), buf, writer, file_id, HttpDataHandler.o, HttpDataHandler.clientLock);
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
 /~~~~//
 if (cacheFileName == null) {   //这是为什么呢 ?实际这是处理第一次创建文件时  ？？？？？？
 String pathInfo = getPathDirs(filelocation);
 cacheFileName = Indexer.cachePath + "/" + file_id + "_" + devId + "_" + pathInfo + "_" + schemaName + "_" + System.currentTimeMillis() / 1000 + ".ori";
 log.warn("get null cache file object for " + schemaName + " " + cacheFileName);
 } else {
 String pathInfo = getPathDirs(filelocation);
 cacheFileName = Indexer.cachePath + "/" + file_id + "_" + devId + "_" + pathInfo + "_" + schemaName + "_" + System.currentTimeMillis() / 1000 + ".ori";
 }
 log.info("new one cache data file " + cacheFileName);
 HttpDataHandler.id2cachefile.put(file_id, cacheFileName);//更改file_id对应的文件名
 HttpDataHandler.writeAvroFile(docsRecord, cacheFileName, true);
 } else {
 cacheFileName = HttpDataHandler.id2cachefile.get(file_id);
 log.info("append data to file " + cacheFileName);
 HttpDataHandler.writeAvroFile(docsRecord, cacheFileName, false);
 }
 */
