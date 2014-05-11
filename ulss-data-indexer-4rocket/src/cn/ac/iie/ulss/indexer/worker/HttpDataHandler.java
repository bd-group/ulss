/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.metastore.MetastoreWrapper;
import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.utiltools.FileFilter;
import cn.ac.iie.ulss.utiltools.LuceneWriterUtil;
import devmap.DevMap;
import iie.metastore.MetaStoreClient;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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
    public static AtomicLong activethreadNum = new AtomicLong(0);
    public static final Object o = new Object();

    @Override
    public void handle(String string, Request rqst, HttpServletRequest hsr, HttpServletResponse hsResonse) {
        HttpDataHandler.activethreadNum.incrementAndGet();
        String reqID = String.valueOf(System.nanoTime());
        long bg = System.currentTimeMillis();
        long bgbg = bg;
        try {
            rqst.setHandled(true);
            String action = rqst.getHeader("cmd").replace(" ", "").replace("    ", "");
            log.info("handle http request " + action + ",request id:" + reqID);
            /*
             * createfile 必须同步，在网络环境中，前端可能因为种种原因导致重复发送一个createfile指令，
             * 如果同时对一个lucene文件进行操作会出问题，create lucene物理文件的频率较低，同步代价可以接受;
             * 但如果磁盘异常，导致物理文件无法创建，需要另外处理
             */
            long file_id = 0;
            String fullPath = "";
            if (action.startsWith("createfile")) {
                synchronized (HttpDataHandler.o) {
                    if (GlobalParas.isTestMode) {
                        file_id = Long.parseLong(action.split("[|]")[1]);
                        fullPath = action.split("[|]")[2];
                        IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, true);
                        log.info("the writer is " + FSWriter);
                        if (FSWriter != null) {
                            FSWriter.commit();
                            //FSWriter.close();
                            //FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, false);
                            DataSourceConfig ds = new DataSourceConfig("sichuan", "t_ybrz", "4test");
                            ConcurrentHashMap<Integer, IndexWriter> hashWriterMap = new ConcurrentHashMap<Integer, IndexWriter>();
                            ConcurrentHashMap<Integer, List<SFile>> luceneSFMap = new ConcurrentHashMap<Integer, List<SFile>>();
                            hashWriterMap.put(0, FSWriter);

                            LuceneFileWriter writer = new LuceneFileWriter(hashWriterMap, luceneSFMap);
                            String topic = GlobalParas.hostName + "_" + "t_ybrz_mq";
                            if (!GlobalParas.consumeTopic.containsKey(topic)) {
                                GlobalParas.datapuller.addTopic(topic, "*");
                                GlobalParas.consumeTopic.put(topic, new AtomicLong(0));
                                log.info("in test mode,the lucene data writer will start consume the topic " + topic);
                            } else {
                                GlobalParas.consumeTopic.get(topic).incrementAndGet();
                                log.info("in test mode,the lucene data writer has been consuming the topic " + topic + ",the file in total is " + GlobalParas.consumeTopic.get(topic).get());
                            }

                            ArrayBlockingQueue buf = new ArrayBlockingQueue(GlobalParas.readbufSize);
                            IndexControler c = new IndexControler(ds, buf, writer, file_id);
                            c.threadPool = Workerpool.hlwrzpool;
                            Thread ct = new Thread(c);
                            ct.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                            ct.start();
                            GlobalParas.id2Createindex.put(file_id, c);
                        } else {
                            log.error("the test lucene file is null,why ??");
                        }
                        try {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("0\nbusiness_ok\nulss_ok");
                        } catch (Exception e) {
                            log.error(e, e);
                        }
                    } else {
                        file_id = Long.parseLong(action.split("[|]")[1]);
                        String filelocation = action.split("[|]")[2];
                        String devId = action.split("[|]")[3];
                        if (GlobalParas.id2Createindex.containsKey(file_id)) {
                            log.warn("receive one command that create file twice --> " + action);
                            try {
                                hsResonse.setStatus(HttpServletResponse.SC_OK);
                                hsResonse.getWriter().println("0\nbusiness_ok\nulss_ok");
                            } catch (Exception e) {
                                log.error(e, e);
                            }
                            return;
                        }
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
                        log.info("now will init file，new file id will be file_id " + file_id + ",request id:" + reqID + ",now the increate state file num is " + GlobalParas.id2Createindex.size() + ",file ids are " + GlobalParas.id2Createindex.keySet());
                        bg = System.currentTimeMillis();
                        try {
                            IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath);
                            if (FSWriter != null) {
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
                                    return;
                                } catch (Exception e) {
                                    log.error(e, e);
                                }
                            }
                        } catch (Exception e) {
                            log.error(e, e);
                        }
                        log.info("init new physical lucene file use " + (System.currentTimeMillis() - bg) + " ms for request id:" + reqID + " <---> " + action);

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

                        if (GlobalParas.id2Createindex.containsKey(file_id)) {
                            log.warn("receive one command that create file twice --> " + action);
                        } else {
                            /*
                             * * * fix me
                             */
                            /*
                             String topic = GlobalParas.hostName + "_" + sf.getTableName() + "_mq";
                             if (!GlobalParas.consumeTopic.containsKey(topic)) {
                             GlobalParas.datapuller.addTopic(topic, "*");
                             GlobalParas.consumeTopic.put(topic, new AtomicLong(0));
                             log.info("the lucene data writer will start consume the topic " + topic);
                             } else {
                             GlobalParas.consumeTopic.get(topic).incrementAndGet();
                             log.info("the lucene data writer has been consuming the topic " + topic + ",the file in total is " + GlobalParas.consumeTopic.get(topic).get());
                             }*/

                            try {
                                List<SFile> luceneFileList = new ArrayList<SFile>();
                                log.info("will get one exist file,the file id;current thread info is " + file_id + ";" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId());
                                sb = new StringBuilder();
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

                                ArrayBlockingQueue buf = new ArrayBlockingQueue(GlobalParas.readbufSize);
                                IndexControler c = new IndexControler(ds, buf, writer, file_id);
                                c.threadPool = Workerpool.hlwrzpool;
                                Thread ct = new Thread(c);
                                ct.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                                ct.start();
                                GlobalParas.id2Createindex.put(file_id, c);

                                log.info("apply one file from metastore use  " + (System.currentTimeMillis() - bg) + " ms for " + reqID + " <---> " + action);
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
                        try {
                            hsResonse.setStatus(HttpServletResponse.SC_OK);
                            hsResonse.getWriter().println("0\nbusiness_ok\nulss_ok");
                        } catch (Exception e) {
                            log.error(e, e);
                        }
                    }
                }
                log.info("create file use " + (System.currentTimeMillis() - bgbg) + " ms for request id:" + reqID + " <---> " + action);
            } else {
                log.info("the unknown cmd is " + action);
                try {
                    hsResonse.setStatus(HttpServletResponse.SC_OK);
                    hsResonse.getWriter().println("0\nbusiness_ok\nulss_ok");
                } catch (Exception e) {
                    log.error(e, e);
                }
            }
            log.info("handle the http request use time in total is " + (System.currentTimeMillis() - bgbg) + " ms for " + " request id:" + reqID + " <---> " + action);
        } catch (Exception e) {
            log.error(e, e);
            try {
                hsResonse.setStatus(HttpServletResponse.SC_OK);
                hsResonse.getWriter().println("0\nbusiness_ok\nulss_ok" + e.getMessage());
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        } finally {
            HttpDataHandler.activethreadNum.decrementAndGet();
        }
    }

    public static HttpDataHandler geHttptDataHandler() {
        HttpDataHandler dataHandler = new HttpDataHandler();
        try {
            Protocol protocol = Protocol.parse(GlobalParas.schemaname2schemaContent.get(GlobalParas.docs));
            HttpDataHandler.docsSchema = protocol.getType(GlobalParas.docs);
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
            MetaStoreClient msc = GlobalParas.clientPool.getClient();
            countC = HttpDataHandler.getBadData(GlobalParas.cachePath, msc);
            GlobalParas.clientPool.realseOneClient(msc);
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

                ArrayBlockingQueue buf = new ArrayBlockingQueue(GlobalParas.readbufSize);
                IndexControler c = new IndexControler(ds, buf, writer, file_id);
                c.threadPool = Workerpool.hlwrzpool;
                Thread ct = new Thread(c);
                ct.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                ct.start();
                GlobalParas.id2Createindex.put(file_id, c);

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
