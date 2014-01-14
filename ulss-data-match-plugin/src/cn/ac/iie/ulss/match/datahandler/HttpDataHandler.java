/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.datahandler;

import cn.ac.iie.ulss.match.worker.BusiMatchworker;
import cn.ac.iie.ulss.match.worker.CDRUpdater;
import cn.ac.iie.ulss.match.worker.Matcher;
import cn.ac.iie.ulss.struct.BusiRecordNode;
import cn.ac.iie.ulss.struct.CDRRecordNode;
import cn.ac.iie.ulss.util.FileFilter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 *
 * @author liucuili
 */
public class HttpDataHandler extends AbstractHandler {

    public static Logger log = Logger.getLogger(HttpDataHandler.class.getName());
    public static ConcurrentHashMap<String, String> schema2cachefile = new ConcurrentHashMap<String, String>();
    private static Schema docsSchema = null;
    private static DatumReader<GenericRecord> docsReader = null;
    final private static Object lock = new Object();

    @Override
    public void handle(String string, Request rqst, HttpServletRequest hsr, HttpServletResponse hsResonse) {
        rqst.setHandled(true);
        ServletInputStream servletInputStream = null;
        ByteArrayOutputStream out = null;

        String region = rqst.getParameter("region");
        if (region != null) {
            region = region.toLowerCase();
        } else {
            region = "";
        }

        String schemanameInstance = null;
        try {
            servletInputStream = rqst.getInputStream();
            out = new ByteArrayOutputStream();
        } catch (IOException ex) {
            log.error(ex, ex);
            try {
                hsResonse.setStatus(HttpServletResponse.SC_OK);
                hsResonse.getWriter().println("-1\n" + " receive data is failed for " + ex.getMessage());
            } catch (Exception ex1) {
                log.error(ex1, ex1);
            }
            return;
        }

        byte[] b = new byte[1024];
        int i = 0;
        try {
            while ((i = servletInputStream.read(b, 0, 1024)) > 0) {
                out.write(b, 0, i);
            }
        } catch (IOException ex) {
            log.error(ex, ex);
            try {
                hsResonse.setStatus(HttpServletResponse.SC_OK);
                hsResonse.getWriter().println("-1\n" + " receive data is failed for " + ex.getMessage());
            } catch (Exception ex1) {
                log.error(ex1, ex1);
            }
            return;
        }
        byte[] req = out.toByteArray();

        log.info("now recv data length " + req.length + ",the cmd is " + rqst.getHeader("cmd") + ",and the host is " + rqst.getRemoteHost() + ":" + rqst.getRemotePort() + ",request conten type:" + rqst.getContentType());
        if (req.length == 0) {
            log.warn("the data length is 0，what is  wrong ? ? ? ");
        }
        ByteArrayInputStream docsbis = new ByteArrayInputStream(req);
        BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
        GenericRecord docsRecord = new GenericData.Record(docsSchema);

        long bg = 0;
        try {
            docsReader.read(docsRecord, docsbd);
            String schemaName = docsRecord.get("doc_schema_name").toString().toLowerCase();

            log.debug("now receive data is " + schemaName + " and the region is " + region);
            /*
             *得到对应的schemaInstance
             */
            schemanameInstance = region + "." + schemaName;

            GenericArray docSet = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");
            log.info("now receive data num in total is --> " + schemanameInstance + ":" + Matcher.schemanameInstance2Total.get(schemanameInstance).addAndGet(docSet.size()));
            Iterator<ByteBuffer> itor = docSet.iterator();
            Random r = new Random();
            String cacheFileName = "";

            bg = System.nanoTime();
            if (schemanameInstance.endsWith("t_cdr")) {
                while (itor.hasNext()) {
                    try {
                        ByteArrayInputStream bis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                        BinaryDecoder bd = new DecoderFactory().binaryDecoder(bis, null);
                        DatumReader<GenericRecord> reader = Matcher.schema2Reader.get(schemaName);
                        GenericRecord record = reader.read(null, bd);
                        CDRRecordNode tmp = new CDRRecordNode();
                        tmp.c_usernum = record.get("c_usernum").toString();
                        tmp.c_imsi = record.get("c_imsi").toString();
                        tmp.c_imei = record.get("c_imei").toString();
                        tmp.c_spcode = (Integer) record.get("c_spcode");
                        tmp.c_ascode = (Integer) record.get("c_spcode");
                        tmp.c_lac = (Integer) record.get("c_lac");
                        tmp.c_ci = (Integer) record.get("c_ci");
                        tmp.c_rac = (Integer) record.get("c_rac");
                        tmp.c_areacode = record.get("c_areacode").toString();
                        tmp.c_homecode = record.get("c_homecode").toString();
                        tmp.c_timestamp = (Long) record.get("c_timestamp");
                        /**/
                        tmp.arriveTime = System.currentTimeMillis();
                        tmp.updateTime = tmp.arriveTime;

                        Matcher.cdrupdaters.get(region).inbuf.put(tmp);
                    } catch (Exception ex) {
                        log.error(ex, ex);
                    }
                }
                log.info("put " + docSet.size() + " cdr use time is " + (System.nanoTime() - bg) / 1000 + " us");
                try {
                    hsResonse.setStatus(HttpServletResponse.SC_OK);
                    hsResonse.getWriter().println("0\nbzs_s\nulss_s");
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            } else {
                List<BusiMatchworker> workers = Matcher.schemaInstance2BusiMatchworkers.get(schemanameInstance);


                synchronized (HttpDataHandler.lock) {
                    try {
                        if (Matcher.schemaInstance2MatchControler.get(schemanameInstance).isShouldNew.get()) {
                            Matcher.schemaInstance2MatchControler.get(schemanameInstance).isShouldNew.set(false);
                            cacheFileName = HttpDataHandler.schema2cachefile.get(schemanameInstance);
                            if (cacheFileName == null) { //第一次创建时，
                                cacheFileName = Matcher.rawdataCachePath + "/" + schemanameInstance + "+" + System.currentTimeMillis() / 1000 + "" + ".ori";
                            } else {
                                HttpDataHandler.closeAvroFile(cacheFileName);
                                cacheFileName = Matcher.rawdataCachePath + "/" + schemanameInstance + "+" + System.currentTimeMillis() / 1000 + "" + ".ori";
                            }
                            log.info("new one cache data file " + cacheFileName);
                            HttpDataHandler.schema2cachefile.put(schemanameInstance, cacheFileName);
                            this.writeAvroFile(docsRecord, cacheFileName, true);
                        } else {
                            cacheFileName = HttpDataHandler.schema2cachefile.get(schemanameInstance);
                            this.writeAvroFile(docsRecord, cacheFileName, false);
                        }
                    } catch (Exception ex) {
                        log.error(ex, ex);
                    }
                }

                BusiMatchworker worker = null;
                ConcurrentHashMap<Integer, LinkedBlockingQueue<BusiRecordNode>> tmpBuf = null;

                bg = System.nanoTime();
                int index = 0;
                while (itor.hasNext()) {
                    try {
                        worker = workers.get(Math.abs(r.nextInt()) % workers.size());
                        ByteArrayInputStream bis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                        BinaryDecoder bd = new DecoderFactory().binaryDecoder(bis, null);
                        DatumReader<GenericRecord> reader = Matcher.schema2Reader.get(schemaName);
                        GenericRecord record = reader.read(null, bd);
                        BusiRecordNode tmp = new BusiRecordNode();

                        tmp.genRecord = record;
                        tmp.arriveTime = System.currentTimeMillis();
                        tmp.updateTime = System.currentTimeMillis();
                        tmp.state = 0;
                        tmpBuf = worker.inbuffer;

                        index = worker.httpReceiveBufferIndex.get();
                        if (!tmpBuf.get(index).offer(tmp)) {
                            String sizes = tmpBuf.get(index).remainingCapacity() + " ：";
//                            for (LinkedBlockingQueue<BusiRecordNode> buf : worker.inbuffer.values()) {
//                                sizes += buf.remainingCapacity() + " ";
//                            }
                            for (int idx = 0; idx < worker.inbuffer.size(); idx++) {
                                sizes += idx + ":" + worker.inbuffer.get(idx).remainingCapacity() + " ";
                            }
                            log.warn("now current window buffer is full,the current buffer index and all buffer remainingCapacity size is：" + schemanameInstance + "[ " + index + " ]" + " -> " + sizes);
                            while (true) {
                                try {
                                    Thread.sleep(100);
                                } catch (Exception e) {
                                    log.error(e, e);
                                }
                                index = worker.httpReceiveBufferIndex.get();
                                if (!tmpBuf.get(index).offer(tmp)) {
                                    log.warn("add data to sliding window retry fail one time for dataflow:" + schemanameInstance + ",and current buffer index is:" + index);
                                    continue;
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (Exception ex) {
                        log.error(ex, ex);
                    }
                }
                log.info("put " + docSet.size() + " " + schemanameInstance + "  use time is " + (System.nanoTime() - bg) / 1000 + " us");
                try {
                    hsResonse.setStatus(HttpServletResponse.SC_OK);
                    hsResonse.getWriter().println("0\nbzs_s\nulss_s");
                } catch (Exception ex1) {
                    log.error(ex1, ex1);
                }
            }
        } catch (Exception ex) {
            log.error("parse the docs is failed for " + ex, ex);
            try {
                hsResonse.setStatus(HttpServletResponse.SC_OK);
                hsResonse.getWriter().println("-1\n" + " parse the docs is failed for " + ex.getMessage());
            } catch (Exception ex1) {
                log.error(ex1, ex1);
            }
        }
    }

    public static HttpDataHandler getDataHandler() {
        String schemaName = "";
        String schemaContent = "";
        HttpDataHandler dataHandler = new HttpDataHandler();
        try {
            List<List<String>> allSchema = Matcher.DBMeta.getAllSchema();
            for (List<String> list : allSchema) {
                schemaName = list.get(0).toLowerCase();
                schemaContent = list.get(1).toLowerCase();
                if (schemaName.equals("docs")) {
                    Protocol protocol = Protocol.parse(schemaContent);
                    HttpDataHandler.docsSchema = protocol.getType("docs");
                    HttpDataHandler.docsReader = new GenericDatumReader<GenericRecord>(HttpDataHandler.docsSchema);
                } else {
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
        int count = HttpDataHandler.getBadData(Matcher.rawdataCachePath);
        log.info("get unresolved data from the disk for match，get data number is " + count);

        return dataHandler;
    }

    public void writeAvroFile(GenericRecord gr, String fname, boolean isNew) {
        try {
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
            dataFileWriter.close();//不要忘记close文件，否则文件写入多次就会造成too many open files 错误
            log.info("write data to raw avro file " + fname + " ok ");
        } catch (Exception ex) {
            log.error("when write the avro data get error:\n " + ex, ex);
        }
    }

    public static void closeAvroFile(String fname) {
        try {
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
        } catch (Exception ex) {
            log.error("when close raw avro file get error:\n " + ex, ex);
        }
    }

    public static int getBadData(String path) {
        int count = 0;
        try {
            File file = new File(path);
            String[] nameList = file.list(new FileFilter(".*\\.ori"));
            DatumReader<GenericRecord> docReader = new GenericDatumReader<GenericRecord>(docsSchema);
            if (nameList == null || nameList.length == 0) {
                return 0;
            }
            Random rdom = new Random();
            int bufferIndex = 0;
            log.info("now begin do the recover operation,get ori file number is " + nameList.length);
            try {
                for (int i = 0; i < nameList.length; i++) {
                    String fileFullName = path + "/" + nameList[i];
                    log.info("now do the recover operation use file " + fileFullName);
                    File f = new File(fileFullName);
                    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(f, docReader);
                    if (dataFileReader != null) {
                        while (dataFileReader.hasNext()) {
                            GenericRecord docsRecord = dataFileReader.next();
                            String schemaName = docsRecord.get("doc_schema_name").toString().toLowerCase();
                            String schemaInstance = nameList[i].split("[+]")[0];
                            DatumReader<GenericRecord> reader = Matcher.schema2Reader.get(schemaName);
                            GenericArray docSet = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");
                            Iterator<ByteBuffer> itor = docSet.iterator();
                            while (itor.hasNext()) {
                                ByteArrayInputStream dxxbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                                BinaryDecoder dxxbd = new DecoderFactory().binaryDecoder(dxxbis, null);
                                GenericRecord record = reader.read(null, dxxbd);
                                BusiRecordNode brc = new BusiRecordNode();
                                brc.setGenericRecord(record);
                                count++;
                                try {
                                    List<BusiMatchworker> workers = Matcher.schemaInstance2BusiMatchworkers.get(schemaInstance);
                                    //List<BusiMatchworker> workers = Matcher.schemaInstance2BusiMatchworkers.get(name);
                                    for (BusiMatchworker worker : workers) {
                                        bufferIndex = Math.abs(rdom.nextInt()) % worker.inbuffer.size();
                                        worker.inbuffer.get(bufferIndex).put(brc);
                                    }
                                } catch (Exception ex) {
                                    log.error(ex, ex);
                                }
                            }
                        }
                    }
                    log.info("rename " + fileFullName + " to " + fileFullName + ".good");
                    f.renameTo(new File(fileFullName + ".good")); //最后要rename
                }
            } catch (Exception ex) {
                log.error("when get the avro data get error:\n " + ex, ex);
            }
        } catch (Exception ex) {
            log.error("when get the avro data get error:\n " + ex, ex);
        }

        return count;
    }

    public static void main(String[] args) throws IOException {
        File file = new File(".");
        String[] nameList = file.list(new FileFilter(".*\\.ori"));
        System.out.println(nameList);
        System.out.println(nameList[0]);
    }
}