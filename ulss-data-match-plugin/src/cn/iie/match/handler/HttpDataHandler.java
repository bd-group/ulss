/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.iie.match.handler;

import cn.iie.struct.RecordNode;
import cn.iie.util.FileFilter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.ServletException;
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
    private static Schema docsSchema = null;
    private static DatumReader<GenericRecord> docsReader = null;
    public static ConcurrentHashMap<String, MatchWorker> schema2Matchworker;
    public static ConcurrentHashMap<String, String> schema2cachefile = new ConcurrentHashMap<String, String>();
    public static String cacheFileName = "";
    final private static Object lk = new Object();

    @Override
    public void handle(String string, Request rqst, HttpServletRequest hsr, HttpServletResponse hsResonse) throws IOException, ServletException {
        rqst.setHandled(true);
        ServletInputStream servletInputStream = rqst.getInputStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] b = new byte[1024];
        int i = 0;
        while ((i = servletInputStream.read(b, 0, 1024)) > 0) {
            out.write(b, 0, i);//如果内存不够了，在这里接收数据时客户端会接到错误，错误原因是java.lang.OutOfMemoryError: Java heap space at java.util.Arrays.copyOf(Arrays.java:2798)
        }
        byte[] req = out.toByteArray();
        log.info("now recv data length " + req.length + ",the cmd is " + rqst.getHeader("cmd") + ",and the host is " + rqst.getRemoteHost() + ":" + rqst.getRemotePort() + ",request conten type:" + rqst.getContentType());

        if (req.length == 0) {
            log.warn("the data length is 0,will just return ...");
        }

        ByteArrayInputStream docsbis = new ByteArrayInputStream(req);
        BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
        GenericRecord docsRecord = new GenericData.Record(docsSchema);
        try {
            docsReader.read(docsRecord, docsbd);
            String schemaName = docsRecord.get("doc_schema_name").toString().toLowerCase();
            log.info("now the receive data name is " + schemaName);
            GenericArray docSet = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");

            log.info("now receive data num in total is -> " + schemaName + ":" + Matcher.schemaname2Total.get(schemaName).addAndGet(docSet.size()));

            Iterator<ByteBuffer> itor = docSet.iterator();
            MatchWorker worker = HttpDataHandler.schema2Matchworker.get(schemaName);
            synchronized (HttpDataHandler.lk) {
                //Lock writeLock = Matcher.schema2ReadWriteLock.get(schemaName).writeLock();
                //writeLock.lock();
                try {
                    if (worker.inbuf.size() >= Matcher.inbufferSize) {
                        log.warn("the data send is too fast,can not deal with it，refuse the data receive onetime ");
                    }
                    if (Matcher.schema2MatchControler.get(schemaName).isShouldNew.get()) {
                        Matcher.schema2MatchControler.get(schemaName).isShouldNew.set(false);
                        cacheFileName = HttpDataHandler.schema2cachefile.get(schemaName);
                        if (cacheFileName == null) {
                            cacheFileName = Matcher.cachePath + "/" + schemaName + "_" + System.currentTimeMillis() / 1000 + "" + ".ori";
                            HttpDataHandler.closeAvroFile(cacheFileName);
                        } else {
                            HttpDataHandler.closeAvroFile(cacheFileName);
                            cacheFileName = Matcher.cachePath + "/" + schemaName + "_" + System.currentTimeMillis() / 1000 + "" + ".ori";
                        }
                        log.info("new one cache data file " + cacheFileName);
                        HttpDataHandler.schema2cachefile.put(schemaName, cacheFileName);
                        HttpDataHandler.writeAvroFile(docsRecord, cacheFileName, true);
                    } else {
                        cacheFileName = HttpDataHandler.schema2cachefile.get(schemaName);
                        HttpDataHandler.writeAvroFile(docsRecord, cacheFileName, false);
                    }
                    while (itor.hasNext()) {  //把解析出的数据放入worker的输入缓冲区中
                        ByteArrayInputStream dxxbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                        BinaryDecoder dxxbd = new DecoderFactory().binaryDecoder(dxxbis, null);
                        DatumReader<GenericRecord> reader = Matcher.schema2Reader.get(schemaName);
                        GenericRecord record = reader.read(null, dxxbd);
                        RecordNode tmp = new RecordNode();
                        tmp.setGenericRecord(record);
                        tmp.setArriveTimestamp(System.currentTimeMillis());
                        tmp.setState(0);
                        try {
                            worker.inbuf.put(tmp);
                        } catch (InterruptedException ex) {
                            log.error(ex, ex);
                        }
                        // if (!worker.inbuf.offer(tmp)) {
                        // hsResonse.getWriter().println("-1\n" + "the data send speed is too fast,can not deal with such big data ! ");
                        // }
                    }
                    hsResonse.getWriter().println("0\nbzs_s\nulss_s");
                } catch (Exception ex) {
                    log.error("parse one record is failed for " + ex, ex);
                    hsResonse.setStatus(HttpServletResponse.SC_OK);
                    hsResonse.getWriter().println("-1\n" + " parse one record is failed for " + ex.getMessage());
                } finally {
                    //writeLock.unlock();
                }
            }
        } catch (Exception ex) {
            log.error("parse the docs is failed for " + ex, ex);
            hsResonse.getWriter().println("-1\n" + " parse the docs is failed for " + ex.getMessage());
        }
    }

    public static HttpDataHandler getDataHandler() {
        HttpDataHandler.schema2Matchworker = new ConcurrentHashMap<String, MatchWorker>();
        String schemaName = "";
        String schemaContent = "";
        HttpDataHandler dataHandler = new HttpDataHandler();
        try {
            List<List<String>> allSchema = Matcher.meta.getAllSchema();
            for (List<String> list : allSchema) {
                schemaName = list.get(0).toLowerCase();
                schemaContent = list.get(1).toLowerCase();
                if (schemaName.equals("docs")) {
                    Protocol protocol = Protocol.parse(schemaContent);
                    HttpDataHandler.docsSchema = protocol.getType("docs");
                    HttpDataHandler.docsReader = new GenericDatumReader<GenericRecord>(HttpDataHandler.docsSchema);
                } else {
                    String mq = list.get(2).toLowerCase();
                    if (Matcher.schema2Matchworker.containsKey(schemaName)) {  //将需要匹配的数据的worker缓存起来，不需要匹配的数据的reader就不需要缓存
                        HttpDataHandler.schema2Matchworker.put(schemaName, Matcher.schema2Matchworker.get(schemaName));
                    }
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

        if (HttpDataHandler.schema2Matchworker.size() < 1) {
            log.warn("no bussiness data schema is found in metadb,please check metadb to ensure that this condition is reasonable");
        }
        try {
            int count = 0;
            count = HttpDataHandler.getBadData(Matcher.cachePath);
            log.info("get unresolved data from the disk for match，get number is " + count);
        } catch (IOException ex) {
            log.error("when get unresolved data error occurs: " + ex, ex);
        }
        return dataHandler;
    }

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
        dataFileWriter.close();//不要忘记close文件，否则文件写入多次就会造成too many open files 错误
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

    public static int getBadData(String path) throws IOException {
        File file = new File(path);
        String[] nameList = file.list(new FileFilter(".*\\.ori"));
        DatumReader<GenericRecord> docReader = new GenericDatumReader<GenericRecord>(docsSchema);
        int count = 0;
        if (nameList == null) {
            return 0;
        }
        log.info("now begin do the recover operation,get ori file number is " + nameList.length);
        for (int i = 0; i < nameList.length; i++) {
            String fileFullName = path + "/" + nameList[i];
            //String fileFullName = nameList[i];
            File f = new File(fileFullName);
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(f, docReader);
            if (dataFileReader != null) {
                while (dataFileReader.hasNext()) {
                    GenericRecord docsRecord = dataFileReader.next();
                    String name = docsRecord.get("doc_schema_name").toString().toLowerCase();
                    DatumReader<GenericRecord> reader = Matcher.schema2Reader.get(name);
                    GenericArray docSet = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");

                    Iterator<ByteBuffer> itor = docSet.iterator();
                    while (itor.hasNext()) {
                        ByteArrayInputStream dxxbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                        BinaryDecoder dxxbd = new DecoderFactory().binaryDecoder(dxxbis, null);
                        GenericRecord record = reader.read(null, dxxbd);
                        RecordNode r = new RecordNode();
                        {
                            r.setState(0);
                            r.setGenericRecord(record);
                            r.setArriveTimestamp(0);
                        }
                        count++;
                        try {
                            //log.info("now init the handler " + HttpDataHandler.schema2Matchworker.keySet() + " " + name);
                            HttpDataHandler.schema2Matchworker.get(name).inbuf.put(r);
                        } catch (InterruptedException ex) {
                            log.error(ex, ex);
                        }
                    }
                }
            }
            f.renameTo(new File(fileFullName + ".good")); //最后要rename
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
