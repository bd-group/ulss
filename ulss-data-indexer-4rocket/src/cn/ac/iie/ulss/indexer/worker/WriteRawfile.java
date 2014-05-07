/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.log4j.Logger;

public class WriteRawfile implements Runnable {

    public static Logger log = Logger.getLogger(WriteRawfile.class.getName());
    IndexControler createIndex;
    public String cachePath;
    public String filelocation;
    public String dev_id;
    public String schemaName = "";
    public ArrayBlockingQueue<GenericRecord> buffer;
    public Schema docsSchema = null;
    public GenericDatumReader docsReader = null;
    public long file_id = 0;

    public WriteRawfile(IndexControler c, String path, String file_location, String devid, String schemaname, long fileid, ArrayBlockingQueue<GenericRecord> buf) {
        this.createIndex = c;
        /*
         */
        this.cachePath = path;
        this.filelocation = file_location;
        this.dev_id = devid;
        this.schemaName = schemaname;
        this.buffer = buf;
        /*
         */
        Protocol protocol = Protocol.parse(GlobalParas.schemaname2schemaContent.get(GlobalParas.docs));
        this.docsSchema = protocol.getType(GlobalParas.docs);
        this.docsReader = new GenericDatumReader<GenericRecord>(this.docsSchema);
        file_id = fileid;
    }

    @Override
    public void run() {
        boolean isNew = true;
        String fileFullName = null;
        String pathInfo = "";
        File f = null;
        List<GenericRecord> innerBuffer = new ArrayList<GenericRecord>(GlobalParas.writeInnerpoolBatchDrainSize);
        DatumWriter<GenericRecord> write = new GenericDatumWriter<GenericRecord>(this.docsSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(write);

        while (true) {
            if (this.createIndex.isShouldNewRaw.get()) {
                if (fileFullName != null) {
                    this.closeAvroFile(fileFullName);
                }
                isNew = true;
                this.createIndex.isShouldNewRaw.set(false);
                pathInfo = getPathDirs(filelocation);
                fileFullName = this.cachePath + "/" + file_id + "+" + dev_id + "+" + pathInfo + "+" + this.schemaName + "_" + System.currentTimeMillis() / 1000 + ".ori";
                f = new File(fileFullName);
                log.info("new one cache data file " + fileFullName);
            }

            this.buffer.drainTo(innerBuffer, GlobalParas.writeInnerpoolBatchDrainSize);
            try {
                if (isNew) {
                    isNew = false;
                    dataFileWriter = dataFileWriter.create(this.docsSchema, f);
                }
                if (innerBuffer.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                    }
                } else {
                    for (GenericRecord gr : innerBuffer) {
                        dataFileWriter.append(gr);
                    }
                    dataFileWriter.flush();
                }
            } catch (Exception e) {
                log.error(e, e);
            }
            innerBuffer.clear();

            if (this.createIndex.isAllOver.get()) {
                log.info("close the last raw file,and the raw file writer exit ...");
                this.closeAvroFile(fileFullName);
                GlobalParas.id2WriteRawfile.remove(this.file_id);
                break;
            }
        }
    }

    public synchronized void writeAvroFile(GenericRecord gr, String fname, boolean isNew) {
        try {
            File f = new File(fname);
            DatumWriter<GenericRecord> write = new GenericDatumWriter<GenericRecord>(null);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(write);
            if (isNew) {
                dataFileWriter = dataFileWriter.create(null, f);
            } else {
                dataFileWriter = dataFileWriter.appendTo(f);
            }
            dataFileWriter.append(gr);
            dataFileWriter.flush();
            dataFileWriter.close();
            log.info("write data to raw avro file " + fname + " ok ");
        } catch (Exception ex) {
            log.error("when write the avro data get error:\n " + ex, ex);
        }
    }

    public synchronized void closeAvroFile(String fname) {
        try {
            DatumWriter<GenericRecord> write = new GenericDatumWriter<GenericRecord>(this.docsSchema);
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
}
