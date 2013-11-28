/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import com.taobao.metamorphosis.Message;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class WriteToFileThread implements Runnable {

    public String topic = null;
    public ArrayBlockingQueue bufferPool = null;
    public ArrayBlockingQueue dataPool = null;
    public File fsmit = null;
    String docsSchemaContent = null;
    Integer dataPoolSize = 1000;
    Integer writeToFileThread = 5;
    static org.apache.log4j.Logger logger = null;
    int acceptTimeout = 30000;
    String dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

    {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(WriteToFileThread.class.getName());
    }

    public WriteToFileThread(ArrayBlockingQueue bufferPool, File fsmit, ArrayBlockingQueue dataPool, String topic) {
        this.bufferPool = bufferPool;
        this.fsmit = fsmit;
        this.dataPool = dataPool;
        dataPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATA_POOL_SIZE);
        writeToFileThread = (Integer) RuntimeEnv.getParam(RuntimeEnv.WRITE_TO_FILE_THREAD);
        this.topic = topic;
    }

    @Override
    public void run() {
        Integer activeThreadCount = (Integer) RuntimeEnv.getParam(RuntimeEnv.ACTIVE_THREAD_COUNT);
        Map<String, ThreadGroup> topicToSendThreadPool = (Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL);
        ThreadGroup sendThreadPool = topicToSendThreadPool.get(topic);
        acceptTimeout = (Integer) RuntimeEnv.getParam(RuntimeEnv.ACCEPT_TIMEOUT);
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        Protocol protocol = null;
        protocol = Protocol.parse(docsSchemaContent);
        Schema docsschema = protocol.getType(GlobalVariables.DOCS);
        DatumWriter<GenericRecord> write = new GenericDatumWriter<GenericRecord>(docsschema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(write);
        DatumReader<GenericRecord> dxreader = new GenericDatumReader<GenericRecord>(docsschema);
        while (true) {
            File out = new File(dataDir + "backup");
            synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_DIR)) {
                if (!out.exists() && !out.isDirectory()) {
                    out.mkdirs();
                    logger.info("create the directory " + dataDir + "backup");
                }
            }

            if (dataPool.isEmpty() && sendThreadPool.activeCount() <= activeThreadCount) {

                String f = fsmit.getName();
                Date d = new Date();
                String fb = format.format(d) + "_" + f + ".old";
                
                if (fsmit.exists()) {
                    logger.info("rename the file " + fsmit.getName());
                    fsmit.renameTo(new File(dataDir + "backup/" + fb));
                }
                byte[] msg = null;
                if (!bufferPool.isEmpty()) {
                    fsmit = new File(dataDir + "backup/" + f);
                    logger.info("create the file " + fsmit.getName());
                    try {
                        dataFileWriter.create(docsschema, fsmit);
                    } catch (Exception ex) {
                        logger.error("can not create the file " + fsmit.getName() + ex, ex);
                        return;
                    }
                    long stime = System.currentTimeMillis();
                    int count = 0;
                    while (count < (dataPoolSize / writeToFileThread)) {
                        Message message = null;
                        try {
                            message = (Message) bufferPool.poll(10, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException ex) {
                            logger.error(ex, ex);
                        }
                        if (message != null) {
                            msg = message.getData();
                            count++;
                            ByteArrayInputStream dxin = new ByteArrayInputStream(msg);
                            BinaryDecoder dxdecoder = DecoderFactory.get().binaryDecoder(dxin, null);
                            GenericRecord dxr = null;;
                            try {
                                dxr = dxreader.read(null, dxdecoder);
                            } catch (IOException ex) {
                                logger.info(" the schema of the data is wrong, can not be writen into the file " + fsmit.getName());
                                try {
                                    dataFileWriter.flush();
                                    dataFileWriter.close();
                                } catch (IOException ex1) {
                                    logger.error(ex1, ex1);
                                }

                                return;
                            }

                            if (dxr != null) {
                                while (true) {
                                    try {
                                        dataFileWriter.append(dxr);
                                        break;
                                    } catch (IOException ex) {
                                        logger.error("IOException when append to the file " + fsmit.getName() + ex, ex);
                                    }
                                }
                            }

                            dataPool.offer(msg);
                            if (count % 100 == 0) {
                                try {
                                    dataFileWriter.flush();
                                } catch (IOException ex) {
                                    logger.error(ex, ex);
                                }
                            }
                        }
                        long etime = System.currentTimeMillis();
                        if ((etime - stime) >= acceptTimeout) {
                            break;
                        }
                    }
                    try {
                        dataFileWriter.flush();
                        dataFileWriter.close();
                    } catch (IOException ex1) {
                        logger.error(ex1, ex1);
                    }
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        logger.error(ex, ex);
                    }
                }
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        }
    }
}
