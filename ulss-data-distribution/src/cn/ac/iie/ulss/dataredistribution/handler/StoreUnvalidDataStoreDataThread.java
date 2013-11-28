/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class StoreUnvalidDataStoreDataThread implements Runnable {

    Rule rule = null;
    ArrayBlockingQueue sdQueue = null;
    String msgSchemaContent = null;
    String msgSchemaName = null;
    String docsSchemaContent = null;
    int size = 100;
    String dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);
    static org.apache.log4j.Logger logger = null;
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(StoreUnvalidDataStoreDataThread.class.getName());
    }

    public StoreUnvalidDataStoreDataThread(ArrayBlockingQueue sdQueue, Rule rule) {
        this.sdQueue = sdQueue;
        this.rule = rule;
    }

    @Override
    public void run() {
        String topic = rule.getTopic();
        String service = rule.getServiceName();
        msgSchemaContent = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMACONTENT)).get(topic);
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        msgSchemaName = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMANAME)).get(topic);

        Protocol protocol = Protocol.parse(docsSchemaContent);
        Schema docsschema = protocol.getType(GlobalVariables.DOCS);
        DatumWriter<GenericRecord> write = new GenericDatumWriter<GenericRecord>(docsschema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(write);
        DatumReader<GenericRecord> dxreader = new GenericDatumReader<GenericRecord>(docsschema);

        int count = 0;
        int count2 = 0;
        File f = null;
        File out = new File(dataDir + "unvaliddata");

        while (true) {
            if (!sdQueue.isEmpty()) {
                count2 = 0;
                synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_DIR)) {
                    if (!out.exists() && !out.isDirectory()) {
                        out.mkdirs();
                        logger.info("create the directory " + dataDir + "unvaliddata");
                    }
                }

                f = new File(dataDir + "unvaliddata/" + topic + service + ".uv");
                if (f.exists()) {
                    try {
                        dataFileWriter.appendTo(f);
                    } catch (IOException ex) {
                        logger.error(ex, ex);
                    }
                } else {
                    try {
                        logger.info("create the file " + f.getName() + " for the topic " + topic + " and service " + service);
                        dataFileWriter.create(docsschema, f);
                    } catch (IOException ex) {
                        logger.error(ex, ex);
                    }
                }

                count = 0;
                while (count < 100) {
                    if (!sdQueue.isEmpty()) {
                        byte[] sendData = pack(sdQueue);
                        if (sendData == null) {
                            continue;
                        }
                        count++;

                        ByteArrayInputStream dxin = new ByteArrayInputStream(sendData);
                        BinaryDecoder dxdecoder = DecoderFactory.get().binaryDecoder(dxin, null);
                        GenericRecord dxr;
                        try {
                            dxr = dxreader.read(null, dxdecoder);
                            dataFileWriter.append(dxr);
                            dataFileWriter.flush();
                            logger.info("write " + size + " unvalid data to the file" + f.getName());
                        } catch (IOException ex) {
                            logger.info("write " + size + " unvalid data to the file" + f.getName() + " error ");
                            logger.error(ex, ex);
                            break;
                        }
                    } else {
                        try {
                            dataFileWriter.flush();          
                        } catch (IOException ex) {
                            logger.error(ex, ex);
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            logger.error(ex, ex);
                        }
                        count++;
                    }
                }
                try {
                    dataFileWriter.flush();
                    dataFileWriter.close();
                } catch (IOException ex) {
                    logger.error(ex,ex);
                }
                Date d = new Date();
                String fb = format.format(d) + "_" + f.getName();
                if (f.exists()) {
                    f.renameTo(new File(dataDir + "unvaliddata/" + fb));
                }
            } else {
                count2++;
                if (count2 >= 100) {
                    ConcurrentHashMap<Rule, ArrayBlockingQueue> unvalidDataStore = (ConcurrentHashMap<Rule, ArrayBlockingQueue>) RuntimeEnv.getParam(GlobalVariables.UNVALID_DATA_STORE);
                    synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_STORE_UNVALIDDATA)) {
                        unvalidDataStore.remove(rule);
                    }
                    break;
                }
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        }
    }

    /**
     *
     * package the data to a message
     */
    byte[] pack(ArrayBlockingQueue abq) {
        Protocol protocoldocs = Protocol.parse(docsSchemaContent);
        Schema docs = protocoldocs.getType(GlobalVariables.DOCS);
        GenericRecord docsRecord = new GenericData.Record(docs);
        docsRecord.put(GlobalVariables.DOC_SCHEMA_NAME, msgSchemaName);
        GenericArray docSet = new GenericData.Array<GenericRecord>((size), docs.getField(GlobalVariables.DOC_SET).schema());
        int count = 0;
        long stime = System.currentTimeMillis();
        while (count < size) {
            byte[] data = (byte[]) abq.poll();
            if (data != null) {
                docSet.add(ByteBuffer.wrap(data));
                count++;
            }
            long etime = System.currentTimeMillis();
            if ((etime - stime) >= 5000) {
                break;
            }
        }
        if (count <= 0) {
            return null;
        }

        logger.info("there are " + count + " unvalid data from the topic " + rule.getTopic() + " to the server " + rule.getServiceName());
        docsRecord.put(GlobalVariables.SIGN, "evan");
        docsRecord.put(GlobalVariables.DOC_SET, docSet);
        DatumWriter<GenericRecord> docsWriter = new GenericDatumWriter<GenericRecord>(docs);
        ByteArrayOutputStream docsbaos = new ByteArrayOutputStream();
        BinaryEncoder docsbe = new EncoderFactory().binaryEncoder(docsbaos, null);
        try {
            docsWriter.write(docsRecord, docsbe);
            docsbe.flush();
        } catch (IOException ex) {
            logger.error(ex, ex);
        }

        //logger.info("send " + count + "msg to the service");
        return docsbaos.toByteArray();
    }
}
