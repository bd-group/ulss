package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import static cn.ac.iie.ulss.dataredistribution.handler.WriteToFileThread.logger;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
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
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class TopicThread implements Runnable {

    String topic = null;
    ArrayList<Rule> ruleSet = null;
    LinkedBlockingQueue bufferPool = null;
    Integer bufferPoolSize = 0;
    ConcurrentLinkedQueue dataPool = null;
    Protocol protocol = null;
    Schema docsschema = null;
    DatumReader<GenericRecord> docsreader = null;
    String msgSchemaContent = null;
    String docsSchemaContent = null;
    ByteArrayInputStream docsin = null;
    BinaryDecoder docsdecoder = null;
    GenericRecord docsGr = null;
    GenericArray msgSet = null;
    Iterator<ByteBuffer> msgitor = null;
    String fileName = null;
    File fsmit = null;
    String dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(TopicThread.class.getName());
    }

    public TopicThread(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        logger.info("start the server for " + topic);

        init();

        if (ruleSet.isEmpty()) {
            logger.info("the topic " + topic + "has no services need data");
        } else {
            dataPool = new ConcurrentLinkedQueue();

            for (int i = 0; i < ((Integer) RuntimeEnv.getParam(RuntimeEnv.TRANSMIT_THREAD)); i++) {
                TransmitThread dtm = new TransmitThread(dataPool, ruleSet, topic);
                Thread tdtm = new Thread(dtm);
                tdtm.setName("TransmitThread－" + topic + "-" + i);
                tdtm.start();
            }

            logger.info("begin pull data for the topic " + topic + " from metaq");
            acceptData();

            for (int i = 0; i < ((Integer) RuntimeEnv.getParam(RuntimeEnv.WRITE_TO_FILE_THREAD)); i++) {
                fileName = dataDir + "backup/" + topic + i + ".bk";
                fsmit = new File(fileName);

                if (fsmit.exists()) {
                    logger.info("handlering the leaving data for the topic " + topic);
                    handlerLeavingData(fsmit);
                    logger.info("handler the leaving data for the topic " + topic + " successfully");
                }

                logger.info("starting writing to file for the topic " + topic);
                WriteToFileThread wtf = new WriteToFileThread(bufferPool, fsmit, dataPool, topic);
                Thread twtf = new Thread(wtf);
                twtf.setName("WriteToFileThread-" + topic + " " + i);
                twtf.start();
            }
        }
    }

    private void init() {
        bufferPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.BUFFER_POOL_SIZE);
        msgSchemaContent = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SCHEMACONTENT)).get(topic);
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        ruleSet = (ArrayList<Rule>) (((ConcurrentHashMap<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES)).get(topic));
        protocol = Protocol.parse(docsSchemaContent);
        docsschema = protocol.getType(GlobalVariables.DOCS);
        docsreader = new GenericDatumReader<GenericRecord>(docsschema);
    }

    /**
     *
     * accept data from the metaq
     */
    private void acceptData() {
        bufferPool = new LinkedBlockingQueue(bufferPoolSize);
        String zkUrl = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);
        logger.info("pulling the data from zk: " + zkUrl + " topic: " + topic);
        DataAccepterThread dataAccepter = new DataAccepterThread(zkUrl, topic, bufferPool);
        Thread tda = new Thread(dataAccepter);
        tda.setName("DataAccepterThread-" + topic);
        tda.start();
    }

    /**
     *
     * handler the leaving data in the file
     */
    private void handlerLeavingData(File f) {
        try {
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(f, docsreader);
            DatumWriter<GenericRecord> write = new GenericDatumWriter<GenericRecord>(docsschema);
            while (dataFileReader.hasNext()) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BinaryEncoder be = new EncoderFactory().binaryEncoder(baos, null);
                GenericRecord result = dataFileReader.next();
                write.write(result, be);
                be.flush();
                docsin = new ByteArrayInputStream(baos.toByteArray());
                docsdecoder = DecoderFactory.get().binaryDecoder(docsin, null);
                try {
                    docsGr = docsreader.read(null, docsdecoder);
                } catch (IOException ex) {
                    logger.info(" split the data package from the topic " + topic + " in the dataPool wrong " + ex, ex);
                }
                msgSet = (GenericData.Array<GenericRecord>) docsGr.get(GlobalVariables.DOC_SET);
                msgitor = msgSet.iterator();
                while (msgitor.hasNext()) {
                    byte[] onedata = ((ByteBuffer) msgitor.next()).array();
                    dataPool.offer(onedata);
                }
            }
        } catch (FileNotFoundException ex) {
            logger.error(ex, ex);
        } catch (IOException ex) {
            logger.error(ex, ex);
        }
    }
}
