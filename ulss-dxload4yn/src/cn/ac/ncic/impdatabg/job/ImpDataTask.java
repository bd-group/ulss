/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.job;

import cn.ac.ncic.impdatabg.commons.datafile.DataFile;
import cn.ac.ncic.impdatabg.commons.Line;
import cn.ac.ncic.impdatabg.commons.RuntimeEnv;
import cn.ac.ncic.impdatabg.job.helper.DataRetriveHelper;
import cn.ac.ncic.impdatabg.util.Doc;
import cn.ac.ncic.impdatabg.util.MQProducerPool;
import cn.ac.ncic.impdatabg.util.QYDXDoc;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public abstract class ImpDataTask implements Runnable {

    protected DataFile dataFile;
    protected String dataType;
    protected String statResDir;
    protected String statDateStr;
    protected String mq = "";
    protected int batchSize;
    protected DataRetriveHelper dataRetriveHelper = null;
    protected List<QYDXDoc> docSet = new ArrayList<QYDXDoc>();
    protected List<Line> lineslst = new ArrayList<Line>();
    protected int totalrows = 0;
    protected int succrows = 0;
    protected int errrows = 0;
    protected long currChkPoint = 0;
    protected int waitExitTimes = 0;
    protected final int BUFFER_SIZE = 10240;
    //doc schema
    Protocol docsProtocol = null;
    private Schema docsSchema = null;
    Protocol docProtocol = null;
    private Schema docSchema = null;
    MQProducerPool mqProducerPool = null;
    private final static Set<String> dataTypeSet = new TreeSet<String>() {
        {
            add("bf_dxx");
        }
    };
    private final static Map<String, DataRetriveHelper> dataRetriveHelperSet = new HashMap<String, DataRetriveHelper>() {
        {
            put("bf_dxx", new DataRetriveHelper(17, 8));
        }
    };
    private final static Map<String, String> mqSet = new HashMap<String, String>() {
        {
            put("bf_dxx", "t_qydx_mq");
        }
    };
    private final static Map<String, Integer> batchSizeSet = new HashMap<String, Integer>() {
        {
            put("bf_dxx", 1000);
        }
    };
    //log
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ImpDataTask.class.getName());
    }

    public ImpDataTask() {
    }

    public ImpDataTask(DataFile pDataFile, String pStatResDir, String pStatDateStr) {
        this.dataFile = pDataFile;
        this.statResDir = pStatResDir;
        this.statDateStr = pStatDateStr;
        this.dataType = getDataType();
        this.mq = mqSet.get(this.dataType);
        Integer batchSizeTmp = batchSizeSet.get(dataType);
        if (batchSizeTmp == null) {
            this.batchSize = RuntimeEnv.batchSize;
        } else {
            this.batchSize = batchSizeTmp;
        }
        this.dataRetriveHelper = dataRetriveHelperSet.get(this.dataType);
        this.mqProducerPool = MQProducerPool.getMQProducerPool(mq, 3);
        assert (this.mqProducerPool != null);
        this.docsProtocol = getProtocol("docs.json");
        assert (this.docsProtocol != null);
        this.docsSchema = docsProtocol.getType("docs");
        this.docProtocol = getProtocol("t_yqdx.json");
        this.docSchema = docProtocol.getType("t_yqdx");
    }

    protected abstract void importDataSuccProcess(List<Line> lst);

    protected abstract void importDataFailProcess(List<Line> lst);

    protected void setData(String[] pData) {
        QYDXDoc qydxDoc = new QYDXDoc();
        docSet.add(qydxDoc);
    }

    protected void importData() {
        boolean succeeded = false;
        try {
            byte[] seObj = pack(docSet);
            if (seObj != null) {
                int tryInsertTimeLimit = 10;
                for (int i = 0; i < tryInsertTimeLimit; i++) {
                    try {
                        mqProducerPool.sendMessage(seObj);
                        succeeded = true;
                        break;
                    } catch (Exception ex2) {
                        logger.warn("loading " + docSet.size() + " data of " + this.dataFile.getFileName() + " into " + this.dataType + " is failed " + i + "st time for " + ex2);
                        try {
                            Thread.sleep(3000);
                        } catch (Exception ex3) {
                        }
                    }
                }
            } else {
                logger.error("some error happened when doing serilization");
            }
        } catch (Exception ex) {
            logger.error("some error happened when doing serilization and sending for " + ex.getMessage(), ex);
        } finally {
            try {
                if (!succeeded) {
                    importDataFailProcess(lineslst);
                    logger.error("loading " + docSet.size() + " data of " + this.dataFile.getFileName() + " into " + this.dataType + " is failed at last");
                } else {
                    importDataSuccProcess(lineslst);
                    logger.info("loading " + docSet.size() + " data of " + this.dataFile.getFileName() + " into " + this.dataType + " is succeeded.");
                }
            } finally {
                docSet.clear();
                lineslst.clear();// no matter succeeded or failed,lineslst must be cleared
            }
        }
    }

    void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception ex) {
        }
    }

    String getDataType() {
        String fileName = dataFile.getFileName().toLowerCase();
        Iterator iter = dataTypeSet.iterator();
        String currdataType;
        while (iter.hasNext()) {
            currdataType = (String) iter.next();
            if (fileName.toLowerCase().contains(currdataType)) {
                return currdataType;
            }
        }
        return null;
    }

    private Protocol getProtocol(String pProtocolFilePath) {
        Protocol protocol = null;
        try {
            protocol = Protocol.parse(new File(pProtocolFilePath));
        } catch (Exception ex) {
            logger.error("get protocol obj unsuccessfully for " + ex.getMessage(), ex);
            protocol = null;
        }
        return protocol;
    }

    public byte[] pack(List<? extends Doc> pDocSet) {
        try {
            GenericRecord docsRecord = new GenericData.Record(docsSchema);
            docsRecord.put("doc_schema_name", "t_qydx");

            int docSetSize = pDocSet.size();
            GenericArray docRecordSet = new GenericData.Array<GenericRecord>(docSetSize, docsSchema.getField("doc_set").schema());

            DatumWriter<GenericRecord> docRecordWriter = new GenericDatumWriter<GenericRecord>(docSchema);
            ByteArrayOutputStream docRecordSOS = new ByteArrayOutputStream();
            BinaryEncoder docRecordBE = new EncoderFactory().binaryEncoder(docRecordSOS, null);

            List<String> docPeropertyNameSet = pDocSet.get(0).getPropertyNameSet();
            for (int i = 0; i < docSetSize; i++) {
                GenericRecord docRecord = new GenericData.Record(docSchema);
                Doc doc = pDocSet.get(i);
                for (String dcoPropPertyName : docPeropertyNameSet) {
                    Object propertyValue = doc.getPropertyValue(dcoPropPertyName);
                    docRecord.put(dcoPropPertyName, propertyValue == null ? "" : propertyValue);
                }
                docRecordWriter.write(docRecord, docRecordBE);
                docRecordBE.flush();
                docRecordSet.add(ByteBuffer.wrap(docRecordSOS.toByteArray()));
                docRecordSOS.reset();
            }
            docsRecord.put("sign", System.nanoTime() + "");
            docsRecord.put("doc_set", docRecordSet);

            DatumWriter<GenericRecord> docsRecordWriter = new GenericDatumWriter<GenericRecord>(docsSchema);
            ByteArrayOutputStream docsRecordSOS = new ByteArrayOutputStream();
            BinaryEncoder docsBE = new EncoderFactory().binaryEncoder(docsRecordSOS, null);
            docsRecordWriter.write(docsRecord, docsBE);
            docsBE.flush();
            return docsRecordSOS.toByteArray();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
