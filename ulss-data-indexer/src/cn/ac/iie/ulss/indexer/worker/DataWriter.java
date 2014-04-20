/*
 * To change this template, choose Tools | Templates
 * and open th
 * e template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.constant.DataTypeMap;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.utiltools.AvroUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

public class DataWriter implements Runnable {

    public static Logger log = Logger.getLogger(DataWriter.class.getName());
    private SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public AtomicLong willwriteDocNum;
    public AtomicLong failwriteDocNum;
    String dbName;
    String tableName;
    String timeLable;
    List<GenericRecord> innerDataBuf;
    int innerDataBufSize;
    volatile boolean isEnd;
    LuceneFileWriter normalLucWriter;
    AtomicBoolean isDiskBad;
    IndexWriter writer;
    /*
     */
    private GenericArray arrayInt = null;
    private GenericArray arrayLong = null;
    private GenericArray arrayFloat = null;
    private GenericArray arrayDouble = null;
    private GenericArray arrayString = null;
    private GenericArray arrayBytes = null;
    private int num_int_val = 0;
    private long num_long_val = 0;
    private double num_double_val = 0;
    private float num_float_val = 0;
    private int arrayIdx = 0;
    private Integer dataTypeCase = 0;
    private String dataType = null;
    private String dataName = null;
    private String strData = null;
    private ByteBuffer bytesdata = null;
    private int writenum = 0;
    private long bg = System.currentTimeMillis();
    /**/
    private LinkedHashMap<String, Field> idx2LuceneFieldMap = null;
    private LinkedHashMap<String, List<FieldSchema>> idx2colMap = null;
    private LinkedHashMap<String, FieldType> idx2LuceneFieldTypeMap = null;
    private ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>> table2idx2colMap = null;
    private ConcurrentHashMap<String, LinkedHashMap<String, Field>> table2idx2LuceneFieldMap = null;
    private ConcurrentHashMap<String, LinkedHashMap<String, FieldType>> table2idx2LuceneFieldTypeMap = null;

    DataWriter(DataSourceConfig ds, LuceneFileWriter norLucWriter, AtomicBoolean isdiskbad) {
        this.dbName = ds.getDbName();
        this.tableName = ds.getTbName();
        this.timeLable = ds.getTimeLable();
        this.innerDataBufSize = Indexer.writeInnerpoolBatchDrainSize;
        this.normalLucWriter = norLucWriter;
        this.isDiskBad = isdiskbad;
        this.innerDataBuf = new ArrayList(innerDataBufSize);
    }

    DataWriter() {
        this.innerDataBuf = new ArrayList(innerDataBufSize);
    }

    @Override
    public void run() {
        if (innerDataBuf.isEmpty()) {
            //log.info("the write buffer size is 0,will return ...");
            return;
        }
        
        bg = System.currentTimeMillis();
        //writenum = this.innerDataBuf.size();
        //this.innerDataBuf.clear();
        try {
            this.table2idx2LuceneFieldMap = Indexer.table2idx2LuceneFieldMapPool.take();
            this.table2idx2colMap = Indexer.table2idx2colMapPool.take();
            this.table2idx2LuceneFieldTypeMap = Indexer.table2idx2LuceneFieldTypeMapPool.take();

            this.idx2LuceneFieldMap = this.table2idx2LuceneFieldMap.get(this.dbName + "." + this.tableName);
            this.idx2colMap = this.table2idx2colMap.get(this.dbName + "." + this.tableName);
            this.idx2LuceneFieldTypeMap = this.table2idx2LuceneFieldTypeMap.get(this.dbName + "." + this.tableName);

            String schemaName = Indexer.tablename2Schemaname.get(this.tableName.toLowerCase());
            String schemaContent = Indexer.schemaname2schemaContent.get(schemaName);
            Protocol protocol = AvroUtils.getProtocol(schemaContent);
            DatumReader<GenericRecord> singleDocReader = AvroUtils.getReader(protocol, schemaName);

            log.info("get cache index info use " + (System.currentTimeMillis() - bg) + " ms");

            GenericRecord docsRecord = null;
            bg = System.currentTimeMillis();
            for (GenericRecord ms : innerDataBuf) {
                if (ms == null) {
                    log.error("the write not get null data！ it is wrong");
                } else if (ms != null) {
                    try {
                        docsRecord = ms;
                        GenericArray docSet = (GenericData.Array<GenericRecord>) docsRecord.get(Indexer.docs_set);
                        Iterator<ByteBuffer> itor = docSet.iterator();
                        while (itor.hasNext()) {
                            Document doc = new Document();
                            ByteArrayInputStream dxxbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                            BinaryDecoder dxxbd = new DecoderFactory().binaryDecoder(dxxbis, null);
                            GenericRecord record;
                            record = singleDocReader.read(null, dxxbd);
                            try {
                                for (String idxName : idx2LuceneFieldMap.keySet()) {
                                    dataType = idx2colMap.get(idxName).get(0).getType();
                                    dataName = idx2colMap.get(idxName).get(0).getName();
                                    Object o = record.get(dataName);
                                    if ("c_inputtime".equalsIgnoreCase(dataName)) {
                                        LongField f = new LongField("c_inputtime", System.currentTimeMillis() / 1000, idx2LuceneFieldTypeMap.get("c_inputtime"));
                                        doc.add(f);
                                        continue;
                                    }
                                    if (o == null) {
                                        //log.debug("get null for " + dataName);
                                        continue;
                                    }
                                    Field field = idx2LuceneFieldMap.get(idxName);
                                    dataTypeCase = DataTypeMap.datatypeMap.get(dataType);
                                    if (dataTypeCase == null) {
                                        log.error("not suppoot data type " + idxName + " " + dataType);
                                    } else {
                                        switch (dataTypeCase) {
                                            case 0: //int
                                                try {
                                                    strData = o.toString().trim();
                                                    if (!"".equalsIgnoreCase(strData)) {
                                                        num_int_val = Integer.parseInt(strData);
                                                        ((IntField) field).setIntValue(num_int_val);
                                                        doc.add(field);
                                                    } else {
                                                        log.debug(idxName + " is blank or tab ");
                                                    }
                                                } catch (Exception e) {
                                                    log.error(idxName + " " + e, e);
                                                }
                                                break;
                                            case 1: //arrayint
                                                try {
                                                    arrayInt = (GenericArray) o;
                                                    for (arrayIdx = 0; arrayIdx < arrayInt.size(); arrayIdx++) {
                                                        IntField f = new IntField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                        strData = arrayInt.get(arrayIdx).toString().trim();
                                                        if (!"".equalsIgnoreCase(strData)) {
                                                            num_int_val = Integer.parseInt(strData);
                                                            f.setIntValue(num_int_val);
                                                            doc.add(f);
                                                        } else {
                                                            log.debug(idxName + " is blank or tab ");
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 2:   //long
                                                try {
                                                    strData = o.toString().trim();
                                                    if (!"".equalsIgnoreCase(strData)) {
                                                        num_long_val = Long.parseLong(strData);
                                                        ((LongField) field).setLongValue(num_long_val);
                                                        doc.add(field);
                                                    } else {
                                                        log.debug(idxName + " is blank or tab ");
                                                    }
                                                } catch (Exception e) {
                                                    log.error(idxName + " " + e, e);
                                                }
                                                break;
                                            case 3:   //arraylong
                                                try {
                                                    arrayLong = (GenericArray) o;
                                                    for (arrayIdx = 0; arrayIdx < arrayLong.size(); arrayIdx++) {
                                                        LongField f = new LongField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                        strData = arrayLong.get(arrayIdx).toString().trim();
                                                        if (!"".equalsIgnoreCase(strData)) {
                                                            num_long_val = Long.parseLong(arrayLong.get(arrayIdx).toString());
                                                            f.setLongValue(num_long_val);
                                                            doc.add(f);
                                                        } else {
                                                            log.debug(idxName + " is blank or tab ");
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    log.error(idxName + " " + e, e);
                                                }
                                                break;
                                            case 4:   //bigint
                                                try {
                                                    strData = o.toString().trim();
                                                    if (!"".equalsIgnoreCase(strData)) {
                                                        num_long_val = Long.parseLong(strData);
                                                        ((LongField) field).setLongValue(num_long_val);
                                                        doc.add(field);
                                                    } else {
                                                        log.debug(idxName + " is blank or tab ");
                                                    }
                                                } catch (Exception e) {
                                                    log.error(idxName + " " + e, e);
                                                }
                                                break;
                                            case 5://array bigint
                                                try {
                                                    arrayLong = (GenericArray) o;
                                                    for (arrayIdx = 0; arrayIdx < arrayLong.size(); arrayIdx++) {
                                                        LongField f = new LongField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                        strData = arrayLong.get(arrayIdx).toString().trim();
                                                        if (!"".equalsIgnoreCase(strData)) {
                                                            num_long_val = Long.parseLong(arrayLong.get(arrayIdx).toString());
                                                            f.setLongValue(num_long_val);
                                                            doc.add(f);
                                                        } else {
                                                            log.debug(idxName + " is blank or tab ");
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    log.error(idxName + " " + e, e);
                                                }
                                                break;
                                            case 6://timestamp
                                                try {
                                                    strData = o.toString().trim();
                                                    if (!"".equalsIgnoreCase(strData)) {
                                                        num_long_val = Long.parseLong(strData);
                                                        ((LongField) field).setLongValue(num_long_val);
                                                        doc.add(field);
                                                    } else {
                                                        log.debug(idxName + " is blank or tab ");
                                                    }
                                                } catch (Exception e) {
                                                    log.error(idxName + " " + e, e);
                                                }
                                                break;
                                            case 7:  //arraytimestamp
                                                try {
                                                    arrayLong = (GenericArray) o;
                                                    for (arrayIdx = 0; arrayIdx < arrayLong.size(); arrayIdx++) {
                                                        LongField f = new LongField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                        strData = arrayLong.get(arrayIdx).toString().trim();
                                                        if (!"".equalsIgnoreCase(strData)) {
                                                            num_long_val = Long.parseLong(arrayLong.get(arrayIdx).toString());
                                                            f.setLongValue(num_long_val);
                                                            doc.add(f);
                                                        } else {
                                                            log.debug(idxName + " is blank or tab ");
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 8://float
                                                try {
                                                    arrayFloat = (GenericArray) o;
                                                    for (arrayIdx = 0; arrayIdx < arrayFloat.size(); arrayIdx++) {
                                                        FloatField f = new FloatField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                        strData = arrayFloat.get(arrayIdx).toString().trim();
                                                        if (!"".equalsIgnoreCase(strData)) {
                                                            num_float_val = Float.parseFloat(strData);
                                                            f.setFloatValue(num_float_val);
                                                            doc.add(f);
                                                        } else {
                                                            log.debug(idxName + " is blank or tab ");
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    log.error(idxName + " " + e, e);
                                                }
                                                break;
                                            case 9://arrayfloat
                                                try {
                                                    strData = o.toString().trim();
                                                    if (!"".equalsIgnoreCase(strData)) {
                                                        num_float_val = Float.parseFloat(strData);
                                                        ((FloatField) field).setFloatValue(num_float_val);
                                                        doc.add(field);
                                                    } else {
                                                        log.debug(idxName + " is blank or tab ");
                                                    }
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 10://doule
                                                try {
                                                    strData = o.toString().trim();
                                                    if (!"".equalsIgnoreCase(strData)) {
                                                        num_double_val = Double.parseDouble(strData);//java.sql.Types.DATEjava的SQL数据类型
                                                        ((DoubleField) field).setDoubleValue(num_double_val);
                                                        doc.add(field);
                                                    }
                                                } catch (Exception e) {
                                                    log.error(idxName + " " + e, e);
                                                }
                                                break;
                                            case 11://arraydouble
                                                try {
                                                    arrayDouble = (GenericArray) o;
                                                    for (arrayIdx = 0; arrayIdx < arrayDouble.size(); arrayIdx++) {
                                                        DoubleField f = new DoubleField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                        strData = arrayDouble.get(arrayIdx).toString().trim();
                                                        if (!"".equalsIgnoreCase(strData)) {
                                                            num_double_val = Double.parseDouble(arrayDouble.get(arrayIdx).toString());
                                                            f.setDoubleValue(num_double_val);
                                                            doc.add(f);
                                                        } else {
                                                            log.debug(idxName + " is blank or tab ");
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 12://string
                                                try {
                                                    if (field instanceof StringField) {
                                                        if (this.tableName.toLowerCase().startsWith("t_dx_rz") && "c_esmnr".equalsIgnoreCase(idxName)) {
                                                            strData = new String(((ByteBuffer) o).array());
                                                            ((StringField) field).setStringValue(strData);
                                                            doc.add(field);
                                                        } else {
                                                            strData = o.toString();
                                                            ((StringField) field).setStringValue(strData);
                                                            doc.add(field);
                                                        }
                                                    } else {
                                                        strData = o.toString();
                                                        ((TextField) field).setStringValue(strData);
                                                        doc.add(field);
                                                    }
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 13://arraystring
                                                try {
                                                    arrayString = (GenericArray) o;
                                                    if (field instanceof StringField) {
                                                        for (arrayIdx = 0; arrayIdx < arrayString.size(); arrayIdx++) {
                                                            StringField f = new StringField(idxName, "", Field.Store.YES);
                                                            f.setStringValue(arrayString.get(arrayIdx).toString());
                                                            doc.add(f);
                                                        }
                                                    } else {
                                                        for (arrayIdx = 0; arrayIdx < arrayString.size(); arrayIdx++) {
                                                            TextField f = new TextField(idxName, "", Field.Store.YES);
                                                            f.setStringValue(arrayString.get(arrayIdx).toString());
                                                            doc.add(f);
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 14://binary
                                                try {
                                                    bytesdata = (ByteBuffer) o;
                                                    field.setBytesValue(bytesdata.array());
                                                    doc.add(field);
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 15://arraybinary
                                                arrayBytes = (GenericArray) o;
                                                try {
                                                    for (arrayIdx = 0; arrayIdx < arrayBytes.size(); arrayIdx++) {
                                                        bytesdata = (ByteBuffer) arrayBytes.get(arrayIdx);
                                                        Field f = new Field(idxName, (new byte[1]), idx2LuceneFieldTypeMap.get(idxName));
                                                        f.setBytesValue(bytesdata.array());
                                                        doc.add(f);
                                                    }
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 16://blob
                                                try {
                                                    strData = o.toString();
                                                    field.setStringValue(strData);
                                                    doc.add(field);
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            case 17://arrayblob
                                                try {
                                                    arrayString = (GenericArray) o;
                                                    for (arrayIdx = 0; arrayIdx < arrayString.size(); arrayIdx++) {
                                                        StringField f = new StringField(idxName, "", Field.Store.YES);
                                                        f.setStringValue(arrayString.get(arrayIdx).toString());
                                                        doc.add(f);
                                                    }
                                                } catch (Exception e) {
                                                    log.error(e, e);
                                                }
                                                break;
                                            default:
                                                log.error("not suppoot data type " + idxName + " " + dataType);
                                                break;
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                log.error(e, e);
                            }
                            try {
                                this.willwriteDocNum.incrementAndGet();
                                if (doc == null) {
                                    log.info("the doc is null,why ?");
                                }
                                if (writer == null) {
                                    log.info("the writer is null,why ?");
                                }
                                writer.addDocument(doc);
                            } catch (IOException e) {
                                this.isDiskBad.set(true);
                                log.error(e, e);
                                failwriteDocNum.incrementAndGet();
                            } catch (Exception e) {
                                log.error(e, e);
                                failwriteDocNum.incrementAndGet();
                            }
                            writenum++;
                        }
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                }
            }
            log.info("write lucene file use " + this.dbName + "." + this.tableName + " " + (System.currentTimeMillis() - bg) + " ms for " + writenum);
            writenum = 0;
            innerDataBuf.clear();
        } catch (Exception ex) {
            log.error(ex, ex);
            innerDataBuf.clear();
        } finally {
            innerDataBuf.clear();
            try {
                Indexer.table2idx2LuceneFieldMapPool.put(this.table2idx2LuceneFieldMap);
                Indexer.table2idx2colMapPool.put(this.table2idx2colMap);
                Indexer.table2idx2LuceneFieldTypeMapPool.put(this.table2idx2LuceneFieldTypeMap);
                //log.info("put the data type cache to pool");
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
        //log.info("write lucene file use " + this.dbName + "." + this.tableName + " " + (System.currentTimeMillis() - bg) + " ms for " + writenum);
    }

    public void setIsEnd(boolean isEnd) {
        this.isEnd = isEnd;
    }

    public boolean isIsEnd() {
        return isEnd;
    }

    public void addWriteDocNum(long inc) {
        willwriteDocNum.addAndGet(inc);
    }

    public long getWriteDocNum() {
        return willwriteDocNum.get();
    }

    public void clearWriteDocNum() {
        willwriteDocNum.set(0l);
    }

    public synchronized String getTimeLable() {
        return timeLable;
    }

    public synchronized void setTimeLable(String timeLable) {
        this.timeLable = timeLable;
    }

    public synchronized String getMD5(byte[] val) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            log.error(ex, ex);
        }
        digest.reset();
        digest.update(val);
        BigInteger bigInt = new BigInteger(1, digest.digest());
        return bigInt.toString(16);
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        AtomicLong al = new AtomicLong(0);
        System.out.println(al.addAndGet(12));
        System.out.println(al.get());
    }
}
/*
 * 
 Schema arrayIntSchema = Schema.createArray(Schema.create(Schema.Type.INT));
 Schema arrayLongSchema = Schema.createArray(Schema.create(Schema.Type.LONG));
 Schema arrayFloatSchema = Schema.createArray(Schema.create(Schema.Type.FLOAT));
 Schema arrayDoubleSchema = Schema.createArray(Schema.create(Schema.Type.DOUBLE));
 Schema arrayStringSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
 Schema arrayBytesSchema = Schema.createArray(Schema.create(Schema.Type.BYTES));
 * 
 */
