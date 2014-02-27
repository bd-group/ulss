/*
 * To change this template, choose Tools | Templates
 * and open th
 * e template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.constant.Constants;
import cn.ac.iie.ulss.indexer.constant.DataTypeMap;
import cn.ac.iie.ulss.struct.BloomFileWriter;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.utiltools.AvroUtils;
import iie.metastore.MetaStoreClient;
import iie.mm.client.ClientAPI;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Table;
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

public class WriteIndexFile implements Runnable {

    public static Logger log = Logger.getLogger(WriteIndexFile.class.getName());
    private SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public AtomicLong writeOffset;
    String dbName;
    String tableName;
    String timeLable;
    String metaQtopic;
    ArrayBlockingQueue dataSrcBuf;
    List<GenericRecord> innerDataBuf;
    int innerDataBufSize = 1;   //1000,实际是每1000条发送一次，包起来作为一个单位发送过来
    volatile boolean isEnd;
    ClientAPI ca;
    boolean isInterval;
    LuceneFileWriter normalLucWriter;
    BloomFileWriter normalBloWriter;
    /*
     */
    String pUnit = "";
    AtomicBoolean isDiskBad;

    WriteIndexFile(DataSourceConfig ds, ArrayBlockingQueue dP,  ClientAPI capi, LuceneFileWriter norLucWriter, BloomFileWriter norBloWriter, AtomicBoolean isdiskbad) {
        dbName = ds.getDbName();
        tableName = ds.getTbName();
        timeLable = ds.getTimeLable();
        dataSrcBuf = dP;
        ca = capi;

        isEnd = false;
        isInterval = false;
        innerDataBufSize = Indexer.writeInnerpoolSize;
        innerDataBuf = new ArrayList(innerDataBufSize);
        normalLucWriter = norLucWriter;
        normalBloWriter = norBloWriter;

        isDiskBad = isdiskbad;
    }

    @Override
    public void run() {
        LinkedHashMap<String, String> colsMap = new LinkedHashMap<String, String>(50, 0.8f);//数据库的列名和列的数据类型(名字)的映射关系
        /*索引名与列名一致
         列名与schema内字段名一致*/
        LinkedHashMap<String, List<FieldSchema>> idx2colMap = new LinkedHashMap<String, List<FieldSchema>>(50, 0.8f);    //索引名和对应的数据库列的映射
        LinkedHashMap<String, Field> idx2LuceneFieldMap = new LinkedHashMap<String, Field>(50, 0.8f);      //索引名和 lucene域的映射
        //LinkedHashMap<String, List<FieldSchema>> idx2DbfidxMap = new LinkedHashMap<String, List<FieldSchema>>(50, 0.8f);   //索引名和 DynamicBloomFilter的映射
        LinkedHashMap<String, FieldType> idx2LuceneFieldTypeMap = new LinkedHashMap<String, FieldType>(50, 0.8f);   // 索引名 和 lucene域的映射

        Table tb = null;
        try {
            tb = Indexer.tablename2Table.get(dbName + "." + tableName);
            log.info("now process table is " + dbName + "." + tableName + " " + tb);

            List<FieldSchema> cols = tb.getSd().getCols();//获取列
            for (FieldSchema col : cols) {
                colsMap.put(col.getName().toLowerCase(), col.getType());
            }

            List<Index> idxList = Indexer.tablename2indexs.get(dbName + "." + tableName);
            for (Index idx : idxList) {
                String idxName = idx.getIndexName().toLowerCase();
                log.debug("index name is " + idxName);
                List<FieldSchema> indexCols = idx.getSd().getCols();
                idx2colMap.put(idxName.toLowerCase(), indexCols);
            }

            /*特殊处理c_inputtime这个字段*/
            FieldType fieldtype = this.getFieldType("true", "true", "long");
            idx2LuceneFieldTypeMap.put("c_inputtime", fieldtype);

            /*下面是生成了idx2fieldMap，生成了索引名与Lucene的域的对应关系*/
            for (Index idx : idxList) {
                if (idx.getIndexHandlerClass().equalsIgnoreCase("lucene")) {
                    Field field;
                    /* 得到索引对应的原始数据对应的列（是一个list，里面列的数据类型、索引类型必须是一致的，否则无法进行索引创建),
                     * 根据list第一个元素得到相关信息:数据的类型;后是创建参数*/
                    String dataType = idx.getSd().getCols().get(0).getType();

                    String lucIdx = idx.getParameters().get("lucene.indexd");
                    String lucStore = idx.getParameters().get("lucene.stored");
                    String lucAnaly = idx.getParameters().get("lucene.analyzed");

                    if (idx.getSd().getCols().size() != 1) {
                        log.error("the index column num is  not 1,now this is not supported !");
                        throw new RuntimeException("the index column num is  not 1,now this is not supported now");
                    }

                    if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_TIMESTAMP) || dataType.equalsIgnoreCase("array<timestamp>")) {
                        FieldType ft = this.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new LongField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.MT_LUCENE_INDEX_TYPE_STRING) || dataType.equalsIgnoreCase("array<string>")) {
                        if ("true".equalsIgnoreCase(lucIdx) && "false".equalsIgnoreCase(lucAnaly)) {
                            field = new StringField(idx.getIndexName().toLowerCase(), "", Field.Store.YES);
                        } else if ("true".equalsIgnoreCase(lucIdx) && "true".equalsIgnoreCase(lucAnaly)) {
                            field = new TextField(idx.getIndexName().toLowerCase(), "", TextField.Store.YES);
                        } else {
                            log.error("the index type is not support now");
                            throw new RuntimeException("the index type is not support now");
                        }
                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_INT) || dataType.equalsIgnoreCase("array<int>")) {
                        FieldType ft = this.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new IntField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_BIGINT) || dataType.equalsIgnoreCase("array<bigint>")) {
                        FieldType ft = this.getFieldType(lucIdx, lucStore, "long");
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new LongField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.DARA_TYPE_FLOAT) || dataType.equalsIgnoreCase("array<float>")) {
                        FieldType ft = this.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new FloatField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_DOUBLE) || dataType.equalsIgnoreCase("array<double>")) {
                        FieldType ft = this.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new DoubleField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_BINARY) || dataType.equalsIgnoreCase("array<binary>")) {
                        FieldType ft = this.getBinaryFieldType(lucIdx, lucStore);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        byte[] val = new byte[1];
                        field = new Field(idx.getIndexName().toLowerCase(), val, ft);
                    } else if (dataType.equalsIgnoreCase("blob") || dataType.equalsIgnoreCase("array<blob>")) {
                        log.debug("now is a blob type for field: " + idx.getIndexName().toLowerCase());
                        field = new StringField(idx.getIndexName().toLowerCase(), "", Field.Store.YES);
                    } else {
                        throw new RuntimeException("Index " + idx.getIndexName() + " type[" + dataType + "] is not supported  now !");
                    }
                    idx2LuceneFieldMap.put(idx.getIndexName().toLowerCase(), field);
                    log.debug("put " + idx.getIndexName().toLowerCase() + " to idx2LuceneFieldMap");
                } else if (idx.getIndexHandlerClass().equalsIgnoreCase("bloomfilter")) {
                    //idx2DbfidxMap.put(idx.getIndexName().toLowerCase(), idx.getSd().getCols());
                    log.info("this is bloomfilter index");
                } else {
                    log.error("the index is not support now except lucene index !");
                }
            }
        } catch (Exception ex) {
            log.error(ex, ex);
        }


        IndexWriter writer = null;
        int hashKey = -100;

        String schemaName = Indexer.tablename2Schemaname.get(this.tableName.toLowerCase());
        log.debug("get schema for table " + this.tableName + " and the schemaname is " + schemaName);
        String schemaContent = Indexer.schemaname2schemaContent.get(schemaName);
        log.debug("schema content for " + schemaName + " is " + schemaContent);

        Protocol protocol = AvroUtils.getProtocol(schemaContent);
        DatumReader<GenericRecord> dxxReader = AvroUtils.getReader(protocol, schemaName);
        log.debug("docsSchema is " + Indexer.docs_protocal);

        Schema arrayIntSchema = Schema.createArray(Schema.create(Schema.Type.INT));
        Schema arrayLongSchema = Schema.createArray(Schema.create(Schema.Type.LONG));
        Schema arrayFloatSchema = Schema.createArray(Schema.create(Schema.Type.FLOAT));
        Schema arrayDoubleSchema = Schema.createArray(Schema.create(Schema.Type.DOUBLE));
        Schema arrayStringSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        Schema arrayBytesSchema = Schema.createArray(Schema.create(Schema.Type.BYTES));

        GenericArray arrayInt = null;
        GenericArray arrayLong = null;
        GenericArray arrayFloat = null;
        GenericArray arrayDouble = null;
        GenericArray arrayString = null;
        GenericArray arrayBytes = null;

        int num_int_val = 0;
        long num_long_val = 0;
        double num_double_val = 0;
        float num_float_val = 0;
        Integer dataTypeCase = 0;
        int arrayIdx = 0;

        String dataType = null;
        String dataName = null;
        String strData = null;
        ByteBuffer bytesdata = null;
        int writenum = 0;
        long bg = System.currentTimeMillis();
        while (true) {
            try {
                if (isEnd && dataSrcBuf.isEmpty()) { //如果应该结束，并且缓冲区为空，则就可以break。这里的isEnd肯定是度线程先结束后才会设置写线程结束的
                    log.info("one write thread stop ok ......");
                    break;
                }
                /*
                 *将不停poll改为了drainto，效率可以提高一倍左右
                 */
                dataSrcBuf.drainTo(innerDataBuf, innerDataBufSize);
                if (innerDataBuf.isEmpty()) {
                    Thread.sleep(100);
                    continue;
                }
            } catch (Exception e) {
                log.error(e, e);
            }
            bg = System.currentTimeMillis();
            for (GenericRecord ms : innerDataBuf) {
                if (ms == null) {
                    log.error("the write not get null data！ it is wrong");
                } else if (ms != null) {
                    try {
                        GenericRecord docsRecord = ms;
                        GenericArray docSet = (GenericData.Array<GenericRecord>) docsRecord.get(Indexer.docs_set_name);
                        Iterator<ByteBuffer> itor = docSet.iterator();

                        while (itor.hasNext()) {
                            Document doc = new Document();
                            ByteArrayInputStream dxxbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                            BinaryDecoder dxxbd = new DecoderFactory().binaryDecoder(dxxbis, null);
                            GenericRecord record;
                            record = dxxReader.read(null, dxxbd);
                            hashKey = 0;
                            writer = this.normalLucWriter.getWriterMap().get(hashKey);

                            try {
                                for (String idxName : idx2LuceneFieldMap.keySet()) {
                                    dataType = idx2colMap.get(idxName).get(0).getType();
                                    dataName = idx2colMap.get(idxName).get(0).getName();
                                    Object o = record.get(dataName);
                                    if (o == null) {
                                        log.debug("get null for " + dataName);
                                        continue;
                                    }
                                    if ("c_inputtime".equalsIgnoreCase(dataName)) {
                                        LongField f = new LongField("c_inputtime", System.currentTimeMillis() / 1000, idx2LuceneFieldTypeMap.get("c_inputtime"));
                                        doc.add(f);
                                        continue;
                                    }

                                    Field field = idx2LuceneFieldMap.get(idxName);
                                    dataTypeCase = DataTypeMap.datatypeMap.get(dataType);

                                    if (dataTypeCase == null) {
                                        log.error("not suppoot data type " + idxName + " " + dataType);
                                    } else {
                                        switch (dataTypeCase) {
                                            case 0: //int
                                                //if (field instanceof IntField) {
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
                                            //} else {
                                            //    log.error("the data type define in platform is not equal to type in the lucene,why ?");
                                            //}
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
                                            case 2:   //long
                                                log.info("the dataplatform does not need long now ");
                                                break;
                                            case 3:   //arraylong
                                                log.info("the dataplatform does not need array<long> now ");
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
                                                        field.setBytesValue(bytesdata.array());
                                                        doc.add(field);
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
                                writer.addDocument(doc);
                            } catch (IOException e) {
                                this.isDiskBad.set(true);
                                log.error(e, e);
                            } catch (Exception e) {
                                log.error(e, e);
                            }
                            writenum++;
                        }
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                }
            }
            log.info("write lucene file to disk use " + (System.currentTimeMillis() - bg) + " ms for " + writenum);
            writenum = 0;
            innerDataBuf.clear();
        }
    }

    String trim86(String src) {
        String des = src;
        String[] prefix = {"86", "+86", "+"};
        for (String p : prefix) {
            if (src.startsWith(p)) {
                des = src.substring(p.length());
                return des;
            }
        }
        return des;
    }

    boolean isNumeric(String type) {
        if (Constants.DATA_TYPE_DATE.equalsIgnoreCase(type)
                || Constants.MT_LUCENE_INDEX_TYPE_NUMBER.equalsIgnoreCase(type)
                || Constants.DATA_TYPE_INT.equalsIgnoreCase(type)
                || Constants.DATA_TYPE_BIGINT.equalsIgnoreCase(type)
                || Constants.DARA_TYPE_FLOAT.equalsIgnoreCase(type)
                || Constants.DATA_TYPE_DOUBLE.equalsIgnoreCase(type)) {
            return true;
        } else {
            return false;
        }
    }

    private FieldType getFieldType(String isIndexed, String isStored, String dataType) {
        FieldType ft = new FieldType();
        if ("true".equalsIgnoreCase(isIndexed)) {
            ft.setIndexed(true);
        } else {
            ft.setIndexed(false);
        }
        if ("true".equalsIgnoreCase(isStored)) {
            ft.setStored(true);
        } else {
            ft.setStored(false);
        }
        if ("long".equalsIgnoreCase(dataType) || "array<long>".equalsIgnoreCase(dataType) || "timestamp".equalsIgnoreCase(dataType) || "array<timestamp>".equalsIgnoreCase(dataType)) {
            ft.setNumericType(FieldType.NumericType.LONG);
        } else if ("int".equalsIgnoreCase(dataType) || "array<int>".equalsIgnoreCase(dataType)) {
            ft.setNumericType(FieldType.NumericType.INT);
        } else if ("float".equalsIgnoreCase(dataType) || "array<float>".equalsIgnoreCase(dataType)) {
            ft.setNumericType(FieldType.NumericType.FLOAT);
        } else if ("double".equalsIgnoreCase(dataType) || "array<double>".equalsIgnoreCase(dataType)) {
            ft.setNumericType(FieldType.NumericType.DOUBLE);
        }
        return ft;
    }

    private FieldType getBinaryFieldType(String isIndexed, String isStored) {
        FieldType ft = new FieldType();
        if ("true".equalsIgnoreCase(isIndexed)) {
            ft.setIndexed(true);
        } else {
            ft.setIndexed(false);
        }
        ft.setIndexed(false);
        if ("true".equalsIgnoreCase(isStored)) {
            ft.setStored(true);
        } else {
            ft.setStored(false);
        }
        return ft;
    }

    public void setIsEnd(boolean isEnd) {
        this.isEnd = isEnd;
    }

    public boolean isIsEnd() {
        return isEnd;
    }

    public void addOffset(long add) {
        writeOffset.addAndGet(add);
    }

    public synchronized String getTimeLable() {
        return timeLable;
    }

    public synchronized void setTimeLable(String timeLable) {
        this.timeLable = timeLable;
    }

    public synchronized long getDateLong(String timeLable) throws ParseException {
        long longSecond = secondFormat.parse(this.getTimeLable()).getTime() / 1000;
        return longSecond;
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