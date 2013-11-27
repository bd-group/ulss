/*
 * To change this template, choose Tools | Templates
 * and open th
 * e template in the editor.
 */
package cn.ac.iie.ulss.indexer;

import cn.ac.iie.ulss.struct.BloomFileWriter;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.util.AvroUtils;
import cn.ac.iie.ulss.util.Constants;
import iie.metastore.MetaStoreClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
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
    MetaStoreClient metastoreCli;
    boolean isInterval;
    LuceneFileWriter normalLucWriter;
    BloomFileWriter normalBloWriter;
    /*
     */
    final Object delayWriteLock;
    String pUnit = "";

    WriteIndexFile(DataSourceConfig ds, ArrayBlockingQueue dP, MetaStoreClient msc, LuceneFileWriter norLucWriter, BloomFileWriter norBloWriter, final Object dWL) {
        dbName = ds.getDbName();
        tableName = ds.getTbName();
        timeLable = ds.getTimeLable();
        dataSrcBuf = dP;
        metastoreCli = msc;

        isEnd = false;
        isInterval = false;
        innerDataBufSize = Indexer.writeInnerpoolSize;
        innerDataBuf = new ArrayList(innerDataBufSize);
        normalLucWriter = norLucWriter;
        normalBloWriter = norBloWriter;

        delayWriteLock = dWL;
    }

    @Override
    public void run() {
        LinkedHashMap<String, String> colsMap = new LinkedHashMap<String, String>(50, 0.8f);//数据库的列名和列的数据类型(名字)的映射关系
        //索引名应该是  dbName + tbName + 列 +后缀,这是创建表时定义好的
        LinkedHashMap<String, List<FieldSchema>> idx2colMap = new LinkedHashMap<String, List<FieldSchema>>(50, 0.8f);    //索引名和对应的数据库列的映射
        LinkedHashMap<String, Field> idx2LuceneFieldMap = new LinkedHashMap<String, Field>(50, 0.8f);      //索引名和 lucene域的映射
        LinkedHashMap<String, List<FieldSchema>> idx2DbfidxMap = new LinkedHashMap<String, List<FieldSchema>>(50, 0.8f);   //索引名和 DynamicBloomFilter的映射
        LinkedHashMap<String, FieldType> idx2LuceneFieldTypeMap = new LinkedHashMap<String, FieldType>(50, 0.8f);   // 索引名 和 lucene域的映射

        //idx2fieldMap是根据元数据生成的,从上面可以看出 从索引名得到Lucene的域。然后根据索引名得到对应的数据库的列,可能一个域对应多个数据库的列
        Table tb = null;

        String lowlevel_partColName = null;
        String lowlevel_physical_partType = null;
        int lowlevel_partNum = 0;

        String highlevel_partColName = null;
        String highlevel_partType = null;
        int highlevel_partNum = 0;

        int partLevel = 0;

        long stLongSecond = 0l;
        long edLongSecond = 0l;
        String delimiter = "";

        try {
            tb = Indexer.tablename2Table.get(dbName + "." + tableName);
            log.info("now process table is " + dbName + "." + tableName);

            delimiter = tb.getSd().getParameters().get("colelction.delim");
            for (String s : tb.getSd().getParameters().keySet()) {
                log.info("the Parameters " + s + " value is " + tb.getSd().getParameters().get(s));
            }
            log.info("the delimiter for " + dbName + "." + tableName + " is " + delimiter);

            //to---do 要进行修改！
            delimiter = "#";

            //获取表的分区信息,如果是多级分区，那么实际的物理分区是按照最底层的分区实现的
            List<PartitionFactory.PartitionInfo> pis = PartitionFactory.PartitionInfo.getPartitionInfo(tb.getFileSplitKeys());
            for (PartitionFactory.PartitionInfo pinfo : pis) {
                if (pinfo.getP_level() >= partLevel) {
                    lowlevel_physical_partType = pinfo.getP_type().getName();
                    lowlevel_partColName = pinfo.getP_col();
                    lowlevel_partNum = pinfo.getP_num();
                    partLevel = pinfo.getP_level();
                }
            }
            log.info("the low split type for table " + this.dbName + "." + this.tableName + " is " + lowlevel_physical_partType);
            for (PartitionFactory.PartitionInfo pinfo : pis) {  //以下是得到最顶级的level，也就是一级分区
                if (pinfo.getP_level() <= partLevel) {
                    highlevel_partType = pinfo.getP_type().getName();
                    highlevel_partColName = pinfo.getP_col();
                    highlevel_partNum = pinfo.getP_num();
                    partLevel = pinfo.getP_level();
                }
            }
            log.info("the high split type for table " + this.dbName + "." + this.tableName + " is " + highlevel_partType);

            //下面是得到数据分区的起始时间和结束时间，注意simple ，这是为什么这里使用同步方法的原因
            //stLongSecond = this.getDateLong(timeLable);
            stLongSecond = 0;
            edLongSecond = stLongSecond;
            long intv = 0l;

            if ("interval".equalsIgnoreCase(highlevel_partType)) {      //如果一级分区是interval，那么就要进行延迟的判断，得到时间戳
                isInterval = true;
                if (pis.get(0).getArgs().size() < 2) {        //获取分区的粒度和分区的间隔
                    throw new RuntimeException("get the table's partition unit and interval error");
                } else {
                    List<String> paras = pis.get(0).getArgs();  //Y year,M mponth,W week,D day,H hour，MI minute。 现在只支持H D W， 因为月和年的时间并不是完全确定的，因此无法进行精确的分区，暂时不支持；分钟级的单位太小，暂时也不支持
                    String unit = paras.get(0);
                    pUnit = unit;

                    log.info("the unit is " + unit + " for " + this.dbName + "." + this.tableName);
                    String interval = paras.get(1);
                    if ("'MI'".equalsIgnoreCase(unit)) {   //单位是秒
                        intv = Long.parseLong(interval) * 60;
                    } else if ("'H'".equalsIgnoreCase(unit)) {   //单位是秒
                        intv = Long.parseLong(interval) * 60 * 60;
                    } else if ("'D'".equalsIgnoreCase(unit)) {
                        intv = Long.parseLong(interval) * 60 * 60 * 24;
                    } else if ("'W'".equalsIgnoreCase(unit)) {
                        intv = Long.parseLong(interval) * 60 * 60 * 24 * 7;
                    } else {
                        throw new RuntimeException("now the partition unit is not support, it only supports --- W week,D day,H hour");
                    }
                }
                edLongSecond += intv;
                //上面得到了数据分区的起始时间和结束时间
                log.info("the begin and end time is " + stLongSecond + " " + edLongSecond + " and the interval is " + intv + " seconds");
            }

            List<FieldSchema> cols = tb.getSd().getCols();//获取列
            for (FieldSchema col : cols) {
                colsMap.put(col.getName().toLowerCase(), col.getType());
            }

            List<Index> idxList = Indexer.tablename2indexs.get(dbName + "." + tableName);
            for (Index idx : idxList) {
                String idxName = idx.getIndexName().toLowerCase();
                log.info("index name is " + idxName);
                List<FieldSchema> indexCols = idx.getSd().getCols();
                idx2colMap.put(idxName.toLowerCase(), indexCols);
            }

            for (Index idx : idxList) {   //下面是生成了idx2fieldMap，生成了索引名与Lucene的域的对应关系
                if (idx.getIndexHandlerClass().equalsIgnoreCase("lucene")) {
                    Field field;
                    //得到索引对应的原始数据对应的列（是一个list，里面列的数据类型、索引类型必须是一致的，否则无法进行索引创建),根据list第一个元素得到相关信息:数据的类型;后是创建参数
                    String dataType = idx.getSd().getCols().get(0).getType();

                    String lucIdx = idx.getParameters().get("lucene.indexd");
                    String lucStore = idx.getParameters().get("lucene.stored");
                    String lucAnaly = idx.getParameters().get("lucene.analyzed");

                    //根据  数据  的 类型继续 索引类型的选择  date建成 number的，而string 则是根据进步进行 分析  来区别
                    if (idx.getSd().getCols().size() != 1) {
                        log.error("the index column num is  not 1,now this is not supported !");
                        throw new RuntimeException("the index column num is  not 1,now this is not supported now");
                    }

                    if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_TIMESTAMP) || dataType.equalsIgnoreCase("array<timestamp>")) {
                        FieldType ft = this.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new LongField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.MT_LUCENE_INDEX_TYPE_STRING) || dataType.equalsIgnoreCase("array<string>")) {
                        //need to improve it
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
                        byte[] val = new byte[2];
                        field = new Field(idx.getIndexName().toLowerCase(), val, ft);
                    } else {
                        throw new RuntimeException("Index " + idx.getIndexName() + " type[" + dataType + "] is not supported  now !");
                    }
                    log.info("put " + idx.getIndexName().toLowerCase() + " to " + "idx2LuceneFieldMap");
                    idx2LuceneFieldMap.put(idx.getIndexName().toLowerCase(), field);   //fields is HashMap，<域的名字，对应的域>
                } else if (idx.getIndexHandlerClass().equalsIgnoreCase("bloomfilter")) {
                    idx2DbfidxMap.put(idx.getIndexName().toLowerCase(), idx.getSd().getCols());
                    log.info("this is bloomfilter  index");
                } else {
                    log.error("the index is not support now except lucene index !");
                }
            }
        } catch (Exception ex) {
            log.error(ex, ex);
        }

        String dateStr = null;
        String dataType = null;
        String dataName = null;

        IndexWriter writer = null;
        int hashKey = -100;

        String schemaName = Indexer.tablename2Schemaname.get(this.tableName.toLowerCase());
        log.info("get schema for table " + this.tableName + " and the schemaname is " + schemaName);

        String schemaContent = Indexer.schemaname2schemaContent.get(schemaName);
        log.info("schema content for " + schemaName + " is " + schemaContent);

        Protocol protocol = AvroUtils.getProtocol(schemaContent);
        DatumReader<GenericRecord> dxxReader = AvroUtils.getReader(protocol, schemaName);

        log.info("docsSchema is " + Indexer.docs_protocal);
        Protocol p = AvroUtils.getProtocol(Indexer.docs_protocal);
        DatumReader<GenericRecord> docsReader = AvroUtils.getReader(p, Indexer.docs_schema_name);

        int count = 0;
        String partString = "";
        int num_int_val = 0;
        long num_long_val = 0;
        double num_double_val = 0;
        float num_float_val = 0;
        long longDate = 0;
        while (true) {
            try {
                //log.debug("the isEnd lable and the data src is empty ? " + isEnd + " " + dataSrcBuf.isEmpty());
                if (isEnd && dataSrcBuf.isEmpty()) {    //如果应该结束，并且缓冲区为空，则就可以break。这里的isEnd肯定是度线程先结束后才会设置写线程结束的
                    log.info("one write thread stop ok ......");
                    break;
                }
                for (int i = 0; i < innerDataBufSize; i++) {
                    GenericRecord gr = (GenericRecord) dataSrcBuf.poll();
                    if (gr != null) {
                        //log.info("the record will be write to lucene is " + gr);
                        innerDataBuf.add(gr); //take方法会自动等待，直到有元素可用,但是poll方法会返回null，不会等待。将null键入到dataBuf中也没有问题
                    }
                }
                if (innerDataBuf.isEmpty()) {
                    //log.info("the write not get data just sleep ...");
                    Thread.sleep(1000);
                    continue;
                }
            } catch (Exception e) {
                log.error(e, e);
            }
            for (GenericRecord ms : innerDataBuf) {
                if (ms == null) {
                    log.error("the write not get null data！ it is wrong");
                } else if (ms != null) {
                    try {
                        GenericRecord docsRecord = ms;
                        GenericArray docSet = (GenericData.Array<GenericRecord>) docsRecord.get(Indexer.docs_set_name);
                        Iterator<ByteBuffer> itor = docSet.iterator();
                        while (itor.hasNext()) {    //得更改系统实现，不能表明。 根据数据类型、得到对应的处理方案
                            Document doc = new Document();
                            ByteArrayInputStream dxxbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                            BinaryDecoder dxxbd = new DecoderFactory().binaryDecoder(dxxbis, null);
                            GenericRecord dxxRecord;
                            dxxRecord = dxxReader.read(null, dxxbd);
                            hashKey = 0;//新版本的文件里只写一个文件

                            writer = this.normalLucWriter.getWriterMap().get(hashKey);
                            log.debug(hashKey + " " + this.normalLucWriter.getWriterMap().keySet());

                            String ss[] = null;
                            String sss[] = null;

                            try {
                                for (String idxName : idx2LuceneFieldMap.keySet()) {
                                    dataType = idx2colMap.get(idxName).get(0).getType();
                                    dataName = idx2colMap.get(idxName).get(0).getName();
                                    Object o = dxxRecord.get(dataName);
                                    if (o == null) {
                                        log.debug("get null for " + dataName);
                                        continue;
                                    }
                                    Field field = idx2LuceneFieldMap.get(idxName);
                                    if (dataType.equalsIgnoreCase("array<timestamp>")) {  //如果是timestamp类型的
                                        String strData = o.toString();
                                        if (!"".equalsIgnoreCase(strData)) {
                                            if (isInterval && dataName.equalsIgnoreCase(highlevel_partColName)) {
                                                log.error("interval partition not support on array type");
                                                throw new RuntimeException("interval partition not support on array type");
                                            }
                                            ss = strData.split(delimiter);
                                            for (int j = 0; j < ss.length; j++) {
                                                LongField f = new LongField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                dateStr = ss[j];
                                                longDate = Long.parseLong(dateStr);
                                                f.setLongValue(longDate);
                                                doc.add(f);
                                            }
                                        }
                                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_TIMESTAMP)) {
                                        String strData = o.toString();
                                        if (!"".equalsIgnoreCase(strData)) {
                                            longDate = Long.parseLong(strData);
                                            ((LongField) field).setLongValue(longDate);
                                            doc.add(field);
                                            if (isInterval) {
                                                if (stLongSecond <= longDate && longDate < edLongSecond) {
                                                } else {
                                                }
                                            }
                                        }
                                    } else if (field instanceof IntField) {
                                        String strData = o.toString();
                                        if (!"".equalsIgnoreCase(strData)) {
                                            if (dataType.equalsIgnoreCase("array<int>")) {
                                                ss = strData.split(delimiter);
                                                for (int j = 0; j < ss.length; j++) {
                                                    IntField f = new IntField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                    num_int_val = Integer.parseInt(ss[j]);
                                                    f.setIntValue(num_int_val);
                                                    doc.add(f);
                                                }
                                            } else {
                                                num_int_val = Integer.parseInt(strData);
                                                ((IntField) field).setIntValue(num_int_val);
                                                doc.add(field);
                                            }
                                        }
                                    } else if ((field instanceof LongField) && dataType.equalsIgnoreCase(Constants.DATA_TYPE_BIGINT)) { //如果是long型的
                                        String strData = o.toString();
                                        if (!"".equalsIgnoreCase(strData)) {
                                            if (dataType.equalsIgnoreCase("array<bigint>")) {
                                                ss = strData.split(delimiter);
                                                for (int j = 0; j < ss.length; j++) {
                                                    LongField f = new LongField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                    num_long_val = Long.parseLong(ss[j]);
                                                    f.setLongValue(num_long_val);
                                                    doc.add(f);
                                                }
                                            } else {
                                                num_long_val = Long.parseLong(strData);
                                                ((LongField) field).setLongValue(num_long_val);
                                                doc.add(field);
                                            }
                                        }
                                    } else if (field instanceof FloatField) {
                                        String strData = o.toString();
                                        if (!"".equalsIgnoreCase(strData)) {
                                            if (dataType.equalsIgnoreCase("array<float>")) {
                                                ss = strData.split(delimiter);
                                                for (int j = 0; j < ss.length; j++) {
                                                    FloatField f = new FloatField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                    num_float_val = Float.parseFloat(ss[j]);
                                                    f.setFloatValue(num_float_val);
                                                    doc.add(f);
                                                }
                                            } else {
                                                num_float_val = Float.parseFloat(strData);
                                                ((FloatField) field).setFloatValue(num_float_val);
                                                doc.add(field);
                                            }
                                        }
                                    } else if (field instanceof DoubleField) {
                                        String strData = o.toString();
                                        if (!"".equalsIgnoreCase(strData)) {
                                            if (dataType.equalsIgnoreCase("array<double>")) {
                                                ss = strData.split(delimiter);
                                                for (int j = 0; j < ss.length; j++) {
                                                    DoubleField f = new DoubleField(idxName, -999, idx2LuceneFieldTypeMap.get(idxName));
                                                    num_double_val = Double.parseDouble(ss[j]);
                                                    f.setDoubleValue(num_double_val);
                                                    doc.add(f);
                                                }
                                            } else {
                                                //java.sql.Types.DATEjava的SQL数据类型
                                                num_double_val = Double.parseDouble(strData);
                                                ((DoubleField) field).setDoubleValue(num_double_val);
                                                doc.add(field);
                                            }
                                        }
                                    } else if (field instanceof TextField) {
                                        String strData = o.toString();
                                        if (dataType.equalsIgnoreCase("array<string>")) {
                                            ss = strData.split(delimiter);
                                            for (int j = 0; j < ss.length; j++) {
                                                TextField f = new TextField(idxName, "", Field.Store.YES);
                                                f.setStringValue(ss[j]);
                                                doc.add(f);
                                            }
                                        } else {
                                            ((TextField) field).setStringValue(strData);
                                            doc.add(field);
                                        }
                                    } else if (field instanceof StringField) {

                                        if (dataType.equalsIgnoreCase("array<string>")) {
                                            String strData = o.toString();
                                            ss = strData.split(delimiter);
                                            for (int j = 0; j < ss.length; j++) {
                                                StringField f = new StringField(idxName, "", Field.Store.YES);
                                                f.setStringValue(ss[j]);
                                                doc.add(f);
                                            }
                                        } else if (this.tableName.toLowerCase().startsWith("t_dx_rz") && "c_esmnr".equalsIgnoreCase(idxName)) {
                                            String strData = new String(((ByteBuffer) o).array());
                                            ((StringField) field).setStringValue(strData);
                                            doc.add(field);
                                        } else {
                                            String strData = o.toString();
                                            ((StringField) field).setStringValue(strData);
                                            doc.add(field);
                                        }
                                    } else {
                                        ByteBuffer data = (ByteBuffer) o;
                                        field.setBytesValue(data.array());
                                        doc.add(field);
                                    }
                                }
                            } catch (Exception e) {
                                log.error(e, e);
                            }
                            try {
                                writer.addDocument(doc);
                            } catch (Exception e) {
                                log.error(e, e);
                            }
                            count++;
                        }
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                }
            }

            innerDataBuf.clear();
//            } catch (Exception ex) {
//                log.error(ex, ex);
//            }
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

    public static void main(String[] args) throws InterruptedException, IOException {
        AtomicLong al = new AtomicLong(0);

        System.out.println(al.addAndGet(12));
        System.out.println(al.get());
    }
}
