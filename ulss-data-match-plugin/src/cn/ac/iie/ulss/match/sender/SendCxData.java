/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.sender;

import cn.ac.iie.ulss.match.worker.Matcher;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;

/**
 *
 * @author liucuili
 */
public class SendCxData implements Runnable {

    public static Logger log = Logger.getLogger(SendCxData.class.getName());
    public String region;
    public String schemaName;
    public String schemanameInstance;
    public int batchSize;
    public List<String> schemaNames = new ArrayList<String>();
    public ConcurrentHashMap<String, MQProducerPool> MQProducer = new ConcurrentHashMap<String, MQProducerPool>();
    public ConcurrentHashMap<String, LinkedBlockingQueue> BufferMap = new ConcurrentHashMap<String, LinkedBlockingQueue>();
    public ConcurrentHashMap<String, Long> sendgabMap = new ConcurrentHashMap<String, Long>();
    /**/
    public LinkedBlockingQueue<GenericRecord> t_cxBuf;
    public LinkedBlockingQueue<GenericRecord> t_cx_rzBuf = new LinkedBlockingQueue<GenericRecord>(3000);
    public LinkedBlockingQueue<GenericRecord> t_cx_rz_ztBuf = new LinkedBlockingQueue<GenericRecord>(3000);
    public LinkedBlockingQueue<GenericRecord> t_cx_rz_mddzBuf = new LinkedBlockingQueue<GenericRecord>(5000);
    public LinkedBlockingQueue<GenericRecord> t_cx_rz_mtzxBuf = new LinkedBlockingQueue<GenericRecord>(5000);
    public LinkedBlockingQueue<GenericRecord> t_cx_rz_tjBuf = new LinkedBlockingQueue<GenericRecord>(10000);
    /**/
    public Schema t_cx_cdrSchema = null;
    public Schema mddzSchema = null;
    public Schema mtzxSchema = null;
    /*
     */
    public Schema t_cx_rzSchema = null;
    public Schema t_cx_rz_ztSchema = null;
    public Schema t_cx_rz_mddzSchema = null;
    public Schema t_cx_rz_mtzxSchema = null;
    public Schema t_cx_rz_tjSchema = null;
    /*
     s*/
    public Set<String> t_cx_cdrFieldNames = new HashSet<String>();
    public Set<String> mddzFieldNames = new HashSet<String>();
    public Set<String> mtzxFieldNames = new HashSet<String>();
    /**/
    public Set<String> t_cx_rzFieldNames = new HashSet<String>();
    public Set<String> t_cx_rz_ztFieldNames = new HashSet<String>();
    public Set<String> t_cx_rz_mddzFieldNames = new HashSet<String>();
    public Set<String> t_cx_rz_mtzxFieldNames = new HashSet<String>();
    /**/
    public Set<String> t_cx_rz_tjFieldNames = new HashSet<String>();
    /**/
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
    GenericArray arrayStringSlt = null;
    GenericArray arrayBytes = null;
    /*
     *
     */
    private SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd HH");

    public void init(String reg, int bs, LinkedBlockingQueue<GenericRecord> b) {
        schemaNames.add("t_cx");
        schemaNames.add("t_cx_rz");
        schemaNames.add("t_cx_rz_zt");
        schemaNames.add("t_cx_rz_mddz");
        schemaNames.add("t_cx_rz_mtzx");
        schemaNames.add("t_cx_rz_tj");
        this.region = reg;
        this.batchSize = 50;
        this.t_cxBuf = b;

        BufferMap.put(region + "." + "t_cx_rz", t_cx_rzBuf);
        BufferMap.put(region + "." + "t_cx_rz_zt", t_cx_rz_ztBuf);
        BufferMap.put(region + "." + "t_cx_rz_mddz", t_cx_rz_mddzBuf);
        BufferMap.put(region + "." + "t_cx_rz_mtzx", t_cx_rz_mtzxBuf);
        BufferMap.put(region + "." + "t_cx_rz_tj", t_cx_rz_tjBuf);

        sendgabMap.put(region + "." + "t_cx_rz", System.currentTimeMillis());
        sendgabMap.put(region + "." + "t_cx_rz_zt", System.currentTimeMillis());
        sendgabMap.put(region + "." + "t_cx_rz_mddz", System.currentTimeMillis());
        sendgabMap.put(region + "." + "t_cx_rz_mtzx", System.currentTimeMillis());
        sendgabMap.put(region + "." + "t_cx_rz_tj", System.currentTimeMillis());

        Protocol protocol = Protocol.parse(Matcher.DBMeta.getSchema(this.region, "t_cx_cdr").get(0).get(1).toLowerCase());//原始的schema

        t_cx_cdrSchema = protocol.getType("t_cx_cdr");
        for (Field f : t_cx_cdrSchema.getFields()) {
            t_cx_cdrFieldNames.add(f.name().toLowerCase());
        }
        mddzSchema = protocol.getType("mddz");
        for (Field f : mddzSchema.getFields()) {
            mddzFieldNames.add(f.name().toLowerCase());
        }
        mtzxSchema = protocol.getType("mtzx");
        for (Field f : mtzxSchema.getFields()) {
            mtzxFieldNames.add(f.name().toLowerCase());
        }
        /*
         * for data platform
         */
        t_cx_rzSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_cx_rz").get(0).get(1).toLowerCase()).getType("t_cx_rz");
        for (Field f : t_cx_rzSchema.getFields()) {
            t_cx_rzFieldNames.add(f.name().toLowerCase());
        }
        /*
         * for datawarehouse
         */
        t_cx_rz_ztSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_cx_rz_zt").get(0).get(1).toLowerCase()).getType("t_cx_rz_zt");
        for (Field f : t_cx_rz_ztSchema.getFields()) {
            t_cx_rz_ztFieldNames.add(f.name().toLowerCase());
        }
        t_cx_rz_mddzSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_cx_rz_mddz").get(0).get(1).toLowerCase()).getType("t_cx_rz_mddz");
        for (Field f : t_cx_rz_mddzSchema.getFields()) {
            t_cx_rz_mddzFieldNames.add(f.name().toLowerCase());
        }
        t_cx_rz_mtzxSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_cx_rz_mtzx").get(0).get(1).toLowerCase()).getType("t_cx_rz_mtzx");
        for (Field f : t_cx_rz_mtzxSchema.getFields()) {
            t_cx_rz_mtzxFieldNames.add(f.name().toLowerCase());
        }
        /*
         */
        t_cx_rz_tjSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_cx_rz_tj").get(0).get(1).toLowerCase()).getType("t_cx_rz_tj");
        for (Field f : t_cx_rz_tjSchema.getFields()) {
            t_cx_rz_tjFieldNames.add(f.name().toLowerCase());
        }


        MQProducer.put(region + "." + "t_cx_rz", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_cx_rz"), 10));
        MQProducer.put(region + "." + "t_cx_rz_zt", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_cx_rz_zt"), 10));
        MQProducer.put(region + "." + "t_cx_rz_mddz", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_cx_rz_mddz"), 10));
        MQProducer.put(region + "." + "t_cx_rz_mtzx", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_cx_rz_mtzx"), 10));
        MQProducer.put(region + "." + "t_cx_rz_tj", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_cx_rz_tj"), 10));
    }

    public byte[] packData(List<GenericRecord> data, String schemaName) throws IOException {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(Matcher.schemaname2Schema.get(schemaName.toLowerCase()));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BinaryEncoder be = new EncoderFactory().binaryEncoder(bos, null);
        Schema docsSchema = Matcher.schemaname2Schema.get("docs");
        GenericRecord docsRecord = new GenericData.Record(Matcher.schemaname2Schema.get("docs"));
        GenericArray docSet = new GenericData.Array<ByteBuffer>(data.size(), Matcher.schemaname2Schema.get("docs").getField("doc_set").schema());

        for (GenericRecord gr : data) {
            log.debug("this is gr " + schemaName + "  " + gr);
            writer.write(gr, be);
            be.flush();
            docSet.add(ByteBuffer.wrap(bos.toByteArray()));
            bos.reset();
        }
        docsRecord.put("doc_schema_name", schemaName);
        docsRecord.put("doc_set", docSet);
        docsRecord.put("sign", "123456");

        DatumWriter<GenericRecord> docsWriter = new GenericDatumWriter<GenericRecord>(docsSchema);
        ByteArrayOutputStream docsBos = new ByteArrayOutputStream();
        BinaryEncoder docsBe = new EncoderFactory().binaryEncoder(docsBos, null);
        docsWriter.write(docsRecord, docsBe);
        docsBe.flush();

        return docsBos.toByteArray();
    }

    public boolean splitRecord(GenericRecord gr) throws InterruptedException {
        log.debug("this is original cx " + gr);
        GenericArray mddzs = (GenericData.Array<GenericRecord>) gr.get("c_mddz_jh");
        GenericArray mtzxs = (GenericData.Array<GenericRecord>) gr.get("c_mtzx_jh");

        /*
         * * *  产生统计表
         */
        int idx = 0;
        int total = 0;
        int mddzSize = 0;
        int mtzxSize = 0;
        String tmpStr = "";
        List<String> mddzpzIds = new ArrayList<String>();
        List<String> mddzPzlxIds = new ArrayList<String>();
        List<String> mtzxsIds = new ArrayList<String>();
        List<String> mtzxsPzlxIds = new ArrayList<String>();
        String[] tmpPzid;
        String[] tmpPzlxid;
        for (Object r : mddzs) {
            GenericRecord tm = (GenericRecord) r;
            tmpStr = tm.get("c_dzpzidjh").toString();
            if ("".equalsIgnoreCase(tmpStr)) {
                continue;
            }
            tmpPzid = tm.get("c_dzpzidjh").toString().split("[,]");
            tmpPzlxid = tm.get("c_dzpzlxidjh").toString().split("[,]");
            mddzSize += tmpPzid.length;
            total += tmpPzid.length;
            mddzpzIds.addAll(Arrays.asList(tmpPzid));
            mddzPzlxIds.addAll(Arrays.asList(tmpPzlxid));
        }
        for (Object r : mtzxs) {
            GenericRecord tm = (GenericRecord) r;
            tmpStr = tm.get("c_mtpzidjh").toString();
            if ("".equalsIgnoreCase(tmpStr)) {
                continue;
            }
            tmpPzid = tm.get("c_mtpzidjh").toString().split("[,]");
            tmpPzlxid = tm.get("c_mtpzlxidjh").toString().split("[,]");
            mtzxSize += tmpPzid.length;
            total += tmpPzid.length;
            mtzxsIds.addAll(Arrays.asList(tmpPzid));
            mtzxsPzlxIds.addAll(Arrays.asList(tmpPzlxid));
        }

        for (idx = 0; idx < total; idx++) {
            GenericRecord cx_rz_tjRecord = new GenericData.Record(this.t_cx_rz_tjSchema);
            if (idx == 0) {
                cx_rz_tjRecord.put("c_sfzt", 1);
            } else {
                cx_rz_tjRecord.put("c_sfzt", 0);
            }
            for (Field f : this.t_cx_rz_tjSchema.getFields()) {
                if (this.t_cx_rzFieldNames.contains(f.name())) {
                    cx_rz_tjRecord.put(f.name(), gr.get(f.name()));
                }
            }
            if (idx < mddzSize) {
                cx_rz_tjRecord.put("c_pzid", mddzpzIds.get(idx));
                cx_rz_tjRecord.put("c_pzlx", mddzPzlxIds.get(idx));
            } else {
                cx_rz_tjRecord.put("c_pzid", mtzxsIds.get(idx - mddzSize));
                cx_rz_tjRecord.put("c_pzlx", mtzxsPzlxIds.get(idx - mddzSize));
            }
            //log.debug("this is one cx_rz_tj " + cx_rz_tj);
            if (!this.t_cx_rz_tjBuf.offer(cx_rz_tjRecord)) {
                return false;
            }
        }

        /*
         * 产生数据仓库的cx_zt表数据
         */
        GenericRecord ztRecord = new GenericData.Record(this.t_cx_rz_ztSchema);
        for (Field f : this.t_cx_rz_ztSchema.getFields()) {
            if (this.t_cx_rzFieldNames.contains(f.name())) {
                ztRecord.put(f.name(), gr.get(f.name()));
            }
        }
        String cxzt_content = " ";
        for (Object r : mtzxs) {
            GenericRecord tmp = (GenericRecord) r;
            if ("text".equalsIgnoreCase(tmp.get("c_mtlx").toString())) {
                try {
                    cxzt_content = cxzt_content + " " + new String(((ByteBuffer) tmp.get("c_nr")).array(), "UTF-8");
                } catch (UnsupportedEncodingException ex) {
                    log.error(ex, ex);
                }
            }
        }
        log.debug("this is one cx_rz_zt " + ztRecord);
        ztRecord.put("c_cxnr", cxzt_content);
        if (!this.t_cx_rz_ztBuf.offer(ztRecord)) {
            return false;
        }
        GenericRecord tmpRecord = null;
        /*
         * 产生数据仓库的cx_mddz表数据
         */
        for (Object r : mddzs) {
            tmpRecord = (GenericRecord) r;
            GenericRecord g = new GenericData.Record(this.t_cx_rz_mddzSchema);
            for (Field f : g.getSchema().getFields()) {
                if ("c_cxid".equalsIgnoreCase(f.name())) {
                    g.put("c_cxid", gr.get("c_cxid"));
                } else if ("c_fssj".equalsIgnoreCase(f.name())) {
                    g.put("c_fssj", gr.get("c_fssj"));
                } else {
                    g.put(f.name(), tmpRecord.get(f.name()));
                }
            }
            log.debug("this is one mddz " + g);
            if (!this.t_cx_rz_mddzBuf.offer(g)) {  //产生cx mddz的 record 数组
                return false;
            }
        }
        /**
         * 产生数据仓库的cx_mtzx表数据,并且为cx_rz表生成哟个record，用于存放多媒体内容的key
         */
        GenericRecord record = new GenericData.Record(this.t_cx_rzSchema);
        Date d = new Date();
        String timekey = "";
        String mmKey = "";
        ByteBuffer bytesdata = null;
        arrayString = new GenericData.Array<String>(mtzxs.size(), arrayStringSchema);
        arrayStringSlt = new GenericData.Array<String>(mtzxs.size(), arrayStringSchema);
        for (Object r : mtzxs) {
            tmpRecord = (GenericRecord) r;
            GenericRecord g = new GenericData.Record(this.t_cx_rz_mtzxSchema);
            for (Field f : g.getSchema().getFields()) {
                if ("c_cxid".equalsIgnoreCase(f.name())) {
                    g.put("c_cxid", gr.get("c_cxid"));
                } else if ("c_fssj".equalsIgnoreCase(f.name())) {
                    g.put("c_fssj", gr.get("c_fssj"));
                } else if ("c_nr".equalsIgnoreCase(f.name())) {
                    d.setTime(Long.parseLong(gr.get("c_fssj").toString()) * 1000);
                    timekey = this.secondFormat.format(d);
                    try {
                        timekey = this.secondFormat.parse(timekey).getTime() / 1000 + "";
                    } catch (ParseException ex) {
                        log.error(ex, ex);
                    }
                    if ("text".equalsIgnoreCase(g.get("c_mtlx").toString())) {
                        mmKey = "t" + timekey + "@" + tmpRecord.get("c_md5").toString();
                    } else if ("image".equalsIgnoreCase(g.get("c_mtlx").toString())) {
                        mmKey = "i" + timekey + "@" + tmpRecord.get("c_md5").toString();
                    } else if ("audio".equalsIgnoreCase(g.get("c_mtlx").toString())) {
                        mmKey = "a" + timekey + "@" + tmpRecord.get("c_md5").toString();
                    } else if ("video".equalsIgnoreCase(g.get("c_mtlx").toString())) {
                        mmKey = "v" + timekey + "@" + tmpRecord.get("c_md5").toString();
                    } else if ("application".equalsIgnoreCase(g.get("c_mtlx").toString())) {
                        mmKey = "o" + timekey + "@" + tmpRecord.get("c_md5").toString();
                    } else {
                        //log.info("the unknown nr data type is " + g.get("c_mtlx"));
                        mmKey = timekey + "@" + tmpRecord.get("c_md5").toString();
                    }
                    bytesdata = (ByteBuffer) tmpRecord.get("c_nr");
                    if (bytesdata == null || bytesdata.array().length == 0 || mmKey == null) {
                        log.info("the  binary data is " + bytesdata.array() + "  " + mmKey);
                    } else {
                        try {
                            Matcher.ClientAPIMap.get(this.region).put(mmKey, bytesdata.array());
                        } catch (Exception e) {
                            log.error(e, e);
                        }
                    }
                    g.put("c_nr", mmKey);
                    arrayString.add(mmKey);
                } else if ("c_slt".equalsIgnoreCase(f.name())) {
                    d.setTime(Long.parseLong(gr.get("c_fssj").toString()) * 1000);
                    timekey = this.secondFormat.format(d);
                    try {
                        timekey = this.secondFormat.parse(timekey).getTime() / 1000 + "";
                    } catch (ParseException ex) {
                        log.error(ex, ex);
                    }

                    if ("image".equalsIgnoreCase(g.get("c_mtlx").toString())) {
                        mmKey = "s" + timekey + "@" + tmpRecord.get("c_md5").toString();
                        bytesdata = (ByteBuffer) tmpRecord.get("c_slt");
                        if (bytesdata == null || bytesdata.array().length == 0 || mmKey == null) {
                            log.info("the  binary data is " + bytesdata.array() + "  " + mmKey);
                        } else {
                            try {
                                Matcher.ClientAPIMap.get(this.region).put(mmKey, bytesdata.array());
                            } catch (Exception e) {
                                log.error(e, e);
                            }
                        }
                    } else {
                        //log.error("the slt data type is " + g.get("c_mtlx"));
                        mmKey = null;
                    }
                    //fix me 
                    if (mmKey != null) {
                        g.put("c_slt", mmKey);
                        arrayStringSlt.add(mmKey);
                    }
                } else {
                    g.put(f.name(), tmpRecord.get(f.name()));
                }
            }
            log.debug("this is one mtzx " + g);
            if (!this.t_cx_rz_mtzxBuf.offer(g)) {//产生cx mddz的 record 数组
                return false;
            }
        }

        record.put("c_nr", arrayString);
        record.put("c_slt", arrayStringSlt);

        /*
         * 产生数据平台cx_rz表使用的record
         */
        int tmpcxnrSize = 0;
        StringBuilder sb = new StringBuilder(100);
        int size = 0;
        for (Field f : this.t_cx_rzSchema.getFields()) {
            if (this.t_cx_cdrFieldNames.contains(f.name())) {
                if (!"c_nr".equalsIgnoreCase(f.name()) && !"c_slt".equalsIgnoreCase(f.name())) {
                    record.put(f.name(), gr.get(f.name()));
                }
            } else if (this.mddzFieldNames.contains(f.name())) {
                if ("c_dzpzidjh".equalsIgnoreCase(f.name())) {
                    size = 0;
                    for (Object r : mddzs) {
                        tmpRecord = (GenericRecord) r;
                        size += tmpRecord.get("c_dzpzidjh").toString().split("[,]").length;
                    }
                    arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                    for (Object r : mddzs) {
                        tmpRecord = (GenericRecord) r;
                        for (int i = 0; i < tmpRecord.get("c_dzpzidjh").toString().split("[,]").length; i++) {
                            arrayString.add(tmpRecord.get("c_dzpzidjh").toString().split("[,]")[i]);
                        }
                    }
                    record.put(f.name(), arrayString);
                } else if ("c_dzpzlxidjh".equalsIgnoreCase(f.name())) {
                    size = 0;
                    for (Object r : mddzs) {
                        tmpRecord = (GenericRecord) r;
                        size += tmpRecord.get("c_dzpzlxidjh").toString().split("[,]").length;
                    }
                    arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                    for (Object r : mddzs) {
                        tmpRecord = (GenericRecord) r;
                        for (int i = 0; i < tmpRecord.get("c_dzpzlxidjh").toString().split("[,]").length; i++) {
                            arrayString.add(tmpRecord.get("c_dzpzlxidjh").toString().split("[,]")[i]);
                        }
                    }
                    record.put(f.name(), arrayString);
                } else if (mddzSchema.getField(f.name()).schema().getType() == Type.INT) {
                    arrayInt = new GenericData.Array<Integer>(mddzs.size(), arrayIntSchema);
                    for (Object r : mddzs) {
                        tmpRecord = (GenericRecord) r;
                        arrayInt.add(tmpRecord.get(f.name()));
                    }
                    record.put(f.name(), arrayInt);
                } else if (mddzSchema.getField(f.name()).schema().getType() == Type.STRING) {
                    arrayString = new GenericData.Array<String>(mddzs.size(), arrayStringSchema);
                    for (Object r : mddzs) {
                        tmpRecord = (GenericRecord) r;
                        arrayString.add(tmpRecord.get(f.name()));
                    }
                    record.put(f.name(), arrayString);
                } else if (mddzSchema.getField(f.name()).schema().getType() == Type.BYTES) {
                    arrayBytes = new GenericData.Array<ByteBuffer>(mddzs.size(), arrayBytesSchema);
                    for (Object r : mddzs) {
                        tmpRecord = (GenericRecord) r;
                        arrayBytes.add(tmpRecord.get(f.name()));
                    }
                    record.put(f.name(), arrayBytes);
                } else {
                    log.error("when gen the cx platform error occurs,not support data type: " + f.schema().getType());
                }
            } else if (this.mtzxFieldNames.contains(f.name()) && !"c_nr".equalsIgnoreCase(f.name()) && !"c_slt".equalsIgnoreCase(f.name())) {
                if ("c_mtpzidjh".equalsIgnoreCase(f.name())) {
                    size = 0;
                    for (Object r : mtzxs) {
                        tmpRecord = (GenericRecord) r;
                        size += tmpRecord.get("c_mtpzidjh").toString().split("[,]").length;
                    }
                    arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                    for (Object r : mtzxs) {
                        tmpRecord = (GenericRecord) r;
                        for (int i = 0; i < tmpRecord.get("c_mtpzidjh").toString().split("[,]").length; i++) {
                            arrayString.add(tmpRecord.get("c_mtpzidjh").toString().split("[,]")[i]);
                        }
                    }
                    record.put(f.name(), arrayString);
                } else if ("c_mtpzlxidjh".equalsIgnoreCase(f.name())) {
                    size = 0;
                    for (Object r : mtzxs) {
                        tmpRecord = (GenericRecord) r;
                        size += tmpRecord.get("c_mtpzlxidjh").toString().split("[,]").length;
                    }
                    arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                    for (Object r : mtzxs) {
                        tmpRecord = (GenericRecord) r;
                        for (int i = 0; i < tmpRecord.get("c_mtpzlxidjh").toString().split("[,]").length; i++) {
                            arrayString.add(tmpRecord.get("c_mtpzlxidjh").toString().split("[,]")[i]);
                        }
                    }
                    record.put(f.name(), arrayString);
                } else if (mtzxSchema.getField(f.name()).schema().getType() == Type.INT) {
                    arrayInt = new GenericData.Array<Integer>(mtzxs.size(), arrayIntSchema);
                    for (Object r : mtzxs) {
                        tmpRecord = (GenericRecord) r;
                        arrayInt.add(tmpRecord.get(f.name()));
                    }
                    record.put(f.name(), arrayInt);
                } else if (mtzxSchema.getField(f.name()).schema().getType() == Type.STRING) {
                    arrayString = new GenericData.Array<String>(mtzxs.size(), arrayStringSchema);
                    for (Object r : mtzxs) {
                        tmpRecord = (GenericRecord) r;
                        arrayString.add(tmpRecord.get(f.name()));
                    }
                    record.put(f.name(), arrayString);
                } else if (mtzxSchema.getField(f.name()).schema().getType() == Type.BYTES) {
                    arrayBytes = new GenericData.Array<ByteBuffer>(mtzxs.size(), arrayBytesSchema);
                    for (Object r : mtzxs) {
                        tmpRecord = (GenericRecord) r;
                        arrayBytes.add(tmpRecord.get(f.name()));
                    }
                    record.put(f.name(), arrayBytes);
                } else {
                    log.error("when gen the cx platform error occurs,not support data type: " + f.schema().getType());
                }
            } else if ("C_MDDZPZIDJH".equalsIgnoreCase(f.name())) {
                size = 0;
                for (Object r : mddzs) {
                    tmpRecord = (GenericRecord) r;
                    size += tmpRecord.get("c_pzidjh").toString().split("[,]").length;
                }
                arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                for (Object r : mddzs) {
                    tmpRecord = (GenericRecord) r;
                    for (int i = 0; i < tmpRecord.get("c_pzidjh").toString().split("[,]").length; i++) {
                        arrayString.add(tmpRecord.get("c_pzidjh").toString().split("[,]")[i]);
                    }
                }
                record.put(f.name(), arrayString);
            } else if ("C_MDDZXX".equalsIgnoreCase(f.name())) {
                arrayString = new GenericData.Array<String>(mddzs.size(), arrayStringSchema);
                for (Object r : mddzs) {
                    tmpRecord = (GenericRecord) r;
                    arrayString.add(sb.append(tmpRecord.get("c_mddzsssid")).append("|").append(tmpRecord.get("c_mddzssqh")).toString());
                    sb.delete(0, sb.length());
                }
                record.put(f.name(), arrayString);
            } else if ("C_MTZXPZIDJH".equalsIgnoreCase(f.name())) {
                size = 0;
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    size += tmpRecord.get("c_pzidjh").toString().split("[,]").length;
                }
                arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    for (int i = 0; i < tmpRecord.get("c_pzidjh").toString().split("[,]").length; i++) {
                        arrayString.add(tmpRecord.get("c_pzidjh").toString().split("[,]")[i]);
                    }
                }
                record.put(f.name(), arrayString);
            } else if ("C_WBZXXX".equalsIgnoreCase(f.name())) {
                size = 0;
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("text".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        size++;
                    }
                }
                arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("text".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        arrayString.add(sb.append(tmpRecord.get("c_mtzlx")).append("|").append(tmpRecord.get("c_md5")).toString());
                        sb.delete(0, sb.length());
                    }
                }
                record.put(f.name(), arrayString);
            } else if ("C_TPZXXX".equalsIgnoreCase(f.name())) {
                size = 0;
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("image".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        size++;
                    }
                }
                arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("image".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        arrayString.add(sb.append(tmpRecord.get("c_mtzlx")).append("|").append(tmpRecord.get("c_md5")).toString());
                        sb.delete(0, sb.length());
                    }
                }
                record.put(f.name(), arrayString);
            } else if ("C_YPZXXX".equalsIgnoreCase(f.name())) {
                size = 0;
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("audio".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        size++;
                    }
                }
                arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("audio".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        arrayString.add(sb.append(tmpRecord.get("c_mtzlx")).append("|").append(tmpRecord.get("c_md5")).toString());
                        sb.delete(0, sb.length());
                    }
                }
                record.put(f.name(), arrayString);
            } else if ("C_SPZXXX".equalsIgnoreCase(f.name())) {
                size = 0;
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("vedio".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        size++;
                    }
                }
                arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("vedio".equalsIgnoreCase(tmpRecord.get("c_md5").toString())) {
                        arrayString.add(sb.append(tmpRecord.get("c_mtzlx")).append("|").append(tmpRecord.get("c_md5")).toString());
                        sb.delete(0, sb.length());
                    }
                }
                record.put(f.name(), arrayString);
            } else if ("C_WSBZXXX".equalsIgnoreCase(f.name())) {
                size = 0;
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("application".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        size++;
                    }
                }
                arrayString = new GenericData.Array<String>(size, arrayStringSchema);
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("application".equalsIgnoreCase(tmpRecord.get("c_md5").toString())) {
                        arrayString.add(sb.append(tmpRecord.get("c_mtzlx")).append("|").append(tmpRecord.get("c_md5")).toString());
                        sb.delete(0, sb.length());
                    }
                }
                record.put(f.name(), arrayString);
            } else if ("C_CXNR".equalsIgnoreCase(f.name())) {
                tmpcxnrSize = 0;
                size = 0;
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("text".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        size++;
                    }
                }
                if (size == 0) {
                    tmpcxnrSize = 1;
                } else {
                    tmpcxnrSize = size;
                }
                arrayString = new GenericData.Array<String>(tmpcxnrSize, arrayStringSchema);
                for (Object r : mtzxs) {
                    tmpRecord = (GenericRecord) r;
                    if ("text".equalsIgnoreCase(tmpRecord.get("c_mtlx").toString())) {
                        try {
                            String content = new String(((ByteBuffer) tmpRecord.get("c_nr")).array(), "UTF-8");
                            arrayString.add(content);
                        } catch (UnsupportedEncodingException ex) {
                            log.error(ex, ex);
                        }
                    }
                }
                if (size == 0) {
                    //log.info("the size for blank array is " + arrayString.size());
                    arrayString.add("");
                    //log.info("after set,the size for blank array is " + arrayString.size());
                }
                record.put(f.name(), arrayString);
            } else {
                //log.error("when gen the platform cx data error occurs,the field name not exits:" + f.name());
            }
        }
        log.debug("this is one cx_rz " + record);
        if (!this.t_cx_rzBuf.offer(record)) {
            return false;
        }
        return true;
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

    @Override
    public void run() {
        long count = 0;
        log.info("start the special send dx thread ");
        ConcurrentHashMap<String, List> listMap = new ConcurrentHashMap<String, List>();
        for (String s : BufferMap.keySet()) {
            listMap.put(s, new ArrayList());
        }

        long currentTime = System.currentTimeMillis();
        boolean isOK = true;
        GenericRecord cxRecord = null;
        GenericRecord failRecord = null;

        while (true) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {
            }
            if (this.t_cxBuf.isEmpty()) {
                for (String s : BufferMap.keySet()) {
                    List tmp = listMap.get(s);
                    LinkedBlockingQueue<GenericRecord> buf = BufferMap.get(s);
                    if (buf.isEmpty()) {
                        currentTime = System.currentTimeMillis();
                        if ((currentTime - this.sendgabMap.get(s)) >= 8000 && !tmp.isEmpty()) {
                            try {
                                log.info("now send data num in total is -> " + s + ":" + Matcher.schemanameInstance2Sendtotal.get(s).addAndGet(tmp.size()));
                                MQProducer.get(s).sendMessage(this.packData(tmp, s.split("[.]")[1]));
                                this.sendgabMap.put(s, currentTime);
                            } catch (Exception ex) {
                                log.error(s + " " + ex, ex);
                            }
                            tmp.clear();
                        }
                    } else {
                        while (!buf.isEmpty()) {
                            GenericRecord gr = buf.poll();
                            if (gr != null) {
                                tmp.add(gr);
                                count++;
                            }
                            if (tmp.size() % batchSize == 0) {
                                try {
                                    log.info("now send data num in total is -> " + s + ":" + Matcher.schemanameInstance2Sendtotal.get(s).addAndGet(tmp.size()));
                                    currentTime = System.currentTimeMillis();
                                    MQProducer.get(s).sendMessage(this.packData(tmp, s.split("[.]")[1]));
                                    this.sendgabMap.put(s, currentTime);
                                } catch (Exception ex) {
                                    log.error(s + "  " + ex, ex);
                                }
                                tmp.clear();
                            }
                        }
                    }
                }
            } else {
                while (!this.t_cxBuf.isEmpty()) {
                    try {
                        if (isOK) {
                            cxRecord = this.t_cxBuf.poll();
                            if (cxRecord != null) {
                                if (!(isOK = this.splitRecord(cxRecord))) { //假如放入buffer中不成功，就是buffer的size超过了其容量，那么就要将数据发出去然后重试
                                    log.error("now split one dx record fail,the buffer is full,will clear and send some data then retry split");
                                    failRecord = cxRecord;
                                }
                            }
                        } else {
                            if (!(isOK = this.splitRecord(failRecord))) { //假如放入buffer中不成功，就是buffer的size超过了其容量，那么就要将数据发出去然后重试
                                log.error("now retry split one dx record fail,the buffer is full,will clear and send some data then retry split");
                            }
                        }
                    } catch (Exception ex) {
                        log.error(ex, ex);
                    }

                    for (String s : BufferMap.keySet()) {
                        List tmp = listMap.get(s);
                        LinkedBlockingQueue<GenericRecord> buf = BufferMap.get(s);
                        if (buf.isEmpty()) {
                            currentTime = System.currentTimeMillis();
                            if ((currentTime - this.sendgabMap.get(s)) >= 8000 && !tmp.isEmpty()) {
                                try {
                                    log.info("now send data num in total is -> " + s + ":" + Matcher.schemanameInstance2Sendtotal.get(s).addAndGet(tmp.size()));
                                    MQProducer.get(s).sendMessage(this.packData(tmp, s.split("[.]")[1]));
                                    this.sendgabMap.put(s, currentTime);
                                } catch (Exception ex) {
                                    log.error(s + "  " + ex, ex);
                                }
                                tmp.clear();
                            }
                        } else {
                            while (!buf.isEmpty()) {
                                GenericRecord gr = buf.poll();
                                if (gr != null) {
                                    tmp.add(gr);
                                    count++;
                                }
                                if (tmp.size() % batchSize == 0) {
                                    try {
                                        log.info("now send data num in total is -> " + s + ":" + Matcher.schemanameInstance2Sendtotal.get(s).addAndGet(tmp.size()));
                                        currentTime = System.currentTimeMillis();
                                        MQProducer.get(s).sendMessage(this.packData(tmp, s.split("[.]")[1]));
                                        this.sendgabMap.put(s, currentTime);
                                    } catch (Exception ex) {
                                        log.error(s + "  " + ex, ex);
                                    }
                                    tmp.clear();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(Type.LONG);
    }
}
