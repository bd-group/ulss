/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.sender;

import cn.ac.iie.ulss.match.worker.Matcher;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
    /**/
    public ConcurrentHashMap<String, MQProducerPool> MQProducer = new ConcurrentHashMap<String, MQProducerPool>();
    public ConcurrentHashMap<String, LinkedBlockingQueue> BufferMap = new ConcurrentHashMap<String, LinkedBlockingQueue>();
    /*
     */
    public ConcurrentHashMap<String, Long> sendgabMap = new ConcurrentHashMap<String, Long>();
    /**/
    public LinkedBlockingQueue<GenericRecord> cxBuf;
    public LinkedBlockingQueue<GenericRecord> t_cx_rzBuf = new LinkedBlockingQueue<GenericRecord>(20000);
    public LinkedBlockingQueue<GenericRecord> t_cx_rz_ztBuf = new LinkedBlockingQueue<GenericRecord>(20000);
    public LinkedBlockingQueue<GenericRecord> t_cx_rz_mddzBuf = new LinkedBlockingQueue<GenericRecord>(20000);
    public LinkedBlockingQueue<GenericRecord> t_cx_rz_mtzxBuf = new LinkedBlockingQueue<GenericRecord>(20000);
    /**/
    public Schema cxSchema = null;
    public Schema mddzSchema = null;
    public Schema mtzxSchema = null;
    /**/
    public Schema t_cx_rzSchema = null;
    public Schema t_cx_rz_ztSchema = null;
    /*
     */
    public Schema t_cx_rz_mddzSchema = null;
    public Schema t_cx_rz_mtzxSchema = null;
    /*
     */
    public Set<String> cxFieldNames = new HashSet<String>();
    public Set<String> mddzFieldNames = new HashSet<String>();
    public Set<String> mtzxFieldNames = new HashSet<String>();
    /**/
    public Set<String> t_cx_rzFieldNames = new HashSet<String>();
    public Set<String> t_cx_rz_ztFieldNames = new HashSet<String>();
    public Set<String> t_cx_rz_mddzFieldNames = new HashSet<String>();
    public Set<String> t_cx_rz_mtzxFieldNames = new HashSet<String>();
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
    GenericArray arrayBytes = null;

    public void init(String reg, int bs, LinkedBlockingQueue<GenericRecord> b) {
        schemaNames.add("cx");//cx_platform
        schemaNames.add("t_cx_rz");
        schemaNames.add("t_cx_rz_zt");
        schemaNames.add("t_cx_rz_mddz");
        schemaNames.add("t_cx_rz_mtzx");
        this.region = reg;
        this.batchSize = bs;
        this.cxBuf = b;

        BufferMap.put(region + "_" + "t_cx_rz", t_cx_rzBuf);
        BufferMap.put(region + "_" + "t_cx_rz_zt", t_cx_rz_ztBuf);
        BufferMap.put(region + "_" + "t_cx_rz_mddz", t_cx_rz_mddzBuf);
        BufferMap.put(region + "_" + "t_cx_rz_mtzx", t_cx_rz_mtzxBuf);

        sendgabMap.put(region + "_" + "t_cx_rz", System.currentTimeMillis());
        sendgabMap.put(region + "_" + "t_cx_rz_zt", System.currentTimeMillis());
        sendgabMap.put(region + "_" + "t_cx_rz_mddz", System.currentTimeMillis());
        sendgabMap.put(region + "_" + "t_cx_rz_mtzx", System.currentTimeMillis());

        Protocol protocol = Protocol.parse(Matcher.DBMeta.getSchema("t_cx_mq").get(0).get(1).toLowerCase());//原始的schema

        cxSchema = protocol.getType("t_cx");
        for (Field f : cxSchema.getFields()) {
            cxFieldNames.add(f.name().toLowerCase());
        }
        mddzSchema = protocol.getType("mddz");
        for (Field f : mddzSchema.getFields()) {
            mddzFieldNames.add(f.name().toLowerCase());
        }
        mtzxSchema = protocol.getType("mtzx");
        for (Field f : t_cx_rz_mtzxSchema.getFields()) {
            mtzxFieldNames.add(f.name().toLowerCase());
        }
        //for data platform
        t_cx_rzSchema = Protocol.parse(Matcher.DBMeta.getSchema("t_cx_rz_mq").get(0).get(1).toLowerCase()).getType("t_cx_rz");
        for (Field f : t_cx_rzSchema.getFields()) {
            t_cx_rzFieldNames.add(f.name().toLowerCase());
        }
        //for datawarehouse
        t_cx_rz_ztSchema = Protocol.parse(Matcher.DBMeta.getSchema("t_cx_rz_zt_mq").get(0).get(1).toLowerCase()).getType("t_cx_rz_zt");
        for (Field f : t_cx_rz_ztSchema.getFields()) {
            t_cx_rz_ztFieldNames.add(f.name().toLowerCase());
        }
        t_cx_rz_mddzSchema = Protocol.parse(Matcher.DBMeta.getSchema("t_cx_rz_mddz_mq").get(0).get(1).toLowerCase()).getType("t_cx_rz_mddz");
        for (Field f : t_cx_rz_mddzSchema.getFields()) {
            t_cx_rz_mddzFieldNames.add(f.name().toLowerCase());
        }
        t_cx_rz_mtzxSchema = Protocol.parse(Matcher.DBMeta.getSchema("t_cx_rz_mtzx_mq").get(0).get(1).toLowerCase()).getType("t_cx_rz_mtzx");
        for (Field f : t_cx_rz_mtzxSchema.getFields()) {
            t_cx_rz_mtzxFieldNames.add(f.name().toLowerCase());
        }
        MQProducer.put("t_cx_rz", MQProducerPool.getMQProducerPool("t_cx_rz_mq", 30));
        MQProducer.put("t_cx_rz_zt", MQProducerPool.getMQProducerPool("t_cx_rz_zt_mq", 30));
        MQProducer.put("t_cx_rz_mddz", MQProducerPool.getMQProducerPool("t_cx_rz_mddz_mq", 30));
        MQProducer.put("t_cx_rz_mtzx", MQProducerPool.getMQProducerPool("t_cx_rz_mtzx_mq", 30));
    }

    public byte[] packData(List<GenericRecord> data, String schemaName) throws IOException {
        log.debug("now packdata for " + schemaName + " and the schema is " + Matcher.schemaname2Schema.get(schemaName.toLowerCase()));
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(Matcher.schemaname2Schema.get(schemaName.toLowerCase()));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BinaryEncoder be = new EncoderFactory().binaryEncoder(bos, null);
        Schema docsSchema = Matcher.schemaname2Schema.get("docs");
        GenericRecord docsRecord = new GenericData.Record(Matcher.schemaname2Schema.get("docs"));
        GenericArray docSet = new GenericData.Array<ByteBuffer>(data.size(), Matcher.schemaname2Schema.get("docs").getField("doc_set").schema());
        for (GenericRecord gr : data) {
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
        GenericArray mddzs = (GenericData.Array<GenericRecord>) gr.get("mddz");
        GenericArray mtzxs = (GenericData.Array<GenericRecord>) gr.get("mtzx");

        GenericRecord ztRecord = new GenericData.Record(this.t_cx_rz_ztSchema);
        for (Field f : this.t_cx_rz_ztSchema.getFields()) {
            if (this.t_cx_rzFieldNames.contains(f.name())) {
                ztRecord.put(f.name(), gr.get(f.name()));
            }
        }
        if (!this.t_cx_rz_ztBuf.offer(ztRecord)) {//产生cx主体的record{
            return false;
        }

        for (Object r : mddzs) {
            GenericRecord rec = (GenericRecord) r;
            log.debug("the mddz record is " + rec);
            if (!this.t_cx_rz_mddzBuf.offer(rec)) {  //产生cx mddz的 record 数组
                return false;
            }
        }
        for (Object r : mtzxs) {
            GenericRecord rec = (GenericRecord) r;
            log.debug("the mtzx record is " + rec);
            if (!this.t_cx_rz_mtzxBuf.offer(rec)) {//产生cx mddz的 record 数组
                return false;
            }
        }

        GenericRecord record = new GenericData.Record(this.t_cx_rzSchema); //产生数据平台使用的record
        for (Field f : this.t_cx_rzSchema.getFields()) {
            if (this.cxFieldNames.contains(f.name())) {
                record.put(f.name(), gr.get(f.name()));
            } else if (this.mddzFieldNames.contains(f.name())) {
                if (f.schema().getType() == Type.INT) {
                    arrayInt = new GenericData.Array<Integer>(mddzs.size(), arrayIntSchema);
                    for (Object r : mddzs) {
                        GenericRecord rec = (GenericRecord) r;
                        arrayInt.add(rec.get(f.name()));
                    }
                    record.put(f.name(), arrayInt);
                } else if (f.schema().getType() == Type.STRING) {
                    arrayString = new GenericData.Array<String>(mddzs.size(), arrayStringSchema);
                    for (Object r : mddzs) {
                        GenericRecord rec = (GenericRecord) r;
                        arrayString.add(rec.get(f.name()));
                    }
                    record.put(f.name(), arrayString);
                } else if (f.schema().getType() == Type.BYTES) {
                    arrayBytes = new GenericData.Array<ByteBuffer>(mddzs.size(), arrayBytesSchema);
                    for (Object r : mddzs) {
                        GenericRecord rec = (GenericRecord) r;
                        arrayBytes.add(rec.get(f.name()));
                    }
                    record.put(f.name(), arrayBytes);
                } else {
                    log.error("when gen the cx platform error occurs,not support data type: " + f.schema().getType());
                }
            } else if (this.mtzxFieldNames.contains(f.name())) {
                if (f.schema().getType() == Type.INT) {
                    arrayInt = new GenericData.Array<Integer>(mddzs.size(), arrayIntSchema);
                    for (Object r : mddzs) {
                        GenericRecord rec = (GenericRecord) r;
                        arrayInt.add(rec.get(f.name()));
                    }
                    record.put(f.name(), arrayInt);
                } else if (f.schema().getType() == Type.STRING) {
                    arrayString = new GenericData.Array<String>(mddzs.size(), arrayStringSchema);
                    for (Object r : mddzs) {
                        GenericRecord rec = (GenericRecord) r;
                        arrayString.add(rec.get(f.name()));
                    }
                    record.put(f.name(), arrayString);
                } else if (f.schema().getType() == Type.BYTES) {
                    arrayBytes = new GenericData.Array<ByteBuffer>(mddzs.size(), arrayBytesSchema);
                    for (Object r : mddzs) {
                        GenericRecord rec = (GenericRecord) r;
                        arrayBytes.add(rec.get(f.name()));
                    }
                    record.put(f.name(), arrayBytes);
                } else {
                    log.error("when gen the cx platform error occurs,not support data type: " + f.schema().getType());
                }
            } else {
                log.error("when gen the platform cx data error occurs,the field name not exits:" + f.name());
            }
        }
        if (!this.t_cx_rzBuf.offer(record)) {
            return false;
        }
        return true;
    }

    @Override
    public void run() {
        long count = 0;
        long currentTime = System.currentTimeMillis();

        log.info("start the special send cx thread ");
        ConcurrentHashMap<String, List> listMap = new ConcurrentHashMap<String, List>();
        for (String s : BufferMap.keySet()) {
            listMap.put(s, new ArrayList());
        }

        boolean isOK = true;
        GenericRecord dxRecord = null;
        GenericRecord failRecord = null;
        while (true) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
            }
            while (!this.cxBuf.isEmpty()) {
                try {
                    if (isOK) {
                        dxRecord = this.cxBuf.poll();
                        if (!(isOK = this.splitRecord(dxRecord))) {
                            log.error("now split one dx record fail,the buffer is full,will clear and send some data then retry split");
                            failRecord = dxRecord;
                        }
                    } else {
                        if (!(isOK = this.splitRecord(failRecord))) {
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
                        if ((currentTime - this.sendgabMap.get(s)) >= 5000 && !tmp.isEmpty()) {
                            try {
                                log.info("now send data num in total is -> " + s + ":" + Matcher.schemanameInstance2Sendtotal.get(s).addAndGet(tmp.size()));
                                MQProducer.get(s).sendMessage(this.packData(tmp, s));
                                this.sendgabMap.put(s, currentTime);
                            } catch (Exception ex) {
                                log.error(ex, ex);
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
                                    MQProducer.get(s).sendMessage(this.packData(tmp, s));
                                    this.sendgabMap.put(s, currentTime);
                                } catch (Exception ex) {
                                    log.error(ex, ex);
                                }
                                tmp.clear();
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
