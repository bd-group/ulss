/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.iie.match.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
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
    public int batchSize;
    public List<String> schemaNames = new ArrayList<String>();
    /*
     */
    public ConcurrentHashMap<String, MQProducerPool> MQProducer = new ConcurrentHashMap<String, MQProducerPool>();
    public ConcurrentHashMap<String, LinkedBlockingQueue> BufferMap = new ConcurrentHashMap<String, LinkedBlockingQueue>();
    /*
     */
    public LinkedBlockingQueue<GenericRecord> dxBuf;
    public LinkedBlockingQueue<GenericRecord> jkdxBuf = new LinkedBlockingQueue<GenericRecord>(100000);
    public LinkedBlockingQueue<GenericRecord> qydxBuf = new LinkedBlockingQueue<GenericRecord>(100000);
    public LinkedBlockingQueue<GenericRecord> gldxBuf = new LinkedBlockingQueue<GenericRecord>(100000);
    public LinkedBlockingQueue<GenericRecord> ccdxBuf = new LinkedBlockingQueue<GenericRecord>(100000);
    /*
     */
    public Schema dxSchema = null;
    public Schema tlv_pzidSchema = null;
    public Schema tlv_gzrwidSchema = null;
    /*
     */
    public Schema dx_rz_jkdxSchema = null;
    public Schema dx_rz_gldxSchema = null;
    public Schema dx_rz_qydxSchema = null;
    public Schema dx_rz_ccdxSchema = null;
    /*
     */
    public Set<Integer> qydxTagSet = new HashSet<Integer>(); //匹配其余短信的tag集合
    public Set<String> dxFieldNames = new HashSet<String>();
    public Set<String> tlv_pzidFieldNames = new HashSet<String>();
    public Set<String> tlv_gzrwidFieldNames = new HashSet<String>();
    /*
     */
    public Set<String> dx_rz_jkdxFieldNames = new HashSet<String>();
    public Set<String> dx_rz_gldxFieldNames = new HashSet<String>();
    public Set<String> dx_rz_qydxFieldNames = new HashSet<String>();
    public Set<String> dx_rz_ccdxFieldNames = new HashSet<String>();

    public void init(int bs, LinkedBlockingQueue<GenericRecord> b) {
        schemaNames.add("dx");
        schemaNames.add("tlv_pzid");
        schemaNames.add("tlv_gzrwid");
        schemaNames.add("t_dx_rz_jkdx");
        schemaNames.add("t_dx_rz_qydx");
        schemaNames.add("t_dx_rz_gldx");
        schemaNames.add("t_dx_rz_ccdx");

        batchSize = bs;
        this.dxBuf = b;

        BufferMap.put("t_dx_rz_jkdx", jkdxBuf);
        BufferMap.put("t_dx_rz_qydx", qydxBuf);
        BufferMap.put("t_dx_rz_gldx", gldxBuf);
        BufferMap.put("t_dx_rz_ccdx", ccdxBuf);

        Protocol protocol = Protocol.parse(Matcher.meta.getSchema("cx_mq").get(0).get(1).toLowerCase());
        dxSchema = protocol.getType("dx");
        for (Field f : dxSchema.getFields()) {
            dxFieldNames.add(f.name().toLowerCase());
        }
        tlv_pzidSchema = protocol.getType("tlv_pzid");
        for (Field f : tlv_pzidSchema.getFields()) {
            tlv_pzidFieldNames.add(f.name().toLowerCase());
        }
        tlv_gzrwidSchema = protocol.getType("tlv_gzrwid");
        for (Field f : tlv_gzrwidSchema.getFields()) {
            tlv_gzrwidFieldNames.add(f.name().toLowerCase());
        }
        dx_rz_jkdxSchema = Protocol.parse(Matcher.meta.getSchema("t_dx_rz_jkdx_mq").get(0).get(1).toLowerCase()).getType("t_dx_rz_jkdx");
        for (Field f : dx_rz_jkdxSchema.getFields()) {
            dx_rz_jkdxFieldNames.add(f.name().toLowerCase());
        }
        dx_rz_gldxSchema = Protocol.parse(Matcher.meta.getSchema("t_dx_rz_gldx_mq").get(0).get(1).toLowerCase()).getType("t_dx_rz_gldx");
        for (Field f : dx_rz_gldxSchema.getFields()) {
            dx_rz_gldxFieldNames.add(f.name().toLowerCase());
        }
        dx_rz_qydxSchema = Protocol.parse(Matcher.meta.getSchema("t_dx_rz_qydx_mq").get(0).get(1).toLowerCase()).getType("t_dx_rz_qydx");
        for (Field f : dx_rz_qydxSchema.getFields()) {
            dx_rz_qydxFieldNames.add(f.name().toLowerCase());
        }
        dx_rz_ccdxSchema = Protocol.parse(Matcher.meta.getSchema("t_dx_rz_ccdx_mq").get(0).get(1).toLowerCase()).getType("t_dx_rz_ccdx");
        for (Field f : dx_rz_ccdxSchema.getFields()) {
            dx_rz_ccdxFieldNames.add(f.name().toLowerCase());
        }

        MQProducer.put("t_dx_rz_qydx", MQProducerPool.getMQProducerPool("t_dx_rz_qydx_mq", 30));
        MQProducer.put("t_dx_rz_ccdx", MQProducerPool.getMQProducerPool("t_dx_rz_ccdx_mq", 30));
        MQProducer.put("t_dx_rz_gldx", MQProducerPool.getMQProducerPool("t_dx_rz_gldx_mq", 30));
        MQProducer.put("t_dx_rz_jkdx", MQProducerPool.getMQProducerPool("t_dx_rz_jkdx_mq", 30));


        qydxTagSet.add(1101);
        qydxTagSet.add(1102);
        qydxTagSet.add(1201);
        qydxTagSet.add(1202);
    }

    public static byte[] packData(List<GenericRecord> data, String schemaName) throws IOException {
        //log.info("now packdata for " + schemaName + " and the schema is " + Matcher.schemaname2Schema.get(schemaName.toLowerCase()));
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(Matcher.schemaname2Schema.get(schemaName.toLowerCase()));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BinaryEncoder be = new EncoderFactory().binaryEncoder(bos, null);

        Schema docsSchema = Matcher.schemaname2Schema.get("docs");
        GenericRecord docsRecord = new GenericData.Record(Matcher.schemaname2Schema.get("docs"));
        /*
         */
        GenericArray docSet = new GenericData.Array<ByteBuffer>(data.size(), Matcher.schemaname2Schema.get("docs").getField("doc_set").schema());
        for (GenericRecord gr : data) {
            log.debug("the data schema is " + gr);
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

    public void splitRecord(GenericRecord gr) throws InterruptedException { //一条普通短信返回 -- 其余短信、监控短信、超长短信、过滤短信四种类型的短信
        int dx_sf_toolong = Integer.parseInt(gr.get("c_sfccdx").toString());
        int dx_gl_lx = Integer.parseInt(gr.get("c_gllx").toString());
        int c_pzlx = -1;
        GenericArray tlv_fgz_pzids = (GenericData.Array<GenericRecord>) gr.get("tlv_fgz_pzids");
        GenericArray tlv_gz_pzids = (GenericData.Array<GenericRecord>) gr.get("tlv_gz_pzids");
        GenericArray tlv_dsx_pzids = (GenericData.Array<GenericRecord>) gr.get("tlv_dsx_pzids");

        if (tlv_fgz_pzids.size() == 0 && tlv_gz_pzids.size() == 0 && tlv_dsx_pzids.size() == 0) {
            qydxBuf.put(newQYDXRecord(gr));
            if (dx_sf_toolong == 1) {
                ccdxBuf.put(newCCDXRecord(gr, null));
            }
            if (dx_gl_lx != 0) {
                gldxBuf.put(newGLDXRecord(gr, null));
            }
        } else {
            Set<Integer> tSet = new HashSet<Integer>();
            for (Object o : tlv_fgz_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                c_pzlx = (Integer) tlv.get("c_pzlx");
                tSet.add(c_pzlx);
            }
            for (Object o : tlv_gz_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                c_pzlx = (Integer) tlv.get("c_pzlx");
                tSet.add(c_pzlx);
            }
            for (Object o : tlv_dsx_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                c_pzlx = (Integer) tlv.get("c_pzlx");
                tSet.add(c_pzlx);
            }

            boolean isQydx = false;
            if (this.qydxTagSet.containsAll(tSet) && this.qydxTagSet.size() == tSet.size()) { //就是拆分出的所有的tag只含有1101、1102、1201、1202这四种值的tag
                isQydx = true;
            }
            for (Object o : tlv_fgz_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                c_pzlx = (Integer) tlv.get("c_pzlx");
                GenericRecord record = newJKDXRecord(gr, tlv);
                jkdxBuf.put(record);
                if (dx_sf_toolong == 1) {
                    ccdxBuf.put(newCCDXRecord(gr, tlv));
                }
                if (dx_gl_lx != 0) {
                    gldxBuf.put(newGLDXRecord(gr, tlv));
                }
//            if (qydxTagSet.contains(c_pzlx)) { //如果有部分特殊的tag则复制一份放入其余短信中，其余短信表中不把tag数据拼接
//                qydxBuf.put(newQYDXRecord(gr));
//            }
                if (isQydx) {
                    qydxBuf.put(newQYDXRecord(gr));
                }
            }

            for (Object o : tlv_gz_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                c_pzlx = (Integer) tlv.get("c_pzlx");
                GenericRecord record = newJKDXRecord(gr, tlv);
                jkdxBuf.put(record);
                if (dx_sf_toolong == 1) {
                    ccdxBuf.put(newCCDXRecord(gr, tlv));
                }
                if (dx_gl_lx != 0) {
                    gldxBuf.put(newGLDXRecord(gr, tlv));
                }
//            if (qydxTagSet.contains(c_pzlx)) { //如果有部分特殊的tag则复制一份放入其余短信中，其余短信表中不把tag数据拼接
//                qydxBuf.put(newQYDXRecord(gr));
//            }
                if (isQydx) {
                    qydxBuf.put(newQYDXRecord(gr));
                }
            }

            for (Object o : tlv_fgz_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                c_pzlx = (Integer) tlv.get("c_pzlx");
                jkdxBuf.put(newJKDXRecord(gr, tlv));
                if (dx_sf_toolong == 1) {
                    ccdxBuf.put(newCCDXRecord(gr, tlv));
                }
                if (dx_gl_lx != 0) {
                    gldxBuf.put(newGLDXRecord(gr, tlv));
                }
//            if (qydxTagSet.contains(c_pzlx)) { //如果有部分特殊的tag则复制一份放入其余短信中，其余短信表中不把tag数据拼接
//                qydxBuf.put(newQYDXRecord(gr));
//            }
                if (isQydx) {
                    qydxBuf.put(newQYDXRecord(gr));
                }
            }
        }
    }

    private GenericRecord newQYDXRecord(GenericRecord gr) {
        GenericRecord record = new GenericData.Record(dx_rz_qydxSchema);
        for (Field f : dx_rz_qydxSchema.getFields()) {
            if (dxFieldNames.contains(f.name())) {
                record.put(f.name(), gr.get(f.name()));
            }
        }
        return record;
    }

    private GenericRecord newJKDXRecord(GenericRecord gr, GenericRecord tlv) {
        GenericRecord record = new GenericData.Record(dx_rz_jkdxSchema);
        for (Field f : dx_rz_jkdxSchema.getFields()) {
            if (dxFieldNames.contains(f.name())) {
                record.put(f.name(), gr.get(f.name()));
            } else {
                if (tlv != null) {
                    for (Iterator<String> it = tlv_pzidFieldNames.iterator(); it.hasNext();) {
                        String fName = it.next();
                        record.put(fName, tlv.get(fName));
                    }
                }
            }
        }
        return record;
    }

    private GenericRecord newCCDXRecord(GenericRecord gr, GenericRecord tlv) {
        GenericRecord record = null;
        record = new GenericData.Record(dx_rz_ccdxSchema);
        for (Field f : dx_rz_ccdxSchema.getFields()) {
            if (dxFieldNames.contains(f.name())) {
                record.put(f.name(), gr.get(f.name()));
            } else {
                if (tlv != null) {
                    for (Iterator<String> it = tlv_pzidFieldNames.iterator(); it.hasNext();) {
                        String fName = it.next();
                        record.put(fName, tlv.get(fName));
                    }
                }
            }
        }
        return record;
    }

    private GenericRecord newGLDXRecord(GenericRecord gr, GenericRecord tlv) {
        GenericRecord record = null;
        record = new GenericData.Record(dx_rz_gldxSchema);
        for (Field f : dx_rz_gldxSchema.getFields()) {
            if (dxFieldNames.contains(f.name())) {
                record.put(f.name(), gr.get(f.name()));
            } else {
                if (tlv != null) {
                    for (Iterator<String> it = tlv_pzidFieldNames.iterator(); it.hasNext();) {
                        String fName = it.next();
                        record.put(fName, tlv.get(fName));
                    }
                }
            }
        }
        return record;
    }

    @Override
    public void run() {
        long count = 0;
        long sleepCount = 0;
        log.info("start the special send dx thread");
        while (true) {
            try {
                Thread.sleep(10);
                sleepCount += 10;
            } catch (InterruptedException ex) {
            }
            while (!this.dxBuf.isEmpty()) {
                try {
                    log.debug("get one result record " + " from the out buffer," + "the size of outbuffer is " + dxBuf.size());
                    this.splitRecord(this.dxBuf.poll());
                    log.debug("get one result record " + "ok from the out buffer," + "the size of outbuffer is " + dxBuf.size());
                } catch (Exception ex) {
                    log.error(ex, ex);
                }

                for (String s : BufferMap.keySet()) {
                    LinkedBlockingQueue<GenericRecord> buf = BufferMap.get(s);
                    if (buf.size() >= batchSize || ((!buf.isEmpty()) && sleepCount >= 1000)) {
                        List<GenericRecord> tmp = new ArrayList<GenericRecord>();
                        while (!buf.isEmpty()) {
                            tmp.add(buf.poll());
                            count++;
                            if (count % batchSize == 0 || buf.isEmpty()) {
                                log.info("begin send the data for " + s + ",the size is " + tmp.size());
                                try {
                                    if (MQProducer.get(s) == null) {
                                        log.error("get null producer for " + s);
                                    }
                                    MQProducer.get(s).sendMessage(SendCxData.packData(tmp, s));
                                } catch (Exception ex) {
                                    log.error(ex, ex);
                                }
                                tmp.clear();
                                sleepCount = 0;
                                log.info("end send the data for " + s + ",the size is " + tmp.size());
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}