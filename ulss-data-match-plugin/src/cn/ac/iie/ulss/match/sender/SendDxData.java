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
public class SendDxData implements Runnable {

    public static Logger log = Logger.getLogger(SendDxData.class.getName());
    /*
     */
    public String region;
    /*
     */
    public int batchSize;
    public List<String> schemaNames = new ArrayList<String>();
    /*
     */
    public ConcurrentHashMap<String, MQProducerPool> MQProducer = new ConcurrentHashMap<String, MQProducerPool>();
    public ConcurrentHashMap<String, LinkedBlockingQueue> BufferMap = new ConcurrentHashMap<String, LinkedBlockingQueue>();
    /*
     */
    public LinkedBlockingQueue<GenericRecord> dxBuf;
    public LinkedBlockingQueue<GenericRecord> jkdxBuf = new LinkedBlockingQueue<GenericRecord>(5000);
    public LinkedBlockingQueue<GenericRecord> qydxBuf = new LinkedBlockingQueue<GenericRecord>(100000);
    public LinkedBlockingQueue<GenericRecord> gldxBuf = new LinkedBlockingQueue<GenericRecord>(50000);
    public LinkedBlockingQueue<GenericRecord> ccdxBuf = new LinkedBlockingQueue<GenericRecord>(50000);
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

    public void init(String reg, int bs, LinkedBlockingQueue<GenericRecord> b) {
        schemaNames.add("dx");
        schemaNames.add("tlv_pzid");
        schemaNames.add("tlv_gzrwid");
        schemaNames.add("t_dx_rz_jkdx");
        schemaNames.add("t_dx_rz_qydx");
        schemaNames.add("t_dx_rz_gldx");
        schemaNames.add("t_dx_rz_ccdx");

        this.region = reg;
        this.batchSize = bs;
        this.dxBuf = b;

        BufferMap.put(region + "." + "t_dx_rz_jkdx", jkdxBuf);
        BufferMap.put(region + "." + "t_dx_rz_qydx", qydxBuf);
        BufferMap.put(region + "." + "t_dx_rz_gldx", gldxBuf);
        BufferMap.put(region + "." + "t_dx_rz_ccdx", ccdxBuf);

        Protocol protocol = Protocol.parse(Matcher.DBMeta.getSchema(region, "dx").get(0).get(1).toLowerCase());
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
        dx_rz_jkdxSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_dx_rz_jkdx").get(0).get(1).toLowerCase()).getType("t_dx_rz_jkdx");
        for (Field f : dx_rz_jkdxSchema.getFields()) {
            dx_rz_jkdxFieldNames.add(f.name().toLowerCase());
        }
        dx_rz_gldxSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_dx_rz_gldx").get(0).get(1).toLowerCase()).getType("t_dx_rz_gldx");
        for (Field f : dx_rz_gldxSchema.getFields()) {
            dx_rz_gldxFieldNames.add(f.name().toLowerCase());
        }
        dx_rz_qydxSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_dx_rz_qydx").get(0).get(1).toLowerCase()).getType("t_dx_rz_qydx");
        for (Field f : dx_rz_qydxSchema.getFields()) {
            dx_rz_qydxFieldNames.add(f.name().toLowerCase());
        }
        dx_rz_ccdxSchema = Protocol.parse(Matcher.DBMeta.getSchema(region, "t_dx_rz_ccdx").get(0).get(1).toLowerCase()).getType("t_dx_rz_ccdx");
        for (Field f : dx_rz_ccdxSchema.getFields()) {
            dx_rz_ccdxFieldNames.add(f.name().toLowerCase());
        }

        MQProducer.put(region + "." + "t_dx_rz_qydx", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_dx_rz_qydx"), 30));
        MQProducer.put(region + "." + "t_dx_rz_ccdx", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_dx_rz_ccdx"), 30));
        MQProducer.put(region + "." + "t_dx_rz_gldx", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_dx_rz_gldx"), 30));
        MQProducer.put(region + "." + "t_dx_rz_jkdx", MQProducerPool.getMQProducerPool(Matcher.DBMeta.getMq(region, "t_dx_rz_jkdx"), 30));


        qydxTagSet.add(1101);
        qydxTagSet.add(1102);
        qydxTagSet.add(1201);
        qydxTagSet.add(1202);
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
            log.debug(writer + "  " + gr + " " + be);
            writer.write(gr, be);
            be.flush();
            docSet.add(ByteBuffer.wrap(bos.toByteArray()));
            bos.reset();
        }
        docsRecord.put("doc_schema_name", schemaName);
        docsRecord.put("doc_set", docSet);
        docsRecord.put("sign", "this is the sign");

        DatumWriter<GenericRecord> docsWriter = new GenericDatumWriter<GenericRecord>(docsSchema);
        ByteArrayOutputStream docsBos = new ByteArrayOutputStream();
        BinaryEncoder docsBe = new EncoderFactory().binaryEncoder(docsBos, null);
        docsWriter.write(docsRecord, docsBe);
        docsBe.flush();

        return docsBos.toByteArray();
    }

    public boolean splitRecord(GenericRecord gr) throws InterruptedException { //一条原始dx返回 -- 其余dx、监控dx、超长dx、过滤dx四种类型的dx
        int dx_sf_toolong = Integer.parseInt(gr.get("c_sfccdx").toString());
        int dx_gl_lx = Integer.parseInt(gr.get("c_gllx").toString());
        int c_pzlx = -1;
        GenericArray tlv_fgz_pzids = (GenericData.Array<GenericRecord>) gr.get("tlv_fgz_pzids");
        GenericArray tlv_gz_pzids = (GenericData.Array<GenericRecord>) gr.get("tlv_gz_pzids");
        GenericArray tlv_dsx_pzids = (GenericData.Array<GenericRecord>) gr.get("tlv_dsx_pzids");
        if (tlv_fgz_pzids.size() == 0 && tlv_gz_pzids.size() == 0 && tlv_dsx_pzids.size() == 0) {
            if (!qydxBuf.offer(newQYDXRecord(gr))) {
                return false;
            }
            if (dx_sf_toolong == 1) {
                if (!ccdxBuf.offer(newCCDXRecord(gr, null))) {
                    return false;
                }
            }
            if (dx_gl_lx != 0) {
                if (!gldxBuf.offer(newGLDXRecord(gr, null))) {
                    return false;
                }
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

            boolean isQydx = true;
            for (Integer tag : tSet) {
                if (!qydxTagSet.contains(tag)) { //就是tag中含有 1101、1102、1201、1202以外的dx，那么就不是一条qy_dx
                    isQydx = false;
                    break;
                }
            }

            for (Object o : tlv_fgz_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                GenericRecord record = newJKDXRecord(gr, tlv);
                if (!jkdxBuf.offer(record)) {
                    return false;
                }
                if (dx_sf_toolong == 1) {
                    if (!ccdxBuf.offer(newCCDXRecord(gr, tlv))) {
                        return false;
                    }
                }
                if (dx_gl_lx != 0) {
                    if (!gldxBuf.offer(newGLDXRecord(gr, tlv))) {
                        return false;
                    }
                }
                if (isQydx) {
                    if (!qydxBuf.offer(newQYDXRecord(gr))) {
                        return false;
                    }
                }
            }

            for (Object o : tlv_gz_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                GenericRecord record = newJKDXRecord(gr, tlv);
                if (!jkdxBuf.offer(record)) {
                    return false;
                }
                if (dx_sf_toolong == 1) {
                    if (!ccdxBuf.offer(newCCDXRecord(gr, tlv))) {
                        return false;
                    }
                }
                if (dx_gl_lx != 0) {
                    if (!gldxBuf.offer(newGLDXRecord(gr, tlv))) {
                        return false;
                    }
                }
                if (isQydx) {
                    if (!qydxBuf.offer(newQYDXRecord(gr))) {
                        return false;
                    }
                }
            }

            for (Object o : tlv_dsx_pzids) {
                GenericRecord tlv = (GenericRecord) o;
                if (!jkdxBuf.offer(newJKDXRecord(gr, tlv))) {
                    return false;
                }
                if (dx_sf_toolong == 1) {
                    if (!ccdxBuf.offer(newCCDXRecord(gr, tlv))) {
                        return false;
                    }
                }
                if (dx_gl_lx != 0) {
                    if (!gldxBuf.offer(newGLDXRecord(gr, tlv))) {
                        return false;
                    }
                }
                if (isQydx) {
                    if (!qydxBuf.offer(newQYDXRecord(gr))) {
                        return false;
                    }
                }
            }

        }
        return true;
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
        log.info("start the special send dx thread ");
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
                sleepCount += 10;
            } catch (InterruptedException ex) {
            }
            while (!this.dxBuf.isEmpty()) {
                try {
                    if (isOK) {
                        dxRecord = this.dxBuf.poll();
                        if (dxRecord != null) {
                            if (!(isOK = this.splitRecord(dxRecord))) { //假如放入buffer中不成功，就是buffer的size超过了其容量，那么就要将数据发出去然后重试
                                log.error("now split one dx record fail,the buffer is full,will clear and send some data then retry split");
                                failRecord = dxRecord;
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
                        if (sleepCount >= 500 && !tmp.isEmpty()) {
                            try {
                                log.info("now send data num in total is -> " + s + ":" + Matcher.schemanameInstance2Sendtotal.get(s).addAndGet(tmp.size()));
                                MQProducer.get(s).sendMessage(this.packData(tmp, s.split("[.]")[1]));
                            } catch (Exception ex) {
                                log.error(ex, ex);
                            }
                            tmp.clear();
                            sleepCount = 0;
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
                                    MQProducer.get(s).sendMessage(this.packData(tmp, s.split("[.]")[1]));
                                } catch (Exception ex) {
                                    log.error(ex, ex);
                                }
                                tmp.clear();
                                sleepCount = 0;
                            }
                        }
                    }
                }
            }
        }
    }
}