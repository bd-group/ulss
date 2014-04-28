/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.sender;

import cn.ac.iie.ulss.match.worker.Matcher;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.avro.Schema;
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
public class SendHlwData implements Runnable {

    public static Logger log = Logger.getLogger(SendHlwData.class.getName());
    public String region;
    public String schemaName;
    public String schemanameInstance;
    private String topic;
    public int batchSize;
    public LinkedBlockingQueue<GenericRecord> outBuf; //与匹配线程共享一个缓冲区，发行线程作为消费者进行消费

    public SendHlwData(String reg, String schema, int bs, LinkedBlockingQueue<GenericRecord> b) {
        this.region = reg;
        this.schemaName = schema;
        this.schemanameInstance = this.region + "." + this.schemaName;
        this.topic = Matcher.DBMeta.getMq(this.region, this.schemaName);
        this.batchSize = bs;
        this.outBuf = b;
    }

    public void send(String topic, byte[] pData, long count) throws Exception {
        RocketMQProducer.sendMessage(topic, pData, count);
    }

    public byte[] packData(List<GenericRecord> data) throws IOException {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(Matcher.schemaname2Schema.get(this.schemaName.toLowerCase()));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BinaryEncoder be = new EncoderFactory().binaryEncoder(bos, null);
        Schema docsSchema = Matcher.schemaname2Schema.get("docs");
        GenericRecord docsRecord = new GenericData.Record(Matcher.schemaname2Schema.get("docs"));
        GenericArray docSet = new GenericData.Array<GenericRecord>(this.batchSize, Matcher.schemaname2Schema.get("docs").getField("doc_set").schema());

        for (GenericRecord gr : data) {
            gr.put("c_qd_szs", Matcher.qd_szs);
            writer.write(gr, be);
            be.flush();
            docSet.add(ByteBuffer.wrap(bos.toByteArray()));
            bos.reset();
        }
        docsRecord.put("doc_schema_name", this.schemaName);
        docsRecord.put("doc_set", docSet);
        docsRecord.put("sign", "this is for sing");

        DatumWriter<GenericRecord> docsWriter = new GenericDatumWriter<GenericRecord>(docsSchema);
        ByteArrayOutputStream docsBos = new ByteArrayOutputStream();
        BinaryEncoder docsBe = new EncoderFactory().binaryEncoder(docsBos, null);
        docsWriter.write(docsRecord, docsBe);
        docsBe.flush();
        return docsBos.toByteArray();
    }

    @Override
    public void run() {
        long count = 0;
        long currentTime = System.currentTimeMillis();
        long sendTime = System.currentTimeMillis();
        List<GenericRecord> tmp = new ArrayList<GenericRecord>();
        while (true) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException ex) {
            }
            if (outBuf.isEmpty()) {
                currentTime = System.currentTimeMillis();
                if (currentTime - sendTime >= 3000 && !tmp.isEmpty()) {
                    try {
                        this.send(this.topic, this.packData(tmp), tmp.size());
                        log.info("the out buffer size is " + outBuf.size() + " now send data num in total is -> " + this.schemanameInstance + ":" + Matcher.schemanameInstance2Sendtotal.get(this.schemanameInstance).addAndGet(tmp.size()));
                        sendTime = System.currentTimeMillis();
                    } catch (Exception ex) {
                        log.error(ex, ex);
                    }
                    tmp.clear();
                }
            } else {
                while (!outBuf.isEmpty()) {
                    count += outBuf.drainTo(tmp, this.batchSize / 2);
                    if (count >= this.batchSize) {
                        try {
                            this.send(this.topic, this.packData(tmp), count);
                            log.info("the out buffer size is " + outBuf.size() + " now send data num in total is -> " + this.schemanameInstance + ":" + Matcher.schemanameInstance2Sendtotal.get(this.schemanameInstance).addAndGet(tmp.size()));
                            sendTime = System.currentTimeMillis();
                        } catch (Exception ex) {
                            log.error(ex, ex);
                        }
                        count = 0;
                        tmp.clear();
                    }
                }
            }
        }
    }
}