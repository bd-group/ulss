/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.datastatics;

import cn.ac.iie.ulss.indexer.worker.HttpDataHandler;
import cn.ac.iie.ulss.indexer.worker.Indexer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
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
public class DataEmiter {

    public static Logger log = org.apache.log4j.Logger.getLogger(HttpDataHandler.class.getName());
    public static int baossPoolSize = 400;
    public static ArrayBlockingQueue<ByteArrayOutputStream> baosPool = null;
    public static ArrayBlockingQueue<  DatumWriter<GenericRecord>> datumwriterPool = null;

    public static void initEmiter() {
        baosPool = new ArrayBlockingQueue<ByteArrayOutputStream>(baossPoolSize);
        datumwriterPool = new ArrayBlockingQueue<DatumWriter<GenericRecord>>(baossPoolSize);
        for (int i = 0; i < baossPoolSize; i++) {
            try {
                baosPool.put(new ByteArrayOutputStream());
            } catch (InterruptedException ex) {
                log.error(ex, ex);
            }
            try {
                DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(Indexer.statVolumeSchema);
                datumwriterPool.put(writer);
            } catch (InterruptedException ex) {
                log.error(ex, ex);
            }
        }
    }

    /*
     * stat_source:DR(数据接收),DW（数据仓库）,DP（数据平台）
     * DP_persi_receive
     * DP_persi_write
     * DP_match_receive
     * DP_match_send
     */
    public static GenericRecord getStaticsRecords(String schemaName, String region, long volume, String volume_type, String remark, String stat_source) {
        GenericRecord record = new GenericData.Record(Indexer.statVolumeSchema);
        record.put("doc_schema_name", schemaName);
        record.put("region", region);
        record.put("volume", volume);
        record.put("volume_type", volume_type);
        record.put("remark", remark);
        record.put("stat_timestamp", System.currentTimeMillis() / 1000);
        record.put("stat_source", stat_source);
        record.put("src_ip", Indexer.hostName);
        return null;
    }

    public static void emit(GenericRecord tmpRecord) {
        DatumWriter<GenericRecord> writer = null;
        ByteArrayOutputStream baos = null;
        try {
            baos = DataEmiter.baosPool.take();
            BinaryEncoder be = new EncoderFactory().binaryEncoder(baos, null);
            writer = DataEmiter.datumwriterPool.take();
            writer.write(tmpRecord, be);
            be.flush();
            baos.reset();
            DataEmiter.baosPool.put(baos);

            Indexer.staticsDataemiter.sendMessage(baos.toByteArray());
        } catch (Exception ex) {
            log.error(ex, ex);
        } finally {
            try {
                DataEmiter.datumwriterPool.put(writer);
            } catch (InterruptedException ex) {
                log.error(ex, ex);
            }
        }
    }
}