/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.data.statistics;

import cn.ac.iie.ulss.statistics.commons.GlobalVariables;
import cn.ac.iie.ulss.statistics.commons.RuntimeEnv;
import com.taobao.metamorphosis.Message;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class SplitAndGet {

    String MQ = null;
    String time = null;
    HashMap<String, AtomicLong[]> timeToCount = null;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Schema msgSchema = null;
    Schema docsSchema = null;
    DatumReader<GenericRecord> docsreader = null;
    DatumReader<GenericRecord> msgreader = null;
    ByteArrayInputStream docsin = null;
    BinaryDecoder docsdecoder = null;
    GenericRecord docsGr = null;
    GenericArray msgSet = null;
    Iterator<ByteBuffer> msgitor = null;
    String msgSchemaContent = null;
    String docsSchemaContent = null;
    String msgSchemaName = null;
    Protocol protocoldocs = null;
    Protocol protocolMsg = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(SplitAndGet.class.getName());
    }

    public SplitAndGet(String MQ, String time, HashMap<String, AtomicLong[]> timeToCount) {
        this.MQ = MQ;
        this.time = time;
        this.timeToCount = timeToCount;

        msgSchemaContent = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_SCHEMACONTENT)).get(MQ);
        docsSchemaContent = (String) RuntimeEnv.getParam(GlobalVariables.DOCS_SCHEMA_CONTENT);
        msgSchemaName = ((Map<String, String>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_SCHEMANAME)).get(MQ);

    }

    public void count(Message message) {
        logger.info("check one message");
        byte[] msg = message.getData();

        protocoldocs = Protocol.parse(docsSchemaContent);
        docsSchema = protocoldocs.getType(GlobalVariables.DOCS);
        docsreader = new GenericDatumReader<GenericRecord>(docsSchema);

        protocolMsg = Protocol.parse(msgSchemaContent);
        msgSchema = protocolMsg.getType(msgSchemaName);
        msgreader = new GenericDatumReader<GenericRecord>(msgSchema);

        docsin = new ByteArrayInputStream(msg);
        docsdecoder = DecoderFactory.get().binaryDecoder(docsin, null);
        try {
            docsGr = docsreader.read(null, docsdecoder);
        } catch (Exception ex) {
            logger.info((new Date()) + " split the data package from the topic " + MQ + " " + ex, ex);
            return;
        }
        msgSet = (GenericData.Array<GenericRecord>) docsGr.get(GlobalVariables.DOC_SET);
        msgitor = msgSet.iterator();

        while (msgitor.hasNext()) {
            byte[] onedata = ((ByteBuffer) msgitor.next()).array();
            ByteArrayInputStream msgbis = new ByteArrayInputStream(onedata);
            BinaryDecoder msgbd = new DecoderFactory().binaryDecoder(msgbis, null);
            GenericRecord dxxRecord;
            try {
                dxxRecord = msgreader.read(null, msgbd);
            } catch (Exception ex) {

                logger.info((new Date()) + " split the one data from the topic " + MQ + " in the dataPool wrong " + ex, ex);
                continue;
            }

            Long datatime = -1L;

            try {
                datatime = (Long) dxxRecord.get(time.toLowerCase());
            } catch (Exception e) {
                logger.error((new Date()) + " the datetime in one data from the topic " + MQ + " is not long");
                continue;
            }

            if (datatime != -1L) {
                Date dtime = new Date();
                dtime.setTime(datatime * 1000);
                dtime.setMinutes(0);
                dtime.setSeconds(0);
                String dt = dateFormat.format(dtime);

                synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_COUNT)) {
                    if (timeToCount.containsKey(dt)) {
                        AtomicLong[] al = timeToCount.get(dt);
                        al[1] = new AtomicLong(1);
                        al[0].incrementAndGet();
                    } else {
                        AtomicLong[] al = new AtomicLong[2];
                        al[0] = new AtomicLong(1);
                        al[1] = new AtomicLong(1);
                        timeToCount.put(dt, al);
                    }
                }
            }
        }
    }
}
