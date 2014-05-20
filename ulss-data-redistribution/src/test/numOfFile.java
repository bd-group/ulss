/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.metamorphosis.exception.MetaClientException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

/**
 *
 * @author evan yang
 */
public class numOfFile {

    static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");

    public static void main(String[] args) throws IOException, MetaClientException, MQClientException {
        final String msgSchemaContent = "{\n"
                + "\"protocol\":\"dx\",\n"
                + "\"types\":[\n"
                + "{\n"
                + "\"type\": \"record\",\n"
                + "\"name\": \"tlv_pzid\",\n"
                + "\"fields\": [\n"
                + "{\n"
                + "\"name\": \"C_PZLX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_JKPZID\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_BGSHSJ\",\n"
                + "\"type\": \"long\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_GSQZXID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_JKFLID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_JKXZID\",\n"
                + "\"type\": \"int\"\n"
                + "}\n"
                + "]\n"
                + "},\n"
                + "\n"
                + "{\n"
                + "\"type\": \"record\",\n"
                + "\"name\": \"tlv_gzrwid\",\n"
                + "\"fields\": [\n"
                + "{\n"
                + "\"name\": \"C_PZLX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_JKPZID\",\n"
                + "\"type\": \"string\"\n"
                + "}\n"
                + "]\n"
                + "},\n"
                + "{\n"
                + "\"type\": \"record\",\n"
                + "\"name\": \"dx\",\n"
                + "\"fields\": [\n"
                + "{\n"
                + "\"name\": \"C_DXXH\",\n"
                + "\"type\": \"long\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YYSIP\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"dx_yys_port\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_JRDID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZ\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZHDLX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZHD\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZYYSID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZSSSID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZSSQH\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZDQSID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZDQQH\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZDQJZ\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZIMSI\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZIMEI\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZDQXZQBS\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZYYSBS\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZDQWZLX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZDQWZXX\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZSPHM\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZ\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZHDLX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZHD\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZYYSID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZSSSID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZSSQH\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZDQSID\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZDQQH\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZIMSI\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZIMEI\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZDQXZQBS\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZYYSBS\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZDQWZLX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZDQWZXX\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZSPHM\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_SFDDD\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_FSSJ\",\n"
                + "\"type\": \"long\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_SFCCDX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_CCDXZTS\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_CCDXXH\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_CCDXXLH\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_BMLX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_DXNR\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_NRZYMD5\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_ESMNR\",\n"
                + "\"type\": \"bytes\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_SFJSCL\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_GLLX\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_YDZSFHMD\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_MDDZSFHMD\",\n"
                + "\"type\": \"int\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"tlv_fgz_pzids\",\n"
                + "\"type\": {\n"
                + "\"type\":\"array\",\n"
                + "\"items\":\"tlv_pzid\"\n"
                + "}\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"tlv_gz_pzids\",\n"
                + "\"type\": {\n"
                + "\"type\":\"array\",\n"
                + "\"items\":\"tlv_gzrwid\"\n"
                + "}\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"tlv_dsx_pzids\",\n"
                + "\"type\": {\n"
                + "\"type\":\"array\",\n"
                + "\"items\":\"tlv_pzid\"\n"
                + "}\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_fdj_ip\",\n"
                + "\"type\": \"string\"\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"C_sfgzbmd\",\n"
                + "\"type\": \"string\"\n"
                + "}\n"
                + "]\n"
                + "}\n"
                + " ,\n"
                + "    {\n"
                + "        \"type\": \"record\",\n"
                + "        \"name\": \"docs\",\n"
                + "        \"fields\": [\n"
                + "        {\n"
                + "            \"name\": \"doc_schema_name\",\n"
                + "            \"type\": \"string\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\": \"sign\",\n"
                + "            \"type\": \"string\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\": \"doc_set\",\n"
                + "            \"type\": {\n"
                + "                \"type\":\"array\",\n"
                + "                \"items\":\"bytes\"\n"
                + "            }\n"
                + "        }\n"
                + "        ]\n"
                + "    }\n"
                + "    ]\n"
                + "}".toLowerCase();

        final Protocol protocol = Protocol.parse(msgSchemaContent);
        Schema msgschema = protocol.getType("docs");
        final DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(msgschema);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_1");
        consumer.setNamesrvAddr("10.228.69.17:9876");
        consumer.setInstanceName("rd_consumer");
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setConsumeConcurrentlyMaxSpan(1);
        consumer.setPullThresholdForQueue(1);
        consumer.setPullBatchSize(1);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setHeartbeatBrokerInterval(120000);
        consumer.subscribe("dx_mq", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            int count = 0;

            /**
             * 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {

                if (msgs != null) {
                    byte[] message = msgs.get(0).getBody();
                    System.out.print(message.length + " ");
                    ByteArrayInputStream dxin = new ByteArrayInputStream(message);
                    BinaryDecoder dxdecoder = DecoderFactory.get().binaryDecoder(dxin, null);
                    GenericRecord dxr = null;
                    try {
                        dxr = reader.read(null, dxdecoder);
                    } catch (Exception ex) {
                    }
                    GenericArray docsSet = (GenericData.Array<GenericRecord>) dxr.get("doc_set");
                    System.out.print(docsSet.size() + " ");
                    Schema msgSchema = protocol.getType("dx");
                    DatumReader<GenericRecord> msgreader = new GenericDatumReader<GenericRecord>(msgSchema);
                    Iterator msgitor = docsSet.iterator();
                    long s = 0L;
                    long s2 = 0L;
                    long s3 = 0L;
                    while (msgitor.hasNext()) {
                        byte[] onedata = ((ByteBuffer) msgitor.next()).array();
                        s += onedata.length;
                        ByteArrayInputStream msgbis = new ByteArrayInputStream(onedata);
                        BinaryDecoder msgbd = new DecoderFactory().binaryDecoder(msgbis, null);
                        GenericRecord dxxRecord = null;
                        try {
                            dxxRecord = msgreader.read(null, msgbd);
                        } catch (IOException ex) {
                        }
                        s2 += dxxRecord.toString().getBytes().length;
                        for (Schema.Field f : msgSchema.getFields()) {
                            if(dxxRecord.get(f.name()) instanceof Integer){
                                s3 += Integer.SIZE/8;
                            }else if (dxxRecord.get(f.name()) instanceof Long){
                                s3 += Long.SIZE/8;
                            }else{
                                s3 += dxxRecord.get(f.name()).toString().getBytes().length;
                            }
                        }
                    }
                    System.out.println(s + " " + s2 + " " + s3);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(numOfFile.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /**
         * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
         */
        consumer.start();

        System.out.println("Consumer Started.");
    }
}
