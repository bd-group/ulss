/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.datadispatch.handler.datadispatch;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 *
 * @author ulss
 */
public class DataDispatcher {

    //doc schema
    Protocol protocol = null;
    String docSchemaName = null;
    String docSchemaContent = null;
    Schema docSchema = null;
    DatumReader<GenericRecord> docReader = null;
    //mq
    String docStoreMQName = null;
    MQProducerPool mqProducerPool = null;
    AtomicLong dataVolume = new AtomicLong();

    private DataDispatcher(String pDocSchemaName, String pDocSchemaContent, String pDocStoreMQName, MQProducerPool pMQProducerPool) {
        docSchemaName = pDocSchemaName;
        docSchemaContent = pDocSchemaContent;
//        protocol = Protocol.parse(docSchemaContent);//maybe failed
//        docSchema = protocol.getType(docSchemaName);
//        docReader = new GenericDatumReader<>(docSchema);
        docStoreMQName = pDocStoreMQName;
        mqProducerPool = pMQProducerPool;
    }

    public static DataDispatcher getDataDispatcher(String pDocSchemaName, String pDocSchemaContent, String pDocStoreMQName) {
        MQProducerPool mqProducerPool = MQProducerPool.getMQProducerPool(pDocStoreMQName, 20);
        if (mqProducerPool != null) {
            return new DataDispatcher(pDocSchemaName, pDocSchemaContent, pDocStoreMQName, mqProducerPool);
        } else {
            return null;
        }
    }

    public void dispatch(byte[] pData) throws Exception {
        mqProducerPool.sendMessage(pData);
    }

    public String getDocSchemaContent() {
        return docSchemaContent;
    }

    public void incDataVolumeStatistics(long pDelta) {
        dataVolume.addAndGet(pDelta);
    }

    public long getDataVolumeStatistics() {
        return dataVolume.longValue();
    }
}
//        GenericArray docsSet = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");
//        System.out.println(Thread.currentThread().getId() + ":" + docsSet.size());
//
//
//        Iterator<ByteBuffer> itor = docsSet.iterator();
//        while (itor.hasNext()) {
//            ByteArrayInputStream docbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
//            BinaryDecoder docbd = new DecoderFactory().binaryDecoder(docbis, null);
//            GenericRecord docRecord = new GenericData.Record(docSchema);
//            try {
//                docReader.read(docRecord, docbd);
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        }
