/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.util;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.log4j.Logger;

/**
 *
 * @author liucuili
 */
public class AvroUtils {

    public static Logger log = Logger.getLogger(AvroUtils.class.getName());

    public static Schema getSchema(Protocol protocol, String schemaName) {
        Schema schema = protocol.getType(schemaName);
        return schema;
    }

    public static DatumReader<GenericRecord> getReader(Protocol protocol, String schemaName) {
        Schema schema = protocol.getType(schemaName);
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        return reader;
    }

    public static Protocol getProtocol(String proContent) {
        Protocol p = Protocol.parse(proContent);
        log.info("protocal is" + proContent);
        return p;
    }
}
