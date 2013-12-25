/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

/**
 *
 * @author evan yang
 */
public class numOfFile {

    public static void main(String[] args) throws IOException {
        String msgSchemaContent = "{\"protocol\": \"t_dx_rz_qydx\",\n"
                + "\"types\": [\n"
                + "{\n"
                + "\"type\": \"record\",\n"
                + "\"name\": \"t_dx_rz_qydx\",\n"
                + "\"fields\": [\n"
                + "{\n"
                + "\"name\": \"c_dxxh\",\n"
                + "\"type\": [\"long\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_yysip\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_jrdid\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzhdlx\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzhd\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzyysid\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzsssid\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzssqh\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzdqsid\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzdqqh\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzdqjz\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydzsphm\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddz\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddzhdlx\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddzhd\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddzyysid\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddzsssid\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddzssqh\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddzdqsid\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddzdqqh\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_mddzsphm\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_sfddd\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_sfjscl\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_fssj\",\n"
                + "\"type\": [\"long\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_sfccdx\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ccdxzts\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ccdxxh\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ccdxxlh\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_bmlx\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_dxnr\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_nrzymd5\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_esmnr\",\n"
                + "\"type\": [\"bytes\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_fdj_ip\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_imsi\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_imei\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_spcode\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_ascode\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_lac\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_ci\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_rac\",\n"
                + "\"type\": [\"int\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_areacode\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "},\n"
                + "{\n"
                + "\"name\": \"c_ydz_homecode\",\n"
                + "\"type\": [\"string\", \"null\"]\n"
                + "}\n"
                + "]\n"
                + "},\n"
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
                + "}";

        Protocol protocol = Protocol.parse(msgSchemaContent);
        Schema msgschema = protocol.getType("docs");
        File f = new File("H:\\17\\unvaliddata\\20131125222500_t_dx_rz_qydx_mqpersi.uv");
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(msgschema);
        DataFileReader<GenericRecord> dataFileReader = null;

        dataFileReader = new DataFileReader<GenericRecord>(f, reader);
        int count = 0;
        while (dataFileReader.hasNext()) {
            GenericRecord dxr = dataFileReader.next();
            GenericArray docsSet = (GenericData.Array<GenericRecord>) dxr.get("doc_set");
            Schema msgSchema = protocol.getType("t_dx_rz_qydx");
            DatumReader<GenericRecord> msgreader = new GenericDatumReader<GenericRecord>(msgSchema);
            Iterator msgitor = docsSet.iterator();
            while (msgitor.hasNext()) {
                byte[] onedata = ((ByteBuffer) msgitor.next()).array();
                ByteArrayInputStream msgbis = new ByteArrayInputStream(onedata);
                BinaryDecoder msgbd = new DecoderFactory().binaryDecoder(msgbis, null);
                GenericRecord dxxRecord;
                count++;
                System.out.println("size : " + docsSet.size() + " count : " + count);
                try {
                    dxxRecord = msgreader.read(null, msgbd);
                    System.out.println(dxxRecord);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        dataFileReader.close();
    }
}
