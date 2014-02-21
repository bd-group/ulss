/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.apache.avro.util.Utf8;

/**
 *
 * @author evan yang
 */
public class numOfFile {

    static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");

    public static void main(String[] args) throws IOException {
        String msgSchemaContent = "{    \n"
                + "    \"protocol\":\"t_dx_rz_fdzl\",\n"
                + "    \"types\":[\n"
                + "    {\n"
                + "        \"type\": \"record\",\n"
                + "        \"name\": \"t_dx_rz_fdzl\",\n"
                + "        \"fields\": [\n"
                + "            {\n"
                + "            \"name\": \"c_dxxh\",\n"
                + "            \"type\": \"long\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\": \"c_yysip\",\n"
                + "            \"type\": \"string\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\": \"c_jrdid\",\n"
                + "            \"type\": \"int\"\n"
                + "        },\n"
                + "        \n"
                + "        {\n"
                + "            \"name\": \"c_fssj\",\n"
                + "            \"type\": \"long\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\": \"c_zllx\",\n"
                + "            \"type\": \"int\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\": \"c_ly\",\n"
                + "            \"type\": \"int\"\n"
                + "        }\n"
                + "\n"
                + "        ]\n"
                + "    }\n"
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
                + "}";

        Protocol protocol = Protocol.parse(msgSchemaContent);
        int count = 0;
        Schema msgschema = protocol.getType("docs");
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(msgschema);

        File out = new File("E:\\fdzl.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(out, true));

        File dir = new File("M:\\fdzl22\\fdzl\\");
        File[] filelist = dir.listFiles();
        for (File f : filelist) {
            DataFileReader<GenericRecord> dataFileReader = null;
            dataFileReader = new DataFileReader<GenericRecord>(f, reader);

            while (dataFileReader.hasNext()) {
                GenericRecord dxr = dataFileReader.next();
                GenericArray docsSet = (GenericData.Array<GenericRecord>) dxr.get("doc_set");
                Schema msgSchema = protocol.getType("t_dx_rz_fdzl");
                DatumReader<GenericRecord> msgreader = new GenericDatumReader<GenericRecord>(msgSchema);
                Iterator msgitor = docsSet.iterator();
//                System.out.println(docsSet.size());
                while (msgitor.hasNext()) {
                    byte[] onedata = ((ByteBuffer) msgitor.next()).array();
                    ByteArrayInputStream msgbis = new ByteArrayInputStream(onedata);
                    BinaryDecoder msgbd = new DecoderFactory().binaryDecoder(msgbis, null);
                    GenericRecord dxxRecord;
                    try {
                        dxxRecord = msgreader.read(null, msgbd);
                        long fssj = (Long) dxxRecord.get("c_fssj") * 1000;
                        Date date = new Date();
                        date.setTime(fssj);
                        if (date.getDate() != 9) {
                            continue;
                        }
//                        count++;
//                        String d = df.format(date);
//                        System.out.println(dxxRecord);
//                        System.out.println((Long) dxxRecord.get("c_dxxh"));
                        //bw.write(((Long) dxxRecord.get("c_fssj")).toString() + "\t" + (Long) dxxRecord.get("c_dxxh") + "\t" + (Utf8) dxxRecord.get("c_yysip") + "\t" + (Integer) dxxRecord.get("c_jrdid") + "\t" + (Integer) dxxRecord.get("c_zllx") + "\t" + (Integer) dxxRecord.get("c_ly") + "\n");
                        if(dxxRecord.toString().equals("{\"c_dxxh\": 2028540945, \"c_yysip\": \"10.143.4.45\", \"c_jrdid\": 921, \"c_fssj\": 1391954339, \"c_zllx\": 2, \"c_ly\": 2}")){
                            System.out.println(f.getName());
                        }
                        //bw.write(dxxRecord.toString() + "\n");
                        //bw.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
//            System.out.println(count);
            dataFileReader.close();
        }
        bw.flush();
        bw.close();
    }
}
