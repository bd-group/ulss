/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import cn.ac.iie.ulss.dataredistribution.dao.SimpleDaoImpl;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class getschema {

    static Logger logger = null;
    private static SimpleDaoImpl simpleDao;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(getschema.class.getName());
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Map<String, String> topicToSchemaContent = new HashMap<String, String>();
        simpleDao = SimpleDaoImpl.getDaoInstance("oracle.jdbc.driver.OracleDriver|jdbc:oracle:thin:@10.248.65.9:1521:meta1|metastore|metastore");
        logger.info("getting schema from oracle...");
        String sql = "select DATASCHEMA_MQ.MQ,DATASCHEMA.SCHEMA_CONTENT from DATASCHEMA_MQ,DATASCHEMA WHERE DATASCHEMA_MQ.SCHEMA_NAME=DATASCHEMA.SCHEMA_NAME";
        List<List<String>> rs = simpleDao.queryForList(sql);
        for (List<String> r1 : rs) {
            topicToSchemaContent.put(r1.get(0), r1.get(1).toLowerCase());
        }

        String sqldocs = "select SCHEMA_CONTENT from DATASCHEMA WHERE SCHEMA_NAME='docs'";
        List<List<String>> rsdocs = simpleDao.queryForList(sqldocs);
        List<String> r2 = rsdocs.get(0);
        String docsSchema = r2.get(0);

        File f = new File("/home/ulss/project/schema.txt");
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f));

        bos.write("docs".getBytes());
        bos.write('\n');
        bos.write(docsSchema.getBytes());
        bos.write('\n');
        bos.write('\n');

        for (String s : topicToSchemaContent.keySet()) {
            bos.write(s.getBytes());
            bos.write('\n');
            bos.write(topicToSchemaContent.get(s).getBytes());
            bos.write('\n');
            bos.write('\n');
        }
        
        bos.flush();
        bos.close();
    }
}
