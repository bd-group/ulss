/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.metastore;

import iie.metastore.MetaStoreClient;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author ulss
 */
public class MetaStoreStub {

    private static MetaStoreStub metaStoreStub = null;
    private MetaStoreClient metaStoreClient = null;
    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(MetaStoreStub.class.getName());
    }

    private MetaStoreStub() {
    }

    public static MetaStoreStub getMetaStoreStub(String pMetaStoreConnectString) throws Exception {
        try {
            MetaStoreClient metaStoreClient = new MetaStoreClient(pMetaStoreConnectString);
            metaStoreStub = new MetaStoreStub();
            metaStoreStub.metaStoreClient = metaStoreClient;
            return metaStoreStub;
        } catch (Exception ex) {
            metaStoreStub = null;
            throw ex;
        }
    }


    public static void main(String[] args) {

        try {
            MetaStoreStub mc = getMetaStoreStub("192.168.1.13");
//            List<String> dbs = mc.metaStoreClient.client.getAllDatabases();
//            for (String db : dbs) {
//                System.out.println(db);
//            }
//            List<String> tables = mc.metaStoreClient.client.getAllTables("default");
//            for (String table : tables) {
//                System.out.println(table);
//            }
//            Table table = mc.metaStoreClient.client.getTable("default", "bf_dxx");
//            Map<String, String> tableParameters = table.getParameters();
//            System.out.println(tableParameters.get("load.round.seconds"));
//            List<String> nodeList = mc.getAvailNodeList();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
