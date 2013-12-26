package cn.ac.iie.ulss.indexer;

import cn.ac.iie.ulss.metastore.DBMeta;
import cn.ac.iie.ulss.util.Configure;
import iie.metastore.MetaStoreClient;
import iie.mm.client.ClientAPI;
import iie.mm.client.ClientConf;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;

public class Indexer {

    public static Logger log = Logger.getLogger(Indexer.class.getName());
    public static String docs_protocal = "";
    public static String docs_set_name = "doc_set";
    public static String docs_schema_name = "docs";
    public static String hostName = "";
    public static Node node = null;
    public static String zkUrl = "";
    public static String offsetZkUrl = "";
    public static ZkClient zc = null;
    public static String metaStoreCient = "";
    public static String metaStoreCientUrl = "";
    /*
     */
    public static String cachePath = "";
    public static String unclosedFilePath = "";
    /*
     */
    public static int metaStoreCientPort = 0;
    public static int writeIndexThreadNum = 4; //每个数据源索引创建线程的数量，default is 4
    public static int duplicateNum = 2;
    public static int httpServerPool = 50;
    public static int intervalWriterKey = -1;
    public static int writeInnerpoolSize = 2;
    public static int readbufSize = 20;
    public static int maxDelaySeconds = 200;
    public static int commitgab = 200;
    /**/
    public static ConcurrentHashMap<String, String> schemaname2schemaContent = new ConcurrentHashMap<String, String>(20, 0.8f);
    public static ConcurrentHashMap<String, Schema> schemaname2Schema = new ConcurrentHashMap<String, Schema>(20, 0.8f);
    /**/
    public static ConcurrentHashMap<String, Table> tablename2Table = new ConcurrentHashMap<String, Table>(10, 0.8f);  //db+table name 到table对象的映射
    public static ConcurrentHashMap<String, ClientAPI> ClientAPIMap = new ConcurrentHashMap<String, ClientAPI>(10, 0.8f);  //db+table name 到table对象的映射
    public static ConcurrentHashMap<String, List<Index>> tablename2indexs = new ConcurrentHashMap<String, List<Index>>(10, 0.8f);
    /**/
    public static ConcurrentHashMap<String, String> tablename2Schemaname = new ConcurrentHashMap<String, String>(10, 0.8f);  //schema的名字到name到table对象的映射
    /*
     */
    public static String docsSchemaContent = null;
    public static Schema docsSchema = null;
    public static DatumReader<GenericRecord> docsReader = null;
    public static MetaStoreClient cli = null;
    public static DBMeta imd = new DBMeta();

    public static String getHostName() {
        String nodeName = "";
        try {
            nodeName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            log.error(ex, ex);
        }
        return nodeName;
    }

    public static void initSchema() {
        List<List<String>> schema2tableList = Indexer.imd.getSchema2table();
        for (int i = 0; i < schema2tableList.size(); i++) {
            log.info("put " + schema2tableList.get(i).get(0).toLowerCase().trim() + "---" + schema2tableList.get(i).get(1).toLowerCase().trim() + " to tablename2Schemaname map");
            Indexer.tablename2Schemaname.put(schema2tableList.get(i).get(0).toLowerCase().trim(), schema2tableList.get(i).get(1).toLowerCase().trim());
        }

        String schemaName = "";
        String schemaContent = "";
        try {
            log.info("getting data schema and metaq from metadb...");
            Indexer.docsSchemaContent = Indexer.imd.getDocSchema();
            List<List<String>> allSchema = imd.getAllSchema();
            for (List<String> list : allSchema) {
                schemaName = list.get(0).toLowerCase();
                schemaContent = list.get(1).toLowerCase().trim();
                log.debug("schema " + schemaName + "'s content is:\n" + schemaContent);
                if (schemaName.equals("docs")) {
                    log.info("init schema docs ...");
                    Indexer.docsSchemaContent = schemaContent;
                    Protocol protocol = Protocol.parse(docsSchemaContent);
                    Indexer.docsSchema = protocol.getType(schemaName);
                    Indexer.docsReader = new GenericDatumReader<GenericRecord>(docsSchema);
                    Indexer.schemaname2Schema.put("docs", docsSchema);
                    log.info("init schema for docs is ok");
                } else {
                    log.info("init schema data reader and schema and  metaq for schema " + schemaName + " ...");
                    //String mqName = list.get(2);
                    Protocol protocol = Protocol.parse(schemaContent);
                    Schema schema = protocol.getType(schemaName);
                    log.debug("put schemaReader、mq for " + schemaName + " to map");
                    //Indexer.metaqname2schemaname.put(mqName, schemaName); //mqName=nc.sichuan; nc.yunnan  etc
                    Indexer.schemaname2Schema.put(schemaName, schema);
                }
                Indexer.schemaname2schemaContent.put(schemaName, schemaContent);
            }
        } catch (Exception ex) {
            log.error("constructing data dispath handler is failed: " + schemaName + " for " + ex, ex);
            System.exit(0);
        }
        log.info("init all data schemas done ");

        if (docsSchema == null) {
            log.error("schema docs is not found in metadb,please check metadb to ensure that docs schema exist");
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        hostName = getHostName();
        Configure cfg = null;
        try {
            cfg = new Configure("config.properties");
        } catch (Exception e) {
            log.error(e + ",the program will exit", e);
            return;
        }

        Indexer.writeIndexThreadNum = Integer.parseInt(cfg.getProperty("writeNumPerRead"));

        Indexer.zkUrl = cfg.getProperty("zkUrl");
        Indexer.offsetZkUrl = cfg.getProperty("offsetZkUrl");
        /**/
        Indexer.metaStoreCient = cfg.getProperty("metaStoreCient");
        Indexer.metaStoreCientUrl = metaStoreCient.split("[:]")[0];
        Indexer.metaStoreCientPort = Integer.parseInt(Indexer.metaStoreCient.split("[:]")[1]);
        Indexer.cachePath = cfg.getProperty("persiCachePath");
        Indexer.unclosedFilePath = cfg.getProperty("unclosedFilePath");

        Indexer.writeIndexThreadNum = cfg.getIntProperty("writeNumPerRead");
        Indexer.writeInnerpoolSize = cfg.getIntProperty("writeBufNum");
        Indexer.readbufSize = cfg.getIntProperty("readBufNum");
        Indexer.httpServerPool = cfg.getIntProperty("httpServerPoolNum");
        Indexer.maxDelaySeconds = cfg.getIntProperty("maxDelayTime");
        Indexer.commitgab = cfg.getIntProperty("commitgab");

        try {
            Indexer.cli = new MetaStoreClient(metaStoreCientUrl, metaStoreCientPort);
        } catch (MetaException ex) {
            log.error(ex, ex);
            return;
        }
        try {
            List<Database> dbs = cli.client.get_all_attributions();
            for (Database db : dbs) {
                String dbName = db.getName();
                String mmurl = db.getParameters().get("mm.url");
                if (mmurl != null && !"".equals(mmurl)) {
                    ClientConf cc = new ClientConf();
                    ClientAPI ca = new ClientAPI(cc);
                    ca.init(mmurl);
                    Indexer.ClientAPIMap.put(dbName, ca);
                    log.info("the mm.url for " + dbName + " is " + mmurl);
                }
                List<String> tbList = cli.client.getAllTables(dbName);
                for (String tbName : tbList) {
                    Table tb = cli.client.getTable(dbName, tbName);
                    tablename2Table.put(dbName + "." + tbName, tb);
                    List<Index> idxList = cli.client.listIndexes(dbName, tbName, (short) -1);
                    tablename2indexs.put(dbName + "." + tbName, idxList);
                    log.info("get " + dbName + "." + tbName + " ok ");
                }
            }
        } catch (Exception ex) {
            log.error(ex, ex);
        }
        try {
            node = cli.client.get_node(Indexer.hostName);
        } catch (MetaException ex) {
            log.error(ex, ex);
            return;
        } catch (TException ex) {
            log.error(ex, ex);
            return;
        }

        Indexer.docs_protocal = imd.getDocSchema();
        Indexer.initSchema();

        log.info("start init the http receive data server ... \n");
        try {
            HttpReceivedata.poolNum = 20;
            HttpReceivedata.init();
            HttpReceivedata.startup();
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
}