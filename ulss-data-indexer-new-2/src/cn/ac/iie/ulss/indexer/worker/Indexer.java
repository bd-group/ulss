package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.constant.DataTypeMap;
import cn.ac.iie.ulss.indexer.metastore.DBMetastore;
import cn.ac.iie.ulss.indexer.metastore.MetastoreClientPool;
import cn.ac.iie.ulss.indexer.datastatics.MQProducerPool;
import cn.ac.iie.ulss.utiltools.Configure;
import cn.ac.iie.ulss.utiltools.MiscTools;
import iie.metastore.MetaStoreClient;
import iie.mm.client.ClientAPI;
import iie.mm.client.ClientConf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Indexer {

    public static Logger log = Logger.getLogger(Indexer.class.getName());
    public static String docs_protocal = "";
    public static String docs_set_name = "doc_set";
    public static String docs_schema_name = "docs";
    public static String hostName = "";
    public static Node node = null;
    public static String zkUrls = "";
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
    public static DBMetastore imd = new DBMetastore();

    /*
     */
    public static AtomicInteger closeDelay = new AtomicInteger(60);//that is 15 seconds
    public static AtomicBoolean isShouldExit = new AtomicBoolean(false);
    /*
     */
    public static MetastoreClientPool clientPool = null;
    public static int clientPoolSize = 6;
    /*
     */
    public static int initMetaInfoThreadnum = 4;
    /*
     */
    public static String statVolumeSchemaContent = null;
    public static Schema statVolumeSchema = null;
    /*
     */
    public static MQProducerPool staticsDataemiter = null;

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
                    Protocol protocol = Protocol.parse(schemaContent);
                    Schema schema = protocol.getType(schemaName);
                    log.debug("put schemaReader、mq for " + schemaName + " to map");
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

    public static void initStatVolumeSchema() {
        Indexer.statVolumeSchemaContent = Indexer.imd.getDocSchema();
        Protocol protocol = Protocol.parse(Indexer.statVolumeSchemaContent);
        Indexer.statVolumeSchema = protocol.getType("data_volume_statdata_volume_stat");
    }

    public static void initDataType() {
        DataTypeMap.initMap();
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        Indexer.hostName = MiscTools.getHostName();
        Configure cfg = null;
        try {
            cfg = new Configure("config.properties");
        } catch (Exception e) {
            log.error(e + ",the program will exit", e);
            return;
        }

        Indexer.writeIndexThreadNum = Integer.parseInt(cfg.getProperty("writeNumPerRead"));

        Indexer.zkUrls = cfg.getProperty("zkUrl");
        /**/
        Indexer.metaStoreCient = cfg.getProperty("metaStoreCient");
        Indexer.metaStoreCientUrl = metaStoreCient.split("[:]")[0];
        Indexer.metaStoreCientPort = Integer.parseInt(Indexer.metaStoreCient.split("[:]")[1]);
        Indexer.clientPoolSize = Integer.parseInt(cfg.getProperty("clientPoolSize"));
        /*
         */
        Indexer.cachePath = cfg.getProperty("persiCachePath");
        Indexer.unclosedFilePath = cfg.getProperty("unclosedFilePath");

        Indexer.writeIndexThreadNum = cfg.getIntProperty("writeNumPerRead");
        Indexer.writeInnerpoolSize = cfg.getIntProperty("writeBufNum");
        Indexer.readbufSize = cfg.getIntProperty("readBufNum");
        Indexer.httpServerPool = cfg.getIntProperty("httpServerPoolNum");
        Indexer.maxDelaySeconds = cfg.getIntProperty("maxDelayTime");
        Indexer.commitgab = cfg.getIntProperty("commitgab");


        {  // init the client pool
            Indexer.clientPool = new MetastoreClientPool(Indexer.clientPoolSize, Indexer.metaStoreCient);
        }

        try {
            MetaStoreClient msc = Indexer.clientPool.getClient();
            List<Database> dbs = msc.client.get_all_attributions();
            Indexer.clientPool.realseOneClient(msc);

            int dbNumPerThread = dbs.size() / Indexer.initMetaInfoThreadnum;
            if (dbNumPerThread == 0) {
                dbNumPerThread = 1;
            }
            List<String> dbNames = new ArrayList<String>();
            for (int i = 0; i < dbs.size(); i++) {
                dbNames.add(dbNames.get(i));
                if (dbNames.size() >= dbNumPerThread || i >= dbs.size() - 1) {
                    Initmetastoreinfo initmetastore = new Initmetastoreinfo(dbNames);
                    Thread initmetasinfoThread = new Thread(initmetastore);
                    initmetasinfoThread.start();
                    dbNames = new ArrayList<String>();
                }
            }

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
            }
        } catch (Exception ex) {
            log.error(ex, ex);
            return;
        }

        Indexer.docs_protocal = imd.getDocSchema();
        Indexer.initSchema();

        Indexer.staticsDataemiter = MQProducerPool.getMQProducerPool("data_volume_stat_mq", 50);

        log.info("start init the http receive data server ... \n");
        try {
            HttpReceivedata.init();
            HttpReceivedata.startup();
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
}