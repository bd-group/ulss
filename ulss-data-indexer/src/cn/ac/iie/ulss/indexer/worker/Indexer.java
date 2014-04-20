package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.datastatics.DataEmiter;
import cn.ac.iie.ulss.indexer.metastore.DBMetastore;
import cn.ac.iie.ulss.indexer.metastore.MetastoreClientPool;
import cn.ac.iie.ulss.shuthandler.KillHandler;
import cn.ac.iie.ulss.utiltools.ConfigureUtil;
import cn.ac.iie.ulss.utiltools.MiscTools;
import iie.metastore.MetaStoreClient;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;

public class Indexer {

    public static Logger log = Logger.getLogger(Indexer.class.getName());
    public static String docs_protocal = "";
    public static String docs_set = "doc_set";
    public static String docs = "docs";
    public static String docs_schema_name = "doc_schema_name";
    public static String hostName = "";
    public static String ip = "";
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
    /*
     * 
     */
    public static int writeInnerpoolBatchDrainSize = 15;
    public static int readbufSize = 20;
    public static int writeMinNumOnce = 10000;
    public static int submitTimeout = 1;
    /*
     * 
     */
    public static int maxDelaySeconds = 200;
    public static int commitgab = 200;
    public static int producerPoolSize = 8;
    /*
     */
    public static int dcxthreadpoolSize = 10;
    public static int cdrthreadpoolSize = 10;
    public static int hlwrzthreadpoolSize = 120;
    public static int luceneWriterMaxParallelNum = 6;  //设置lucene的最大并发数，防止并发过大浪费资源
    /*
     */
    public static ConcurrentHashMap<String, String> schemaname2schemaContent = new ConcurrentHashMap<String, String>(20, 0.8f);
    public static ConcurrentHashMap<String, Schema> schemaname2Schema = new ConcurrentHashMap<String, Schema>(20, 0.8f);
    /*
     *
     */
    public static ConcurrentHashMap<String, Table> tablename2Table = new ConcurrentHashMap<String, Table>(10, 0.8f);  //db+table name 到table对象的映射
    public static ConcurrentHashMap<String, List<Index>> tablename2indexs = new ConcurrentHashMap<String, List<Index>>(10, 0.8f);
    /**/
    public static ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>> table2idx2colMap = new ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>>(50, 0.8f);
    public static ConcurrentHashMap<String, LinkedHashMap<String, Field>> table2idx2LuceneFieldMap = new ConcurrentHashMap<String, LinkedHashMap<String, Field>>(50, 0.8f);
    public static ConcurrentHashMap<String, LinkedHashMap<String, FieldType>> table2idx2LuceneFieldTypeMap = new ConcurrentHashMap<String, LinkedHashMap<String, FieldType>>(50, 0.8f);
    /**/
    public static ArrayBlockingQueue<ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>>> table2idx2colMapPool = new ArrayBlockingQueue(Indexer.hlwrzthreadpoolSize * 4);
    public static ArrayBlockingQueue<ConcurrentHashMap<String, LinkedHashMap<String, Field>>> table2idx2LuceneFieldMapPool = new ArrayBlockingQueue(Indexer.hlwrzthreadpoolSize * 4);
    public static ArrayBlockingQueue<ConcurrentHashMap<String, LinkedHashMap<String, FieldType>>> table2idx2LuceneFieldTypeMapPool = new ArrayBlockingQueue(Indexer.hlwrzthreadpoolSize * 4);
    /*
     *
     */
    public static ConcurrentHashMap<String, String> tablename2Schemaname = new ConcurrentHashMap<String, String>(10, 0.8f);  //schema的名字到name到table对象的映射
    /*
     * 
     */
    public static String docsSchemaContent = null;
    public static Schema docsSchema = null;
    public static DatumReader<GenericRecord> docsReader = null;
    public static DBMetastore imd = new DBMetastore();
    /*
     */
    public static AtomicInteger closeDelay = new AtomicInteger(15);//that is 15 seconds
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
    public static String staticsMq = null;
    /*
     * 
     */
    public static boolean isTestMode = false;

    /*
     * for lucene paras 
     */
    public static double luceneBufferRam = 5;
    public static int luceneBufferDocs = 5000;
    public static int luceneMaxThreadStates = 8;

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        ConfigureUtil cfg = null;
        try {
            cfg = new ConfigureUtil("config.properties");
        } catch (Exception e) {
            log.error(e + ",the program will exit", e);
            return;
        }
        Indexer.hostName = MiscTools.getHostName();
        Indexer.ip = MiscTools.getIpAddress();

        Indexer.zkUrls = cfg.getProperty("zkUrl");
        Indexer.metaStoreCient = cfg.getProperty("metaStoreCient");
        Indexer.metaStoreCientUrl = metaStoreCient.split("[:]")[0];
        Indexer.metaStoreCientPort = Integer.parseInt(Indexer.metaStoreCient.split("[:]")[1]);
        Indexer.clientPoolSize = Integer.parseInt(cfg.getProperty("clientPoolSize"));

        Indexer.cachePath = cfg.getProperty("persiCachePath");
        Indexer.unclosedFilePath = cfg.getProperty("unclosedFilePath");

        /*
         * for  fast write data
         */
        Indexer.writeIndexThreadNum = cfg.getIntProperty("writeNumPerRead");
        Indexer.writeInnerpoolBatchDrainSize = cfg.getIntProperty("writeInnerpoolBatchDrainSize");
        Indexer.writeMinNumOnce = cfg.getIntProperty("writeMinNumOnce");
        Indexer.readbufSize = cfg.getIntProperty("readBufNum");
        Indexer.submitTimeout = cfg.getIntProperty("submitTimeout");
        /*
         *
         */
        Indexer.httpServerPool = cfg.getIntProperty("httpServerPoolNum");
        Indexer.maxDelaySeconds = cfg.getIntProperty("maxDelayTime");
        Indexer.commitgab = cfg.getIntProperty("commitgab");
        Indexer.producerPoolSize = cfg.getIntProperty("producerPoolSize");
        Indexer.staticsMq = cfg.getProperty("staticsMetaq");

        /*
         * 
         */
        Indexer.dcxthreadpoolSize = Integer.parseInt(cfg.getProperty("dcxthreadpoolSize"));
        Indexer.cdrthreadpoolSize = Integer.parseInt(cfg.getProperty("cdrthreadpoolSize"));
        Indexer.hlwrzthreadpoolSize = Integer.parseInt(cfg.getProperty("hlwrzthreadpoolSize"));
        Indexer.luceneWriterMaxParallelNum = Integer.parseInt(cfg.getProperty("luceneWriterMaxParaThread"));

        Indexer.initMetaInfoThreadnum = cfg.getIntProperty("initMetaInfoThreadnum");
        Indexer.clientPool = new MetastoreClientPool(Indexer.clientPoolSize, Indexer.metaStoreCientUrl, Indexer.metaStoreCientPort);
        /*
         * 
         */
        Indexer.isTestMode = cfg.getBooleanProperty("isTestMode");
        /*
         * for lucene
         */
        Indexer.luceneBufferRam = Double.parseDouble(cfg.getProperty("luceneBufferRam"));
        Indexer.luceneBufferDocs = Integer.parseInt(cfg.getProperty("luceneBufferDocs"));
        Indexer.luceneMaxThreadStates = Integer.parseInt(cfg.getProperty("luceneMaxThreadStates"));



        InitEnvTools.initDataType();
        try {
            MetaStoreClient msc = Indexer.clientPool.getClient();
            List<Database> dbs = msc.client.get_all_attributions();
            List<Thread> threads = new ArrayList<Thread>();
            Indexer.clientPool.realseOneClient(msc);
            int dbNumPerThread = dbs.size() / Indexer.initMetaInfoThreadnum;
            if (dbNumPerThread == 0) {
                dbNumPerThread = 1;
            }
            log.info("the db num is " + dbs.size() + " and db num per thread is " + dbNumPerThread);
            List<String> dbNames = new ArrayList<String>();
            for (int i = 0; i <= dbs.size() - 1; i++) {
                dbNames.add(dbs.get(i).getName());
                if (dbNames.size() >= dbNumPerThread || i == dbs.size() - 1) {
                    Initmetainfo initmetastore = new Initmetainfo(dbNames);
                    Thread initmetasinfoThread = new Thread(initmetastore);
                    initmetasinfoThread.start();
                    dbNames = new ArrayList<String>();
                    threads.add(initmetasinfoThread);
                }
            }
            for (Thread t : threads) {
                t.join();
            }

//            for (Database db : dbs) {
//                String dbName = db.getName();
//                String mmurl = db.getParameters().get("mm.url");
//                if (mmurl != null && !"".equals(mmurl)) {
//                    ClientConf cc = new ClientConf();
//                    ClientAPI ca = new ClientAPI(cc);
//                    //ca.quit();
//                    ca.init(mmurl);
//                    Indexer.ClientAPIMap.put(dbName, ca);
//                    log.info("the mm.url for " + dbName + " is " + mmurl);
//                }
//            }
        } catch (Exception ex) {
            log.error(ex, ex);
            return;
        }

        Indexer.docs_protocal = imd.getDocSchema();
        InitEnvTools.initSchema();
        InitEnvTools.initIndexinfo();
        Workerpool.initWorkpool();

        log.info("begin init the statics data emiter ... ");
        InitEnvTools.initStatVolumeSchema();
        DataEmiter.initEmiter();
        log.info("init the statics data emiter ok");

        log.info("start init the http receive data server ... \n");
        try {
            HttpGetdataServer.init();
            HttpGetdataServer.startup();
        } catch (Exception ex) {
            log.error(ex, ex);
        }

        log.info("init the http server ok,will init the kill handler");
        KillHandler killhandle = new KillHandler();
        killhandle.registerSignal("TERM");
        log.info("init the kill handler done ");

        Monitor monitor = new Monitor();
        Thread monitorThread = new Thread(monitor);
        monitorThread.start();

        boolean isover = false;
        while (!(Indexer.isShouldExit.get() && isover)) {
            isover = true;
            try {
                Thread.sleep(20000);
            } catch (InterruptedException ex) {
            }
            for (IndexControler c : HttpDataHandler.id2Createindex.values()) {
                if (!c.isAllOver.get()) {
                    isover = false;
                    break;
                }
            }

            if (Indexer.isShouldExit.get() && isover) {
                log.info("the system can exit safely,will destroy the producer and work pool，wait ... ");
                DataEmiter.stopEmiter();
                try {
                    Workerpool.dcxpool.shutdown();
                    Workerpool.cdrpool.shutdown();
                    Workerpool.hlwrzpool.shutdown();
                } catch (Exception e) {
                    log.error(e, e);
                }
                break;
            }
        }
    }
}
