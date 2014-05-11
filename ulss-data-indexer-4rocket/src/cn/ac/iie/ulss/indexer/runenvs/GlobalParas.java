/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.runenvs;

import cn.ac.iie.ulss.indexer.metastore.DBMetastore;
import cn.ac.iie.ulss.indexer.metastore.MetastoreClientPool;
import cn.ac.iie.ulss.indexer.worker.IndexControler;
import cn.ac.iie.ulss.indexer.worker.Indexer;
import cn.ac.iie.ulss.indexer.worker.RocketDataPuller;
import cn.ac.iie.ulss.indexer.worker.RocketdataHandler;
import cn.ac.iie.ulss.indexer.worker.WriteRawfile;
import cn.ac.iie.ulss.utiltools.ConfigureUtil;
import cn.ac.iie.ulss.utiltools.MiscTools;
import com.alibaba.rocketmq.common.message.MessageExt;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;

/**
 *
 * @author work
 */
public class GlobalParas {

    public static Logger log = Logger.getLogger(GlobalParas.class.getName());
    public static int dcxthreadpoolSize = 10;
    public static int cdrthreadpoolSize = 10;
    public static int hlwthreadpoolSize = 120;
    public static int submitTimeout = 1;
    public static int writeMinNumOnce = 10000;
    /*
     * for lucene
     */
    public static int luceneMaxThreadStates = 8;
    public static int luceneWriterMaxParallelNum = 6; //设置lucene的最大并发数，防止并发过大浪费资源
    public static double luceneBufferRam = 5;
    public static int luceneBufferDocs = 5000;
    public static int commitgab = 200;
    public static AtomicInteger closeDelay = new AtomicInteger(15); //that is 15 seconds
    /*
     * for rocket
     */
    public static String rocketNameServer = "";
    public static int producerPoolSize = 8;
    public static String rocketConsumeGroupPrefix = "";
    public static String rocketProducerGroupPrefix = "";
    public static String errorDataTopicSuffix = "";
    public static int rocketMaxConsumeThread = 8;
    public static int rocketPullDatabufferSize = 500;
    public static int rocketConsumeMessageBatchMaxSize = 20;
    public static int pullThresholdForQueue = 10;
    public static int rocketConsumesetMaxMessageSizeMB = 8;
    public static String error_file_mq = "error_file_mq";
    public static int clientCallbackExecutorThreads = 40;
    public static int compressMsgBodyOverHowmuchInKB = 512;
    public static String rocketConsumTopics = "";
    public static ConcurrentHashMap<String, String> consumeTopicsMap = new ConcurrentHashMap<String, String>();
    /*
     *  for index control
     */
    public static ConcurrentHashMap<Long, IndexControler> id2Createindex = new ConcurrentHashMap<Long, IndexControler>(); //file_id到createIndex的映射
    public static ConcurrentHashMap<Long, WriteRawfile> id2WriteRawfile = new ConcurrentHashMap<Long, WriteRawfile>(); //file_id到createIndex的映射
    public static ConcurrentHashMap<Long, Long> removedFileId = new ConcurrentHashMap<Long, Long>(); //存储已经删除了的文件，是有问题的文件
    public static ConcurrentHashMap<Long, Long> diskErrorFileId = new ConcurrentHashMap<Long, Long>(); //存储损坏文件无法写入，比如磁盘无法写入导致的，一旦写入失败就再也不会恢复
    /*
     */
    //从元数据获取数据信息时出错，比如应该放到节点1对应队列的文件放到了节点2对应的文件，这种情况是会发生的
    public static ConcurrentHashMap<Long, Long> metaORdeviceErrorFileId = new ConcurrentHashMap<Long, Long>();
    /*
     * for metastore,metastore client pool
     */
    public static String metaStoreCientUrl = "";
    public static int metaStoreCientPort = 0;
    public static int clientPoolSize = 6;
    public static int readbufSize = 20;
    public static MetastoreClientPool clientPool = null;
    /*
     *
     */
    public static int maxDelaySeconds = 200;

    /*
     * for lucene  metapool
     */
    public static ArrayBlockingQueue<ConcurrentHashMap<String, LinkedHashMap<String, Field>>> table2idx2LuceneFieldMapPool = new ArrayBlockingQueue(GlobalParas.hlwthreadpoolSize * 4);
    public static ArrayBlockingQueue<ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>>> table2idx2colMapPool = new ArrayBlockingQueue(GlobalParas.hlwthreadpoolSize * 4);
    public static ArrayBlockingQueue<ConcurrentHashMap<String, LinkedHashMap<String, FieldType>>> table2idx2LuceneFieldTypeMapPool = new ArrayBlockingQueue(GlobalParas.hlwthreadpoolSize * 4);

    /*
     * for metastore
     */
    public static String metaStoreCient = "";
    /*
     * for meta
     */
    public static ConcurrentHashMap<String, List<Index>> tablename2indexs = new ConcurrentHashMap<String, List<Index>>(10, 0.8F);
    public static ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>> table2idx2colMap = new ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>>(50, 0.8F);
    public static ConcurrentHashMap<String, Schema> schemaname2Schema = new ConcurrentHashMap<String, Schema>(20, 0.8F);
    public static ConcurrentHashMap<String, Table> tablename2Table = new ConcurrentHashMap<String, Table>(10, 0.8F); //db+table name 到table对象的映射
    public static ConcurrentHashMap<String, LinkedHashMap<String, Field>> table2idx2LuceneFieldMap = new ConcurrentHashMap<String, LinkedHashMap<String, Field>>(50, 0.8F);
    public static ConcurrentHashMap<String, String> tablename2Schemaname = new ConcurrentHashMap<String, String>(10, 0.8F); //schema的名字到name到table对象的映射
    public static ConcurrentHashMap<String, LinkedHashMap<String, FieldType>> table2idx2LuceneFieldTypeMap = new ConcurrentHashMap<String, LinkedHashMap<String, FieldType>>(50, 0.8F);
    /*
     *
     */
    public static boolean isTestMode = false;
    /* 
     * for db
     */
    public static DBMetastore imd = new DBMetastore();
    /*
     * for  misc
     */
    public static String unclosedFilePath = "";
    public static String cachePath = "";
    public static int intervalWriterKey = -1;
    public static int duplicateNum = 2;
    public static String hostName = "";
    public static int httpServerPool = 50;
    public static AtomicBoolean isShouldExit = new AtomicBoolean(false);
    /*
     * for sys
     */
    public static String ip = "";
    /*
     * for  avro  schema
     */
    public static String docs_protocal = "";
    public static Schema docsSchema = null;
    public static DatumReader<GenericRecord> docsReader = null;
    public static String docsSchemaContent = null;
    public static String docs_schema_name = "doc_schema_name";
    public static String docs = "docs";
    public static String docs_set = "doc_set";
    public static String sign = "sign";
    public static ConcurrentHashMap<String, String> schemaname2schemaContent = new ConcurrentHashMap<String, String>(20, 0.8F);

    /*
     * for zk
     */
    public static String zkUrls = "";
    /*
     * for   statVolume
     */
    public static String staticsMq = null;
    public static String statVolumeSchemaContent = null;
    public static Schema statVolumeSchema = null;
    /*
     */
    public static int initMetaInfoThreadnum = 4;
    /*
     * for some ?
     */
    public static int writeInnerpoolBatchDrainSize = 15;
    public static int writeIndexThreadNum = 4; //每个数据源索引创建线程的数量，default is 4
    /*
     * 
     */
    public static ConcurrentHashMap<String, AtomicLong> consumeTopic = new ConcurrentHashMap<String, AtomicLong>();
    public static ConcurrentHashMap<String, AtomicLong> getdataStatics = new ConcurrentHashMap<String, AtomicLong>();
    public static RocketDataPuller datapuller = null;

    /*
     * 
     */
    public static int badfileinfoReserveSeconds = 3600;
    public static int badfileinfoCheckGab = 60;
    /*
     * 
     */
    public static int indexerBufferFullSleepMilis = 600;


    /*
     *  for thread pool
     */
    public static void init() {
        ConfigureUtil cfg = null;
        try {
            cfg = new ConfigureUtil("config.properties");
        } catch (Exception e) {
            log.error(e + ",the program will exit", e);
            return;
        }
        GlobalParas.hostName = MiscTools.getHostName();
        GlobalParas.ip = MiscTools.getIpAddress();

        GlobalParas.zkUrls = cfg.getProperty("zkUrl");
        GlobalParas.metaStoreCient = cfg.getProperty("metaStoreCient");
        GlobalParas.metaStoreCientUrl = GlobalParas.metaStoreCient.split("[:]")[0];
        GlobalParas.metaStoreCientPort = Integer.parseInt(GlobalParas.metaStoreCient.split("[:]")[1]);
        GlobalParas.clientPoolSize = Integer.parseInt(cfg.getProperty("clientPoolSize"));

        GlobalParas.cachePath = cfg.getProperty("persiCachePath");
        GlobalParas.unclosedFilePath = cfg.getProperty("unclosedFilePath");

        /*
         * for  fast write data
         */
        GlobalParas.writeIndexThreadNum = cfg.getIntProperty("writeNumPerRead");
        GlobalParas.writeInnerpoolBatchDrainSize = cfg.getIntProperty("writeInnerpoolBatchDrainSize");
        GlobalParas.writeMinNumOnce = cfg.getIntProperty("writeMinNumOnce");
        GlobalParas.readbufSize = cfg.getIntProperty("readBufNum");
        GlobalParas.submitTimeout = cfg.getIntProperty("submitTimeout");
        /*
         *
         */
        GlobalParas.httpServerPool = cfg.getIntProperty("httpServerPoolNum");
        GlobalParas.maxDelaySeconds = cfg.getIntProperty("maxDelayTime");
        GlobalParas.commitgab = cfg.getIntProperty("commitgab");
        GlobalParas.producerPoolSize = cfg.getIntProperty("producerPoolSize");
        GlobalParas.staticsMq = cfg.getProperty("staticsMetaq");

        /*
         * 
         */
        GlobalParas.dcxthreadpoolSize = Integer.parseInt(cfg.getProperty("dcxthreadpoolSize"));
        GlobalParas.cdrthreadpoolSize = Integer.parseInt(cfg.getProperty("cdrthreadpoolSize"));
        GlobalParas.hlwthreadpoolSize = Integer.parseInt(cfg.getProperty("hlwrzthreadpoolSize"));
        GlobalParas.luceneWriterMaxParallelNum = Integer.parseInt(cfg.getProperty("luceneWriterMaxParaThread"));

        GlobalParas.initMetaInfoThreadnum = cfg.getIntProperty("initMetaInfoThreadnum");
        GlobalParas.clientPool = new MetastoreClientPool(GlobalParas.clientPoolSize, GlobalParas.metaStoreCientUrl, GlobalParas.metaStoreCientPort);
        /*
         * 
         */
        GlobalParas.isTestMode = cfg.getBooleanProperty("isTestMode");
        /*
         * for lucene
         */
        GlobalParas.luceneBufferRam = Double.parseDouble(cfg.getProperty("luceneBufferRam"));
        GlobalParas.luceneBufferDocs = Integer.parseInt(cfg.getProperty("luceneBufferDocs"));
        GlobalParas.luceneMaxThreadStates = Integer.parseInt(cfg.getProperty("luceneMaxThreadStates"));

        /*
         * for rocket
         */
        GlobalParas.rocketNameServer = cfg.getProperty("rocketNameServer");
        GlobalParas.rocketConsumeGroupPrefix = cfg.getProperty("rocketConsumeGroupPrefix");
        GlobalParas.rocketProducerGroupPrefix = cfg.getProperty("rocketProducerGroupPrefix");
        GlobalParas.rocketMaxConsumeThread = cfg.getIntProperty("rocketMaxConsumeThread");
        GlobalParas.rocketConsumeMessageBatchMaxSize = cfg.getIntProperty("rocketConsumeMessageBatchMaxSize");
        GlobalParas.rocketPullDatabufferSize = cfg.getIntProperty("rocketDatabufferSize");
        GlobalParas.error_file_mq = cfg.getProperty("error_file_mq");
        GlobalParas.clientCallbackExecutorThreads = cfg.getIntProperty("clientCallbackExecutorThreads");
        GlobalParas.compressMsgBodyOverHowmuchInKB = cfg.getIntProperty("compressMsgBodyOverHowmuchInKB");
        GlobalParas.rocketConsumTopics = cfg.getProperty("rocketConsumTopics");

        for (String s : GlobalParas.rocketConsumTopics.split("[,]")) {
            GlobalParas.consumeTopicsMap.put(s, s);
            GlobalParas.getdataStatics.put(GlobalParas.hostName + "_" + s, new AtomicLong());
        }

        GlobalParas.pullThresholdForQueue = cfg.getIntProperty("pullThresholdForQueue");

        GlobalParas.badfileinfoCheckGab = cfg.getIntProperty("badfileinfoCheckGab");
        GlobalParas.badfileinfoReserveSeconds = cfg.getIntProperty("badfileinfoReserveSeconds");

        GlobalParas.indexerBufferFullSleepMilis = cfg.getIntProperty("indexerBufferFullSleepMilis");
    }

    public static void main(String[] ars) {
        GlobalParas.init();
        System.out.println(GlobalParas.consumeTopicsMap);

        for (String s : GlobalParas.consumeTopicsMap.keySet()) {
            if ("603-SJGL-CL-SEV-0031_t_cx_rz_mq".contains(s)) {
                System.out.println(s);
            }
        }
    }
}
