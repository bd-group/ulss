package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.datastatics.DataEmiter;
import cn.ac.iie.ulss.indexer.metastore.MetastoreClientPool;
import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import cn.ac.iie.ulss.shuthandler.KillHandler;
import cn.ac.iie.ulss.utiltools.ConfigureUtil;
import cn.ac.iie.ulss.utiltools.MiscTools;
import iie.metastore.MetaStoreClient;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Indexer {

    public static Logger log = Logger.getLogger(Indexer.class.getName());

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");

        GlobalParas.init();


        InitEnvCachedpara.initDataType();

        try {
            MetaStoreClient msc = GlobalParas.clientPool.getClient();
            List<Database> dbs = msc.client.get_all_attributions();
            List<Thread> threads = new ArrayList<Thread>();
            GlobalParas.clientPool.realseOneClient(msc);
            int dbNumPerThread = dbs.size() / GlobalParas.initMetaInfoThreadnum;
            if (dbNumPerThread == 0) {
                dbNumPerThread = 1;
            }
            log.info("the db num is " + dbs.size() + " and db num per thread is " + dbNumPerThread);
            List<String> dbNames = new ArrayList<String>();
            for (int i = 0; i <= dbs.size() - 1; i++) {
                dbNames.add(dbs.get(i).getName());
                if (dbNames.size() >= dbNumPerThread || i == dbs.size() - 1) {
                    InitMetainfo initmetastore = new InitMetainfo(dbNames);
                    Thread initmetasinfoThread = new Thread(initmetastore);
                    initmetasinfoThread.start();
                    dbNames = new ArrayList<String>();
                    threads.add(initmetasinfoThread);
                }
            }
            for (Thread t : threads) {
                t.join();
            }
        } catch (Exception ex) {
            log.error(ex, ex);
            return;
        }

        GlobalParas.docs_protocal = GlobalParas.imd.getDocSchema();
        InitEnvCachedpara.initSchema();
        InitEnvCachedpara.initIndexinfo();
        Workerpool.initWorkpool();

        log.info("begin init the statics data emiter ... ");
        InitEnvCachedpara.initStatVolumeSchema();
        //DataEmiter.initEmiter();
        log.info("init the statics data emiter ok");


        /*
         * fix me   
         */
        List<String> topics = new ArrayList<String>();

        String[] oritopics = GlobalParas.rocketConsumTopics.split("[,]");
        for (String s : oritopics) {
            topics.add(GlobalParas.hostName + "_" + s);
        }
        /*
         * 
         */
        RocketMQProducer.init(GlobalParas.hostName + "_" + GlobalParas.rocketProducerGroupPrefix + "_producergroup", GlobalParas.rocketNameServer);

        ArrayBlockingQueue<GenericRecord> shareBuffer = new ArrayBlockingQueue<GenericRecord>(GlobalParas.rocketPullDatabufferSize);
        GlobalParas.datapuller = new RocketDataPuller(GlobalParas.hostName + "_" + GlobalParas.rocketConsumeGroupPrefix + "_consumergroup", topics, "*", shareBuffer);
        Thread pulldataThread = new Thread(GlobalParas.datapuller);
        pulldataThread.start();


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
        while (!(GlobalParas.isShouldExit.get() && isover)) {
            isover = true;
            try {
                Thread.sleep(20000);
            } catch (InterruptedException ex) {
            }
            for (IndexControler c : GlobalParas.id2Createindex.values()) {
                if (!c.isAllOver.get()) {
                    isover = false;
                    break;
                }
            }

            if (GlobalParas.isShouldExit.get() && isover) {
                log.info("the system can exit safely,will destroy the producer and work pool，wait ... ");
                //DataEmiter.stopEmiter();
                try {
                    Workerpool.shutWorkpool();
                } catch (Exception e) {
                    log.error(e, e);
                }

                RocketMQProducer.shutProducer();
                log.info("shut the rocket producer ok ...");
                break;
            }
        }
    }
}
