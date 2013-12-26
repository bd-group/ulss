/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.worker;

import cn.ac.iie.ulss.match.datahandler.HttpGetDataServer;
import cn.ac.iie.ulss.match.sender.SendCxData;
import cn.ac.iie.ulss.match.sender.SendDxData;
import cn.ac.iie.ulss.match.sender.SendHlwData;
import cn.ac.iie.ulss.match.sender.SendUtilData;
import cn.ac.iie.ulss.struct.CDRRecordNode;
import cn.ac.iie.ulss.util.Configure;
import cn.ac.iie.ulss.util.MatchDBMeta;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Matcher {

    public static Logger log = Logger.getLogger(Matcher.class.getName());
    public static MatchDBMeta DBMeta = new MatchDBMeta();
    public static String docs_set_name = "doc_set";
    public static String docsName = "docs";
    /*
     * 
     */
    public static String rawdataCachePath = "";
    public static String positionDumpPath = "";
    /*
     * about zk config
     */
    public static String zkUrl = "";
    public static ZkClient zc;
    /*
     *about region config
     */
    public static String[] allregions = null;
    /*
     *
     */
    public static int maxStreamNum = 2; //最大处理的流式数据的number
    public static int httpPoolNum = 50;
    public static int matchThreadNum = 1;
    public static int cdrUpdateThreadNum = 1;
    public static int sendBatchSizeNum = 1000;
    public static int busidataInbufferSize = 20000;
    public static int cdrInbufferSize = 100000;
    public static int outbufferSize = 40000;
    public static int topMapSize = 64;
    /*
     *
     */
    public static int maxProcessCdrPerMillis = 20;
    /*
     * about position dump 
     */
    public static int maxStorePositionPerNumber = 2;
    public static int posDumpIntervalMiliSeconds = 43200;
    public static int maxPosStoreMiliSeconds = 10800;
    /*
     *
     */
    public static int qd_szs = -1;
    /*
     *
     */
    public static ConcurrentHashMap<String, MatchControler> schemaInstance2MatchControler = new ConcurrentHashMap<String, MatchControler>();
    public static ConcurrentHashMap<String, List<BusiMatchworker>> schemaInstance2BusiMatchworkers = new ConcurrentHashMap<String, List<BusiMatchworker>>();
    public static ConcurrentHashMap<String, String> metaq2schemaname = new ConcurrentHashMap<String, String>();
    public static ConcurrentHashMap<String, String> schemanameInstance2metaq = new ConcurrentHashMap<String, String>();
    public static ConcurrentHashMap<String, Schema> schemaname2Schema = new ConcurrentHashMap<String, Schema>();
    public static ConcurrentHashMap<String, DatumReader<GenericRecord>> schema2Reader = new ConcurrentHashMap<String, DatumReader<GenericRecord>>();
    public static ConcurrentHashMap<String, AtomicLong> schemanameInstance2Total = new ConcurrentHashMap<String, AtomicLong>();
    public static ConcurrentHashMap<String, AtomicLong> schemanameInstance2Sendtotal = new ConcurrentHashMap<String, AtomicLong>();
    public static ConcurrentHashMap<String, AtomicLong> schemanameInstance2MatchOKTotal = new ConcurrentHashMap<String, AtomicLong>();
    public static ConcurrentHashMap<String, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>> cdrMaps = new ConcurrentHashMap<String, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>>();
    public static ConcurrentHashMap<String, CDRUpdater> cdrupdaters = new ConcurrentHashMap<String, CDRUpdater>();
    /*
     *
     */
    public static String docsSchemaContent = null;
    public static Schema docsSchema = null;
    public static DatumReader<GenericRecord> docsReader = null;

    public static void initSchema() {
        log.info("start init all the schama \n ");
        String schemaName = "";
        String schemaContent = "";
        try {
            log.info("getting data schema and metaq from metadb...");
            Matcher.docsSchemaContent = Matcher.DBMeta.getDocSchema();
            List<List<String>> allSchema = DBMeta.getAllSchema();
            for (List<String> list : allSchema) {
                schemaName = list.get(0).toLowerCase();
                schemaContent = list.get(1).toLowerCase();
                log.debug("schema " + schemaName + "'s content is:\n" + schemaContent);
                if (schemaName.equals("docs")) {
                    log.info("init schema docs ...");
                    Matcher.docsSchemaContent = schemaContent;
                    Protocol protocol = Protocol.parse(docsSchemaContent);
                    Matcher.docsSchema = protocol.getType(schemaName);
                    Matcher.docsReader = new GenericDatumReader<GenericRecord>(docsSchema);
                    Matcher.schemaname2Schema.put("docs", docsSchema);
                    log.info("init schema for docs is ok");
                } else {
                    log.info("init schema data reader and schema and  metaq for schema " + schemaName + " ...");

                    Protocol protocol = Protocol.parse(schemaContent);
                    Schema schema = protocol.getType(schemaName);
                    DatumReader<GenericRecord> schemaReader = new GenericDatumReader<GenericRecord>(schema);

                    Matcher.schemaname2Schema.put(schemaName, schema);
                    Matcher.schema2Reader.put(schemaName, schemaReader);

                    String[] regions = Matcher.allregions;
                    for (String s : regions) {
                        if ("local".equalsIgnoreCase(s)) {
                            s = "";
                        }
                        String schemaInstance = s.toLowerCase() + "." + schemaName;
                        log.info("put " + schemaInstance + " to the statics total map ");
                        Matcher.schemanameInstance2Total.put(schemaInstance, new AtomicLong(0));
                        Matcher.schemanameInstance2Sendtotal.put(schemaInstance, new AtomicLong(0));
                        Matcher.schemanameInstance2MatchOKTotal.put(schemaInstance, new AtomicLong(0));
                    }
                }
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

    public static void initMq2Schema() {
        List<List<String>> mq2schema = Matcher.DBMeta.getMq2Schema();
        String mq = null;
        String schemaname = null;
        for (List<String> list : mq2schema) {
            log.info("put " + mq + " " + schemaname + " to metaq2schemaname  map");
            mq = list.get(0).toLowerCase();
            schemaname = list.get(1).toLowerCase();
            Matcher.metaq2schemaname.put(mq, schemaname);
        }
    }

    public static ConcurrentHashMap< String, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>> initCDRWorkers() {
        ConcurrentHashMap<String, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>> cdrAllMaps = new ConcurrentHashMap< String, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>>();
        String[] regions = Matcher.allregions;
        List<String> cdrRule = (List<String>) Matcher.DBMeta.getRules("cdr").get(0);
        String rule_content = cdrRule.get(0);
        String[] paras = rule_content.split("[|]");

        for (int m = 0; m < regions.length; m++) {
            String reg = regions[m].toLowerCase();
            if ("local".equalsIgnoreCase(reg)) {
                reg = "";
            }
            String schemaName = paras[0].toLowerCase();
            int smWinSize = Integer.parseInt(paras[1]);
            int winNum = Integer.parseInt(paras[2]);
            String[] acJoinAttributes = paras[3].toLowerCase().split("[,]");
            int timeOutDeal = Integer.parseInt(paras[4]);

            ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> topCdrMap = RecoverPos.imp(Matcher.positionDumpPath + "/");
            if (topCdrMap == null) {
                topCdrMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>(1024 * 32, 0.8f);
                for (long i = 0; i < Matcher.topMapSize; i++) {
                    ConcurrentHashMap<String, List<CDRRecordNode>> mtMap = new ConcurrentHashMap<String, List<CDRRecordNode>>(1024 * 32, 0.8f);
                    topCdrMap.put(i, mtMap);
                }
            }
            final Object cdrLock = new Object();
            CDRUpdater c = null;
            LinkedBlockingQueue<CDRRecordNode> inbuf = new LinkedBlockingQueue<CDRRecordNode>(Matcher.cdrInbufferSize);
            for (int i = 0; i < Matcher.cdrUpdateThreadNum; i++) {
                if (i == 0) {
                    c = new CDRUpdater(reg, schemaName, smWinSize, winNum, acJoinAttributes, timeOutDeal, topCdrMap, inbuf, new AtomicBoolean(true), cdrLock);
                    Matcher.cdrupdaters.put(reg, c);
                } else {
                    c = new CDRUpdater(reg, schemaName, smWinSize, winNum, acJoinAttributes, timeOutDeal, topCdrMap, inbuf, new AtomicBoolean(false), cdrLock);
                }
                Thread tc = new Thread(c);
                tc.setName(reg + "-cdr-" + i);
                tc.start();
            }
            log.info("put " + reg + " to the cdr topmaps");
            cdrAllMaps.put(reg, topCdrMap);
            Matcher.cdrMaps.put(reg, topCdrMap);
        }
        return cdrAllMaps;
    }

    public static HashMap<String, List<MatchControler>> initBusiWorkers(List<String> bustype) {
        ConcurrentHashMap< String, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>> cdrTopMaps = initCDRWorkers();
        HashMap<String, List<MatchControler>> busiControlerMap = new HashMap<String, List<MatchControler>>();

        for (String business : bustype) {
            List<List<String>> matchrules = Matcher.DBMeta.getRules(business);
            if (matchrules == null) {
                log.warn("there is no valid data match rule for " + business);
                continue;
            }

            String reg = "";
            String schemanameInstance = "";
            String[] regions = Matcher.allregions;

            log.info("the region number is " + regions.length);

            for (int m = 0; m < regions.length; m++) {
                reg = regions[m];
                if ("local".equals(reg)) {
                    reg = "";
                }
                log.info("start init all the match workers for " + schemanameInstance);

                List<MatchControler> controlers = new ArrayList<MatchControler>();

                for (List<String> lst : matchrules) {
                    String[] ss = lst.get(0).split("[;]");
                    int intputbufferSize = Integer.parseInt(ss[1].split("[,]")[0]);
                    int outputbufferSize = Integer.parseInt(ss[1].split("[,]")[1]);
                    int matchThreadNumber = Integer.parseInt(ss[1].split("[,]")[2]);
                    String resultSchemaName = ss[2].toLowerCase();
                    for (int i = 0; i < Matcher.maxStreamNum; i++) {
                        String[] paras = ss[0].split("[|]");
                        final ReadWriteLock rwLock = new ReentrantReadWriteLock();
                        String schemaName = paras[0].trim().toLowerCase();

                        schemanameInstance = reg + "." + schemaName;

                        String mSchemaName = paras[1].trim().toLowerCase();
                        int smWinSize = Integer.parseInt(paras[2].trim());
                        int winNum = Integer.parseInt(paras[3].trim());
                        String[] acJoinAttributes = paras[4].toLowerCase().trim().split("[,]");

                        String fJoinAttribute = paras[5].trim().split("[,]")[0].toLowerCase();
                        String mfJoinAttribute = paras[5].trim().split("[,]")[1].toLowerCase();

                        long maxDevi = Long.parseLong(paras[6].trim());
                        int timeOutDeal = Integer.parseInt(paras[7].trim());

                        if (i == 0) {
                            String[] resultOwnAttributes = lst.get(1).split("[,]");
                            for (int j = 0; j < resultOwnAttributes.length; j++) {
                                resultOwnAttributes[j] = resultOwnAttributes[j].toLowerCase();
                            }
                            String[] resultOtherAttributes = lst.get(2).split("[,]");
                            for (int j = 0; j < resultOtherAttributes.length; j++) {
                                resultOtherAttributes[j] = resultOtherAttributes[j].toLowerCase();
                            }
                            MatchControler mc = new MatchControler(reg, schemaName, mSchemaName, resultSchemaName, smWinSize, winNum, intputbufferSize, outputbufferSize, matchThreadNumber,
                                    acJoinAttributes, fJoinAttribute, mfJoinAttribute, maxDevi,
                                    resultOwnAttributes, resultOtherAttributes, timeOutDeal,
                                    cdrTopMaps.get(reg), rwLock.readLock());
                            mc.init();
                            controlers.add(mc);

                            Thread tc = new Thread(mc);
                            tc.start();
                            Matcher.schemaInstance2MatchControler.put(schemanameInstance, mc);
                            List<BusiMatchworker> workersList = new ArrayList<BusiMatchworker>();
                            workersList.addAll(Arrays.asList(mc.matchWorkers));
                            Matcher.schemaInstance2BusiMatchworkers.put(schemanameInstance, workersList);
                        }
                    }
                }
                busiControlerMap.put(reg + "." + business, controlers);
            }
        }
        return busiControlerMap;
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        Configure cfg = null;
        try {
            cfg = new Configure("config.properties");
        } catch (Exception e) {
            log.error(e + ",the program will exit", e);
            return;
        }
        Matcher.zc = new ZkClient(zkUrl = cfg.getProperty("zkUrl"));
        Matcher.httpPoolNum = cfg.getIntProperty("httpPoolNum");
        Matcher.sendBatchSizeNum = cfg.getIntProperty("batchSendNum");
        Matcher.matchThreadNum = cfg.getIntProperty("matchThreadNum");
        Matcher.cdrUpdateThreadNum = cfg.getIntProperty("cdrThreadNum");
        /**/
        Matcher.busidataInbufferSize = cfg.getIntProperty("inbufferSize");
        Matcher.cdrInbufferSize = cfg.getIntProperty("cdrInbufferSize");
        Matcher.outbufferSize = cfg.getIntProperty("outbufferSize");
        Matcher.rawdataCachePath = cfg.getProperty("matchcachePath");
        Matcher.positionDumpPath = cfg.getProperty("positionDumpPath");
        /**/
        Matcher.qd_szs = cfg.getIntProperty("qd_szs");
        HttpGetDataServer.poolNum = Matcher.httpPoolNum;
        Matcher.topMapSize = cfg.getIntProperty("topMapSize");
        Matcher.maxProcessCdrPerMillis = cfg.getIntProperty("maxProcessNumPerMillis");
        Matcher.posDumpIntervalMiliSeconds = cfg.getIntProperty("posDumpIntervalHours") * 3600 * 1000;
        Matcher.maxPosStoreMiliSeconds = cfg.getIntProperty("zombieNumberStoreMaxhours") * 3600 * 1000;
        /**/
        Matcher.maxStorePositionPerNumber = cfg.getIntProperty("maxStorePositionNumber");
        /**/
        Matcher.allregions = cfg.getProperty("regions").split("[,]");

        Matcher.initSchema();
        List<String> busiTypes = new ArrayList<String>();
        busiTypes.addAll(Arrays.asList(args));

        HashMap<String, List<MatchControler>> busiControlerMap = Matcher.initBusiWorkers(busiTypes);
        log.info("init the sent thread ... \n");

        String[] regions = Matcher.allregions;
        for (int m = 0; m < regions.length; m++) {
            String busiTypeInstance = "";
            String s = regions[m];
            if ("local".equalsIgnoreCase(s)) {
                s = "";
            }

            for (String busiData : busiTypes) {
                busiTypeInstance = s + "." + busiData;
                if ("dx".equalsIgnoreCase(busiData)) {
                    log.info("new the send thread for the dx and cdr");
                    for (MatchControler mc : busiControlerMap.get(busiTypeInstance)) {
                        for (int i = 0; i < 2; i++) {
                            SendDxData ssd = new SendDxData();
                            ssd.init(s, Matcher.sendBatchSizeNum, mc.outBuf);
                            Thread senThread = new Thread(ssd);
                            senThread.start();
                        }
                    }

                } else if ("cx".equalsIgnoreCase(busiData)) {
                    log.info("new the send thread for the cx and cdr");
                    for (MatchControler mc : busiControlerMap.get(busiTypeInstance)) {
                        for (int i = 0; i < 2; i++) {
                            SendCxData sd = new SendCxData();
                            sd.init(s, 5, mc.outBuf);
                            Thread senThread = new Thread(sd);
                            senThread.start();
                        }
                    }
                } else if ("hlw".equalsIgnoreCase(busiData)) {
                    log.info("new the  send thread for the hlw and cdr ");
                    for (MatchControler mc : busiControlerMap.get(busiTypeInstance)) {//实际只有一个MatchControler，最终的效果是只有一个发送线程5i///////////////////////////////////////////////////////////////、                                   '                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              /////                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     '''''''''                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        for (int i = 0; i < 4; i++) {
                            SendHlwData ssd = new SendHlwData(s, mc.resultSchemaName, Matcher.sendBatchSizeNum, mc.outBuf);
                            Thread senThread = new Thread(ssd);
                            senThread.start();
                        }
                    }
                } else {
                    log.info("new the send thread for the general data and cdr");
                    for (MatchControler mc : busiControlerMap.get(busiTypeInstance)) {
                        for (int i = 0; i < 2; i++) {
                            SendUtilData sd = new SendUtilData(s, mc.resultSchemaName, Matcher.sendBatchSizeNum, mc.outBuf);
                            Thread senThread = new Thread(sd);
                            senThread.start();
                        }
                    }
                }
            }
        }

        log.info("start init the match http receive data server ...");
        try {
            HttpGetDataServer.init();
            HttpGetDataServer.startup();
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
}