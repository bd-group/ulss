/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.iie.match.handler;

import cn.iie.struct.RecordNode;
import cn.iie.util.Configure;
import cn.iie.util.MatchMeta;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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

//比对的规则格式为:数据中心名|表名|消息队列名|单个小窗口的大小|每个大窗口有几个小的窗口(时间长短，毫秒)|
//单个小窗口的大小*每个大窗口有几个小的窗口，就是整个时间滑动窗口的大小
public class Matcher {

    public static Logger log = Logger.getLogger(Matcher.class.getName());
    public static String docs_set_name = "doc_set";
    public static String docsName = "docs";
    /**/
    public static String hostName = "";
    public static String cachePath = "";
    public static String bustype = "";
    /**/
    public static String zkUrl = "";
    public static String offsetZkUrl = "";
    public static String metaStoreCient = "";
    public static String metaStoreCientUrl = "";
    public static int metaStoreCientPort = 0;
    /**/
    public static ZkClient zc;
    /**/
    public static int maxStreamNum = 2;
    /**/
    public static MatchMeta meta = new MatchMeta();
    /**/
    public static int httpPoolNum = 30;
    public static int matchThreadNum = 1;
    public static int batchSizeNum = 1000;
    public static int inbufferSize = 20000;
    public static int outbufferSize = 40000;
    public static int topMapSize = 32;
    public static int maxProcessNumPerMillis = 20;
    /**/
    public static int qd_szs = -1;
    /**/
    public static final Object c = new Object();
    public static ConcurrentHashMap<String, MatchControler> schema2MatchControler = new ConcurrentHashMap<String, MatchControler>();
    public static ConcurrentHashMap<String, MatchWorker> schema2Matchworker = new ConcurrentHashMap<String, MatchWorker>();
    public static ConcurrentHashMap<String, ReadWriteLock> schema2ReadWriteLock = new ConcurrentHashMap<String, ReadWriteLock>();
    public static ConcurrentHashMap<String, Schema> schemaname2Schema = new ConcurrentHashMap<String, Schema>();
    /**/
    public static ConcurrentHashMap<String, String> schemaname2Tablename = new ConcurrentHashMap<String, String>(10, 0.8f);  //schema的名字到name到table对象的映射
    public static ConcurrentHashMap<String, String> tablename2Schemaname = new ConcurrentHashMap<String, String>(10, 0.8f);  //schema的名字到name到table对象的映射
    public static ConcurrentHashMap<String, String> schemaname2Metaq = new ConcurrentHashMap<String, String>();
    public static ConcurrentHashMap<String, String> metaq2schemaname = new ConcurrentHashMap<String, String>();
    public static ConcurrentHashMap<String, DatumReader<GenericRecord>> schema2Reader;
    /**/
    public static ConcurrentHashMap<String, AtomicLong> schemaname2Total = new ConcurrentHashMap<String, AtomicLong>();
    public static ConcurrentHashMap<String, AtomicLong> schemaname2Sendtotal = new ConcurrentHashMap<String, AtomicLong>();
    /**/
    public static String docsSchemaContent = null;
    public static Schema docsSchema = null;
    public static DatumReader<GenericRecord> docsReader = null;

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
        log.info("start init all the schama \n ");
        List<List<String>> schema2tableList = Matcher.meta.getSchema2table();
        for (int i = 0; i < schema2tableList.size(); i++) {
            Matcher.schemaname2Tablename.put(schema2tableList.get(i).get(0).toLowerCase(), schema2tableList.get(i).get(1).toLowerCase());
            Matcher.tablename2Schemaname.put(schema2tableList.get(i).get(1).toLowerCase(), schema2tableList.get(i).get(0).toLowerCase());
        }
        Matcher.schemaname2Schema = new ConcurrentHashMap<String, Schema>();
        HttpDataHandler.schema2Matchworker = new ConcurrentHashMap<String, MatchWorker>();
        Matcher.schema2Matchworker = new ConcurrentHashMap<String, MatchWorker>();
        Matcher.schema2Reader = new ConcurrentHashMap<String, DatumReader<GenericRecord>>();

        String schemaName = "";
        String schemaContent = "";
        try {
            log.info("getting data schema and metaq from metadb...");
            Matcher.docsSchemaContent = Matcher.meta.getDocSchema();
            List<List<String>> allSchema = meta.getAllSchema();
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
                    String mq = list.get(2);
                    Protocol protocol = Protocol.parse(schemaContent);
                    Schema schema = protocol.getType(schemaName);
                    DatumReader<GenericRecord> schemaReader = new GenericDatumReader<GenericRecord>(schema);

                    log.debug("put schemaReader、mq for " + schemaName + " to map");
                    Matcher.metaq2schemaname.put(mq, schemaName);
                    Matcher.schemaname2Metaq.put(schemaName, mq);
                    Matcher.schemaname2Schema.put(schemaName, schema);
                    Matcher.schema2Reader.put(schemaName, schemaReader);

                    Matcher.schemaname2Total.put(schemaName, new AtomicLong(0));
                    Matcher.schemaname2Sendtotal.put(schemaName, new AtomicLong(0));

                    if (Matcher.schema2Matchworker.containsKey(schemaName)) {  //将需要匹配的数据的worker缓存起来，不需要匹配的数据的reader就不需要缓存
                        HttpDataHandler.schema2Matchworker.put(schemaName, Matcher.schema2Matchworker.get(schemaName));
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

    public static List<MatchControler> initWorkers(String bustype) {
        log.info("start init all the match workers \n");
        List<MatchControler> ls = new ArrayList<MatchControler>();
        List<List<String>> rules = Matcher.meta.getRules(bustype);
        for (List<String> lst : rules) {
            String[] ss = lst.get(0).split("[;]");
            String resultSchemaName = ss[2].toLowerCase();

            ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>>(1024 * 32, 0.8f);
            ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>> topMtMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, List<RecordNode>>>(1024 * 32, 0.8f);

            for (long i = 0; i < Matcher.topMapSize; i++) {
                ConcurrentHashMap<String, List<RecordNode>> mp = new ConcurrentHashMap<String, List<RecordNode>>(1024 * 32, 0.8f);
                topMap.put(i, mp);
                ConcurrentHashMap<String, List<RecordNode>> mtMap = new ConcurrentHashMap<String, List<RecordNode>>(1024 * 32, 0.8f);
                topMtMap.put(i, mtMap);
            }

            final Object matchContolLock = new Object();
            for (int i = 0; i < Matcher.maxStreamNum; i++) { //现在最多只支持两路数据的比对，超过了就无法保证正确性
                String[] paras = ss[i].split("[|]"); //得到规则，然后进行规则的解析，得到相应的参数;针对两种数据类型进行controler的创建，进行数据比对线程的创建，然后运行
                final ReadWriteLock rwLock = new ReentrantReadWriteLock();
                String schemaName = paras[0].toLowerCase();
                Matcher.schema2ReadWriteLock.put(schemaName, rwLock);
                String mSchemaName = paras[1].toLowerCase();
                int smWinSize = Integer.parseInt(paras[2]);
                int winNum = Integer.parseInt(paras[3]);
                String[] acJoinAttributes = paras[4].toLowerCase().split("[,]");

                String fJoinAttribute = paras[5].split("[,]")[0].toLowerCase();
                String mfJoinAttribute = paras[5].split("[,]")[1].toLowerCase();

                long maxDevi = Long.parseLong(paras[6]);
                int timeOutDeal = Integer.parseInt(paras[7]);
                if (i == 0) {
                    String[] resOwnAttributes = lst.get(1).split("[,]");
                    for (int j = 0; j < resOwnAttributes.length; j++) {
                        resOwnAttributes[j] = resOwnAttributes[j].toLowerCase();
                    }
                    String[] resOtherAttributes = lst.get(2).split("[,]");
                    for (int j = 0; j < resOtherAttributes.length; j++) {
                        resOtherAttributes[j] = resOtherAttributes[j].toLowerCase();
                    }

                    MatchControler mc = new MatchControler(schemaName, mSchemaName, resultSchemaName, smWinSize, winNum,
                            acJoinAttributes, fJoinAttribute, mfJoinAttribute, maxDevi,
                            resOwnAttributes, resOtherAttributes, timeOutDeal,
                            topMap, topMtMap, matchContolLock, rwLock.readLock());
                    Matcher.schema2MatchControler.put(schemaName, mc);
                    ls.add(mc);
                    Thread tc = new Thread(mc);
                    tc.start();
                } else {
                    String[] resOwnAttributes = lst.get(2).split("[,]");
                    for (int j = 0; j < resOwnAttributes.length; j++) {
                        resOwnAttributes[j] = resOwnAttributes[j].toLowerCase();
                    }
                    String[] resOtherAttributes = lst.get(1).split("[,]");
                    for (int j = 0; j < resOtherAttributes.length; j++) {
                        resOtherAttributes[j] = resOtherAttributes[j].toLowerCase();
                    }
                    MatchControler mc = new MatchControler(schemaName, mSchemaName, resultSchemaName, smWinSize, winNum,
                            acJoinAttributes, fJoinAttribute, mfJoinAttribute, maxDevi,
                            resOwnAttributes, resOtherAttributes, timeOutDeal,
                            topMtMap, topMap, matchContolLock, rwLock.readLock());
                    Matcher.schema2MatchControler.put(schemaName, mc);
                    ls.add(mc);
                    Thread tc = new Thread(mc);
                    tc.start();
                }
            }
        }
        return ls;
    }

    public static void main(String[] args) {
        Matcher.bustype = args[0];

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
        Matcher.batchSizeNum = cfg.getIntProperty("batchSendNum");
        Matcher.matchThreadNum = cfg.getIntProperty("matchThreadNum");
        Matcher.inbufferSize = cfg.getIntProperty("inbufferSize");
        Matcher.outbufferSize = cfg.getIntProperty("outbufferSize");
        Matcher.cachePath = cfg.getProperty("matchcachePath");
        Matcher.qd_szs = cfg.getIntProperty("qd_szs");
        HttpGetDataServer.poolNum = Matcher.httpPoolNum;
        Matcher.topMapSize = cfg.getIntProperty("topMapSize");
        Matcher.maxProcessNumPerMillis = cfg.getIntProperty("maxProcessNumPerMillis");

        Matcher.initSchema();
        List<MatchControler> ls = Matcher.initWorkers(Matcher.bustype);

        log.info("start init the sent thread ... \n");

        if ("dx".equalsIgnoreCase(Matcher.bustype)) {
            log.info("new the send thread for the dx and cdr");
            for (MatchControler mc : ls) {
                SendDxData ssd = new SendDxData();
                ssd.init(Matcher.batchSizeNum, mc.outBuf);
                Thread senThread = new Thread(ssd);
                senThread.start();
            }
        } else if ("cx".equalsIgnoreCase(Matcher.bustype)) {
            log.info("new the send thread for the cx and cdr");
            for (MatchControler mc : ls) {
                SendCxData sd = new SendCxData();
                sd.init(Matcher.batchSizeNum, mc.outBuf);
                Thread senThread = new Thread(sd);
                senThread.start();
            }
        } else if ("hlw".equalsIgnoreCase(Matcher.bustype)) {
            log.info("new the  send thread for the hlw and cdr");
            for (MatchControler mc : ls) {
                SendHlwData ssd = new SendHlwData(mc.resultSchemaName, Matcher.batchSizeNum, mc.outBuf);
                Thread senThread = new Thread(ssd);
                senThread.start();
            }
        } else {
            log.info("new the send thread for the general data and cdr");
            for (MatchControler mc : ls) {
                SendUtilData sd = new SendUtilData(mc.resultSchemaName, Matcher.batchSizeNum, mc.outBuf);
                Thread senThread = new Thread(sd);
                senThread.start();
            }
        }

        log.info("start init the match http receive data server ... \n");
        try {
            HttpGetDataServer.init();
            HttpGetDataServer.startup();
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
}