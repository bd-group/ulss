/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.MetaStoreClientPool;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CreateOperation;
import org.apache.hadoop.hive.metastore.api.CreatePolicy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.http.HttpResponse;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class GetFileFromMetaStore {

    String keyinterval = null;
    RNode node = null;
    Rule rule = null;
    String topic = null;
    String serviceName = null;
    String partT = null;
    String keywords = null;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd:HH");
    private static final int METASTORE_RETRY_INIT = 2000;
    private static final int ZOOKEEPER_RETRY_INIT = 2000;
    public static AtomicLong HANDLER_CLIENT_SIZE = new AtomicLong(0);
//    private static final Long HANDLER_CLIENT_SIZE_MAX = 20L;
    private static final int CREATEFILE_RETRY_INIT = 2000;
    private static MetaStoreClientPool mscp = (MetaStoreClientPool) RuntimeEnv.getParam(GlobalVariables.METASTORE_CLIENT_POOL);
    private static ConcurrentHashMap<String, Object[]> valueToFile = (ConcurrentHashMap<String, Object[]>) RuntimeEnv.getParam(GlobalVariables.VALUE_TO_FILE);
    int attempSize = 1;
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(GetFileFromMetaStore.class.getName());
    }

    public GetFileFromMetaStore(String keyinterval, RNode node, Rule rule) {
        this.keyinterval = keyinterval;
        this.node = node;
        this.rule = rule;
        this.topic = rule.getTopic();
        this.serviceName = rule.getServiceName();
        this.partT = node.getPartType();
        this.keywords = node.getKeywords();

    }

    /**
     *
     * check and get a file for the invervla ,if metastore has no such file ,
     * create a new one
     */
    void getFileForInverval() {
        String sendIP = "";
        Long f_id = 0L;
        String road = "";
//        while (true) {
//            if (HANDLER_CLIENT_SIZE.longValue() < HANDLER_CLIENT_SIZE_MAX) {
//                logger.info("the handler_client is smaller than the max size , then checkfileforinverval");
//                HANDLER_CLIENT_SIZE.addAndGet(1L);
        Object[] ob = new Object[3];

        String[] keytime = keyinterval.split("\\|");

        Long stime = 0L;
        Long etime = 0L;

        try {
            Date ds = dateFormat.parse(keytime[0]);
            Date de = dateFormat.parse(keytime[1]);
            stime = ds.getTime() / 1000;
            etime = de.getTime() / 1000;
        } catch (ParseException ex) {
            logger.info(ex, ex);
        } catch (Exception ex) {
            logger.error(ex, ex);
        }

        String[] pa = partT.split("\\|");
        List<SplitValue> list = new ArrayList<SplitValue>();
        for (int j = 1; j <= 2; j++) { //设置一级和二级划分值
            if (j == 1) {
                SplitValue sv1 = new SplitValue();
                sv1.setVerison(Long.parseLong(pa[pa.length - 1]));
                sv1.setLevel(j);//如果是interval分区，设置两个特征值，一个是上限一个是下限
                sv1.setValue("" + stime);
                sv1.setSplitKeyName(keywords.split("\\|")[0]);
                list.add(sv1);

                SplitValue sv2 = new SplitValue();
                sv2.setVerison(Long.parseLong(pa[pa.length - 1]));
                sv2.setLevel(j);//如果是interval分区，设置两个特征值，一个是上限一个是下限
                sv2.setValue("" + etime);
                sv2.setSplitKeyName(keywords.split("\\|")[0]);
                list.add(sv2);
            } else if (j == 2) {
                SplitValue sv = new SplitValue();
                sv.setVerison(Long.parseLong(pa[pa.length - 1]));
                sv.setLevel(j);
                sv.setValue(node.getHashNum() + "-" + node.getName());//设置哈希键值
                sv.setSplitKeyName(keywords.split("\\|")[1]);
                list.add(sv);
            }
        }

        ob = getUsefulFileFromMetaDB(list);

        sendIP = (String) ob[0];
        f_id = (Long) ob[1];
        road = (String) ob[2];

        if (sendIP == null || f_id == 0L || road == null || sendIP.equals("") || road.equals("") || sendIP == "" || road == "") {
            logger.info("can't get aomlsf , need to get zk for " + topic + " " + keyinterval + " " + node.getName());
            sendIP = "";
            f_id = 0L;
            road = "";
            ob[0] = "";
            ob[1] = 0L;
            ob[2] = "";

            String zkCluster = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);
            int zkSessionTimeout = 30000;
            ZkClient zk = new ZkClient(zkCluster, zkSessionTimeout);
            int ZKretryInterval = ZOOKEEPER_RETRY_INIT;
            int ZKretryAttempt = 0;

            while (true) {
                if (!zk.exists("/ulss")) {
                    try {
                        zk.createPersistent("/ulss");
                        logger.info("/ulss created");
                    } catch (Exception e) {
                        logger.info("root exists : other master has created the /ulss" + e, e);
                    }
                }

                if (!zk.exists("/ulss/redistribution")) {
                    try {
                        zk.createPersistent("/ulss/redistribution");
                        logger.info("/ulss/redistribution created");
                    } catch (Exception e) {
                        logger.info("root exists : other master has created the /ulss/redistribution" + e, e);
                    }
                }

                if (!zk.exists("/ulss/redistribution/" + topic + keyinterval + "lock")) {
                    try {
                        zk.createEphemeral("/ulss/redistribution/" + topic + keyinterval + "lock", topic + keyinterval + node.getName());
                        logger.info("new lock " + topic + keyinterval + "lock" + " is created");
                    } catch (Exception e) {
                        try {
                            Thread.sleep(2000);
                        } catch (Exception ex) {
                            logger.info(ex, ex);
                        }
                        continue;
                    }

                    ob = getFileFromMetaDB(list);
                    sendIP = (String) ob[0];
                    f_id = (Long) ob[1];
                    road = (String) ob[2];

                    if (sendIP == null || f_id == 0L || road == null || sendIP.equals("") || road.equals("")) {
                        sendIP = "";
                        f_id = 0L;
                        road = "";
                        ob[0] = "";
                        ob[1] = 0L;
                        ob[2] = "";

                        ob = createNewFile(list);
                        if ("".equals((String) ob[0]) && (Long) ob[1] == 0L && "".equals((String) ob[2])) {
                            continue;
                        } else {
                            sendIP = (String) ob[0];
                            f_id = (Long) ob[1];
                            road = (String) ob[2];
                            break;
                        }
                    } else {
                        logger.debug("choose the file " + f_id + " to the road " + road + " to the sendIP " + sendIP);
                        break;
                    }
                } else {
                    logger.debug("the lock is exists");
                    if (ZKretryInterval < 30000) {
                        ZKretryInterval = ZKretryInterval + ZOOKEEPER_RETRY_INIT;
                    } else {
                        ZKretryInterval = 30000;
                    }
                    logger.info("get the zoookeeper lock " + String.format("On retry attempt %d . Sleeping %d seconds.", ++ZKretryAttempt, ZKretryInterval / 1000));
                    try {
                        Thread.sleep(ZKretryInterval);
                    } catch (Exception ex) {
                        logger.error(ex, ex);
                    }
                }
            }
            zk.close();
            logger.debug("disconnect the zookeeper");
        } else {
            logger.info("choose the file " + f_id + " to the road " + road + " to the sendIP " + sendIP);
        }

        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_VALUE_TO_FILE)) {
            valueToFile.put(topic + keyinterval + node.getName(), ob);
            logger.info("choose the " + topic + " " + keyinterval + " " + node.getName() + " " + ob[0] + " " + ob[1] + " " + ob[2]);
        }

//                HANDLER_CLIENT_SIZE.decrementAndGet();
//                break;
//            } else {
//                logger.info("Handler Client for get file Size is larger than " + HANDLER_CLIENT_SIZE_MAX);
//                try {
//                    Thread.sleep(5000);
//                } catch (InterruptedException ex) {
//                    logger.error(ex, ex);
//                }
//            }
//        }
    }

    /**
     *
     * get file from the metastore before get the zk lock
     */
    private Object[] getUsefulFileFromMetaDB(List<SplitValue> list) {
        Date date = new Date();
        String time = dateFormat2.format(date);
        logger.info(time + " start get a useful file from metadb for " + topic + " " + partT.split("\\|")[0] + " " + partT.split("\\|")[1] + " " + keyinterval);
        Object[] ob = new Object[3];
        String getsendIP = "";
        Long getf_id = 0L;
        String getroad = "";

        MetaStoreClientPool.MetaStoreClient cli = mscp.getClient();
        try {
            IMetaStoreClient icli = cli.getHiveClient();
            List<SFile> lsf = null;

            int attemp = 0;
            while (attemp <= attempSize) {
                try {
                    lsf = icli.filterTableFiles(partT.split("\\|")[0], partT.split("\\|")[1], list);
                    logger.info("get the filelist for " + topic + " " + partT.split("\\|")[0] + " " + partT.split("\\|")[1] + " " + keyinterval + " " + lsf);
                    break;
                } catch (Exception ex) {
                    logger.error("can not get the SFileList for " + topic + " " + partT.split("\\|")[0] + " " + partT.split("\\|")[1] + " " + keyinterval + ex, ex);
                    rcmetastore(icli);
                    attemp++;
//                    try {
//                        Thread.sleep(2000);
//                    } catch (Exception e) {
//                    }
                }
            }

            if (lsf != null && !lsf.isEmpty()) {
                List<SFile> alsf = new ArrayList<SFile>();
                for (SFile s : lsf) {
                    if (s.getLoad_status() == MetaStoreConst.MFileLoadStatus.OK) {
                        alsf.add(s);
                    }
                }

                List<SFile> aolsf = new ArrayList<SFile>();
                if (!alsf.isEmpty()) {
                    logger.info("the alsf for " + topic + " " + keyinterval + " " + node.getName() + " has " + alsf.size() + " available file");
                    for (SFile s : alsf) {
                        if (s.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                            aolsf.add(s);
                        }
                    }

                    SFile aomlsf = null;

                    if (!aolsf.isEmpty()) {
                        aomlsf = new SFile();
                        aomlsf.setLength(Long.MAX_VALUE);
                        logger.info("the aolsf for " + topic + " " + keyinterval + " " + node.getName() + " has " + aolsf.size() + " available and open file");
                        for (SFile s : aolsf) {
                            if (s.getLength() <= aomlsf.getLength()) {
                                aomlsf = s;
                            }
                        }

                        logger.info("the aomlsf for " + topic + " " + keyinterval + " " + node.getName() + " is " + aomlsf);

                        if (aomlsf.getLocations() == null) {
                            logger.info("the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " has no locations");
                        } else {
                            int visit = -1;
                            for (int i = 0; i < aomlsf.getLocations().size(); i++) {
                                logger.debug(aomlsf.getLocations().get(i).getVisit_status());
                                if (aomlsf.getLocations().get(i).getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                                    visit = i;
                                }
                            }

                            if (visit == -1) {
                                logger.info("the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " has no location is online");
                            } else {
                                try {
                                    getsendIP = aomlsf.getLocations().get(visit).getNode_name() + rule.getIPList()[0];
                                    //sendIP = "192.168.1." + aomlsf.getLocations().get(visit).getNode_name().substring(4) + rule.getIPList()[0];
                                    getf_id = aomlsf.getFid();
                                    getroad = aomlsf.getLocations().get(visit).getLocation() + "|" + aomlsf.getLocations().get(visit).getDevid();
                                    if (getsendIP == null || getf_id == 0L || getroad == null || getsendIP.equals("") || getroad.equals("") || getsendIP == "" || getroad == "") {
                                        logger.info("there is unvaid information in the file " + aomlsf + " for the topic " + topic + " " + keyinterval + " " + node.getName() + " to the road " + getroad + " to the IP " + getsendIP);
                                        setBad(icli, aomlsf);
                                        aolsf.remove(aomlsf);
                                        alsf.remove(aomlsf);
                                    } else {
                                        logger.info("choose the file " + aomlsf + " for the topic " + topic + " " + keyinterval + " " + node.getName() + " to the road " + getroad + " to the sendIP " + getsendIP);
                                    }
                                } catch (Exception e) {
                                    logger.info("there is unvaid information in the file " + aomlsf + " for the topic " + topic + " " + keyinterval + " " + node.getName() + " to the road " + getroad + " to the IP " + getsendIP);
                                }
                            }
                        }
                    } else {
                        logger.info(" the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " has no available and useful file");
                    }
                } else {
                    logger.info(" the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " has no available file");
                }
            } else {
                logger.info("the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " is empty");
            }
        } finally {
            cli.release();
        }

        ob[0] = getsendIP;
        ob[1] = getf_id;
        ob[2] = getroad;
        return ob;
    }

    /**
     *
     * get file from the metadb after get the zk lock
     */
    private Object[] getFileFromMetaDB(List<SplitValue> list) {
        logger.info("after get zk , get or reopen a file for " + topic + " " + keyinterval + " " + node.getName() + " from the metadb!");
        Object[] ob = new Object[3];
        String getsendIP = "";
        Long getf_id = 0L;
        String getroad = "";

        MetaStoreClientPool.MetaStoreClient cli = mscp.getClient();
        try {
            IMetaStoreClient icli = cli.getHiveClient();
            List<SFile> lsf = null;
            int attemp = 0;
            while (attemp <= attempSize) {
                try {
                    lsf = icli.filterTableFiles(partT.split("\\|")[0], partT.split("\\|")[1], list);
                    logger.info("get the filelist for " + topic + " " + partT.split("\\|")[0] + " " + partT.split("\\|")[1] + " " + keyinterval + " " + lsf);
                    break;
                } catch (Exception ex) {
                    logger.error("can not get the SFileList for " + topic + " " + partT.split("\\|")[0] + " " + partT.split("\\|")[1] + " " + keyinterval + ex, ex);
                    rcmetastore(icli);
                    attemp++;
                }
//                try {
//                    Thread.sleep(1000);
//                } catch (Exception e) {
//                }
            }

            if (lsf != null && !lsf.isEmpty()) {
                List<SFile> alsf = new ArrayList<SFile>();
                for (SFile s : lsf) {
                    if (s.getLoad_status() == MetaStoreConst.MFileLoadStatus.OK) {
                        alsf.add(s);
                    }
                }

                List<SFile> aolsf = new ArrayList<SFile>();
                if (!alsf.isEmpty()) {
                    logger.info("the alsf for " + topic + " " + keyinterval + " " + node.getName() + " has " + alsf.size() + " available file");
                    for (SFile s : alsf) {
                        if (s.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                            aolsf.add(s);
                        }
                    }

                    SFile aomlsf = null;

                    while (!aolsf.isEmpty()) {
                        aomlsf = new SFile();
                        aomlsf.setLength(Long.MAX_VALUE);
                        logger.info("the aolsf for " + topic + " " + keyinterval + " " + node.getName() + " has " + aolsf.size() + " available and open file");
                        for (SFile s : aolsf) {
                            if (s.getLength() <= aomlsf.getLength()) {
                                aomlsf = s;
                            }
                        }

                        if (aomlsf.getLocations() == null) {
                            logger.info("the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " has no locations");
                            setBad(icli, aomlsf);
                            aolsf.remove(aomlsf);
                            alsf.remove(aomlsf);
                            continue;
                        }

                        int visit = -1;

                        for (int i = 0; i < aomlsf.getLocations().size(); i++) {
                            if (aomlsf.getLocations().get(i).getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                                visit = i;
                            }
                        }

                        if (visit == -1) {
                            logger.info("the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " has no locations");
                            setBad(icli, aomlsf);
                            aolsf.remove(aomlsf);
                            alsf.remove(aomlsf);
                        } else {
                            try {
                                getsendIP = aomlsf.getLocations().get(visit).getNode_name() + rule.getIPList()[0];
                                //sendIP = "192.168.1." + aomlsf.getLocations().get(visit).getNode_name().substring(4) + rule.getIPList()[0];
                                getf_id = aomlsf.getFid();
                                getroad = aomlsf.getLocations().get(visit).getLocation() + "|" + aomlsf.getLocations().get(visit).getDevid();
                                if (getsendIP == null || getf_id == 0L || getroad == null || getsendIP.equals("") || getroad.equals("") || getsendIP == "" || getroad == "") {
                                    logger.info("there is unvaid information in the file " + aomlsf + " for the topic " + topic + " to the road " + getroad + " to the IP " + getsendIP);
                                    setBad(icli, aomlsf);
                                    aolsf.remove(aomlsf);
                                    alsf.remove(aomlsf);
                                } else {
                                    logger.info("choose the file " + aomlsf + " for the topic " + topic + " " + keyinterval + " " + node.getName() + " to the road " + getroad + " to the sendIP " + getsendIP);
                                    break;
                                }
                            } catch (Exception e) {
                                logger.info("there is no online location in the file " + aomlsf + " for the topic " + topic + " to the road " + getroad + " to the IP " + getsendIP);
                                setBad(icli, aomlsf);
                                aolsf.remove(aomlsf);
                                alsf.remove(aomlsf);
                            }
                        }
                    }

                    if (getsendIP == null || getf_id == 0L || getroad == null || getsendIP.equals("") || getroad.equals("") || getsendIP == "" || getroad == "") {
                        logger.info("the SFileList for " + topic + " " + keyinterval + " " + node.getName() + "has no available and open and useful file");
                        getsendIP = "";
                        getf_id = 0L;
                        getroad = "";

                        for (SFile s : alsf) {
                            if (s.getLoad_status() == MetaStoreConst.MFileLoadStatus.BAD) {
                                alsf.remove(s);
                            }
                        }

                        while (alsf.size() > 1) {
                            logger.info("the alsf for " + topic + " " + keyinterval + " " + node.getName() + "has " + alsf.size() + " available and not open file");
                            aomlsf = new SFile();
                            aomlsf.setLength(Long.MAX_VALUE);

                            for (SFile s : alsf) {
                                if (s.getLength() < aomlsf.getLength()) {
                                    aomlsf = s;
                                }
                            }

                            attemp = 0;
                            while (attemp <= attempSize) {
                                Date date = new Date();
                                String time = dateFormat2.format(date);
                                try {
                                    if (icli.reopen_file(aomlsf.getFid())) {
                                        Long oldf_id = aomlsf.getFid();
                                        int attemp2 = 0;
                                        while (attemp2 <= attempSize) {
                                            try {
                                                aomlsf = icli.get_file_by_id(oldf_id);
                                                break;
                                            } catch (Exception ex) {
                                                logger.error("can not get the file " + oldf_id);
                                                rcmetastore(icli);
                                                attemp++;
//                                                try {
//                                                    Thread.sleep(2000);
//                                                } catch (Exception e1) {
//                                                }
                                            }
                                        }
                                        logger.info(time + " reopen the file for " + topic + " " + keyinterval + " " + node.getName() + " " + aomlsf + " successfully");
                                        break;
                                    } else {
                                        logger.info(time + " cannotreopen the file for " + topic + " " + keyinterval + " " + node.getName() + " " + aomlsf);
                                        attemp++;
                                    }
                                } catch (Exception ex) {
                                    logger.error(time + " cannotreopen the file for " + topic + " " + keyinterval + " " + node.getName() + " " + aomlsf + ex, ex);
                                    rcmetastore(icli);
                                    attemp++;
                                }
//                                try {
//                                    Thread.sleep(2000);
//                                } catch (Exception e) {
//                                }
                            }

                            if (aomlsf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                                Date date = new Date();
                                String time = dateFormat2.format(date);
                                logger.info( time + " the file for " + topic + " " + keyinterval + " " + node.getName() + " has been reopened " + aomlsf);

                                int visit = -1;
                                if (aomlsf.getLocations() == null) {
                                    logger.info("the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " has no locations");
                                    setBad(icli, aomlsf);
                                    alsf.remove(aomlsf);
                                } else {
                                    for (int i = 0; i < aomlsf.getLocations().size(); i++) {
                                        logger.debug(aomlsf.getLocations().get(i).getVisit_status());
                                        if (aomlsf.getLocations().get(i).getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                                            visit = i;
                                        }
                                    }

                                    if (visit == -1) {
                                        logger.info("the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " has no location is online");
                                        setBad(icli, aomlsf);
                                        alsf.remove(aomlsf);
                                    } else {
                                        try {
                                            getsendIP = aomlsf.getLocations().get(visit).getNode_name() + rule.getIPList()[0];
                                            //sendIP = "192.168.1." + aomlsf.getLocations().get(visit).getNode_name().substring(4) + rule.getIPList()[0];
                                            getf_id = aomlsf.getFid();
                                            getroad = aomlsf.getLocations().get(visit).getLocation() + "|" + aomlsf.getLocations().get(visit).getDevid();
                                            if (getsendIP == null || getf_id == 0L || getroad == null || getsendIP.equals("") || getroad.equals("") || getsendIP == "" || getroad == "") {
                                                getsendIP = "";
                                                getf_id = 0L;
                                                getroad = "";
                                                logger.info("there is unvaid information in the file " + aomlsf + " for the topic " + topic + " " + keyinterval + " " + node.getName() + " to the road " + getroad + " to the IP " + getsendIP);
                                                setBad(icli, aomlsf);
                                                alsf.remove(aomlsf);
                                            } else {
                                                logger.info("choose the file " + aomlsf + " for the topic " + topic + " " + keyinterval + " " + node.getName() + " to the road " + getroad + " to the sendIP " + getsendIP);
                                                break;
                                            }
                                        } catch (Exception e) {
                                            getsendIP = "";
                                            getf_id = 0L;
                                            getroad = "";
                                            logger.info("there is unvaid information in the file " + aomlsf + " for the topic " + topic + " " + keyinterval + " " + node.getName() + " to the road " + getroad + " to the IP " + getsendIP);
                                            setBad(icli, aomlsf);
                                            alsf.remove(aomlsf);
                                        }
                                    }
                                }
                            } else {
                                alsf.remove(aomlsf);
                                logger.info("the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " has not been reopened");
                            }
                        }
                    } else {
                        logger.debug("choose the file " + aomlsf + " to the road " + getroad + " to the sendIP " + getsendIP);
                    }
                } else {
                    logger.info(" the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " has no available file");
                }
            } else {
                logger.info("the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " is empty");
            }
        } finally {
            cli.release();
        }

        ob[0] = getsendIP;
        ob[1] = getf_id;
        ob[2] = getroad;
        return ob;
    }

    /**
     *
     * create a new file by the interval
     *
     */
    private Object[] createNewFile(List<SplitValue> list) {
        String sendIP = "";
        Long f_id = 0L;
        String road = "";
        Object[] ob = new Object[3];
        ob[0] = "";
        ob[1] = 0L;
        ob[2] = "";

        CreatePolicy cp = new CreatePolicy();
        //cp.setOperation(CreateOperation.CREATE_NEW);
        cp.setOperation(CreateOperation.CREATE_NEW_RANDOM);

        MetaStoreClientPool.MetaStoreClient cli = mscp.getClient();
        SFile sf2 = null;
        int attemp = 0;
        try {
            IMetaStoreClient icli = cli.getHiveClient();

            List<NodeGroup> lng = null;
            attemp = 0;
            while (attemp <= attempSize) {
                try {
                    lng = icli.getTableNodeGroups(partT.split("\\|")[0], partT.split("\\|")[1]);
                    break;
                } catch (Exception ex) {
                    logger.error("can not get the List<nodeGroup>" + ex, ex);
                    rcmetastore(icli);
                    attemp++;
                }
//                try {
//                    Thread.sleep(500);
//                } catch (Exception e) {
//                }
            }

            ArrayList<String> nodeNames = new ArrayList<String>();
            if (lng != null && !lng.isEmpty()) {
                for (NodeGroup ng : lng) {
                    for (Node n : ng.getNodes()) {
                        nodeNames.add(n.getNode_name());
                    }
                }
            }

            int CFretryInterval = CREATEFILE_RETRY_INIT;
            int CFretryAttempt = 0;

            while (true) {
                try {
                    Date date = new Date();
                    String time = dateFormat2.format(date);
                    logger.info(time + " begin to create file for " + topic + " " + keyinterval + " " + node.getName() + " " + partT.split("\\|")[0] + " " + partT.split("\\|")[1]);
                    sf2 = icli.create_file_by_policy(cp, 2, partT.split("\\|")[0], partT.split("\\|")[1], list);
                    if (sf2 == null) {
                        if (CFretryInterval < 30000) {
                            CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                        } else {
                            CFretryInterval = 30000;
                        }
                        logger.info("create file for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
                        try {
                            Thread.sleep(CFretryInterval);
                        } catch (Exception ex) {
                            logger.error(ex, ex);
                        }
                        continue;
                    }
                    logger.info( time + " create file from metastore successfully for " + topic + " " + keyinterval + " " + node.getName());
                    String nodeN = sf2.getLocations().get(0).getNode_name();
                    if (nodeN == null) {
                        if (nodeNames.isEmpty()) {
                            logger.info("thres is no node in the nodeGroups for " + partT.split("\\|")[0] + partT.split("\\|")[1]);
                            try {
                                Thread.sleep(2000);
                            } catch (Exception ex) {
                                logger.error(ex, ex);
                            }
                            continue;
                        } else {
                            Random r = new Random();
                            int ran = r.nextInt(nodeNames.size());
                            if (nodeNames.get(ran) == null || nodeNames.get(ran).isEmpty()) {
                                logger.info("thres is null node in the nodeGroups for " + partT.split("\\|")[0] + partT.split("\\|")[1]);
                                try {
                                    Thread.sleep(2000);
                                } catch (Exception ex) {
                                    logger.error(ex, ex);
                                }
                                continue;
                            }
                            sendIP = nodeNames.get(ran) + rule.getIPList()[0];
                        }
                    } else {
                        sendIP = nodeN + rule.getIPList()[0];
                    }
                    f_id = sf2.getFid();
                    road = sf2.getLocations().get(0).getLocation() + "|" + sf2.getLocations().get(0).getDevid();

                    if (road == null) {
                        sendIP = "";
                        f_id = 0L;
                        road = "";
                        logger.info("road is null for " + sf2 + topic + " " + keyinterval + " " + node.getName());
                        try {
                            Thread.sleep(2000);
                        } catch (Exception ex) {
                            logger.error(ex, ex);
                        }
                        continue;
                    }

                    SendCreateFileCMD scfc = new SendCreateFileCMD(sendIP, f_id, road);
                    HttpResponse hp = scfc.send();
                    if (hp == null) {
                        logger.error("the file  for " + topic + " " + keyinterval + " " + node.getName() + " " + sf2 + " can not be set online");
                        sendIP = "";
                        f_id = 0L;
                        road = "";
                        setBad(icli, sf2);

                        if (CFretryInterval < 30000) {
                            CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                        } else {
                            CFretryInterval = 30000;
                        }
                        logger.info("create file for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
                        try {
                            Thread.sleep(CFretryInterval);
                        } catch (Exception ex) {
                            logger.error(ex, ex);
                        }

                        continue;
                    } else {
                        logger.debug("sendCreateFileCMD : " + hp.getStatusLine());
                        if (hp.getStatusLine().getStatusCode() != 200) {
                            logger.error("the file  for " + topic + " " + keyinterval + " " + node.getName() + " " + sf2 + " can not be set online");
                            sendIP = "";
                            f_id = 0L;
                            road = "";
                            setBad(icli, sf2);

                            if (CFretryInterval < 30000) {
                                CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                            } else {
                                CFretryInterval = 30000;
                            }
                            logger.info("create file for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
                            try {
                                Thread.sleep(CFretryInterval);
                            } catch (Exception ex) {
                                logger.error(ex, ex);
                            }

                            continue;
                        } else {
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            try {
                                hp.getEntity().writeTo(out);
                            } catch (Exception ex) {
                                logger.error("the file  for " + topic + " " + keyinterval + " " + node.getName() + " " + sf2 + " can not be set online" + hp.getStatusLine() + ex, ex);
                                sendIP = "";
                                f_id = 0L;
                                road = "";
                                setBad(icli, sf2);
                                if (CFretryInterval < 30000) {
                                    CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                                } else {
                                    CFretryInterval = 30000;
                                }
                                logger.info("create file for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));

                                try {
                                    Thread.sleep(CFretryInterval);
                                } catch (Exception ex2) {
                                    logger.error(ex2, ex2);
                                }
                                continue;
                            }

                            String resonseEn = new String(out.toByteArray());
                            if ("-1".equals(resonseEn.split("[\n]")[0])) {
                                logger.info(resonseEn.split("[\n]")[1]);
                                logger.error("the file  for " + topic + " " + keyinterval + " " + node.getName() + " " + sf2 + " can not be set online" + hp.getStatusLine());
                                sendIP = "";
                                f_id = 0L;
                                road = "";

                                setBad(icli, sf2);

                                if (CFretryInterval < 30000) {
                                    CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                                } else {
                                    CFretryInterval = 30000;
                                }
                                logger.info("create file for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
                                try {
                                    Thread.sleep(CFretryInterval);
                                } catch (Exception ex) {
                                    logger.error(ex, ex);
                                }

                                continue;
                            } else {
                                logger.info( time + " this file " + f_id + " " + topic + " " + keyinterval + " " + node.getName() + " " + road + " " + sendIP + " has been set online");
                                ob[0] = sendIP;
                                ob[1] = f_id;
                                ob[2] = road;
                                break;
                            }
                        }
                    }
                } catch (Exception ex) {
                    logger.error("cannot create the file for " + topic + " " + keyinterval + " " + node.getName() + ex, ex);
                    rcmetastore(icli);
                    if (CFretryInterval < 30000) {
                        CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                    } else {
                        CFretryInterval = 30000;
                    }
                    logger.info("create file for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
                    try {
                        Thread.sleep(CFretryInterval);
                    } catch (Exception ex1) {
                        logger.error(ex1, ex1);
                    }
                    continue;
                }
            }
            logger.info("new useful file for " + topic + " " + keyinterval + " " + node.getName() + " has be created");
        } finally {
            cli.release();
        }
        return ob;
    }

    /**
     *
     * set the file bad , it means this file will not be used again
     *
     */
    private void setBad(IMetaStoreClient icli, SFile sf) {
        int attemp = 0;
        while (attemp <= attempSize) {
            try {
                icli.set_loadstatus_bad(sf.getFid());
                Date date = new Date();
                String time = dateFormat2.format(date);
                logger.info(time + " has set bad the file " + sf);
                break;
            } catch (Exception ex) {
                logger.error("can not set bad the file " + sf + " " + ex, ex);
                rcmetastore(icli);
                attemp++;
            }
        }
    }

    /**
     *
     * reconnect the metastore until it is connected
     */
    private void rcmetastore(IMetaStoreClient icli) {
        int MSretryInterval = METASTORE_RETRY_INIT;
        int MSretryAttempt = 0;
        while (true) {
            try {
                icli.reconnect();
                break;
            } catch (MetaException ex) {
                if (MSretryInterval <= 30000) {
                    MSretryInterval = MSretryInterval + METASTORE_RETRY_INIT;
                } else {
                    MSretryInterval = 30000;
                }

                logger.info("reconnect to the metastore " + String.format("On retry attempt %d . Sleeping %d seconds.", ++MSretryAttempt, MSretryInterval / 1000));
                try {
                    Thread.sleep(MSretryInterval);
                } catch (InterruptedException ie) {
                    logger.info(ie, ie);
                }
            }
        }
    }
}
