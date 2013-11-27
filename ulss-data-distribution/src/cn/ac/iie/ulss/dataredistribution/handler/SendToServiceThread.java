/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.MetaStoreClientPool;
import cn.ac.iie.ulss.dataredistribution.tools.MetaStoreClientPool.MetaStoreClient;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class SendToServiceThread implements Runnable {

    Long version = 0L;
    String sendIP = null;
    String topic = null;
    RNode node = null;
    String serviceName = null;
    byte[] sendData = null;
    Map<RNode, Object> sendRows = null;
    static org.apache.log4j.Logger logger = null;
    Rule rule = null;
    String keyinterval = null;
    Long f_id = 0L;
    String road = null;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    int attempSize = 2;
    private static final int METASTORE_RETRY_INIT = 2000;
    private static final int CREATEFILE_RETRY_INIT = 2000;
    private static final int ZOOKEEPER_RETRY_INIT = 2000;
    int count = 0;
    static long limit = 10000;
    static byte[] li = new byte[0];

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(SendToServiceThread.class.getName());
    }

    public SendToServiceThread(byte[] sendData, RNode node, String topic, String serviceName, Rule rule, String sendIP, String keyinterval, Long f_id, String road, int count, Long version) {
        this.sendData = sendData;
        this.node = node;
        this.topic = topic;
        this.serviceName = serviceName;
        this.rule = rule;
        this.sendIP = sendIP;
        this.keyinterval = keyinterval;
        this.f_id = f_id;
        this.road = road;
        this.count = count;
        this.version = version;
    }

    @Override
    public void run() {
//        ConcurrentHashMap<Node, Integer> nodeToThreadNum = (ConcurrentHashMap<Node, Integer>) RuntimeEnv.getParam("nodeToThreadNum");
//        synchronized (nodeToThreadNum) {
//            int num = nodeToThreadNum.get(node);
//            num++;
//            nodeToThreadNum.put(node, num);
//        }
        if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3) {
            sendToType0123();
        } else if (rule.getType() == 4) {
            sendToType4();
        }
//        synchronized (nodeToThreadNum) {
//            int num = nodeToThreadNum.get(node);
//            num--;
//            nodeToThreadNum.put(node, num);
//        }
    }

    /**
     *
     * get file from the metastore before get the zk lock
     */
    private Object[] getUsefulFileFromMetaDB(List<SplitValue> list) {
        Object[] ob = new Object[3];
        String getsendIP = "";
        Long getf_id = 0L;
        String getroad = "";

        String partType = rule.getPartType();
        MetaStoreClientPool mscp = (MetaStoreClientPool) RuntimeEnv.getParam(GlobalVariables.METASTORE_CLIENT_POOL);
        MetaStoreClient cli = mscp.getClient();
        try {
            IMetaStoreClient icli = cli.getHiveClient();
            List<SFile> lsf = null;

            int attemp = 0;
            while (attemp <= attempSize) {
                try {
                    lsf = icli.filterTableFiles(partType.split("\\|")[0], partType.split("\\|")[1], list);
                    logger.info("get the filelist for " + topic + " " + partType.split("\\|")[0] + " " + partType.split("\\|")[1] + " " + list + " " + lsf);
                    break;
                } catch (Exception ex) {
                    logger.error("can not get the SFileList " + ex, ex);
                    rcmetastore(icli);
                    attemp++;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
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
                    logger.info(" the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " has " + alsf.size() + " available file");
                    for (SFile s : alsf) {
                        if (s.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                            aolsf.add(s);
                        }
                    }

                    SFile aomlsf = null;

                    if (!aolsf.isEmpty()) {
                        aomlsf = new SFile();
                        aomlsf.setLength(Long.MAX_VALUE);
                        logger.info(" the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " has " + aolsf.size() + " available and open file");
                        for (SFile s : aolsf) {
                            if (s.getLength() <= aomlsf.getLength()) {
                                aomlsf = s;
                            }
                        }

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
                                logger.info("there is no location is online in the file " + aomlsf);
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
                                        logger.debug("this file " + aomlsf + " for the topic " + topic + " to the road " + getroad + " to the IP " + getsendIP + " has online location");
                                        logger.info("choose the file " + aomlsf + " to the road " + getroad + " to the sendIP " + getsendIP);
                                    }
                                } catch (Exception e) {
                                    logger.info("there is unvaid information in the file " + aomlsf + " for the topic " + topic + " to the road " + getroad + " to the IP " + getsendIP);
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
     * set the file bad when send message but get exception back
     */
    private void setBadFile(Long oldf_id) {
        MetaStoreClientPool mscp = (MetaStoreClientPool) RuntimeEnv.getParam(GlobalVariables.METASTORE_CLIENT_POOL);
        MetaStoreClient cli = mscp.getClient();
        try {
            IMetaStoreClient icli = cli.getHiveClient();
            int attemp = 0;
            while (attemp <= attempSize) {
                try {
                    //sf.setLoad_status(MetaStoreConst.MFileLoadStatus.BAD);
                    //icli.close_file(sf);
                    icli.set_loadstatus_bad(oldf_id);
                    logger.info("has set bad the file " + oldf_id + " successfully!");
                    break;
                } catch (Exception ex) {
                    logger.error("can not set bad the file " + oldf_id + " " + ex, ex);
                    rcmetastore(icli);
                    attemp++;
                }
                try {
                    Thread.sleep(2000);
                } catch (Exception e1) {
                }
            }
        } finally {
            cli.release();
        }
    }

    /**
     *
     * reconnect the metastore until it is connected
     */
    public synchronized void rcmetastore(IMetaStoreClient icli) {
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

                logger.info("metastore " + String.format("On retry attempt %d . Sleeping %d seconds.", ++MSretryAttempt, MSretryInterval / 1000));
                try {
                    Thread.sleep(MSretryInterval);
                } catch (InterruptedException ie) {
                    // Do nothing
                }
            }
        }
    }

    /**
     *
     * get file from the metadb after get the zk lock
     */
    private Object[] getFileFromMetaDB(List<SplitValue> list) {
        Object[] ob = new Object[3];
        String getsendIP = "";
        Long getf_id = 0L;
        String getroad = "";

        MetaStoreClientPool mscp = (MetaStoreClientPool) RuntimeEnv.getParam(GlobalVariables.METASTORE_CLIENT_POOL);
        MetaStoreClient cli = mscp.getClient();
        try {
            IMetaStoreClient icli = cli.getHiveClient();

            List<SFile> lsf = null;

            int attemp = 0;
            while (attemp <= attempSize) {
                try {
                    lsf = icli.filterTableFiles(rule.getPartType().split("\\|")[0], rule.getPartType().split("\\|")[1], list);
                    logger.info("get the filelist for " + topic + " " + rule.getPartType().split("\\|")[0] + " " + rule.getPartType().split("\\|")[1] + " " + list + " " + lsf);
                    break;
                } catch (Exception ex) {
                    logger.error("can not get the SFileList " + ex, ex);
                    rcmetastore(icli);
                    attemp++;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
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
                    logger.info(" the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " has " + alsf.size() + " available file");
                    for (SFile s : alsf) {
                        if (s.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                            aolsf.add(s);
                        }
                    }

                    SFile aomlsf = null;

                    while (!aolsf.isEmpty()) {
                        aomlsf = new SFile();
                        aomlsf.setLength(Long.MAX_VALUE);
                        logger.info(" the SFileList for " + topic + " " + keyinterval + " " + node.getName() + " has " + aolsf.size() + " available and open file");
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
                            logger.debug(aomlsf.getLocations().get(i).getVisit_status());
                            if (aomlsf.getLocations().get(i).getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                                visit = i;
                            }
                        }

                        if (visit == -1) {
                            logger.info("there is no location is online in the file " + aomlsf);
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
                                    logger.debug("this file " + aomlsf + " for the topic " + topic + " to the road " + getroad + " to the IP " + getsendIP + " has online location");
                                    logger.info("choose the file " + aomlsf + " to the road " + getroad + " to the sendIP " + getsendIP);
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
                        logger.info("the SFileList for " + topic + " " + keyinterval + " " + node.getName() + "has no open file or the open file has no useful location");
                        getsendIP = "";
                        getf_id = 0L;
                        getroad = "";

                        for (SFile s : alsf) {
                            if (s.getLoad_status() == MetaStoreConst.MFileLoadStatus.BAD) {
                                alsf.remove(s);
                            }
                        }

                        while (alsf.size() > 1) {
                            logger.info(" for " + topic + " " + keyinterval + " " + node.getName() + "alsf size is " + alsf.size());
                            aomlsf = new SFile();
                            aomlsf.setLength(Long.MAX_VALUE);

                            for (SFile s : alsf) {
                                if (s.getLength() < aomlsf.getLength()) {
                                    aomlsf = s;
                                }
                            }

                            attemp = 0;
                            while (attemp <= attempSize) {
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
                                                try {
                                                    Thread.sleep(2000);
                                                } catch (Exception e1) {
                                                }
                                            }
                                        }
                                        logger.info("reopen the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " successfully");
                                        break;
                                    } else {
                                        logger.info("can not reopen the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName());
                                        attemp++;
                                    }
                                } catch (Exception ex) {
                                    logger.error("can not reopen the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + ex, ex);
                                    rcmetastore(icli);
                                    attemp++;
                                }
                                try {
                                    Thread.sleep(1000);
                                } catch (Exception e) {
                                }
                            }

                            if (aomlsf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                                logger.info("the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName() + " has been reopened");

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
                                        logger.info("there is no location is online in the file " + aomlsf);
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
                                                logger.info("there is unvalid information in the file " + aomlsf + " for the topic " + topic + " to the road " + getroad + " to the IP " + getsendIP);
                                                setBad(icli, aomlsf);
                                                alsf.remove(aomlsf);
                                            } else {
                                                logger.debug("this file " + aomlsf + " for the topic " + topic + " to the road " + getroad + " to the IP " + getsendIP + " has online location");
                                                logger.info("choose the file " + aomlsf + " to the road " + getroad + " to the sendIP " + getsendIP);
                                                break;
                                            }
                                        } catch (Exception e) {
                                            getsendIP = "";
                                            getf_id = 0L;
                                            getroad = "";
                                            logger.info("there is no online location in the file " + aomlsf + " for the topic " + topic + " to the road " + getroad + " to the IP " + getsendIP);
                                            setBad(icli, aomlsf);
                                            alsf.remove(aomlsf);
                                        }
                                    }
                                }
                            } else {
                                alsf.remove(aomlsf);
                                logger.info("can not reopen the file " + aomlsf + " for " + topic + " " + keyinterval + " " + node.getName());
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
     * send the message typed 0/1/2/3 to the server
     */
    private void sendToType0123() {
        HttpClient httpClient = null;
        httpClient = new DefaultHttpClient();
        httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 10000);
        httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 30000);
        String url = "http://" + sendIP;
        HttpPost httppost = null;
        HttpResponse response = null;
        int i;
        for (i = 0; i < 3; i++) {
            try {
                httppost = new HttpPost(url);
                httppost.setHeader("cmd", "data");
                InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(sendData), -1);
                reqEntity.setContentType("binary/octet-stream");
                reqEntity.setChunked(true);
                httppost.setEntity(reqEntity);
                response = httpClient.execute(httppost);
                if (response.getStatusLine().getStatusCode() == 200) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    response.getEntity().writeTo(out);
                    String resonseEn = new String(out.toByteArray());
                    if ("-1".equals(resonseEn.split("[\n]")[0])) {
                        logger.error(resonseEn.split("[\n]")[1]);
                        EntityUtils.consume(response.getEntity());
                        continue;
                    }
                    Date d = new Date();
                    ConcurrentHashMap<String, AtomicLong> ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
                    AtomicLong al = ruleToCount.get(topic + serviceName);
                    Long l = al.addAndGet(count);
                    logger.info(d + " " + l + " send message to the " + topic + " " + rule.getServiceName() + " " + url + " successfully");
                    break;
                } else {
                    logger.info(response.getStatusLine());
                    EntityUtils.consume(response.getEntity());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex1) {
                        logger.error(ex1, ex1);
                    } catch (Exception ex1) {
                        logger.error(ex1, ex1);
                    }
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex1) {
                    logger.error(ex1, ex1);
                } catch (Exception ex1) {
                    logger.error(ex1, ex1);
                }
            }
        }
        if (i >= 3) {
            logger.info("send to the " + url + "failed or timeout");
            ArrayList<RNode> nodeurls = rule.getNodeUrls();

            if (rule.getNodeUrls().contains(node)) {
                Object[] o = new Object[3];
                o[0] = node;
                o[1] = rule;
                o[2] = sendData;
                ConcurrentLinkedQueue<Object[]> detectNodeList = (ConcurrentLinkedQueue<Object[]>) RuntimeEnv.getParam(GlobalVariables.DETECT_NODELIST);
                detectNodeList.add(o);
            }
            rule.removeNode(node);

            ConcurrentLinkedQueue<Object[]> strandedDataTransmit = (ConcurrentLinkedQueue<Object[]>) RuntimeEnv.getParam(GlobalVariables.STRANDED_DATA_TRANSMIT);
            Object[] o = new Object[2];
            o[0] = rule;
            o[1] = sendData;
            strandedDataTransmit.add(o);
        }
        httpClient.getConnectionManager().shutdown();
    }

    /**
     *
     * send the message typed 4 to the server
     */
    private void sendToType4() {
        HttpClient httpClient = null;
        httpClient = new DefaultHttpClient();
        httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 2000);
        httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 20000);
        String url = "http://" + sendIP;
        HttpPost httppost = null;
        HttpResponse response = null;
        int i;
        int flag = 0;

        for (i = 0; i < 3; i++) {
            try {
                httppost = new HttpPost(url);
                httppost.setHeader("cmd", f_id + "|" + road);
                InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(sendData), -1);
                reqEntity.setContentType("binary/octet-stream");
                reqEntity.setChunked(true);
                httppost.setEntity(reqEntity);
                response = httpClient.execute(httppost);
                if (response.getStatusLine().getStatusCode() == 200) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    response.getEntity().writeTo(out);
                    String resonseEn = new String(out.toByteArray());
                    if ("-1".equals(resonseEn.split("[\n]")[0])) {
                        logger.error(resonseEn.split("[\n]")[1]);
                        EntityUtils.consume(response.getEntity());
                        continue;
                    }
                    if ("-2".equals(resonseEn.split("[\n]")[0])) {
                        logger.error(resonseEn.split("[\n]")[1]);
                        flag = -2;
                        EntityUtils.consume(response.getEntity());
                        break;
                    }
                    Date d = new Date();
                    ConcurrentHashMap<String, AtomicLong> ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam("ruleToCount");
                    AtomicLong al = ruleToCount.get(topic + serviceName);
                    Long l = al.addAndGet(count);
                    logger.info(d + " " + l + " send messages to the " + topic + " " + rule.getServiceName() + " " + url + " successfully");
                    break;
                } else {
                    logger.info(response.getStatusLine());
                    EntityUtils.consume(response.getEntity());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex1) {
                        logger.error(ex1, ex1);
                    } catch (Exception ex1) {
                        logger.error(ex1, ex1);
                    }
                }
            } catch (Exception e) {
                logger.info(e);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex1) {
                    logger.error(ex1, ex1);
                } catch (Exception ex1) {
                    logger.error(ex1, ex1);
                }
            }
        }

        if (i >= 3) {
            logger.info("send to the " + url + "failed or timeout");
            changeFileTosend(flag);
        }

        httpClient.getConnectionManager().shutdown();
    }

    private void changeFileTosend(int flag) {
        ConcurrentHashMap<String, Object[]> valueToFile = (ConcurrentHashMap<String, Object[]>) RuntimeEnv.getParam(GlobalVariables.VALUE_TO_FILE);

        synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_VALUE_TO_FILE)) {
            Object[] obj = valueToFile.get(topic + keyinterval + node.getName());
            String newsendIP = "";
            Long newf_id = 0L;
            String newroad = "";
            if (obj[0] == null) {
                newsendIP = "";
            } else {
                newsendIP = (String) obj[0];
            }

            if (obj[1] == null) {
                newf_id = 0L;
            } else {
                newf_id = (Long) obj[1];
            }

            if (obj[2] == null) {
                newroad = "";
            } else {
                newroad = (String) obj[2];
            }

            if (newsendIP.equals("") || newsendIP == "" || newf_id == 0L || newroad.equals("") || newroad == "" || (newsendIP.equals(sendIP) && newf_id == f_id && newroad.equals(road))) {
                Long oldf_id = f_id;
                sendIP = "";
                f_id = 0L;
                road = "";
                Object[] ob = new Object[3];

                if (flag != -2) {
                    setBadFile(oldf_id);
                }

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

                List<SplitValue> list = new ArrayList<SplitValue>();

                for (int j = 1; j <= 2; j++) { //设置一级和二级划分值
                    if (j == 1) {
                        SplitValue sv1 = new SplitValue();
                        sv1.setVerison(version);
                        sv1.setLevel(j);//如果是interval分区，设置两个特征值，一个是上限一个是下限
                        sv1.setValue("" + stime);
                        sv1.setSplitKeyName(rule.getKeywords().split("\\|")[0]);
                        list.add(sv1);

                        SplitValue sv2 = new SplitValue();
                        sv2.setVerison(version);
                        sv2.setLevel(j);//如果是interval分区，设置两个特征值，一个是上限一个是下限
                        sv2.setValue("" + etime);
                        sv2.setSplitKeyName(rule.getKeywords().split("\\|")[0]);
                        list.add(sv2);
                    } else if (j == 2) {
                        SplitValue sv = new SplitValue();
                        sv.setVerison(version);
                        sv.setLevel(j);
                        sv.setValue(rule.getNodeUrls().size() + "-" + node.getName());//设置哈希键值
                        sv.setSplitKeyName(rule.getKeywords().split("\\|")[1]);
                        list.add(sv);
                    }
                }

                ob = getUsefulFileFromMetaDB(list);

                sendIP = (String) ob[0];
                f_id = (Long) ob[1];
                road = (String) ob[2];

                if (sendIP == null || f_id == 0L || road == null || sendIP.equals("") || road.equals("") || sendIP == "" || road == "") {
                    sendIP = "";
                    f_id = 0L;
                    road = "";
                    ob[0] = "";
                    ob[1] = 0L;
                    ob[2] = "";
                    logger.info("need to get zk for " + topic + " " + keyinterval + " " + node.getName());
                    String zkCluster = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);

                    int zkSessionTimeout = 30000;
                    ZkClient zk = new ZkClient(zkCluster, zkSessionTimeout);
                    int ZKretryInterval = ZOOKEEPER_RETRY_INIT;
                    int ZKretryAttempt = 0;

                    while (true) {
                        if (!zk.exists("/ulss")) {
                            logger.debug("existsss " + zk.exists("/ulss"));
                            try {
                                zk.createPersistent("/ulss");
                                logger.debug("/ulss created");
                            } catch (Exception e) {
                                logger.debug("root exists : other master has created the /ulss");
                            }
                        }

                        if (!zk.exists("/ulss/redistribution")) {
                            logger.debug("existsss " + zk.exists("/ulss/redistribution"));
                            try {
                                zk.createPersistent("/ulss/redistribution");
                                logger.debug("/ulss/redistribution created");
                            } catch (Exception e) {
                                logger.debug("root exists : other master has created the /ulss/redistribution");
                            }
                        }

                        if (!zk.exists("/ulss/redistribution/lock")) {

                            try {
                                zk.createEphemeral("/ulss/redistribution/" + topic + keyinterval + "lock", keyinterval);
                                logger.debug("new lock " + keyinterval + " created");
                            } catch (Exception e) {
                                logger.debug("the lock has been created");
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

                            if (sendIP == null || f_id == 0L || road == null || sendIP.equals("") || road.equals("") || sendIP == "" || road == "") {
//                                   sf2 = icli.create_file("NODE33", 2, m[2], map.get(topic), list);
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

                valueToFile.put(topic + keyinterval + node.getName(), ob);
                logger.info("choose the " + topic + " " + keyinterval + " " + node.getName() + ob);

                Object[] o = new Object[7];
                o[0] = node;
                o[1] = sendIP;
                o[2] = keyinterval;
                o[3] = f_id;
                o[4] = road;
                o[5] = count;
                o[6] = version;
                ConcurrentHashMap<Map<Rule, byte[]>, Object[]> strandedDataSend = (ConcurrentHashMap<Map<Rule, byte[]>, Object[]>) RuntimeEnv.getParam(GlobalVariables.STRANDED_DATA_SEND);
                Map<Rule, byte[]> m2 = new HashMap<Rule, byte[]>();
                m2.put(rule, sendData);
                strandedDataSend.put(m2, o);
            } else {
                Object[] o = new Object[7];
                o[0] = node;
                o[1] = newsendIP;
                o[2] = keyinterval;
                o[3] = newf_id;
                o[4] = newroad;
                o[5] = count;
                o[6] = version;
                ConcurrentHashMap<Map<Rule, byte[]>, Object[]> strandedDataSend = (ConcurrentHashMap<Map<Rule, byte[]>, Object[]>) RuntimeEnv.getParam(GlobalVariables.STRANDED_DATA_SEND);
                Map<Rule, byte[]> m2 = new HashMap<Rule, byte[]>();
                m2.put(rule, sendData);
                strandedDataSend.put(m2, o);
                logger.debug("choose the " + topic + " " + keyinterval + " " + node.getName() + o);
            }
        }
    }

    /**
     *
     * set the file bad , it means this file will not be used again
     *
     */
    public void setBad(IMetaStoreClient icli, SFile sf) {
        int attemp = 0;
        while (attemp <= attempSize) {
            try {
                //sf.setLoad_status(MetaStoreConst.MFileLoadStatus.BAD);
                icli.set_loadstatus_bad(sf.getFid());
                logger.error("has set bad the file " + sf);
                //icli.close_file(aomlsf);
                break;
            } catch (Exception ex) {
                logger.error("can not set bad the file " + sf + " " + ex, ex);
                rcmetastore(icli);
                attemp++;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }
        }
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
        MetaStoreClientPool mscp = (MetaStoreClientPool) RuntimeEnv.getParam(GlobalVariables.METASTORE_CLIENT_POOL);
        MetaStoreClient cli = mscp.getClient();
        SFile sf2 = null;
        int attemp = 0;
        try {
            IMetaStoreClient icli = cli.getHiveClient();

            List<NodeGroup> lng = null;
            attemp = 0;
            while (attemp <= attempSize) {
                try {
                    lng = icli.getTableNodeGroups(rule.getPartType().split("\\|")[0], rule.getPartType().split("\\|")[1]);
                    break;
                } catch (Exception ex) {
                    logger.error("can not get the List<nodeGroup>" + ex, ex);
                    rcmetastore(icli);
                    attemp++;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
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
                    logger.info("begin to create file " + rule.getPartType().split("\\|")[0] + " " + rule.getPartType().split("\\|")[1] + " " + list);
                    sf2 = icli.create_file_by_policy(cp, 2, rule.getPartType().split("\\|")[0], rule.getPartType().split("\\|")[1], list);
                    if (sf2 == null) {
                        logger.debug("can not create file for the topic " + topic + " " + keyinterval + " " + node.getName() + " from metastore ");
                        if (CFretryInterval < 30000) {
                            CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                        } else {
                            CFretryInterval = 30000;
                        }
                        logger.info("CREATE FILE for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
                        try {
                            Thread.sleep(CFretryInterval);
                        } catch (Exception ex) {
                            logger.error(ex, ex);
                        }
                        continue;
                    }
                    logger.info("create file for the topic " + topic + " " + keyinterval + " " + node.getName() + " from metastore ");
                    String nodeN = sf2.getLocations().get(0).getNode_name();
                    if (nodeN == null) {
                        if (nodeNames.isEmpty()) {
                            logger.debug("thres is no node in the nodeGroups for " + rule.getPartType().split("\\|")[0] + rule.getPartType().split("\\|")[1]);
                            Thread.sleep(2000);
                            continue;
                        } else {
                            Random r = new Random();
                            int ran = r.nextInt(nodeNames.size());
                            if (nodeNames.get(ran) == null || nodeNames.get(ran).isEmpty()) {
                                logger.debug("thres is null node in the nodeGroups for " + rule.getPartType().split("\\|")[0] + rule.getPartType().split("\\|")[1]);
                                Thread.sleep(2000);
                                continue;
                            }
                            sendIP = nodeNames.get(ran) + rule.getIPList()[0];
                            //sendIP = "192.168.1." + nodeNames.get(ran).substring(4) + rule.getIPList()[0];
                        }
                    } else {
                        sendIP = nodeN + rule.getIPList()[0];
                        //sendIP = "192.168.1." + nodeN.substring(4) + rule.getIPList()[0];
                    }
                    f_id = sf2.getFid();
                    road = sf2.getLocations().get(0).getLocation() + "|" + sf2.getLocations().get(0).getDevid();

                    if (road == null) {
                        sendIP = "";
                        f_id = 0L;
                        road = "";
                        logger.debug("road is null" + " for " + sf2 + topic + " " + keyinterval + " " + node.getName());
                        Thread.sleep(2000);
                        continue;
                    }

                    SendCreateFileCMD scfc = new SendCreateFileCMD(sendIP, f_id, road);
                    HttpResponse hp = scfc.send();
                    if (hp == null) {
                        logger.error("the file  for " + sf2 + topic + " " + keyinterval + " " + node.getName() + " can not be set online");
                        sendIP = "";
                        f_id = 0L;
                        road = "";
                        setBad(icli, sf2);

                        if (CFretryInterval < 30000) {
                            CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                        } else {
                            CFretryInterval = 30000;
                        }
                        logger.info("CREATE FILE for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
                        try {
                            Thread.sleep(CFretryInterval);
                        } catch (Exception ex) {
                            logger.error(ex, ex);
                        }

                        continue;
                    } else {
                        logger.debug("sendCreateFileCMD : " + hp.getStatusLine());
                        if (hp.getStatusLine().getStatusCode() != 200) {
                            logger.error("the file  for " + sf2 + topic + " " + keyinterval + " " + node.getName() + " can not be set online");
                            sendIP = "";
                            f_id = 0L;
                            road = "";
                            setBad(icli, sf2);

                            if (CFretryInterval < 30000) {
                                CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                            } else {
                                CFretryInterval = 30000;
                            }
                            logger.info("CREATE FILE for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
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
                                logger.error(ex, ex);
                                logger.error("the file  for " + sf2 + topic + " " + keyinterval + " " + node.getName() + " can not be set online " + hp.getStatusLine());
                                sendIP = "";
                                f_id = 0L;
                                road = "";
                                setBad(icli, sf2);
                                if (CFretryInterval < 30000) {
                                    CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                                } else {
                                    CFretryInterval = 30000;
                                }
                                logger.info("CREATE FILE for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));

                                Thread.sleep(CFretryInterval);

                                continue;
                            }

                            String resonseEn = new String(out.toByteArray());
                            if ("-1".equals(resonseEn.split("[\n]")[0])) {
                                logger.info(resonseEn.split("[\n]")[1]);
                                logger.error("the file  for " + f_id + topic + " " + keyinterval + " " + node.getName() + " can not be set online " + hp.getStatusLine());
                                sendIP = "";
                                f_id = 0L;
                                road = "";

                                setBad(icli, sf2);

                                if (CFretryInterval < 30000) {
                                    CFretryInterval = CFretryInterval + CREATEFILE_RETRY_INIT;
                                } else {
                                    CFretryInterval = 30000;
                                }
                                logger.info("CREATE FILE for " + topic + " " + keyinterval + " " + node.getName() + String.format(" On retry attempt %d . Sleeping %d seconds.", ++CFretryAttempt, CFretryInterval / 1000));
                                try {
                                    Thread.sleep(CFretryInterval);
                                } catch (Exception ex) {
                                    logger.error(ex, ex);
                                }

                                continue;
                            } else {
                                logger.info("this file " + f_id + " for the topic " + topic + " to the road " + road + " to the IP " + sendIP + " has been set online");
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
            logger.info("new File for " + topic + " " + keyinterval + " " + node.getName() + " has be created");
        } finally {
            cli.release();
        }
        return ob;
    }
}