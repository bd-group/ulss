/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.MD5NodeLocator;
import cn.ac.iie.ulss.dataredistribution.dao.SimpleDaoImpl;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.DynamicAllocate;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.NodeLocator;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import cn.ac.iie.ulss.dataredistribution.tools.MetaStoreClientPool;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionInfo;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class GetRuleFromDB {

    static Logger logger = null;
    String time = null;
    ConcurrentHashMap<String, ArrayList<Rule>> topicToRules = null;
    ArrayList<String> transmitrule = null;
    Map<RNode, Integer> nodeToThreadNum = (Map<RNode, Integer>) RuntimeEnv.getParam(GlobalVariables.NODE_TO_THREADNUM);
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDaoImpl simpleDao;
    int attempSize = 2;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GetRuleFromDB.class.getName());
    }

    public void start() {
        topicToRules = (ConcurrentHashMap<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES);
        transmitrule = (ArrayList<String>) RuntimeEnv.getParam(GlobalVariables.TRANSMITRULE);
        
        getRules(); //get rules from the oracle

        MessageTransferStation.init(topicToRules); // initialise the message transfer station 

        Set topicset = topicToRules.keySet();
        String topickey = null;
        for (Iterator it = topicset.iterator(); it.hasNext();) {
            topickey = (String) it.next();
            TopicThread topicthread = new TopicThread(topickey); // starting the transmit server by topic 
            Thread tt = new Thread(topicthread);
            tt.start();
        }

        GetMessageFromMetaStore gmfms = new GetMessageFromMetaStore(); // getting the change message from the metastore
        Thread tgmfms = new Thread(gmfms);
        tgmfms.start();

        DetectTransmitRule dtr = new DetectTransmitRule();
        Thread tdtr = new Thread(dtr);
        tdtr.start();
    }

    /**
     *
     * get rules from the oracle
     */
    private void getRules() {
        logger.info("getting data transmit rule from oracle...");
        String dbCluster = (String) RuntimeEnv.getParam(RuntimeEnv.DB_CLUSTER);
        simpleDao = SimpleDaoImpl.getDaoInstance(dbCluster);
        String sql = "select topic,service,nodeurls,type,keywords,filters,valid,region from data_transmitrule";//datatransmit_rules";
        List<List<String>> newRs = simpleDao.queryForList(sql);
        String[] re = ((String) RuntimeEnv.getParam(RuntimeEnv.REGION)).split("\\|");
        List<String> regions = Arrays.asList(re);
        for (List<String> r : newRs) {
            String valid = r.get(6);

            if (valid == null || valid.equals("")) {
                logger.info("a rule has no valid");
            } else if (valid.equals("0")) {
            } else if (valid.equals("1")) {

                String topic = r.get(0);

                if (topic == null || topic.equals("")) {
                    logger.error("the topic in the rule is null or wrong " + topic);
                    continue;
                } else {
                    if (!topic.matches("[^/\\\\\\\\<>*?|\\\"]+")) {
                        logger.error("the name of topic is valid , it should include /\\<>*?|\" " + topic);
                        continue;
                    }
                }

                String serviceName = r.get(1);

                if (serviceName == null || serviceName.equals("")) {
                    logger.error("the serviceName in the rule is null or wrong " + topic);
                    continue;
                } else {
                    if (!serviceName.matches("[^/\\\\\\\\<>*?|\\\"]+")) {
                        logger.error("the name of serviceName is valid , it should include /\\<>*?|\" ");
                        continue;
                    }
                }

                String nodeUrls = r.get(2);

                if (nodeUrls == null || nodeUrls.equals("")) {
                    logger.error("the nodeUrls in the rule is null or wrong");
                    continue;
                } else {
                    if (!nodeUrls.endsWith("/")) {
                        logger.error("the name of nodeUrls is valid , it should include /\\<>*?|\" ");
                        continue;
                    }
                }

                int type = -1;
                try {
                    type = Integer.parseInt(r.get(3));
                    if (type != 0 && type != 1 && type != 2 && type != 3 && type != 4) {
                        logger.error("the rule_type in the rule is null or wrong");
                        continue;
                    }
                } catch (Exception e) {
                    logger.error("the rule_type in the rule is null or wrong " + topic + e, e);
                    continue;
                }

                String keywords = r.get(4);
                if (keywords != null) {
                    keywords = keywords.trim();
                }

                String filters = r.get(5);
                if (filters != null) {
                    filters = filters.trim();
                }

                String region = r.get(7);
                if (region != null) {
                    region = region.trim();
                }

                ConcurrentHashMap<String, AtomicLong> ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
                AtomicLong al = new AtomicLong(0);
                ruleToCount.put(topic + serviceName, al);

                MD5NodeLocator nodelocator = null;
                String[] IPList = null;
                Map<RNode, String> nodeToIP = new HashMap<RNode, String>();
                ArrayList<String> deadIP = new ArrayList<String>();
                String partType = null;

                if (type == 0 || type == 1 || type == 2 || type == 3) {
                    transmitrule.add(topic + serviceName);
                    IPList = nodeUrls.split("\\;");
                    ArrayList nurl = new ArrayList<RNode>();
                    for (int i = 0; i < IPList.length; i++) {
                        RNode node = new RNode(IPList[i]);
                        nodeToThreadNum.put(node, 0);
                        nurl.add(node);
                        nodeToIP.put(node, node.getName());
                    }
                    DynamicAllocate dynamicallocate = new DynamicAllocate();
                    dynamicallocate.setNodes(nurl);
                    nodelocator = dynamicallocate.getMD5NodeLocator();

                    Rule s = new Rule(topic, serviceName, nurl, type, keywords, filters, nodelocator, IPList, nodeToIP, deadIP, partType);
                    Map<Rule, String> ruleToControl = (Map<Rule, String>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_CONTROL);
                    ruleToControl.put(s, "start");

                    if (topicToRules.containsKey(topic)) {
                        ArrayList<Rule> set = (ArrayList<Rule>) topicToRules.get(topic);
                        set.add(s);
                    } else {
                        ArrayList<Rule> set = new ArrayList<Rule>();
                        set.add(s);
                        topicToRules.put(topic, set);
                    }
                } else if (type == 4) {
                    if (regions.contains(region)) {
                        Rule s = null;
                        s = getFromMetaStore(topic, serviceName, nodeUrls, type, keywords, filters, region);

                        if (s != null) {
                            Map<Rule, String> ruleToControl = (Map<Rule, String>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_CONTROL);
                            ruleToControl.put(s, "start");
                            transmitrule.add(topic + serviceName);
                            if (topicToRules.containsKey(topic)) {
                                ArrayList<Rule> set = (ArrayList<Rule>) topicToRules.get(topic);
                                set.add(s);
                            } else {
                                ArrayList<Rule> set = new ArrayList<Rule>();
                                set.add(s);
                                topicToRules.put(topic, set);
                            }
                        } else {
                            logger.error("getFromMetaStore error");
                            continue;
                        }
                    } else {
                        logger.info("this rule's region will be not in the regions " + region);
                    }
                } else {
                    logger.error("the type in the rule " + type + "is wrong!");
                    continue;
                }
            } else {
                logger.error("a valid is wrong");
            }
        }

        logger.info("parsing data schemas is finished");
    }

    /**
     *
     * get rules from the metastore
     */
    private Rule getFromMetaStore(String topic, String serviceName, String nodeUrls, int type, String keywords, String filters, String region) {
        keywords = "";
        filters = "";
        logger.info("getting the part_rule from the metaStore");
        Rule r = null;
        ArrayList nurl = new ArrayList<RNode>();
        NodeLocator nodelocator = null;
        String[] IPList = null;
        Map<RNode, String> nodeToIP = new HashMap<RNode, String>();
        ArrayList<String> deadIP = new ArrayList<String>();
        String partType = null;
        MetaStoreClientPool mscp = (MetaStoreClientPool) RuntimeEnv.getParam(GlobalVariables.METASTORE_CLIENT_POOL);
        if (mscp == null) {
            String metaStoreClientString = (String) RuntimeEnv.getParam("metaStoreClientString");
            int metaStoreClientPoolSize = (Integer) RuntimeEnv.getParam("metaStoreClientPoolSize");
            HiveConf hc = new HiveConf();
            hc.set("hive.metastore.uris", "thrift://" + metaStoreClientString);
            MetaStoreClientPool newmscp = new MetaStoreClientPool(metaStoreClientPoolSize, hc);
            RuntimeEnv.addParam(GlobalVariables.METASTORE_CLIENT_POOL, newmscp);
            mscp = newmscp;
        }
        MetaStoreClientPool.MetaStoreClient cli = mscp.getClient();
        try {
            IMetaStoreClient icli = cli.getHiveClient();
            logger.info("connected the metastore");
            Table bf_dxx = null;

            Map<String, String> topicToTBName = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_TBNAME);

            int attemp = 0;
            while (attemp <= attempSize) {
                try {
                    bf_dxx = icli.getTable(region, topicToTBName.get(topic));
                    List<FieldSchema> allSplitKeys = bf_dxx.getFileSplitKeys();
                    List<FieldSchema> splitKeys = new ArrayList<FieldSchema>();

                    Long version = 0L;
                    for (FieldSchema fs : allSplitKeys) {
                        if (fs.getVersion() >= version) {
                            version = fs.getVersion();
                        }
                    }

                    for (FieldSchema fs : allSplitKeys) {
                        if (fs.getVersion() == version) {
                            splitKeys.add(fs);
                        }
                    }
                    logger.info("the fieldScahma " + version + " has " + splitKeys.size() + " splitKeys");

                    if (splitKeys.isEmpty()) {
                        logger.info("There is no splitkeys in the table " + topicToTBName.get(topic));
                        return null;
                    }

                    String split_name_l1 = "";
                    String split_name_l2 = "";
                    String part_type_l1 = "";
                    String part_type_l2 = "";
                    int l1_part_num = 0;
                    int l2_part_num = 0;

                    List<PartitionInfo> pis = PartitionInfo.getPartitionInfo(splitKeys);

                    if (pis.size() == 2) {
                        StringBuilder tmps = new StringBuilder();
                        tmps.append(region);
                        tmps.append("|");
                        tmps.append(topicToTBName.get(topic));
                        String unit = null;
                        String interval = null;
                        for (PartitionFactory.PartitionInfo pinfo : pis) {
                            if (pinfo.getP_level() == 1) {         //分区是第几级？一级还是二级(现在支持一级interval、hash和一级interv、二级hash 三种分区方式)
                                split_name_l1 = pinfo.getP_col();  //使用哪一列进行分区
                                part_type_l1 = pinfo.getP_type().getName(); //这级分区的方式，是hash还是interval ?
                                l1_part_num = pinfo.getP_num();   //分区有多少个，如果是hash的话n个分区，那么特征值就是0,1,2 。。。 n-1

                                if ("interval".equalsIgnoreCase(part_type_l1)) {
                                    if (pis.get(0).getArgs().size() < 2) {
                                        logger.error("get the table's partition unit and interval error");
                                        return null;
                                    } else {
                                        //Y year,M mponth,W week,D day,H hour，M minute。 现在只支持H D W， 因为月和年的时间并不是完全确定的，因此无法进行精确的分区，暂时不支持；分钟级的单位太小，暂时也不支持
                                        List<String> paras = pinfo.getArgs();
                                        unit = paras.get(0);
                                        interval = paras.get(1);
                                        tmps.append("|interval|");
                                        tmps.append(unit);
                                        tmps.append("|");
                                        tmps.append(interval);
                                    }
                                } else {
                                    logger.error(topic + " this system only support the interval for the first partition");
                                    return null;
                                }
                            }

                            if (pinfo.getP_level() == 2) {
                                split_name_l2 = pinfo.getP_col();
                                part_type_l2 = pinfo.getP_type().getName();
                                l2_part_num = pinfo.getP_num();
                                //keywords = part_type_l2;
                                if ("hash".equalsIgnoreCase(part_type_l2)) {
                                    nurl = new ArrayList<RNode>();
                                    for (int i = 0; i < l2_part_num; i++) {
                                        RNode node = new RNode("" + i);
                                        nurl.add(node);
                                    }
                                    DynamicAllocate dynamicallocate = new DynamicAllocate();
                                    dynamicallocate.setNodes(nurl);
                                    nodelocator = dynamicallocate.getHashCodeNodeLocator();
                                    tmps.append("|hash|");
                                    tmps.append(version);
                                    partType = tmps.toString();
                                } else {
                                    logger.error(topic + " this system only support the hash for the second partition");
                                    return null;
                                }
                            }
                        }

                        keywords = split_name_l1 + "|" + split_name_l2;
                        IPList = new String[1];
                        IPList[0] = nodeUrls;
                        logger.info(topic + " the partitionRules is " + partType + " " + keywords + " " + IPList[0]);
                        r = new Rule(topic, serviceName, nurl, type, keywords, filters, nodelocator, IPList, nodeToIP, deadIP, partType);
                        break;
                    } else {
                        logger.error(topic + " this system only support the two levelpartitions");
                        return null;
                    }
                } catch (Exception ex) {
                    logger.error(region + "　" + topic + " " + topicToTBName.get(topic) + ex, ex);
                    try {
                        icli.reconnect();
                    } catch (MetaException ex1) {
                        logger.error(ex1, ex1);
                    }
                    attemp++;
                }
            }
        } finally {
            cli.release();
        }
        return r;
    }
}