/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.MD5NodeLocator;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.DynamicAllocate;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import cn.ac.iie.ulss.dataredistribution.tools.MetaStoreClientPool;
import cn.ac.iie.ulss.dataredistribution.tools.MetaStoreClientPool.MetaStoreClient;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class GetMessageFromMetaStore implements Runnable {

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static Logger logger = null;
    static Map<String, ArrayList<RNode>> topicToNodes = (Map<String, ArrayList<RNode>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_NODES);
    int attempSize = 2;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GetRuleFromDB.class.getName());
    }

    @Override
    public void run() {
        logger.info("getting the message from the metastore");
        String mszkurl = (String) RuntimeEnv.getParam(RuntimeEnv.METASTORE_ZK_CLUSTER);
        String zkUrl = mszkurl.split("\\|")[0];
        String topic = mszkurl.split("\\|")[1];
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
        zkConfig.zkConnect = zkUrl;
        metaClientConfig.setZkConfig(zkConfig);
        final MessageSessionFactory sessionFactory;
        try {
            sessionFactory = new MetaMessageSessionFactory(metaClientConfig);

            ConsumerConfig cc = new ConsumerConfig("dateredistribution_consumer");

            final MessageConsumer consumer = sessionFactory.createConsumer(cc);

            logger.info("init the consumer ok,begin receive data from the metastore ");

            consumer.subscribe(topic, 5 * 1024 * 1024, new MessageListener() {
                @Override
                public void recieveMessages(Message message) {
                    DDLMsg msg = new DDLMsg();
                    String data = new String(message.getData());
                    msg = DDLMsg.fromJson(data);
                    int eventId = (int) msg.getEvent_id();
                    String db_name = null;
                    String table_name = null;
                    String column_name = null;
                    String column_type = null;
                    Long version = -1L;

                    Map<String, Object> topicToRules = (Map<String, Object>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES);
                    switch (eventId) {
                        case 1101: // 新建表
                            logger.info("the message from metastore for 1101!!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            break;

                        case 1102: // 修改表名
                            logger.info("the message from metastore for 1102!!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            String old_table_name = (String) msg.getMsg_data().get("old_table_name");
                            break;

                        case 1105: // 划分规则改变
                            logger.info("the message from metastore for 1105!!! , begin to change the transmit rule");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            version = Long.parseLong((String) msg.getMsg_data().get("version"));

                            Object[] o = getChangedRule(db_name, table_name, version);
                            String keywords = (String) o[0];
                            String partType = (String) o[1];
                            int hashNum = 0;
                            try {
                                hashNum = (Integer) o[2];
                            } catch (Exception ex) {
                                logger.error(ex, ex);
                            }
                            String topic = (String) o[3];
                            ArrayList<RNode> alr = topicToNodes.get(topic);
                            if (keywords != null && partType != null && hashNum != 0 && topic != null) {
                                ArrayList<Rule> rules = (ArrayList<Rule>) topicToRules.get(topic);
                                for (Rule r : rules) {
                                    if (r.getType() == 4 ){
                                        ArrayList<RNode> oldnurl = r.getNodeUrls();
                                        Map<RNode, Rule> nodeToRule = (Map<RNode, Rule>) RuntimeEnv.getParam(GlobalVariables.NODE_TO_RULE);
                                        Map<RNode, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();
                                        ArrayList<RNode> nurl = new ArrayList<RNode>();
                                        for (int i = 0; i < hashNum; i++) {
                                            RNode node = new RNode("" + i);
                                            node.setHashNum(hashNum);
                                            node.setKeywords(keywords);
                                            node.setPartType(partType);
                                            node.setType(4);
                                            nurl.add(node);
                                            nodeToRule.put(node, r);
                                            alr.add(node);
                                            if (!messageTransferStation.containsKey(node)) {
                                                ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = new ConcurrentHashMap<String, ConcurrentLinkedQueue>();
                                                messageTransferStation.put(node, chm);
                                            }
                                        }
                                        DynamicAllocate dynamicallocate = new DynamicAllocate();
                                        dynamicallocate.setNodes(nurl);
                                        MD5NodeLocator nodelocator = dynamicallocate.getMD5NodeLocator();

                                        r.changerule(nurl, nodelocator, keywords, partType);
                                        logger.info("change the partitioninfo for " + r.getTopic() + " " + r.getServiceName() + " to " + partType + " " + keywords + " " + hashNum);

                                        RemoveNodeFromMessageTransferStation rnfmts = new RemoveNodeFromMessageTransferStation(r, oldnurl);
                                        Thread t = new Thread(rnfmts);
                                        t.start();
                                    }
                                }
                            } else {
                                logger.error("change the transmit rule for " + db_name + " " + table_name + " " + version + " error!");
                            }

                            break;

                        case 1201: // 删除列
                            logger.info("the message from metastore for 1201!!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            column_name = (String) msg.getMsg_data().get("column_name");
                            column_type = (String) msg.getMsg_data().get("column_type");
                            break;

                        case 1202: //新增列
                            logger.info("the message from metastore for 1202 !!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            column_name = (String) msg.getMsg_data().get("column_name");
                            column_type = (String) msg.getMsg_data().get("column_type");
                            break;

                        case 1203: //修改列名
                            logger.info("the message from metastore for 1203 !!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            column_name = (String) msg.getMsg_data().get("column_name");
                            String old_column_name = (String) msg.getMsg_data().get("old_column_name");
                            break;
                        case 1207: //删除表
                            logger.info("the message from metastore for 1207 !!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            break;
//                        case 1307: //文件状态改变
//                            logger.info("the transmitRule is change for 1307 !!!");
//                            db_name = (String) msg.getMsg_data().get("db_name");
//                            table_name = (String) msg.getMsg_data().get("table_name");
//                            TBNameToTopic = (Map<String, String>) RuntimeEnv.getParam("TBNameToTopic");
//                            topic = TBNameToTopic.get(table_name);
//                            long f_id = Long.parseLong((String) msg.getMsg_data().get("f_id"));
//                            int new_status = Integer.parseInt((String) msg.getMsg_data().get("new_status"));
//
//                            if (new_status == 1) {
//                                logger.info("get the file be closed message");
//                                metaStoreClient = (String) RuntimeEnv.getParam("metaStoreClient");
//                                m = metaStoreClient.split("\\:");
//                                cli = null;
//                                try {
//                                    cli = new MetaStoreClient(m[0], Integer.parseInt(m[1]));//("192.168.1.13", 10101);
//                                } catch (MetaException ex) {
//                                    logger.info(ex, ex);
//                                }
//
//                                icli = cli.client;
//                                try {
//                                    SFile f = icli.get_file_by_id(f_id);
//                                    List<SplitValue> s = f.getValues();
//                                    String interval = null;
//                                    String hash = null;
//                                    Long[] time = new Long[2];
//                                    int i = 0;
//                                    String ha = null;
//                                    for (SplitValue sv : s) {
//                                        if(sv.getLevel() == 1){
//                                            time[i] = Long.parseLong(sv.getValue());
//                                            i++;
//                                        }else{
//                                            ha = sv.getSplitKeyName();
//                                        }
//                                    }
//
//                                    String keyinterval = null;
//                                    
//                                    if(time[0]<time[1]){
//                                        keyinterval = dateFormat.format(new Date(time[0]*1000))+"|"+dateFormat.format(new Date(time[1]*1000));
//                                    }else{
//                                        keyinterval = dateFormat.format(new Date(time[1]*1000))+"|"+dateFormat.format(new Date(time[0]*1000));
//                                    }
//                                    
//                                    Set<Rule> sr = (Set<Rule>) topicToRules.get(topic);
//                                    for(Rule r : sr){
//                                        if(r.getType() == 4){
//                                            ArrayList<Node> al = r.getNodeUrls();
//                                            for(Node n : al){
//                                                if(n.getName().equals(ha)){
//                                                    Map<Node, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();
//                                                    ConcurrentHashMap<String, ArrayBlockingQueue> chm = (ConcurrentHashMap<String, ArrayBlockingQueue>) messageTransferStation.get(n);
//                                                    chm.remove(keyinterval);
//                                                    logger.info("the messageTransferStation remove the sendQueue for " + topic+ " " + interval + " " + n.getName());
//                                                }
//                                            }
//                                        }
//                                    }
//                                } catch (FileOperationException ex) {
//                                    logger.info(ex, ex);
//                                } catch (MetaException ex) {
//                                    logger.info(ex, ex);
//                                } catch (TException ex) {
//                                    logger.info(ex, ex);
//                                }
//
//                            }
//
//                            break;
                        default:
                            //logger.info("Event is useless");
                            break;
                    }
                }

                public Object[] getChangedRule(String db_name, String table_name, Long version) {
                    Map<String, String> metaToTopic = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.META_TO_TOPIC);
                    String topic = metaToTopic.get(db_name + table_name);
                    Table bf_dxx = null;

                    MetaStoreClientPool mscp = (MetaStoreClientPool) RuntimeEnv.getParam(GlobalVariables.METASTORE_CLIENT_POOL);
                    MetaStoreClient cli = mscp.getClient();
                    int attemp = 0;
                    String split_name_l1 = ""; //String split_name_l1 =  pis.get(0).getP_col()??getP_type().getName();
                    String split_name_l2 = "";
                    String part_type_l1 = "";
                    String part_type_l2 = "";
                    int l1_part_num = 0;
                    int l2_part_num = 0;
                    String partType = null;
                    String keywords = null;
                    try {
                        IMetaStoreClient icli = cli.getHiveClient();
                        attemp = 0;
                        while (attemp <= attempSize) {
                            try {
                                bf_dxx = icli.getTable(db_name, table_name);

                                List<FieldSchema> allsplitKeys = bf_dxx.getFileSplitKeys();

                                List<FieldSchema> splitKeys = new ArrayList<FieldSchema>();

                                for (FieldSchema sf : allsplitKeys) {
                                    if (sf.getVersion() == version) {
                                        splitKeys.add(sf);
                                    }
                                }

                                if (splitKeys.isEmpty()) {
                                    logger.info("can not get the fieldSchema for " + db_name + " " + table_name + " " + version);
                                    break;
                                }

                                List<PartitionFactory.PartitionInfo> pis = PartitionFactory.PartitionInfo.getPartitionInfo(splitKeys);

                                if (pis.size() == 2) {
                                    StringBuilder tmps = new StringBuilder();
                                    tmps.append(db_name);
                                    tmps.append("|");
                                    tmps.append(table_name);
                                    String unit = null;
                                    String interval = null;
                                    for (PartitionFactory.PartitionInfo pinfo : pis) {
                                        if (pinfo.getP_level() == 1) {         //分区是第几级？一级还是二级(现在支持一级interval、hash和一级interv、二级hash 三种分区方式)
                                            split_name_l1 = pinfo.getP_col();  //使用哪一列进行分区
                                            part_type_l1 = pinfo.getP_type().getName(); //这级分区的方式，是hash还是interval ?
                                            l1_part_num = pinfo.getP_num();   //分区有多少个，如果是hash的话n个分区，那么特征值就是0,1,2 。。。 n-1

                                            if ("interval".equalsIgnoreCase(part_type_l1)) {
                                                if (pinfo.getArgs().size() < 2) {
                                                    logger.error("get the table's partition unit and interval error");
                                                    break;
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
                                                logger.error("this system only support the interval for the first partition");
                                                break;
                                            }
                                        }

                                        if (pinfo.getP_level() == 2) {
                                            split_name_l2 = pinfo.getP_col();
                                            part_type_l2 = pinfo.getP_type().getName();
                                            l2_part_num = pinfo.getP_num();

                                            if ("hash".equalsIgnoreCase(part_type_l2)) {
                                                tmps.append("|hash|");
                                                tmps.append(version);
                                                partType = tmps.toString();
                                            } else {
                                                logger.error("this system only support the hash for the second partition");
                                                break;
                                            }
                                        }
                                    }
                                    keywords = split_name_l1 + "|" + split_name_l2;
                                } else {
                                    logger.error("this system only support the two levelpartitions");
                                    break;
                                }
                                break;
                            } catch (Exception ex) {
                                logger.error(ex, ex);
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
                    Object[] ob = new Object[4];
                    ob[0] = keywords;
                    ob[1] = partType;
                    ob[2] = l2_part_num;
                    ob[3] = topic;
                    logger.info("change the transmit rule for " + db_name + " " + table_name + " " + keywords + " " + partType + " " + l2_part_num + " " + topic);
                    return ob;
                }

                @Override
                public Executor getExecutor() {
                    return null;
                }
            });

            consumer.completeSubscribe();
        } catch (MetaClientException ex) {
            logger.error(ex, ex);
        }
    }
}
