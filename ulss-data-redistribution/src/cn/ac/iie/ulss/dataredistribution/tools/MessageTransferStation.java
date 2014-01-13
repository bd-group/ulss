/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.tools;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.handler.DataSenderThread;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.client.HttpClient;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class MessageTransferStation {

    static ArrayList<Rule> ruleSet = null;
    static Integer sendPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_POOL_SIZE);
    static Integer sendThreadPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_THREAD_POOL_SIZE);
    private static Map<RNode, Object> messageTransferStation = new ConcurrentHashMap<RNode, Object>();
    static ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = null;
    static Map<String, HttpClient> topicToHttpclient = null;
    static Map<String, ThreadGroup> topicToSendThreadPool = (Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL);
    static Map<RNode, Rule> nodeToRule = (Map<RNode, Rule>) RuntimeEnv.getParam(GlobalVariables.NODE_TO_RULE);
    static Map<String, ArrayList<RNode>> topicToNodes = (Map<String, ArrayList<RNode>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_NODES);
    static Map<String, AtomicLong> topicToPackage = (Map<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_PACKAGE);
    static ConcurrentHashMap<String, AtomicLong> ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(MessageTransferStation.class.getName());
    }

    /**
     *
     * initialise the message transfer station
     */
    public static void init(ConcurrentHashMap<String, ArrayList<Rule>> topics) {
        logger.info("init the messageTransferStation");

        Set topicset = topics.keySet();
        String topickey = null;
        topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
        topicToHttpclient = (Map<String, HttpClient>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_HTTPCLIENT);
        for (Iterator it = topicset.iterator(); it.hasNext();) {
            topickey = (String) it.next();

            AtomicLong pac = new AtomicLong(0);
            topicToPackage.put(topickey, pac);
                    
            HttpClient httpclient = HttpConnectionManager.getHttpClient(sendThreadPoolSize);
            topicToHttpclient.put(topickey, httpclient);

            AtomicLong acceptCount = new AtomicLong(0);
            topicToAcceptCount.put(topickey, acceptCount);

            ArrayList<RNode> alr = new ArrayList<RNode>();
            topicToNodes.put(topickey, alr);

            ThreadGroup sendThreadPool = new ThreadGroup(topickey);
            topicToSendThreadPool.put(topickey, sendThreadPool);
            ruleSet = (((ConcurrentHashMap<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES)).get(topickey));
            for (Rule rule : ruleSet) {
                AtomicLong rulecount = new AtomicLong(0);
                ruleToCount.put(rule.getTopic() + rule.getServiceName(), rulecount);
                
                if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3 || rule.getType() == 100) {
                    ArrayList nodeurls = rule.getNodeUrls();
                    for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                        RNode node = (RNode) itit.next();
                        nodeToRule.put(node, rule);
                        alr.add(node);
                        if (!messageTransferStation.containsKey(node)) {
                            int datasendersize = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATASENDER_THREAD);
                            ConcurrentLinkedQueue clq = new ConcurrentLinkedQueue();
                            messageTransferStation.put(node, clq);
                            String value = null;
                            for (int i = 0; i < datasendersize; i++) {
                                DataSenderThread dst = new DataSenderThread(clq, node, rule, value);
                                Thread tdst = new Thread(dst);
                                tdst.setName("DataSenderThread-" + topickey + "-" + node.getName());
                                tdst.start();
                            }
                        }
                    }
                } else if (rule.getType() == 4) {
                    ArrayList nodeurls = rule.getNodeUrls();
                    for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                        RNode node = (RNode) itit.next();
                        nodeToRule.put(node, rule);
                        alr.add(node);
                        if (!messageTransferStation.containsKey(node)) {
                            ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = new ConcurrentHashMap<String, ConcurrentLinkedQueue>();
                            messageTransferStation.put(node, chm);
                        }
                    }
                }
            }
        }
    }

    /**
     *
     * add rule to the transfer station
     */
    public static void addRule(Rule rule) {
        ArrayList<RNode> alr = topicToNodes.get(rule.getTopic());
        synchronized (alr) {
            if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3 || rule.getType() == 100) {
                ArrayList nodeurls = rule.getNodeUrls();
                for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                    RNode node = (RNode) itit.next();
                    nodeToRule.put(node, rule);
                    alr.add(node);
                    if (!messageTransferStation.containsKey(node)) {
                        int datasendersize = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATASENDER_THREAD);
                        ConcurrentLinkedQueue clq = new ConcurrentLinkedQueue();
                        messageTransferStation.put(node, clq);
                        String value = null;
                        for (int i = 0; i < datasendersize; i++) {
                            DataSenderThread dst = new DataSenderThread(clq, node, rule, value);
                            Thread tdst = new Thread(dst);
                            tdst.setName("DataSenderThread-" + rule.getTopic() + "-" + node.getName());
                            tdst.start();
                        }
                    }
                }
            } else if (rule.getType() == 4) {
                ArrayList nodeurls = rule.getNodeUrls();
                for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                    RNode node = (RNode) itit.next();
                    nodeToRule.put(node, rule);
                    alr.add(node);
                    if (!messageTransferStation.containsKey(node)) {
                        ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = new ConcurrentHashMap<String, ConcurrentLinkedQueue>();
                        messageTransferStation.put(node, chm);
                    }
                }
            }
        }
    }

    public static Map<RNode, Object> getMessageTransferStation() {
        return messageTransferStation;
    }
}
