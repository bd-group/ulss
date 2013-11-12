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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class MessageTransferStation {

    static ArrayList<Rule> ruleSet = null;
    static Integer sendPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_POOL_SIZE);
    private static Map<RNode, Object> messageTransferStation = new ConcurrentHashMap<RNode, Object>();
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(MessageTransferStation.class.getName());
    }

    /**
     *
     * initialise the message transfer station
     */
    public static void init(ConcurrentHashMap<String,ArrayList<Rule>> topics) {
        logger.info("init the messageTransferStation");
        Map<String, ThreadGroup> topicToSendThreadPool = (Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL);
        Map<RNode, Rule> nodeToRule = (Map<RNode, Rule>) RuntimeEnv.getParam(GlobalVariables.NODE_TO_RULE);
        Set topicset = topics.keySet();
        String topickey = null;
        for (Iterator it = topicset.iterator(); it.hasNext();) {
            topickey = (String) it.next();      
            ConcurrentHashMap<String, AtomicLong[]> topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong[]>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
            AtomicLong[] al = new AtomicLong[2];
            al[0] = new AtomicLong(0);
            al[1] = new AtomicLong(0);
            topicToAcceptCount.put(topickey, al);

            ThreadGroup sendThreadPool = new ThreadGroup(topickey);
            topicToSendThreadPool.put(topickey, sendThreadPool);
            ruleSet = (((ConcurrentHashMap<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES)).get(topickey));
            for (Rule rule : ruleSet) {
                if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3 || rule.getType() == 100) {
                    ArrayList nodeurls = rule.getNodeUrls();
                    for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                        RNode node = (RNode) itit.next();
                        nodeToRule.put(node, rule);
                        if (!messageTransferStation.containsKey(node)) {
                            ArrayBlockingQueue abq = new ArrayBlockingQueue(sendPoolSize);
                            messageTransferStation.put(node, abq);
                            String value = null;
                            DataSenderThread dst = new DataSenderThread(abq, sendPoolSize, node, rule.getTopic(), rule.getServiceName(), sendThreadPool, rule, value);
                            Thread tdst = new Thread(dst);
                            tdst.start();
                        }
                    }
                } else if (rule.getType() == 4) {
                    String[] pt = rule.getPartType().split("\\|");

                    if (pt.length == 4) {
                        ArrayList nodeurls = rule.getNodeUrls();
                        for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                            RNode node = (RNode) itit.next();
                            nodeToRule.put(node, rule);
                            if (!messageTransferStation.containsKey(node)) {
                                ConcurrentHashMap<String, ArrayBlockingQueue> chm = new ConcurrentHashMap<String, ArrayBlockingQueue>();
                                messageTransferStation.put(node, chm);
                            }
                        }
                    }
                }
            }
        }
    }

    public static Map<RNode, Object> getMessageTransferStation() {
        return messageTransferStation;
    }
}
