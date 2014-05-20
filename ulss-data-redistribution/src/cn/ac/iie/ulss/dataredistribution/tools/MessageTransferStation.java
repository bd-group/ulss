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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class MessageTransferStation {

    static ArrayList<Rule> ruleSet = null;
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
    public static void init(ConcurrentHashMap<String, ArrayList<Rule>> topics) {
        logger.info("init the messageTransferStation");
        for (String topickey : topics.keySet()) {
            ruleSet = (((ConcurrentHashMap<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES)).get(topickey));
            for (Rule rule : ruleSet) {
                if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3 || rule.getType() == 100) {
                    ArrayList nodeurls = rule.getNodeUrls();
                    for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                        RNode node = (RNode) itit.next();
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
                        if (!messageTransferStation.containsKey(node)) {
                            ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = new ConcurrentHashMap<String, ConcurrentLinkedQueue>();
                            messageTransferStation.put(node, chm);
                        }
                    }     
                }
            }
        }
    }

//    /**
//     *
//     * add rule to the transfer station
//     */
//    public static void addRule(Rule rule) {
//        ArrayList<RNode> alr = topicToNodes.get(rule.getTopic());
//        synchronized (alr) {
//            if (rule.getType() == 0 || rule.getType() == 1 || rule.getType() == 2 || rule.getType() == 3 || rule.getType() == 100) {
//                ArrayList nodeurls = rule.getNodeUrls();
//                for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
//                    RNode node = (RNode) itit.next();
//                    nodeToRule.put(node, rule);
//                    alr.add(node);
//                    if (!messageTransferStation.containsKey(node)) {
//                        int datasendersize = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATASENDER_THREAD);
//                        ConcurrentLinkedQueue clq = new ConcurrentLinkedQueue();
//                        messageTransferStation.put(node, clq);
//                        String value = null;
//                        for (int i = 0; i < datasendersize; i++) {
//                            DataSenderThread dst = new DataSenderThread(clq, node, rule, value);
//                            Thread tdst = new Thread(dst);
//                            tdst.setName("DataSenderThread-" + rule.getTopic() + "-" + node.getName());
//                            tdst.start();
//                        }
//                    }
//                }
//            } else if (rule.getType() == 4) {
//                ArrayList nodeurls = rule.getNodeUrls();
//                for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
//                    RNode node = (RNode) itit.next();
//                    nodeToRule.put(node, rule);
//                    alr.add(node);
//                    if (!messageTransferStation.containsKey(node)) {
//                        ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = new ConcurrentHashMap<String, ConcurrentLinkedQueue>();
//                        messageTransferStation.put(node, chm);
//                    }
//                }
//                
//                CreateFileThread cft = new CreateFileThread(rule);
//                Thread tcft = new Thread(cft);
//                tcft.setName("CreateFileThread-" + rule.getTopic() + rule.getServiceName());
//                tcft.start();
//            }
//        }
//    }

    public static Map<RNode, Object> getMessageTransferStation() {
        return messageTransferStation;
    }
}
