/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class DetectAcceptTimeout implements Runnable {

    BlockEmitter emitter = null;
    String topic = null;
    ConcurrentLinkedQueue dataPool = null;
    Map<String, ThreadGroup> topicToSendThreadPool = null;
    Integer activeThreadCount = 0;
    ThreadGroup sendThreadPool = null;
    Map<String, ArrayList<RNode>> topicToNodes = null;
    Map<String, AtomicLong> topicToPackage = null;
    AtomicLong packagecount = new AtomicLong(0);
    Long activePackageLimit = 0L;
    Map<String, Map<String, AtomicLong>> ruleToThreadPoolSize = null;
    Map<String, AtomicLong> serviceToThreadSize = null;
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DetectAcceptTimeout.class.getName());
    }

    public DetectAcceptTimeout(BlockEmitter emitter, String topic, ConcurrentLinkedQueue dataPool) {
        this.emitter = emitter;
        this.topic = topic;
        this.dataPool = dataPool;
    }

    @Override
    public void run() {
        topicToSendThreadPool = (Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL);
        sendThreadPool = topicToSendThreadPool.get(topic);
//        activeThreadCount = (Integer) RuntimeEnv.getParam(RuntimeEnv.ACTIVE_THREAD_COUNT);
        topicToNodes = (Map<String, ArrayList<RNode>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_NODES);
        topicToPackage = (Map<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_PACKAGE);
        packagecount = topicToPackage.get(topic);
        ruleToThreadPoolSize = (Map<String, Map<String, AtomicLong>>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_THREADPOOLSIZE);
        serviceToThreadSize = ruleToThreadPoolSize.get(topic);

        long time = 0L;
        long count = 0L;
        StringBuffer threadSize = new StringBuffer();
        while (true) {
            while (!dataPool.isEmpty() || packagecount.get() > activePackageLimit || sendThreadPool.activeCount() > activeThreadCount || !isEmpty()) {
                threadSize.delete(0, threadSize.length());
                for (String str : serviceToThreadSize.keySet()) {
                    threadSize.append(" ");
                    threadSize.append(str);
                    threadSize.append(" ");
                    threadSize.append(serviceToThreadSize.get(str));
                }

                logger.info(topic + " dataPool's size is " + dataPool.size() + " and the packagecount is " + packagecount.get() + " and the activeSendThreadCount's size is " + sendThreadPool.activeCount()
                        + threadSize + " and the nodeNums of the MessageTransferStation is " + MessageTransferStation.getMessageTransferStation().size() + " and the num of queue in the "
                        + "MessageTransferStation is " + numOfQueues());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    //donothing
                }

                count = 0L;
            }

            threadSize.delete(0, threadSize.length());
            for (String str : serviceToThreadSize.keySet()) {
                threadSize.append(" ");
                threadSize.append(str);
                threadSize.append(" ");
                threadSize.append(serviceToThreadSize.get(str));
            }

            logger.info(topic + " dataPool's size is " + dataPool.size() + " and the packagecount is " + packagecount.get() + " and the activeSendThreadCount's size is " + sendThreadPool.activeCount()
                    + threadSize + " and the nodeNums of the MessageTransferStation is " + MessageTransferStation.getMessageTransferStation().size() + " and the num of queue in the "
                    + "MessageTransferStation is " + numOfQueues());

            if (emitter.getCount() > 0) {
                if (emitter.getTime() != time) {
                    time = emitter.getTime();
                    count = 1L;
                } else {
                    count++;
                }

                if (count > 10) {
                    emitter.emit(null, "timeout", 0L);
                } else {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        //donothing
                    }
                }
            } else {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    //donothing
                }
            }
        }

    }

    private boolean isEmpty() {
        Map<RNode, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();
        ArrayList<RNode> alr = topicToNodes.get(topic);
        synchronized (alr) {
            Iterator it = alr.iterator();
            while (it.hasNext()) {
                RNode n = (RNode) it.next();
                if (messageTransferStation.containsKey(n)) {
                    if (n.getType() == 4) {
                        ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) messageTransferStation.get(n);
                        if (chm != null) {
                            for (ConcurrentLinkedQueue clq : chm.values()) {
                                if (!clq.isEmpty()) {
                                    logger.info("the messageTransferStation for " + topic + " is not empty!");
                                    return false;
                                }
                            }
                        } else {
                            logger.info("the chm for " + n + " is null");
                        }
                    } else {
                        ConcurrentLinkedQueue clq = (ConcurrentLinkedQueue) messageTransferStation.get(n);
                        if (clq != null) {
                            if (!clq.isEmpty()) {
                                logger.info("the messageTransferStation for " + topic + " is not empty!");
                                return false;
                            }
                        } else {
                            logger.info("the clq for " + n + " is null");
                        }
                    }
                } else {
                    it.remove();
                }
            }

            logger.info("the messageTransferStation for " + topic + " is empty!");

            Map<String, ArrayList<Rule>> topicToRules = (Map<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES);
            ArrayList<Rule> rules = topicToRules.get(topic);
            for (Rule n : rules) {
                if (n.getNodeUrls().isEmpty()) {
                    return false;
                }
            }

            logger.info("every topic's rule has node");
        }

        return true;
    }

    private int numOfQueues() {
        int count = 0;
        Map<RNode, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();
        for (RNode n : messageTransferStation.keySet()) {
            if (n.getType() == 4) {
                ConcurrentHashMap<String, ConcurrentLinkedQueue> chm = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) messageTransferStation.get(n);
                count += chm.size();
            } else {
                count++;
            }
        }
        return count;
    }
}
