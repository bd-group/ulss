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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class PrintEnvironment implements Runnable {

    Map<String, ArrayList<Rule>> topicToRules = null;
    Map<String, Map<String, AtomicLong>> ruleToThreadPoolSize = null;
    Map<String, AtomicLong> serviceToThreadSize = null;
    ConcurrentLinkedQueue[] dataPool = null;
    Map<String, ConcurrentLinkedQueue[]> topicToDataPool = null;
    Map<String, AtomicLong> topicToPackage = null;
    AtomicLong packagecount = null;
    ConcurrentLinkedQueue<Object[]> strandedDataTransmit = null;
    org.apache.log4j.Logger logger = null;

    {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(PrintEnvironment.class.getName());
    }

    @Override
    public void run() {
        init();
        StringBuffer threadSize = new StringBuffer();
        StringBuffer datapoolsize = new StringBuffer();
        while (true) {
            for (String topic : topicToRules.keySet()) {
                serviceToThreadSize = ruleToThreadPoolSize.get(topic);
                dataPool = topicToDataPool.get(topic);
                packagecount = topicToPackage.get(topic);
                if (serviceToThreadSize == null || dataPool == null || packagecount == null) {
                    continue;
                }
                threadSize.delete(0, threadSize.length());
                for (String str : serviceToThreadSize.keySet()) {
                    threadSize.append(" ");
                    threadSize.append(str);
                    threadSize.append(" ");
                    threadSize.append(serviceToThreadSize.get(str));
                }

                datapoolsize.delete(0, datapoolsize.length());
                for (int j = 0; j < dataPool.length; j++) {
                    datapoolsize.append(" ");
                    datapoolsize.append(dataPool[j].size());
                }

                logger.info(topic + " dataPool's size is" + datapoolsize + " and the packagecount is " + packagecount.get() + " "
                        + threadSize + " and the nodeNums of the MessageTransferStation is " + MessageTransferStation.getMessageTransferStation().size() + " and the num of queue in the "
                        + "MessageTransferStation is " + numOfQueues() + " starndedDataTransmit'size is " + strandedDataTransmit.size());
            }

            try {
                Thread.sleep(5000);
            } catch (Exception ex) {
//                Logger.getLogger(PrintEnvironment.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void init() {
        topicToRules = (Map<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES);
        ruleToThreadPoolSize = (Map<String, Map<String, AtomicLong>>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_THREADPOOLSIZE);
        topicToDataPool = (Map<String, ConcurrentLinkedQueue[]>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_DATAPOOL);
        topicToPackage = (Map<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_PACKAGE);
        strandedDataTransmit = (ConcurrentLinkedQueue<Object[]>) RuntimeEnv.getParam(GlobalVariables.STRANDED_DATA_TRANSMIT);
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
