/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
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
public class DetectAcceptTimeout implements Runnable {

    BlockEmitter[] emitters = null;
    AtomicLong acceptcount = null;
    AtomicLong time = null;
    AtomicLong version = null;
    String topic = null;
    ConcurrentLinkedQueue[] dataPool = null;
    Integer activeThreadCount = 0;
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

    public DetectAcceptTimeout(BlockEmitter[] emitters, String topic, ConcurrentLinkedQueue[] dataPool, AtomicLong acceptcount, AtomicLong version, AtomicLong stime) {
        this.emitters = emitters;
        this.topic = topic;
        this.dataPool = dataPool;
        this.acceptcount = acceptcount;
        this.time = stime;
        this.version = version;
    }

    @Override
    public void run() {
        topicToNodes = (Map<String, ArrayList<RNode>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_NODES);
        topicToPackage = (Map<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_PACKAGE);
        packagecount = topicToPackage.get(topic);
        ruleToThreadPoolSize = (Map<String, Map<String, AtomicLong>>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_THREADPOOLSIZE);
        serviceToThreadSize = ruleToThreadPoolSize.get(topic);

        long localtime = 0L;
        long count = 0L;
        while (true) {
            while (!datapoolisEmpty() || packagecount.get() > activePackageLimit || !isfull() || hasDataToWrite()) { //|| !isEmpty()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    //donothing
                }
                count = 0L;
            }

            if (acceptcount.longValue() > 0) {
                if (time.longValue() != localtime) {
                    localtime = time.longValue();
                    count = 1L;
                } else {
                    count++;
                }

                if (count > 2) {
                    synchronized (version) {
                        acceptcount.set(0);
                        version.incrementAndGet();
                    }
                    for (int j = 0; j < emitters.length; j++) {
                        emitters[j].emit(null, "timeout", 0L);
                    }
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (Exception ex) {
                        //donothing
                    }
                }
            } else {
                try {
                    Thread.sleep(2000);
                } catch (Exception ex) {
                    //donothing
                }
            }
        }
    }

    private boolean hasDataToWrite() {
        ConcurrentHashMap<Rule, ConcurrentLinkedQueue> UnvalidDataStore = (ConcurrentHashMap<Rule, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.UNVALID_DATA_STORE);
        ConcurrentHashMap<String, ConcurrentLinkedQueue> uselessDataStore = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.USELESS_DATA_STORE);
        ConcurrentLinkedQueue clq = uselessDataStore.get(topic);
        if (clq != null) {
            if (!clq.isEmpty()) {
                logger.info("the queue of uselessDataStore for " + topic + " is not empty " + clq.size());
                return true;
            }
        }
        for (Rule r : UnvalidDataStore.keySet()) {
            if (r.getTopic().equals(topic)) {
                ConcurrentLinkedQueue clq2 = UnvalidDataStore.get(r);
                if (clq2 != null) {
                    if (!clq2.isEmpty()) {
                        logger.info("the queue of unvalidDataStore for " + topic + " " + r.getServiceName() + " is not empty " + clq2.size());
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean datapoolisEmpty() {
        for(int i = 0 ; i< dataPool.length ; i ++ ){
            if(!dataPool[i].isEmpty()){
                return false;
            }
        }    
        return true;
    }
    
    private boolean isfull() {
        for (String str : serviceToThreadSize.keySet()) {
            if(serviceToThreadSize.get(str).longValue() > activeThreadCount){
                return false;
            }
        }
        return true;
    }
}
