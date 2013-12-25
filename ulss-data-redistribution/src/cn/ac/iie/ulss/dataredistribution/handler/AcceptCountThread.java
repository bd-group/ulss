/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
class AcceptCountThread implements Runnable{

    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(AcceptCountThread.class.getName());
    }

    @Override
    public void run() {
        ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
        while(true){
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                logger.error(ex,ex);
            }
            
            for(String topic : topicToAcceptCount.keySet()){
                logger.info("accept " + topicToAcceptCount.get(topic) + " messages from the topic " + topic);
            }
        }
    }
}
