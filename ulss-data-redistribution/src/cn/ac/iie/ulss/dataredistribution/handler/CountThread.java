/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
class CountThread implements Runnable {

    static Date date = null;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(CountThread.class.getName());
    }

    @Override
    public void run() {
        date = new Date();
        date.setMinutes(0);
        date.setSeconds(0);
        ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
        ConcurrentHashMap<String, AtomicLong> ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                logger.error(ex, ex);
            }
            Date newdate = new Date();
            newdate.setMinutes(0);
            newdate.setSeconds(0);
            if (newdate.after(date)) {
                for (String topic : topicToAcceptCount.keySet()) {
                    topicToAcceptCount.get(topic).set(0L);
                }

                for (String rule : ruleToCount.keySet()) {
                    ruleToCount.get(rule).set(0L);
                }
                date = newdate;
            }
            String time = dateFormat.format(date);
            for (String topic : topicToAcceptCount.keySet()) {
                logger.info( time + "this hour accept " + topicToAcceptCount.get(topic) + " messages from " + topic);
            }

            for (String rule : ruleToCount.keySet()) {
                logger.info( time + "this hour send " + ruleToCount.get(rule) + " messages for " + rule);
            }
        }
    }
}
