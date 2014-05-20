/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class CountThread implements Runnable {

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd:HH");
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(CountThread.class.getName());
    }

    @Override
    public void run() {
        Calendar cal = Calendar.getInstance();
        cal.set(12, 0);
        cal.set(13, 0);
        cal.set(14, 0);
        Long mtime = cal.getTimeInMillis();
        ConcurrentHashMap<String, AtomicLong> topicToAcceptCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_ACCEPTCOUNT);
        ConcurrentHashMap<String, AtomicLong> ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
        ConcurrentHashMap<String, AtomicLong> ruleToFilterCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_FILTERCOUNT);
        Calendar newcal = null;
        Date date = null;
        String time = null;
        while (true) {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException ex) {
                //logger.error(ex, ex);
            }
            newcal = Calendar.getInstance();
            newcal.set(12, 0);
            newcal.set(13, 0);
            newcal.set(14, 0);
            if (newcal.getTimeInMillis() > mtime) {
                date = new Date();
                time = dateFormat.format(date);
                for (String topic : topicToAcceptCount.keySet()) {
                    logger.info(time + " this hour final accept " + topicToAcceptCount.get(topic) + " messages from " + topic);
                }

                for (String rule : ruleToCount.keySet()) {
                    logger.info(time + " this hour final send " + ruleToCount.get(rule) + " messages for " + rule);
                }

                for (String rule : ruleToFilterCount.keySet()) {
                    logger.info(time + " this hour final filter " + ruleToFilterCount.get(rule) + " messages for " + rule);
                }

                for (String topic : topicToAcceptCount.keySet()) {
                    topicToAcceptCount.get(topic).set(0L);
                }

                for (String rule : ruleToCount.keySet()) {
                    ruleToCount.get(rule).set(0L);
                }

                for (String rule : ruleToFilterCount.keySet()) {
                    ruleToFilterCount.get(rule).set(0L);
                }

                mtime = newcal.getTimeInMillis();
            }
            date = new Date();
            time = dateFormat.format(date);
            for (String topic : topicToAcceptCount.keySet()) {
                logger.info(time + " this hour accept " + topicToAcceptCount.get(topic) + " messages from " + topic);
            }

            for (String rule : ruleToCount.keySet()) {
                logger.info(time + " this hour send " + ruleToCount.get(rule) + " messages for " + rule);
            }

            for (String rule : ruleToFilterCount.keySet()) {
                logger.info(time + " this hour filter " + ruleToFilterCount.get(rule) + " messages for " + rule);
            }
        }
    }
}
