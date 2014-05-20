package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class CreateFileThread implements Runnable {

    Rule rule = null;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    org.apache.log4j.Logger logger = null;

    {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(CreateFileThread.class.getName());
    }

    public CreateFileThread(Rule rule) {
        this.rule = rule;
    }

    @Override
    public void run() {
        String unit = (rule.getPartType().split("\\|"))[3];
        String interval = (rule.getPartType().split("\\|"))[4];
        Long nowtime = System.currentTimeMillis() / 1000;
        String keyinterval = getKeyInterval(nowtime, unit, interval);
        ArrayList nodeurls = rule.getNodeUrls();
        for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
            RNode node = (RNode) itit.next();
            GetFileFromMetaStore gffm = new GetFileFromMetaStore(keyinterval, node, rule);
            gffm.getFileForInverval();
        }
        String keyinterval2 = getNextKeyInterval(nowtime, unit, interval, 1);
        for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
            RNode node = (RNode) itit.next();
            GetFileFromMetaStore gffm = new GetFileFromMetaStore(keyinterval2, node, rule);
            gffm.getFileForInverval();
        }

        long next = 1;
        String nextKeyinterval = getNextKeyInterval(nowtime, unit, interval, next);
        long sleeptime = getSleeptime(unit, interval);
        try {
            Thread.sleep(sleeptime);
        } catch (InterruptedException ex) {
        }
        while (true) {
            nowtime = System.currentTimeMillis() / 1000;
            if (!getKeyInterval(nowtime, unit, interval).endsWith(nextKeyinterval)) {
                try {
                    Thread.sleep(sleeptime);
                } catch (InterruptedException ex) {
                }
                continue;
            }

            logger.info("start createfilethread server");
            unit = (rule.getPartType().split("\\|"))[3];
            interval = (rule.getPartType().split("\\|"))[4];
            sleeptime = getSleeptime(unit, interval);
            nextKeyinterval = getNextKeyInterval(nowtime, unit, interval, next);
            nodeurls = rule.getNodeUrls();
            for (Iterator itit = nodeurls.iterator(); itit.hasNext();) {
                RNode node = (RNode) itit.next();
                GetFileFromMetaStore gffm = new GetFileFromMetaStore(nextKeyinterval, node, rule);
                gffm.getFileForInverval();
            }
        }
    }

    /**
     *
     * get the keyinterval by time ,unit and interval
     */
    private String getKeyInterval(Long time, String unit, String interval) {
        String st = null;
        String et = null;

        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000);
        String keyinterval = null;

        if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位
            cal.set(14, 0);
            cal.set(13, 0);
            cal.set(12, cal.get(12) - cal.get(12) % Integer.parseInt(interval));
            st = dateFormat.format(cal.getTime());
            if (cal.get(12) + Integer.parseInt(interval) > 60) {
                cal.set(12, 60);
            } else {
                cal.set(12, cal.get(12) + Integer.parseInt(interval));
            }
            et = dateFormat.format(cal.getTime());
            keyinterval = st + "|" + et;
        } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
            cal.set(14, 0);
            cal.set(13, 0);
            cal.set(12, 0);
            cal.set(11, cal.get(11) - cal.get(11) % Integer.parseInt(interval));
            st = dateFormat.format(cal.getTime());
            if (cal.get(11) + Integer.parseInt(interval) > 24) {
                cal.set(11, 24);
            } else {
                cal.set(11, cal.get(11) + Integer.parseInt(interval));
            }
            et = dateFormat.format(cal.getTime());
            keyinterval = st + "|" + et;
        } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
            cal.set(14, 0);
            cal.set(13, 0);
            cal.set(12, 0);
            cal.set(11, 0);
            cal.set(6, cal.get(6) - cal.get(6) % Integer.parseInt(interval));
            st = dateFormat.format(cal.getTime());
            cal.set(6, cal.get(6) + Integer.parseInt(interval));
            et = dateFormat.format(cal.getTime());
            keyinterval = st + "|" + et;
        } else {
            logger.error("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
        }
        return keyinterval;
    }

    private String getNextKeyInterval(Long nowtime, String unit, String interval, long next) {
        String keyinterval = null;
        if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位
            nowtime += Integer.parseInt(interval) * 60 * next;
            keyinterval = getKeyInterval(nowtime, unit, interval);
        } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
            nowtime += Integer.parseInt(interval) * 60 * 60 * next;
            keyinterval = getKeyInterval(nowtime, unit, interval);
        } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
            nowtime += Integer.parseInt(interval) * 60 * 60 * 24 * next;
            keyinterval = getKeyInterval(nowtime, unit, interval);
        } else {
            logger.error("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
        }
        return keyinterval;
    }

    private long getSleeptime(String unit, String interval) {
        Long sleeptime = 1000L;
        if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位
            sleeptime = Long.parseLong(interval) * 60 * 1000 / 10;
        } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
            sleeptime = Long.parseLong(interval) * 60 * 60 * 1000 / 10;
        } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
            sleeptime = Long.parseLong(interval) * 60 * 60 * 24 * 1000 / 10;
        } else {
            logger.error("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
        }
        return sleeptime;
    }
}
