/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.http.HttpResponse;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class updateNodeToUrls implements Runnable {

    ArrayList<RNode> nurl = null;
    String[] IPList = null;
    Rule r = null;
    Map<RNode, String> nodeToIP = null;
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(updateNodeToUrls.class.getName());
    }

    public updateNodeToUrls(Rule r) {
        this.r = r;
        nurl = r.getNodeUrls();
        IPList = r.getIPList();
    }

    @Override
    public void run() {
        String[] ts = r.getPartType().split("\\:");
        if (ts.length == 1) {
            Date d = new Date();
            int minute = d.getMinutes();
            int second = d.getSeconds();
            long t = (minute * 60 + second) * 1000;
            try {
                Thread.sleep(3600000 - t + 100);
            } catch (InterruptedException ex) {
                logger.error(ex, ex);
            }

            while (true) {
                logger.info("begin to rechoose the nodes to send for " + r.getTopic() + " " + r.getServiceName());
                Map<Rule, String> ruleToControl = (Map<Rule, String>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_CONTROL);
                ConcurrentHashMap<RNode, Integer> nodeToThreadNum = (ConcurrentHashMap<RNode, Integer>) RuntimeEnv.getParam(GlobalVariables.NODE_TO_THREADNUM);
                if (ruleToControl.get(r).equals("start")) {
                    ruleToControl.put(r, "stop");
                }

                for (RNode n : nurl) {
                    while (nodeToThreadNum.get(n) > 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            logger.error(ex, ex);
                        }
                    }
                }

                nodeToIP = null;
                nurl = r.getNodeUrls();
                IPList = r.getIPList();
                Map<RNode, String> oldNodeToIP = r.getNodeToIP();
                ArrayList<String> deadIP = new ArrayList<String>();

                ArrayList<String> oldiplist = new ArrayList<String>();
                for (RNode n : nurl) {
                    String ip = oldNodeToIP.get(n);
                    if (ip == null) {
                        continue;
                    }
                    int flag = 0;
                    for (String si : oldiplist) {
                        if (si.equals(ip)) {
                            flag = 1;
                            break;
                        }
                    }
                    if (flag == 0) {
                        oldiplist.add(ip);
                        Date date = new Date();
                        int hour = date.getHours() - 1;
                        date.setSeconds(0);
                        date.setMinutes(0);
                        String time = updateNodeToUrls.dateFormat.format(date);
                        SendControl sc = new SendControl(ip, "stop", "db1", time, "1");
                        HttpResponse hr = sc.send();
                    }
                }

                if (IPList.length <= 0) {
                    r.setNodeToIP(nodeToIP);
                } else {
                    Map<RNode, String> nodeToIP = new HashMap<RNode, String>();

                    ArrayList<String> ips = new ArrayList<String>();
                    for (int p = 0; p < IPList.length; p++) {
                        if (!deadIP.contains(IPList[p])) {
                            ips.add(IPList[p]);
                        }
                    }
                    for (int i = 0; i < nurl.size(); i++) {
                        while (true) {
                            if (deadIP.size() >= IPList.length) {
                                logger.info("all the IPList for " + r.getTopic() + " " + r.getServiceName() + " cann't be connectted");
                                break;
                            } else if (ips.isEmpty()) {
                                ips = new ArrayList<String>();
                                for (int p = 0; p < IPList.length; p++) {
                                    if (!deadIP.contains(IPList[p])) {
                                        ips.add(IPList[p]);
                                    }
                                }
                            } else {
                                Random ran = new Random();
                                int next = ran.nextInt(ips.size());
                                Date date = new Date();
                                date.setSeconds(0);
                                date.setMinutes(0);
                                String time = updateNodeToUrls.dateFormat.format(date);

                                SendControl sc = new SendControl(ips.get(next), "start", "db1", time, "" + i);
                                HttpResponse hr = sc.send();
                                if (hr != null) {
                                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                                    try {
                                        hr.getEntity().writeTo(out);
                                    } catch (IOException ex) {
                                        logger.error(ex, ex);
                                    }
                                    String resonseEn = new String(out.toByteArray());
                                    if ("-1".equals(resonseEn.split("[\n]")[0])) {
                                        logger.error(resonseEn.split("[\n]")[1]);
                                        deadIP.add(ips.get(next));
                                        ips.remove(next);
                                    } else {
                                        nodeToIP.put((RNode) nurl.get(i), ips.get(next));
                                        logger.info(((RNode) nurl.get(i)).getName() + ": " + ips.get(next));
                                        ips.remove(next);
                                        break;
                                    }
                                } else {
                                    deadIP.add(ips.get(next));
                                    ips.remove(next);
                                }
                            }
                        }
                    }
                    r.setNodeToIP(nodeToIP);
                }

                r.setDeadIP(deadIP);
                if (ruleToControl.get(r).equals("stop")) {
                    ruleToControl.put(r, "start");
                }

                d = new Date();
                minute = d.getMinutes();
                second = d.getSeconds();
                t = (minute * 60 + second) * 1000;
                try {
                    Thread.sleep(3600000 - t);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        } else if (ts.length >= 3) {
            String unit = ts[0];
            int interval = Integer.parseInt(ts[1]);
            long partitionInverval = 0l;
            long waitt = 0l;
            Date d = new Date();
            int hour = d.getHours();
            int minute = d.getMinutes();
            int second = d.getSeconds();
            int date = d.getDate();

            if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位
                waitt = ((interval - 1 - (minute % interval)) * 60 + 60 - second) * 1000;
            } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
                waitt = ((interval - 1 - hour % interval) * 60 * 60 + (59 - minute) * 60 + 60 - second) * 1000;
            } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
                waitt = ((interval - 1 - date % interval) * 24 * 60 * 60 + (23 - hour) * 60 * 60 + (59 - minute) * 60 + 60 - second) * 1000;
            } else {
                throw new RuntimeException("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
            }
            try {
                Thread.sleep(waitt);
            } catch (InterruptedException ex) {
                logger.error(ex, ex);
            }

            while (true) {
                logger.info("begin to rechoose the nodes for " + r.getTopic() + " " + r.getServiceName() + " to send");
                Map<Rule, String> ruleToControl = (Map<Rule, String>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_CONTROL);
                Map<RNode, Integer> nodeToThreadNum = (Map<RNode, Integer>) RuntimeEnv.getParam(GlobalVariables.NODE_TO_THREADNUM);
                if (ruleToControl.get(r).equals("start")) {
                    ruleToControl.put(r, "stop");
                }

                for (RNode n : nurl) {
                    while (nodeToThreadNum.get(n) > 0) {
                        try {
                            logger.info("waitting for the nodeToThreadNum change to zero");
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            logger.error(ex, ex);
                        }
                    }
                }

                nodeToIP = null;
                nurl = r.getNodeUrls();
                IPList = r.getIPList();
                Map<RNode, String> oldNodeToIP = r.getNodeToIP();
                ArrayList<String> deadIP = new ArrayList<String>();

                ArrayList<String> oldiplist = new ArrayList<String>();
                for (RNode n : nurl) {
                    String ip = oldNodeToIP.get(n);
                    if (ip == null) {
                        continue;
                    }
                    int flag = 0;
                    for (String si : oldiplist) {
                        if (si.equals(ip)) {
                            flag = 1;
                            break;
                        }
                    }
                    if (flag == 0) {
                        oldiplist.add(ip);
                        d = new Date();
                        if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位                            
                            d.setMinutes(d.getMinutes() - d.getMinutes() % interval);
                            d.setSeconds(0);
                        } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
                            d.setHours(d.getHours() - d.getDate() % interval);
                            d.setMinutes(0);
                            d.setSeconds(0);
                        } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
                            d.setDate(d.getDate() - d.getDate() % interval);
                            d.setHours(0);
                            d.setMinutes(0);
                            d.setSeconds(0);
                        } else {
                            throw new RuntimeException("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
                        }
                        String time = updateNodeToUrls.dateFormat.format(d);
                        SendControl sc = new SendControl(ip, "stop", "db1", time, "1");
                        HttpResponse hr = sc.send();
                    }
                }

                if (IPList.length <= 0) {
                    r.setNodeToIP(nodeToIP);
                } else {
                    Map<RNode, String> nodeToIP = new HashMap<RNode, String>();

                    ArrayList<String> ips = new ArrayList<String>();
                    for (int p = 0; p < IPList.length; p++) {
                        if (!deadIP.contains(IPList[p])) {
                            ips.add(IPList[p]);
                        }
                    }
                    for (int i = 0; i < nurl.size(); i++) {
                        while (true) {
                            if (deadIP.size() >= IPList.length) {
                                logger.info("all the IPList cann't be connectted");
                                break;
                            }
                            if (ips.isEmpty()) {
                                ips = new ArrayList<String>();
                                for (int p = 0; p < IPList.length; p++) {
                                    if (!deadIP.contains(IPList[p])) {
                                        ips.add(IPList[p]);
                                    }
                                }
                            } else {
                                Random ran = new Random();
                                int next = ran.nextInt(ips.size());
                                d = new Date();
                                if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位                            
                                    d.setMinutes(d.getMinutes() - d.getMinutes() % interval);
                                    d.setSeconds(0);
                                } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
                                    d.setHours(d.getHours() - d.getDate() % interval);
                                    d.setMinutes(0);
                                    d.setSeconds(0);
                                } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
                                    d.setDate(d.getDate() - d.getDate() % interval);
                                    d.setHours(0);
                                    d.setMinutes(0);
                                    d.setSeconds(0);
                                } else {
                                    throw new RuntimeException("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
                                }
                                String time = updateNodeToUrls.dateFormat.format(d);
                                SendControl sc = new SendControl(ips.get(next), "start", "db1", time, "" + i);
                                HttpResponse hr = sc.send();
                                if (hr != null) {
                                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                                    try {
                                        hr.getEntity().writeTo(out);
                                    } catch (IOException ex) {
                                        logger.error(ex, ex);
                                    }
                                    String resonseEn = new String(out.toByteArray());
                                    if ("-1".equals(resonseEn.split("[\n]")[0])) {
                                        logger.error(resonseEn.split("[\n]")[1]);
                                        deadIP.add(ips.get(next));
                                        ips.remove(next);
                                    } else {
                                        nodeToIP.put((RNode) nurl.get(i), ips.get(next));
                                        logger.info(((RNode) nurl.get(i)).getName() + ": " + ips.get(next));
                                        ips.remove(next);
                                        break;
                                    }
                                } else {
                                    deadIP.add(ips.get(next));
                                    ips.remove(next);
                                }
                            }
                        }
                    }
                    r.setNodeToIP(nodeToIP);
                }

                r.setDeadIP(deadIP);
                if (ruleToControl.get(r).equals("stop")) {
                    ruleToControl.put(r, "start");
                }

                d = new Date();
                hour = d.getHours();
                minute = d.getMinutes();
                second = d.getSeconds();
                date = d.getDate();

                if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位
                    waitt = ((interval - 1 - (minute % interval)) * 60 + 60 - second) * 1000;
                } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
                    waitt = ((interval - 1 - hour % interval) * 60 * 60 + (59 - minute) * 60 + 60 - second) * 1000;
                } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
                    waitt = ((interval - 1 - date % interval) * 24 * 60 * 60 + (23 - hour) * 60 * 60 + (59 - minute) * 60 + 60 - second) * 1000;
                } else {
                    throw new RuntimeException("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
                }
                try {
                    Thread.sleep(waitt);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        }
    }
}
