/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.monitorhandler;

import cn.ac.iie.ulss.match.worker.Matcher;
import cn.ac.iie.ulss.struct.CDRRecordNode;
import cn.ac.iie.ulss.util.FileFilter;
import cn.ac.iie.ulss.util.SimpleHash;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 *
 * @author liucuili
 */
public class MonitorHandler extends AbstractHandler {

    public static Logger log = Logger.getLogger(MonitorHandler.class.getName());
    private static MonitorHandler monitorHandler = null;
    private static ConcurrentHashMap< String, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>> cdrMap;

    @Override
    public void handle(String string, Request rqst, HttpServletRequest hsr, HttpServletResponse hsResonse) {
        rqst.setHandled(true);
        String opType = rqst.getParameter("op");
        String responseContent = "";
        try {
            if (opType.equalsIgnoreCase("getPosition")) {
                String paras = rqst.getParameter("phone");
                String region = rqst.getParameter("region");

                log.info("now receive the request " + opType + "and the para is " + region + "." + paras);
                if (paras == null || paras.isEmpty()) {
                    responseContent = "parameter phonenumber is not defined";
                } else {
                    if (responseContent == null) {
                        responseContent = "no such phonenumber named " + paras;
                    } else {
                        long bucket = SimpleHash.getSimpleHash(paras) % Matcher.topMapSize;
                        List<CDRRecordNode> CDRRecords = MonitorHandler.cdrMap.get(region).get(bucket).get(paras);
                        if (CDRRecords != null && CDRRecords.size() > 0) {
                            for (CDRRecordNode node : CDRRecords) {
                                responseContent += node.toString() + "\n";
                            }
                        } else if (CDRRecords == null || CDRRecords.isEmpty()) {
                            responseContent = "no position information for " + paras;
                        }
                    }
                }
            } else if (opType.equalsIgnoreCase("getSize")) {
                long cdrKeyCount = 0;
                String region = rqst.getParameter("region");

                if (region != null && (!"".equalsIgnoreCase(region))) {
                    ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> tmp = cdrMap.get(region);
                    if (tmp != null) {
                        for (long i : tmp.keySet()) {
                            cdrKeyCount += tmp.get(i).size();
                        }
                        responseContent = "now in the map,the phonenumber size is: " + cdrKeyCount;
                    } else {
                        responseContent = "please check your region paras ";
                    }
                } else {
                    responseContent = "please check your region paras ";
                }
            } else if (opType.equalsIgnoreCase("listAllNumber")) {
                String numbers = "";
                String paras = rqst.getParameter("count");
                String region = rqst.getParameter("region");

                if (region != null && (!"".equalsIgnoreCase(region))) {
                    ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> tmpMap = cdrMap.get(region);
                    ConcurrentHashMap<String, List<CDRRecordNode>> tmp = null;
                    if (tmpMap != null) {
                        if (paras == null || "".equalsIgnoreCase(paras.trim())) {
                            for (long i : tmpMap.keySet()) {
                                tmp = tmpMap.get(i);
                                for (String phone : tmp.keySet()) {
                                    numbers += phone + "\n";
                                }
                            }
                            responseContent = "now in the map,the phonenumber list is:\n " + numbers;
                        } else {
                            int count = 0;
                            int requestCount = Integer.parseInt(paras.trim());
                            boolean isdone = false;
                            for (long i : tmpMap.keySet()) {
                                if (isdone) {
                                    break;
                                }
                                tmp = tmpMap.get(i);
                                for (String phone : tmp.keySet()) {
                                    numbers += phone + "\n";
                                    count++;
                                    if (count >= requestCount) {
                                        isdone = true;
                                        break;
                                    }
                                }
                            }
                            responseContent = "now in the map,the phonenumber list is:\n " + numbers;
                        }
                    } else {
                        responseContent = "please check your region paras ";
                    }
                } else {
                    responseContent = "please check your region paras ";
                }
            } else if (opType.equalsIgnoreCase("listDetail")) {
                String detail = "";
                String paras = rqst.getParameter("count");
                String region = rqst.getParameter("region");

                ConcurrentHashMap<String, List<CDRRecordNode>> tmp = null;
                List<CDRRecordNode> tmpList = null;
                if (region != null && (!"".equalsIgnoreCase(region))) {
                    ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>> tmpMap = cdrMap.get(region);
                    if (tmpMap != null) {
                        if (paras == null || "".equalsIgnoreCase(paras.trim())) {
                            for (long i : tmpMap.keySet()) {
                                tmp = tmpMap.get(i);
                                for (String phone : tmp.keySet()) {
                                    detail += phone + ":: ";
                                    tmpList = tmp.get(phone);
                                    for (CDRRecordNode c : tmpList) {
                                        detail += c.toString() + " ; ";
                                    }
                                    detail += "\n";
                                }
                            }
                            responseContent = "now in the map the phonenumber and position detail is:\n " + detail;
                        } else {
                            int count = 0;
                            int requestCount = Integer.parseInt(paras.trim());
                            boolean isdone = false;
                            for (long i : tmpMap.keySet()) {
                                tmp = tmpMap.get(i);
                                if (isdone) {
                                    break;
                                }
                                for (String phone : tmp.keySet()) {
                                    detail += phone + ":: ";
                                    tmpList = tmp.get(phone);
                                    for (CDRRecordNode c : tmpList) {
                                        detail += c.toString() + " ; ";
                                    }
                                    detail += "\n";
                                    count++;
                                    if (count >= requestCount) {
                                        isdone = true;
                                        break;
                                    }
                                }
                            }
                            responseContent = "now in the map the phonenumber and position detail is:\n " + detail;
                        }
                    } else {
                        responseContent = "please check your region paras ";
                    }
                } else {
                    responseContent = "please check your region paras ";
                }
            } else if (opType.equalsIgnoreCase("dumpposition")) {
                String region = rqst.getParameter("region");
                if (region == null || "".equalsIgnoreCase(region)) {
                    for (String s : Matcher.cdrupdaters.keySet()) {
                        log.info("now will dump the position data for " + s);
                        Matcher.cdrupdaters.get(s).isDumping.set(true);
                    }
                } else {
                    Matcher.cdrupdaters.get(region).isDumping.set(true);
                }
                responseContent = "it is doing the dump operation,please wait ...";
            } else {
                responseContent = "unknown operation";
            }
        } catch (Exception e) {
            log.error(e, e);
            responseContent = null;
            responseContent = e.toString();
        }

        try {
            hsResonse.setStatus(HttpServletResponse.SC_OK);
            hsResonse.getWriter().println(responseContent);
        } catch (IOException ex) {
            log.error(ex, ex);
        }
    }

    public static MonitorHandler getMonitorHandler(ConcurrentHashMap< String, ConcurrentHashMap<Long, ConcurrentHashMap<String, List<CDRRecordNode>>>> map) {
        monitorHandler = new MonitorHandler();
        MonitorHandler.cdrMap = map;
        return monitorHandler;
    }

    public static void main(String[] args) throws IOException {
        File file = new File(".");
        String[] nameList = file.list(new FileFilter(".*\\.ori"));
        System.out.println(nameList);
        System.out.println(nameList[0]);
    }
}