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
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class HandlerDetectNodeThread implements Runnable {

    ConcurrentLinkedQueue<Object[]> detectNodeList = null;
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(HandlerDetectNodeThread.class.getName());
    }
    private Iterable<RNode> key;

    public HandlerDetectNodeThread(ConcurrentLinkedQueue<Object[]> detectNodeList) {
        this.detectNodeList = detectNodeList;
    }

    /**
     *
     * detect the nodes in the detectNodeList
     */
    @Override
    public void run() {
        while (true) {
            if (detectNodeList.isEmpty()) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            } else {
                Object[] o = detectNodeList.poll();
                if (o != null) {
                    RNode n = (RNode) o[0];
                    Rule r = (Rule) o[1];
                    byte[] data = (byte[]) o[2];

                    ArrayList<RNode> detectNode = (ArrayList<RNode>) RuntimeEnv.getParam(GlobalVariables.DETECT_NODE);

                    synchronized (GlobalVariables.SYN_DETECT_NODE) {
                        if (!detectNode.contains(n)) {
                            DetectNodeThread d = new DetectNodeThread(n, r, data);
                            Thread td = new Thread(d);
                            td.start();
                        }
                    }
                }
            }
        }
    }
}
