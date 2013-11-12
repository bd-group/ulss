/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.MessageTransferStation;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
class RemoveNodeFromMessageTransferStation implements Runnable {

    ArrayList<RNode> nurl = null;
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(GetRuleFromDBThread.class.getName());
    }

    public RemoveNodeFromMessageTransferStation(ArrayList<RNode> nurl) {
        this.nurl = nurl;
    }

    @Override
    public void run() {
        Map<RNode, Object> messageTransferStation = MessageTransferStation.getMessageTransferStation();

        for (RNode n : nurl) {
            ConcurrentHashMap<String, ArrayBlockingQueue> chm = (ConcurrentHashMap<String, ArrayBlockingQueue>) messageTransferStation.get(n);
            while (true) {
                if (chm.isEmpty()) {
                    synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_MESSAGETRANSFERSTATION)) {
                        messageTransferStation.remove(n);
                    }
                    logger.info("messageTransmitStation remove the node " + n.getName());
                    break;
                } else {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        logger.error(ex, ex);
                    }
                }
            }
        }
    }
}
