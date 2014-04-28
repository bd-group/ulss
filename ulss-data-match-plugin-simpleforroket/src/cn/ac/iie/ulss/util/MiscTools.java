/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.log4j.Logger;

/**
 *
 * @author liucuili
 */
public class MiscTools {

    public static Logger log = Logger.getLogger(AvroUtils.class.getName());

    public static String getHostName() {
        String nodeName = "";
        try {
            nodeName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            log.error(ex, ex);
        }
        return nodeName;
    }

    public static String getIpAddress() {
        String ip = "";
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException ex) {
            log.error(ex, ex);
        }
        return ip;
    }
}