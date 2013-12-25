/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.datadispatch.handler.infopub;

import cn.ac.iie.ulss.datadispatch.commons.RuntimeEnv;
import cn.ac.iie.ulss.datadispatch.handler.datadispatch.DataDispatchHandler;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 *
 * @author alexmu
 */
public class InfoPubHandler extends AbstractHandler {

    private static InfoPubHandler infoPubHandler = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(InfoPubHandler.class.getName());
    }

    private InfoPubHandler() {
    }

    public static InfoPubHandler getInfoPubHandler() {
        if (infoPubHandler == null) {
            infoPubHandler = new InfoPubHandler();
        }
        return infoPubHandler;
    }

    @Override
    public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        baseRequest.setHandled(true);

        String opType = baseRequest.getParameter("op");
        logger.debug("opType:" + opType);

        String responseContent = "";

        if (opType.equals("getDOS")) {
            String schemaName = baseRequest.getParameter("schemaName");
            if (schemaName == null || schemaName.isEmpty()) {
                responseContent = "-1\nparameter schemaName is not defined";
            } else {
                responseContent = DataDispatchHandler.getDocSchemaContent(schemaName);
                if (responseContent == null) {
                    responseContent = "-1\nno such schema named " + schemaName;
                } else {
                    responseContent = "0\n" + responseContent;
                }
            }
        } else if (opType.equals("getLoadServerList")) {
            String loadCluster = (String) RuntimeEnv.getParam(RuntimeEnv.LOAD_CLUSTER);
            String[] loadServers = loadCluster.split(",");
            for (String loadServer : loadServers) {
                if (responseContent.isEmpty()) {
                    responseContent = loadServer;
                } else {
                    responseContent += "\n" + loadServer;
                }
            }
            responseContent = "0\n" + responseContent;
        } else if (opType.equals("getDataLoadVolumeStat")) {
            Map<String, Long> dataVolumeStatisticSet = null;
            try {
                dataVolumeStatisticSet = DataDispatchHandler.getDataVolumeStatics();
                Set<Entry<String, Long>> entrySet = dataVolumeStatisticSet.entrySet();
                for (Entry entry : entrySet) {
                    responseContent += entry.getKey() + "-->" + entry.getValue() + "\n";
                }
                responseContent = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()) + "\n" + responseContent;
            } catch (Exception ex) {
                responseContent = "get data volume statistics unsuccessfully for " + ex.getMessage();
            }
        } else {
            responseContent = "-1\nunknown op";
        }

        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
        httpServletResponse.getWriter().println(responseContent);
    }
}
