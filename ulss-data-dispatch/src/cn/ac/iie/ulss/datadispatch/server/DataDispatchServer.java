/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.datadispatch.server;

import cn.ac.iie.ulss.datadispatch.commons.RuntimeEnv;
import cn.ac.iie.ulss.datadispatch.config.Configuration;
import cn.ac.iie.ulss.datadispatch.handler.datadispatch.DataDispatchHandler;
import cn.ac.iie.ulss.datadispatch.handler.infopub.InfoPubHandler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 *
 * @author alexmu
 */
public class DataDispatchServer {

    public static int poolNum = 0;
    static Server server = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataDispatchServer.class.getName());
    }

    public static void showUsage() {
        System.out.println("Usage:java -jar ");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        //DataDispatchServer.poolNum = Integer.parseInt(args[0]);
         DataDispatchServer.poolNum=100;
        System.out.println("the thread pool num is " + DataDispatchServer.poolNum);

        try {
            init();
            startup();
        } catch (Exception ex) {
            logger.error("starting data dispatcher server is failed for " + ex.getMessage(), ex);
        }

        System.exit(0);
    }

    private static void startup() throws Exception {
        logger.info("starting data dispatch server...");
        server.start();
        logger.info("start data dispatch server successfully");
        server.join();
    }

    private static void init() throws Exception {
        String configurationFileName = "data-dispatch.properties";
        logger.info("initializing data dispatch server...");
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }

        logger.info("initializng runtaime enviroment...");
        if (!RuntimeEnv.initialize(conf)) {
            throw new Exception("initializng runtime enviroment is failed");
        }
        logger.info("initialize runtime enviroment successfully");

        String serverIP = conf.getString("jettyServerIP", "");
        if (serverIP.isEmpty()) {
            throw new Exception("definition jettyServerIP is not found in " + configurationFileName);
        }

        int serverPort = conf.getInt("jettyServerPort", -1);
        if (serverPort == -1) {
            throw new Exception("definition jettyServerPort is not found in " + configurationFileName);
        }

        Connector connector = new SelectChannelConnector();
        connector.setHost(serverIP);
        connector.setPort(serverPort);

        server = new Server();
        server.setConnectors(new Connector[]{connector});
        server.setThreadPool(new QueuedThreadPool(DataDispatchServer.poolNum));

        ContextHandler dataLoadContext = new ContextHandler("/dataload");
        DataDispatchHandler dataDispatchHandler = DataDispatchHandler.getDataDispatchHandler();
        if (dataDispatchHandler == null) {
            throw new Exception("initializing dataDispatchHandler is failed");
        }

        dataLoadContext.setHandler(dataDispatchHandler);

        ContextHandler infoPubContext = new ContextHandler("/infopub");
        InfoPubHandler infoDispatchHandler = InfoPubHandler.getInfoPubHandler();
        if (infoDispatchHandler == null) {
            throw new Exception("initializing infoPubHandler is failed");
        }

        infoPubContext.setHandler(infoDispatchHandler);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{infoPubContext, dataLoadContext});
        server.setHandler(contexts);
        logger.info("intialize data dispatch server successfully");
    }
}
