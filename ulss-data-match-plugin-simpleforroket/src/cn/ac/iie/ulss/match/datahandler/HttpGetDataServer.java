/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.datahandler;

import cn.ac.iie.ulss.match.monitorhandler.MonitorHandler;
import cn.ac.iie.ulss.match.worker.Matcher;
import cn.ac.iie.ulss.util.Configure;
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
 * @author liucuili
 */
public class HttpGetDataServer {

    public static Logger log = Logger.getLogger(HttpGetDataServer.class.getName());
    public static int poolNum = 0;
    static Server server = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
    }

    public static void startup() throws Exception {
        log.info("starting match data receive server...");
        server.start();
        server.setGracefulShutdown(3600000);
        log.info("start match data receive successfully");
        //server.join();
    }

    public static void stop() {
        log.info("will stop match data receive server ...");
        try {
            server.stop();
        } catch (Exception e) {
            log.error(e);
        }
    }

    public static void init() throws Exception {
        String configFile = "config.properties";
        log.info("initializing the http get data server ...");
        Configure conf = new Configure(configFile);
        if (conf == null) {
            throw new Exception("reading " + configFile + " is failed.");
        }

        String serverIP = conf.getProperty("jettyServerIP");
        if (serverIP.isEmpty()) {
            throw new Exception("definition jettyServerIP is not found in " + configFile);
        }
        int serverPort = conf.getIntProperty("jettyServerPort");
        if (serverPort == -1) {
            throw new Exception("definition jettyServerPort is not found in " + configFile);
        }

        Connector connector = new SelectChannelConnector();
        connector.setHost(serverIP);
        connector.setPort(serverPort);

        server = new Server();
        server.setConnectors(new Connector[]{connector});
        server.setThreadPool(new QueuedThreadPool(HttpGetDataServer.poolNum));


        ContextHandler monitorContext = new ContextHandler("/monitor");
        MonitorHandler monitorHandler = MonitorHandler.getMonitorHandler(Matcher.cdrMaps);
        if (monitorHandler == null) {
            throw new Exception("init monitorHandler is failed");
        }
        monitorContext.setHandler(monitorHandler);


        ContextHandler dataReceiveContext = new ContextHandler("/datareceive");
        HttpDataHandler dataHandler = HttpDataHandler.getDataHandler();
        if (dataHandler == null) {
            throw new Exception("init dataHandler is failed");
        }
        dataReceiveContext.setHandler(dataHandler);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{monitorContext, dataReceiveContext});
        server.setHandler(contexts);

        log.info("init data receive server ok ");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        HttpGetDataServer.poolNum = Integer.parseInt(args[0]);

        try {
            init();
            startup();
        } catch (Exception ex) {
            log.error("starting data dispatcher server is failed for " + ex.getMessage(), ex);
        }
        System.exit(0);
    }
}