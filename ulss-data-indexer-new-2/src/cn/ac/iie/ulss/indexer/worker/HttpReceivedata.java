/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import cn.ac.iie.ulss.utiltools.Configure;
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
public class HttpReceivedata {

    public static Logger log = Logger.getLogger(HttpReceivedata.class.getName());
    public static int poolNum = 100;
    static Server server = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
    }

    public static void startup() throws Exception {
        log.info("starting persisis data http receive data server...");
        server.start();
        log.info("start persisis data http receive data server ok");
        server.join();
    }

    public static void stop() {
        log.info("will stop indexer data receive server ...");
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
        server.setThreadPool(new QueuedThreadPool(Indexer.httpServerPool));

        ContextHandler dataReceiveContext = new ContextHandler("/datareceive");

        /*
         * @comment
         * 初始化dataHandler时使用ReadWriteLock是为了匹配线程
         * 在将数据从缓冲区中拉取时不能同时往缓冲区中写数据，防止死循环
         */
        //final ReadWriteLock lock = new ReentrantReadWriteLock();DataHandler dataHandler = DataHandler.getDataHandler(lock);
        HttpDataHandler dataHandler = HttpDataHandler.getDataHandler();
        if (dataHandler == null) {
            throw new Exception("init dataHandler is failed");
        }
        dataReceiveContext.setHandler(dataHandler);
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{dataReceiveContext});
        server.setHandler(contexts);
        log.info("init data receive server ok ");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Indexer.httpServerPool = Integer.parseInt(args[0]);
        try {
            init();
            startup();
        } catch (Exception ex) {
            log.error("starting data dispatcher server is failed for " + ex.getMessage(), ex);
        }
        System.exit(0);
    }
}