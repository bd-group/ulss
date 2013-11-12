package cn.ac.iie.ulss.dataredistribution.server;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.config.Configuration;
import cn.ac.iie.ulss.dataredistribution.handler.GetRuleFromDBThread;
import cn.ac.iie.ulss.dataredistribution.tools.GetSchemaFromDB;
import cn.ac.iie.ulss.dataredistribution.tools.GetTBNameFromDB;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author evan yang
 */
public class StartDataRedistributionServer {

    static final String CONFIGURATIONFILENAME = "data_redistribution.properties";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(StartDataRedistributionServer.class.getName());
    }

    /**
     *
     * main function
     */
    public static void main(String[] arg) {
        logger.info("intializing data_redistribution client...");
        try {
            init();
            run();
        } catch (Exception ex) {
            logger.info(ex);
        }
    }

    /**
     *
     * init the Environment and the Global Variables
     */
    private static void init() throws Exception {

        logger.info("getting configuration from configuration file " + CONFIGURATIONFILENAME);
        Configuration conf = Configuration.getConfiguration(CONFIGURATIONFILENAME);
        if (conf == null) {
            throw new Exception("reading " + CONFIGURATIONFILENAME + " is failed.");
        }

        logger.info("initializng runtaime enviroment...");
        if (!RuntimeEnv.initialize(conf)) {
            throw new Exception("initializng runtime enviroment is failed");
        }
        logger.info("initialize runtime enviroment successfully");

        logger.info("initializng Global Variables...");
        GlobalVariables.initialize();
        logger.info("initialize Global Variables  successfully");
    }

    /**
     *
     * run the main server
     */
    private static void run() throws FileNotFoundException, IOException, InterruptedException {
        logger.info("starting data transmit client...");
        GetSchemaFromDB.getSchemaFromDB();
        GetTBNameFromDB.getTBNameFromDB();
        GetRuleFromDBThread gr = new GetRuleFromDBThread();
        Thread tgr = new Thread(gr);
        tgr.start();
    }
}
