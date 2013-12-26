/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.data.statistics;

import cn.ac.iie.ulss.statistics.commons.GlobalVariables;
import cn.ac.iie.ulss.statistics.commons.RuntimeEnv;
import cn.ac.iie.ulss.statistics.config.Configuration;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class UlssDataStatistics {

    static final String CONFIGURATIONFILENAME = "data_statistics_config.properties";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(UlssDataStatistics.class.getName());
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
        GetSchemaFromDB.getSchemaFromDB();
        GetMQFromDB.get();
        Server.run();
        logger.info("starting data statistics client...");
    }
}
