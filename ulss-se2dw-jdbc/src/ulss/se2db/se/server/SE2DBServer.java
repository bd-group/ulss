/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.se.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ulss.se2db.commons.RuntimeEnv;
import ulss.se2db.config.Configuration;
import ulss.se2db.handler.TableSe2DBHandler;
import ulss.se2db.metastore.TableSe2DBRule;

/**
 *
 * @author ulss
 */
public class SE2DBServer {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(SE2DBServer.class.getName());
    }

    public static void showUsage() {
        System.out.println("Usage:java -jar ");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        try {
            init();
            startup();
            while (true) {
                Thread.sleep(10000);
            }
        } catch (Exception ex) {
            logger.error("starting se2db is failed for " + ex.getMessage(), ex);
        }
        System.exit(0);
    }

    private static void startup() throws Exception {
        Map<String, TableSe2DBHandler> tableHandlerSet = new HashMap<String, TableSe2DBHandler>();

        while (true) { 

            logger.info("checking se2db rule in metastore....");
            Map<String, List> tableSe2DBRuleOPSet = RuntimeEnv.metaStoreManger.getTableSe2DBRuleSet();

            List<TableSe2DBRule> newTableSe2DBRuleList = tableSe2DBRuleOPSet.get("new");
            if (newTableSe2DBRuleList.size() > 0) {
                logger.info("found " + newTableSe2DBRuleList.size() + " new se2db rules");
                for (TableSe2DBRule tableSe2DBRule : newTableSe2DBRuleList) {
                    TableSe2DBHandler tableHandler = TableSe2DBHandler.getTableSe2DBHandler(tableSe2DBRule);
                    RuntimeEnv.metaStoreManger.registerHandler(tableHandler);
                    tableHandlerSet.put(tableSe2DBRule.getRuleName(), tableHandler);
                }
            } else {
                logger.info("no new se2db rules was found");
            }


            List<TableSe2DBRule> updatedTableSe2DBRuleList = tableSe2DBRuleOPSet.get("updated");
            if (updatedTableSe2DBRuleList.size() > 0) {
                logger.info("found " + updatedTableSe2DBRuleList.size() + " updated se2db rules");
                for (TableSe2DBRule tableSe2DBRule : updatedTableSe2DBRuleList) {
                    TableSe2DBHandler tableHandler = tableHandlerSet.get(tableSe2DBRule.getRuleName());
                    assert (tableHandler != null);
                    tableHandler.act2TableSe2DBRuleChange();
                }
            } else {
                logger.info("no updated se2db rules was found");
            }


            List<TableSe2DBRule> deletedTableSe2DBRuleList = tableSe2DBRuleOPSet.get("deleted");
            if (deletedTableSe2DBRuleList.size() > 0) {
                logger.info("found " + deletedTableSe2DBRuleList.size() + " deleted se2db rules");
                for (TableSe2DBRule tableSe2DBRule : deletedTableSe2DBRuleList) {
                    TableSe2DBHandler tableHandler = tableHandlerSet.get(tableSe2DBRule.getRuleName());
                    if (tableHandler == null) {
                        continue;
                    }
                    tableHandler.act2TableSe2DBRuleChange();
                    tableHandlerSet.remove(tableSe2DBRule.getRuleName());
                }
            } else {
                logger.info("no deleted se2db rules was found");
            }
            logger.info("checking se2db rule in metastore is finished.");

            try {
                logger.info("waiting....");
                Thread.sleep(10000);
            } catch (Exception ex) {
            }
        }
    }

    private static void init() throws Exception {
        String configurationFileName = "ulss-se2db.properties";
        logger.info("initializing se2db server...");
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed");
        }

        logger.info("initializng runtime enviroment...");
        if (!RuntimeEnv.initialize(conf)) {
            throw new Exception("initializng runtime enviroment is failed");
        }
        logger.info("initialize runtime enviroment successfully");
    }
}
