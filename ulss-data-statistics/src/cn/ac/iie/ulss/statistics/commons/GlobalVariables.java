package cn.ac.iie.ulss.statistics.commons;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class GlobalVariables {

    public static final String SYN_COUNT = "synCount";
    public static final String MQ_TO_COUNT = "MQToCount";
    public static final String MQ_TO_SCHEMACONTENT = "MQToSchemaContent";
    public static final String MQ_TO_SCHEMANAME = "MQToSchemaName";
    public static final String DOCS_SCHEMA_CONTENT = "docsSchemaContent";
    public static final String MQ_TO_TIME = "MQToTime";
    public static final String MQ_TO_CONSUMER = "MQToConsumer";
    public static final String DOCS = "docs";
    public static final String DOC_SET = "doc_set";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GlobalVariables.class.getName());
    }

    /**
     *
     * init the Global Variables
     */
    public static void initialize() {

        byte[] synCount = new byte[0];
        RuntimeEnv.addParam(SYN_COUNT, synCount);

        logger.info("setting the MQToCount to the Global Variables");
        HashMap<String, HashMap<String, AtomicLong[]>> MQToCount = new HashMap<String, HashMap<String, AtomicLong[]>>();
        RuntimeEnv.addParam(MQ_TO_COUNT, MQToCount);

        logger.info("setting the MQToSchemaContent to the Global Variables");
        Map<String, String> MQToSchemaContent = new HashMap<String, String>();
        RuntimeEnv.addParam(MQ_TO_SCHEMACONTENT, MQToSchemaContent);

        logger.info("setting the MQToSchemaName to the Global Variables");
        Map<String, String> MQToSchemaName = new HashMap<String, String>();
        RuntimeEnv.addParam(MQ_TO_SCHEMANAME, MQToSchemaName);

        logger.info("setting the mqToTime to the Global Variables");
        Map<String, String> mqToTime = new HashMap<String, String>();
        RuntimeEnv.addParam(MQ_TO_TIME, mqToTime);

        logger.info("setting the MQToConsumer to the Global Variables");
        Map<String, MessageConsumer> MQToConsumer = new HashMap<String, MessageConsumer>();
        RuntimeEnv.addParam(MQ_TO_CONSUMER, MQToConsumer);
    }
}
