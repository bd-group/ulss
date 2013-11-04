/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.datadispatch.handler.datadispatch;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.ulss.datadispatch.commons.RuntimeEnv;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 *
 * @author alexmu
 */
public class DataDispatchHandler extends AbstractHandler {

    //docs shcema
    private static String docsSchemaContent = null;
    private static Schema docsSchema = null;
    private static DatumReader<GenericRecord> docsReader = null;
    private static DataDispatchHandler dataDispatchHandler = null;
    private Map<String, DataDispatcher> docSchema2Dispatcher = new HashMap<String, DataDispatcher>();
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataDispatchHandler.class.getName());
    }
    
    private DataDispatchHandler() {
    }
    
    public static DataDispatchHandler getDataDispatchHandler() {
        if (dataDispatchHandler != null) {
            return dataDispatchHandler;
        }
        dataDispatchHandler = new DataDispatchHandler();
        ResultSet rs = null;
        try {
            logger.info("get data schema and corresponding message queue from metadb...");
            while (true) {
                try {
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery("select dataschema.schema_name,dataschema.schema_content,dataschema_mq.mq from dataschema left outer join dataschema_mq on dataschema.schema_name=dataschema_mq.schema_name");
                    break;
                } catch (Exception ex) {
                    logger.warn("get information from metadb unsuccessfully for " + ex.getMessage(), ex);
                    try {
                        Thread.sleep(500);
                    } catch (Exception ex_) {
                    }
                }
            }
            logger.info("get data schema and corresponding message queue from metadb successfully");
            logger.info("parsing data schemas...");
            while (rs.next()) {
                String schemaName = rs.getString("schema_name").toLowerCase();
                String schemaContent = rs.getString("schema_content").toLowerCase();
                String mq = rs.getString("mq");
                logger.info("schema " + schemaName + " found");
                logger.info("schema " + schemaName + "'s content is:\n" + schemaContent);
                if (schemaName.equals("docs")) {
                    logger.info("parsing schema docs...");
                    docsSchemaContent = schemaContent;
                    Protocol protocol = Protocol.parse(docsSchemaContent);//maybe failed
                    docsSchema = protocol.getType(schemaName);
                    docsReader = new GenericDatumReader<GenericRecord>(docsSchema);
                    logger.info("parsing schema docs is finished successfully");
                } else {
                    logger.info("constructing data dispatcher for schema " + schemaName + "...");
                    DataDispatcher dataDispatcher = DataDispatcher.getDataDispatcher(schemaName, schemaContent, mq);
                    if (dataDispatcher != null) {
                        dataDispatchHandler.docSchema2Dispatcher.put(schemaName, dataDispatcher);
                        logger.info("constructing data dispatcher for schema " + schemaName + " is finished successfully");
                    } else {
                        dataDispatchHandler = null;
                        logger.info("constructing data dispatcher for schema " + schemaName + " is failed");
                        break;
                    }
                }
            }
            
            logger.info("parsing data schemas is finished");
            
            if (docsSchema == null) {
                logger.error("schema docs is not found in metadb,please check metadb to ensure that docs schema exist");
                dataDispatchHandler = null;
            }
            
            if (dataDispatchHandler.docSchema2Dispatcher.size() < 1) {
                logger.warn("no bussiness data schema is found in metadb,please check metadb to ensure that this condition is reasonable");
            }
            
        } catch (Exception ex) {
            logger.error("constructing data dispath handler is failed for " + ex.getMessage(), ex);
            dataDispatchHandler = null;
        } finally {
            Connection tmpConn = null;
            try {
                tmpConn = rs.getStatement().getConnection();
            } catch (Exception ex) {
            }
            try {
                rs.close();
            } catch (Exception ex) {
            }
            try {
                tmpConn.close();
            } catch (Exception ex) {
            }
        }
        return dataDispatchHandler;
    }
    
    public static String getDocSchemaContent(String pSchemaName) {
        DataDispatcher dataDispatcher = dataDispatchHandler.docSchema2Dispatcher.get(pSchemaName);
        
        if (dataDispatcher == null) {
            return null;
        } else {
            return dataDispatcher.getDocSchemaContent();
        }
    }
    
    @Override
    public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        logger.debug("receive the request.begin deal with it");
        baseRequest.setHandled(true);
        ServletInputStream servletInputStream = baseRequest.getInputStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] b = new byte[1024];
        int i = 0;
        while ((i = servletInputStream.read(b, 0, 1024)) > 0) {
            out.write(b, 0, i);
        }
        
        byte[] req = out.toByteArray();
        logger.debug("recv data length:" + req.length);
        logger.debug(baseRequest.getRemoteHost() + ":" + baseRequest.getRemotePort() + ",request conten type:" + baseRequest.getContentType());
        
        ByteArrayInputStream docsbis = new ByteArrayInputStream(req);//tuning
        BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
        
        GenericRecord docsRecord = new GenericData.Record(docsSchema);
        logger.debug("docs schema is " + docsSchema);
        logger.debug("now docsRecord is  " + docsRecord);
        try {
            docsReader.read(docsRecord, docsbd);
            logger.debug("after init the docsRecord is  " + docsRecord);
            logger.debug("current schema name:" + docsRecord.get("doc_schema_name").toString());
            
            DataDispatcher dataDispatcher = docSchema2Dispatcher.get(docsRecord.get("doc_schema_name").toString());
            if (dataDispatcher == null) {
                ResultSet rs = null;
                try {
                    String sql = "select dataschema.schema_name,dataschema.schema_content,dataschema_mq.mq from dataschema inner join dataschema_mq on dataschema.schema_name=dataschema_mq.schema_name and dataschema.schema_name='" + docsRecord.get("doc_schema_name").toString() + "'";
                    logger.debug(sql);
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    if (rs.next()) {
                        String schemaName = rs.getString("schema_name").toLowerCase();
                        String schemaContent = rs.getString("schema_content").toLowerCase();
                        String mq = rs.getString("mq");
                        dataDispatcher = DataDispatcher.getDataDispatcher(schemaName, schemaContent, mq);
                        if (dataDispatcher != null) {
                            dataDispatchHandler.docSchema2Dispatcher.put(schemaName, dataDispatcher);
                            logger.info("constructing data dispatcher for schema " + schemaName + " is finished successfully");
                            
                            dataDispatcher.dispatch(req);
                            
                            httpServletResponse.getWriter().println("0\nbzs_s\nulss_s");
                        } else {
                            String warningInfo = "constructing data dispatcher for schema " + schemaName + " is failed";
                            logger.warn(warningInfo);
                            httpServletResponse.getWriter().println("-1\n" + warningInfo);
                        }
                    } else {
                        String warningInfo = "unkonow schema " + docsRecord.get("doc_schema_name").toString();
                        logger.warn(warningInfo);
                        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                        httpServletResponse.getWriter().println("-1\n" + warningInfo);
                        throw new Exception(warningInfo);
                    }
                } catch (Exception ex) {
                    String warningInfo = "constructing data dispatcher for schema " + docsRecord.get("doc_schema_name").toString() + " is failed for " + ex.getMessage();
                    logger.warn(warningInfo, ex);
                    httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                    httpServletResponse.getWriter().println("-1\n" + warningInfo);
                } finally {
                    Connection tmpConn = null;
                    try {
                        tmpConn = rs.getStatement().getConnection();
                    } catch (Exception ex) {
                    }
                    try {
                        rs.close();
                    } catch (Exception ex) {
                    }
                    try {
                        tmpConn.close();
                    } catch (Exception ex) {
                    }
                }
            } else {
                dataDispatcher.dispatch(req);
                httpServletResponse.getWriter().println("0\nbzs_s\nulss_s");
            }
        } catch (Exception ex) {
            String warningInfo = "data parsing is failed for " + ex.getMessage();
            logger.warn(warningInfo, ex);
            httpServletResponse.getWriter().println("-1\n" + warningInfo);
        }
    }
}
