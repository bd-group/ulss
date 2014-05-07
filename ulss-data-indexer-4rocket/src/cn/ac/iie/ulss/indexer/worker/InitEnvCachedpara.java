/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.runenvs.Constants;
import cn.ac.iie.ulss.indexer.runenvs.DataTypeMap;
import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.log4j.Logger;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

/**
 *
 * @author work
 */
public class InitEnvCachedpara {

    public static Logger log = Logger.getLogger(InitEnvCachedpara.class.getName());

    public static void initSchema() {
        List<List<String>> schema2tableList = GlobalParas.imd.getSchema2table();
        for (int i = 0; i < schema2tableList.size(); i++) {
            log.info("put " + schema2tableList.get(i).get(0).toLowerCase().trim() + "---" + schema2tableList.get(i).get(1).toLowerCase().trim() + " to tablename2Schemaname map");
            GlobalParas.tablename2Schemaname.put(schema2tableList.get(i).get(0).toLowerCase().trim(), schema2tableList.get(i).get(1).toLowerCase().trim());
        }
        String schemaName = "";
        String schemaContent = "";
        try {
            log.info("getting data schema and metaq from metadb...");
            GlobalParas.docsSchemaContent = GlobalParas.imd.getDocSchema();
            List<List<String>> allSchema = GlobalParas.imd.getAllSchema();
            for (List<String> list : allSchema) {
                schemaName = list.get(0).toLowerCase();
                schemaContent = list.get(1).toLowerCase().trim();
                log.debug("schema " + schemaName + "'s content is:\n" + schemaContent);
                if (schemaName.equals("docs")) {
                    log.info("init schema docs ...");
                    GlobalParas.docsSchemaContent = schemaContent;
                    Protocol protocol = Protocol.parse(GlobalParas.docsSchemaContent);
                    GlobalParas.docsSchema = protocol.getType(schemaName);
                    GlobalParas.docsReader = new GenericDatumReader<GenericRecord>(GlobalParas.docsSchema);
                    GlobalParas.schemaname2Schema.put("docs", GlobalParas.docsSchema);
                    log.info("init schema for docs is ok");
                } else {
                    log.info("init schema data reader and schema and  metaq for schema " + schemaName + " ...");
                    Protocol protocol = Protocol.parse(schemaContent);
                    Schema schema = protocol.getType(schemaName);
                    log.debug("put schemaReader、mq for " + schemaName + " to map");
                    GlobalParas.schemaname2Schema.put(schemaName, schema);
                }
                GlobalParas.schemaname2schemaContent.put(schemaName, schemaContent);
            }
        } catch (Exception ex) {
            log.error("constructing data dispath handler is failed: " + schemaName + " for " + ex, ex);
            System.exit(0);
        }
        log.info("init all data schemas done ");

        if (GlobalParas.docsSchema == null) {
            log.error("schema docs is not found in metadb,please check metadb to ensure that docs schema exist");
            System.exit(0);
        }
    }

    public static void initStatVolumeSchema() {
        GlobalParas.statVolumeSchemaContent = GlobalParas.imd.getStatVolumeSchema();
        Protocol protocol = Protocol.parse(GlobalParas.statVolumeSchemaContent);
        GlobalParas.statVolumeSchema = protocol.getType("data_volume_stat");
    }

    public static void initDataType() {
        DataTypeMap.initMap();
    }

    public static void initIndexinfo(ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>> table2idx2colMap,
            ConcurrentHashMap<String, LinkedHashMap<String, Field>> table2idx2LuceneFieldMap,
            ConcurrentHashMap<String, LinkedHashMap<String, FieldType>> table2idx2LuceneFieldTypeMap,
            String dbName, String tableName) {
        table2idx2colMap = new ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>>(50, 0.8f);
        table2idx2LuceneFieldMap = new ConcurrentHashMap<String, LinkedHashMap<String, Field>>(50, 0.8f);
        table2idx2LuceneFieldTypeMap = new ConcurrentHashMap<String, LinkedHashMap<String, FieldType>>(50, 0.8f);

        LinkedHashMap<String, List<FieldSchema>> idx2colMap = new LinkedHashMap<String, List<FieldSchema>>(50, 0.8f);//索引名和对应的数据库列的映射
        LinkedHashMap<String, Field> idx2LuceneFieldMap = new LinkedHashMap<String, Field>(50, 0.8f);//索引名和lucene域的映射
        LinkedHashMap<String, FieldType> idx2LuceneFieldTypeMap = new LinkedHashMap<String, FieldType>(50, 0.8f);//索引名 和 lucene域类型的映射
        try {
            log.info("now init information for " + dbName + "." + tableName);
            List<Index> idxList = GlobalParas.tablename2indexs.get(dbName + "." + tableName);
            for (Index idx : idxList) {
                String idxName = idx.getIndexName().toLowerCase();
                log.debug("index name is " + idxName);
                List<FieldSchema> indexCols = idx.getSd().getCols();
                idx2colMap.put(idxName.toLowerCase(), indexCols);
            }
            FieldType fieldtype = InitEnvCachedpara.getFieldType("true", "true", "long");
            idx2LuceneFieldTypeMap.put("c_inputtime", fieldtype);
            for (Index idx : idxList) {
                if (idx.getIndexHandlerClass().equalsIgnoreCase("lucene")) {
                    Field field;
                    String dataType = idx.getSd().getCols().get(0).getType();
                    String lucIdx = idx.getParameters().get("lucene.indexd");
                    String lucStore = idx.getParameters().get("lucene.stored");
                    String lucAnaly = idx.getParameters().get("lucene.analyzed");
                    if (idx.getSd().getCols().size() != 1) {
                        log.error("the index column num is  not 1,now this is not supported !");
                        throw new RuntimeException("the index column num is  not 1,now this is not supported now");
                    }
                    if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_TIMESTAMP) || dataType.equalsIgnoreCase("array<timestamp>")) {
                        FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new LongField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.MT_LUCENE_INDEX_TYPE_STRING) || dataType.equalsIgnoreCase("array<string>")) {
                        if ("true".equalsIgnoreCase(lucIdx) && "false".equalsIgnoreCase(lucAnaly)) {
                            field = new StringField(idx.getIndexName().toLowerCase(), "", Field.Store.YES);
                        } else if ("true".equalsIgnoreCase(lucIdx) && "true".equalsIgnoreCase(lucAnaly)) {
                            field = new TextField(idx.getIndexName().toLowerCase(), "", TextField.Store.YES);
                        } else {
                            log.error("the index type is not support now");
                            throw new RuntimeException("the index type is not support now");
                        }
                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_INT) || dataType.equalsIgnoreCase("array<int>")) {
                        FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new IntField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_BIGINT) || dataType.equalsIgnoreCase("array<bigint>")) {
                        FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, "long");
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new LongField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.DARA_TYPE_FLOAT) || dataType.equalsIgnoreCase("array<float>")) {
                        FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new FloatField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_DOUBLE) || dataType.equalsIgnoreCase("array<double>")) {
                        FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, dataType);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        field = new DoubleField(idx.getIndexName().toLowerCase(), -999, ft);
                    } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_BINARY) || dataType.equalsIgnoreCase("array<binary>")) {
                        FieldType ft = InitEnvCachedpara.getBinaryFieldType(lucIdx, lucStore);
                        idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                        byte[] val = new byte[1];
                        field = new Field(idx.getIndexName().toLowerCase(), val, ft);
                    } else if (dataType.equalsIgnoreCase("blob") || dataType.equalsIgnoreCase("array<blob>")) {
                        log.debug("now is a blob type for field: " + idx.getIndexName().toLowerCase());
                        field = new StringField(idx.getIndexName().toLowerCase(), "", Field.Store.YES);
                    } else {
                        throw new RuntimeException("Index " + idx.getIndexName() + " type[" + dataType + "] is not supported  now !");
                    }
                    idx2LuceneFieldMap.put(idx.getIndexName().toLowerCase(), field);
                    log.debug("put " + idx.getIndexName().toLowerCase() + " to idx2LuceneFieldMap");
                } else if (idx.getIndexHandlerClass().equalsIgnoreCase("bloomfilter")) {
                    log.info("this is bloomfilter index,but now it is not supported");
                } else {
                    log.error("the index is not support now except lucene index !");
                }
            }
        } catch (Exception ex) {
            log.error(ex, ex);
        }
        try {
            table2idx2colMap.put(dbName + "." + tableName, idx2colMap);
            table2idx2LuceneFieldMap.put(dbName + "." + tableName, idx2LuceneFieldMap);
            table2idx2LuceneFieldTypeMap.put(dbName + "." + tableName, idx2LuceneFieldTypeMap);
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }

    public static void initIndexinfo() {
        for (int i = 0; i < GlobalParas.hlwthreadpoolSize * 2; i++) {
            ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>> table2idx2colMap = table2idx2colMap = new ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>>(50, 0.8f);
            ConcurrentHashMap<String, LinkedHashMap<String, Field>> table2idx2LuceneFieldMap = table2idx2LuceneFieldMap = new ConcurrentHashMap<String, LinkedHashMap<String, Field>>(50, 0.8f);
            ConcurrentHashMap<String, LinkedHashMap<String, FieldType>> table2idx2LuceneFieldTypeMap = table2idx2LuceneFieldTypeMap = new ConcurrentHashMap<String, LinkedHashMap<String, FieldType>>(50, 0.8f);

            for (String tableFullName : GlobalParas.tablename2Table.keySet()) {
                LinkedHashMap<String, List<FieldSchema>> idx2colMap = new LinkedHashMap<String, List<FieldSchema>>(50, 0.8f);
                LinkedHashMap<String, Field> idx2LuceneFieldMap = new LinkedHashMap<String, Field>(50, 0.8f);
                LinkedHashMap<String, FieldType> idx2LuceneFieldTypeMap = new LinkedHashMap<String, FieldType>(50, 0.8f);
                try {
                    //log.info("now init lucene information for " + tableFullName);
                    List<Index> idxList = GlobalParas.tablename2indexs.get(tableFullName);
                    for (Index idx : idxList) {
                        String idxName = idx.getIndexName().toLowerCase();
                        log.debug("index name is " + idxName);
                        List<FieldSchema> indexCols = idx.getSd().getCols();
                        idx2colMap.put(idxName.toLowerCase(), indexCols);
                    }
                    FieldType fieldtype = InitEnvCachedpara.getFieldType("true", "true", "long");
                    idx2LuceneFieldTypeMap.put("c_inputtime", fieldtype);
                    for (Index idx : idxList) {
                        if (idx.getIndexHandlerClass().equalsIgnoreCase("lucene")) {
                            Field field;
                            String dataType = idx.getSd().getCols().get(0).getType();
                            String lucIdx = idx.getParameters().get("lucene.indexd");
                            String lucStore = idx.getParameters().get("lucene.stored");
                            String lucAnaly = idx.getParameters().get("lucene.analyzed");
                            if (idx.getSd().getCols().size() != 1) {
                                log.error("the index column num is  not 1,now this is not supported !");
                                throw new RuntimeException("the index column num is  not 1,now this is not supported now");
                            }
                            if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_TIMESTAMP) || dataType.equalsIgnoreCase("array<timestamp>")) {
                                FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, dataType);
                                idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                                field = new LongField(idx.getIndexName().toLowerCase(), -999, ft);
                            } else if (dataType.equalsIgnoreCase(Constants.MT_LUCENE_INDEX_TYPE_STRING) || dataType.equalsIgnoreCase("array<string>")) {
                                if ("true".equalsIgnoreCase(lucIdx) && "false".equalsIgnoreCase(lucAnaly)) {
                                    field = new StringField(idx.getIndexName().toLowerCase(), "", Field.Store.YES);
                                } else if ("true".equalsIgnoreCase(lucIdx) && "true".equalsIgnoreCase(lucAnaly)) {
                                    field = new TextField(idx.getIndexName().toLowerCase(), "", TextField.Store.YES);
                                } else {
                                    log.error("the index type is not support now");
                                    throw new RuntimeException("the index type is not support now");
                                }
                            } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_INT) || dataType.equalsIgnoreCase("array<int>")) {
                                FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, dataType);
                                idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                                field = new IntField(idx.getIndexName().toLowerCase(), -999, ft);
                            } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_BIGINT) || dataType.equalsIgnoreCase("array<bigint>")) {
                                FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, "long");
                                idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                                field = new LongField(idx.getIndexName().toLowerCase(), -999, ft);
                            } else if (dataType.equalsIgnoreCase(Constants.DARA_TYPE_FLOAT) || dataType.equalsIgnoreCase("array<float>")) {
                                FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, dataType);
                                idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                                field = new FloatField(idx.getIndexName().toLowerCase(), -999, ft);
                            } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_DOUBLE) || dataType.equalsIgnoreCase("array<double>")) {
                                FieldType ft = InitEnvCachedpara.getFieldType(lucIdx, lucStore, dataType);
                                idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                                field = new DoubleField(idx.getIndexName().toLowerCase(), -999, ft);
                            } else if (dataType.equalsIgnoreCase(Constants.DATA_TYPE_BINARY) || dataType.equalsIgnoreCase("array<binary>")) {
                                FieldType ft = InitEnvCachedpara.getBinaryFieldType(lucIdx, lucStore);
                                idx2LuceneFieldTypeMap.put(idx.getIndexName().toLowerCase(), ft);
                                byte[] val = new byte[1];
                                field = new Field(idx.getIndexName().toLowerCase(), val, ft);
                            } else if (dataType.equalsIgnoreCase("blob") || dataType.equalsIgnoreCase("array<blob>")) {
                                log.debug("now is a blob type for field: " + idx.getIndexName().toLowerCase());
                                field = new StringField(idx.getIndexName().toLowerCase(), "", Field.Store.YES);
                            } else {
                                throw new RuntimeException("Index " + idx.getIndexName() + " type[" + dataType + "] is not supported  now !");
                            }
                            idx2LuceneFieldMap.put(idx.getIndexName().toLowerCase(), field);
                            log.debug("put " + idx.getIndexName().toLowerCase() + " to idx2LuceneFieldMap");
                        } else if (idx.getIndexHandlerClass().equalsIgnoreCase("bloomfilter")) {
                            log.info("this is bloomfilter index,but now it is not supported");
                        } else {
                            log.error("the index is not support now except lucene index !");
                        }
                    }
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
                try {
                    table2idx2colMap.put(tableFullName, idx2colMap);
                    table2idx2LuceneFieldMap.put(tableFullName, idx2LuceneFieldMap);
                    table2idx2LuceneFieldTypeMap.put(tableFullName, idx2LuceneFieldTypeMap);
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            }

            try {
                GlobalParas.table2idx2LuceneFieldMapPool.put(table2idx2LuceneFieldMap);
                GlobalParas.table2idx2LuceneFieldTypeMapPool.put(table2idx2LuceneFieldTypeMap);
                GlobalParas.table2idx2colMapPool.put(table2idx2colMap);
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }

    private static boolean isNumeric(String type) {
        if (Constants.DATA_TYPE_DATE.equalsIgnoreCase(type)
                || Constants.MT_LUCENE_INDEX_TYPE_NUMBER.equalsIgnoreCase(type)
                || Constants.DATA_TYPE_INT.equalsIgnoreCase(type)
                || Constants.DATA_TYPE_BIGINT.equalsIgnoreCase(type)
                || Constants.DARA_TYPE_FLOAT.equalsIgnoreCase(type)
                || Constants.DATA_TYPE_DOUBLE.equalsIgnoreCase(type)) {
            return true;
        } else {
            return false;
        }
    }

    private static FieldType getFieldType(String isIndexed, String isStored, String dataType) {
        FieldType ft = new FieldType();
        if ("true".equalsIgnoreCase(isIndexed)) {
            ft.setIndexed(true);
        } else {
            ft.setIndexed(false);
        }
        if ("true".equalsIgnoreCase(isStored)) {
            ft.setStored(true);
        } else {
            ft.setStored(false);
        }
        if ("long".equalsIgnoreCase(dataType) || "array<long>".equalsIgnoreCase(dataType) || "timestamp".equalsIgnoreCase(dataType) || "array<timestamp>".equalsIgnoreCase(dataType)) {
            ft.setNumericType(FieldType.NumericType.LONG);
        } else if ("int".equalsIgnoreCase(dataType) || "array<int>".equalsIgnoreCase(dataType)) {
            ft.setNumericType(FieldType.NumericType.INT);
        } else if ("float".equalsIgnoreCase(dataType) || "array<float>".equalsIgnoreCase(dataType)) {
            ft.setNumericType(FieldType.NumericType.FLOAT);
        } else if ("double".equalsIgnoreCase(dataType) || "array<double>".equalsIgnoreCase(dataType)) {
            ft.setNumericType(FieldType.NumericType.DOUBLE);
        }
        return ft;
    }

    private static FieldType getBinaryFieldType(String isIndexed, String isStored) {
        FieldType ft = new FieldType();
        if ("true".equalsIgnoreCase(isIndexed)) {
            ft.setIndexed(true);
        } else {
            ft.setIndexed(false);
        }
        ft.setIndexed(false);
        if ("true".equalsIgnoreCase(isStored)) {
            ft.setStored(true);
        } else {
            ft.setStored(false);
        }
        return ft;
    }
}
