package cn.ac.iie.ulss.indexer.constant;

/**
 *
 * @author yulei
 */
public final class Constants {

    /**
     * The application scope attribute under which our user database is stored.
     */
    public static final Double RAM_BUFFER_SIZE_MB = 512.0;
    public static final Integer BUFFERED_WRITER_MAX_DOCSNUM = 100000;
    public static final Integer MERGE_FACTORS = 15;
    public static final Long INDEXWRITE_COMMIT_INTERVAL = 10L;
    public static final String MT_LUCENE_INDEX_TYPE_STRING = "STRING";
    public static final String MT_LUCENE_INDEX_TYPE_NUMBER = "NUMBER";
    //...
    public static final String DATA_TYPE_STRING = "STRING";
    public static final String DATA_TYPE_TIMESTAMP = "TIMESTAMP";
    public static final String DATA_TYPE_DATE = "DATE";
    public static final String DATA_TYPE_INT = "INT";
    public static final String DATA_TYPE_BIGINT = "BIGINT";
    public static final String DARA_TYPE_FLOAT = "FLOAT";
    public static final String DATA_TYPE_DOUBLE = "DOUBLE";
    public static final String DATA_TYPE_NUMBER = "NUMBER";
    public static final String DATA_TYPE_BINARY = "BINARY";
    //...
    public static final String UDF_TRIEM86 = "trim86";
    public static final int DATE_HOUR_UST_MIUS_CST = 8;
    public static final String ENCODE_UTF8 = "UTF-8";
    public static final String ENCODE_GB18030 = "GB18030";
    public static final String ENCODE_GBK = "GBK";
    public static final String ENCODE_ISO = "ISO-8859-1";
    public static final String ENCODE_UTF16BE = "UTF-16BE";
    public static final String ENCODE_UTF16LE = "UTF-16LE";
    public static final String[] ENCODE_CHARSET = {ENCODE_UTF8, ENCODE_GB18030, ENCODE_GBK, ENCODE_ISO, ENCODE_UTF16BE, ENCODE_UTF16LE};
    public static int DEFAULT_PRECISION_STEP = 8;
}
