package cn.ac.iie.ulss.indexer.runenvs;

public class Constants {

    /**
     * The application scope attribute under which our user database is stored.
     */
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