/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.constant;

/**
 *
 * @author liucuili
 */
public enum DataTypeEnum {

    INT, LONG, BIGINT, TIMESTAMP, FLOAT, DOUBLE, STRING, BINARY, BLOB, ARRAYINT, ARRRAYLONG, ARRAYBIGINT, ARRAYTIMESTAMP,
    ARRAYFLOAT, ARRAYDOUBLE, ARRAYSTRING, ARRAYBINARY, ARRAYBLOB;

    public static DataTypeEnum getDataType(String dataType) {
        return valueOf(dataType.toUpperCase());
    }

    public void caseDataType(String type) {
        switch (DataTypeEnum.getDataType(type)) {
            case INT:
                System.out.println("this is a int");
                break;
            case LONG:
                System.out.println("this is a long");
                break;
            default:
                System.out.println("this is a default");
                break;
        }
    }
}