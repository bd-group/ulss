/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.struct;

import java.io.Serializable;

/**
 *
 * @author liucuili
 */
public class CDRRecordNode implements Serializable {

    public String c_usernum;
    /**/
    public String c_imsi;
    public String c_imei;
    public int c_spcode;
    public int c_ascode;
    public int c_lac;
    public int c_ci;
    public int c_rac;
    public String c_areacode;
    public String c_homecode;
    /**/
    public long c_timestamp;     //秒数，表名这条对应cdr记录的时间戳
    /**/
    public long arriveTime;      //毫秒数，什么时候位置信息到达的
    public long updateTime;      //毫秒数，什么时候更新的这条位置信息

    public String getc_usernum() {
        return c_usernum;
    }

    public void setc_usernum(String c_usernum) {
        this.c_usernum = c_usernum;
    }

    public String getc_imsi() {
        return c_imsi;
    }

    public void setc_imsi(String c_imsi) {
        this.c_imsi = c_imsi;
    }

    public String getc_imei() {
        return c_imei;
    }

    public void setc_imei(String c_imei) {
        this.c_imei = c_imei;
    }

    public int getc_spcode() {
        return c_spcode;
    }

    public void setc_spcode(int c_spcode) {
        this.c_spcode = c_spcode;
    }

    public int getc_ascode() {
        return c_ascode;
    }

    public void setc_ascode(int c_ascode) {
        this.c_ascode = c_ascode;
    }

    public int getc_lac() {
        return c_lac;
    }

    public void setc_lac(int c_lac) {
        this.c_lac = c_lac;
    }

    public int getc_ci() {
        return c_ci;
    }

    public void setc_ci(int c_ci) {
        this.c_ci = c_ci;
    }

    public int getc_rac() {
        return c_rac;
    }

    public void setc_rac(int c_rac) {
        this.c_rac = c_rac;
    }

    public String getc_areacode() {
        return c_areacode;
    }

    public void setc_areacode(String c_areacode) {
        this.c_areacode = c_areacode;
    }

    public String getc_homecode() {
        return c_homecode;
    }

    public void setc_homecode(String c_homecode) {
        this.c_homecode = c_homecode;
    }

    public long getc_timestamp() {
        return c_timestamp;
    }

    public void setc_timestamp(long c_timestamp) {
        this.c_timestamp = c_timestamp;
    }

    public long getupdateTime() {
        return updateTime;
    }

    public void setupdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public long getarriveTime() {
        return arriveTime;
    }

    public void setarriveTime(long arriveTime) {
        this.arriveTime = arriveTime;
    }

//    public GenericRecord getGenRecord() {
//        return genRecord;
//    }
//
//    public void setGenRecord(GenericRecord genRecord) {
//        this.genRecord = genRecord;
//    }
//
//    public GenericRecord getGenericRecord() {
//        return genRecord;
//    }
//
//    public void setGenericRecord(GenericRecord rec) {
//        this.genRecord = rec;
//    }
    @Override
    public String toString() {
        return "imsi:" + this.c_imsi
                + " imei:" + this.c_imei
                + " spcode:" + this.c_spcode
                + " ascode:" + this.c_ascode
                + " lac:" + this.c_lac
                + " ci:" + this.c_ci
                + " rac:" + this.c_rac
                + " areacode:" + this.c_areacode
                + " homecode:" + this.c_homecode
                + " timestamp:" + this.c_timestamp
                + " arriveTime:" + this.arriveTime
                + " updateTime:" + this.updateTime;
    }

    public static void main(String[] args) {
        System.out.println('s');
        System.out.println((char) 120 + "sds");

        CDRRecordNode c = new CDRRecordNode();
        c.c_areacode = "1";
        System.out.println(c.toString());
    }
}