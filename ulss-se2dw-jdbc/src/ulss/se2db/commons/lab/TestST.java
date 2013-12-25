<<<<<<< HEAD
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.commons.lab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 *
 * @author alexmu
 */
public class TestST {

    public static void main(String[] args) {
        Properties pros = new Properties();
        Connection conn = null;
        try {
            Class.forName("com.oscar.Driver");
            conn = DriverManager.getConnection("jdbc:oscar://10.168.17.1:2003/nodedb", "cluster", "cluster");
            Statement stat = conn.createStatement();
            stat.setFetchSize(1000);
            StringBuffer sb = new StringBuffer();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long step = 2 * 3600 * 1000;

            for (long startTimeMSec = 1355760000000L; startTimeMSec < 1356969600000L; startTimeMSec += step) {
                long endTimeMSec = startTimeMSec + step;
                String startTimestampStr = sdf.format(new Date(startTimeMSec));
                String endTimestampStr = sdf.format(new Date(endTimeMSec));
                System.out.println(startTimestampStr + "----" + endTimestampStr);
                long startTime = System.nanoTime();

                ResultSet rs = null;
                try {
                    String sql = "select \"UID\",sex,isreal,nick_name,rel_name,user_email,user_location,user_birth,unix_timestamp(create_at),fansnum,friendsnum,statusnum,favouritesnum,description,blog_url,photo_url,sp_type,unix_timestamp(input_time) from mb_user where input_time>='" + startTimestampStr + "' and input_time<'" + endTimestampStr + "'";
                    System.out.println(sql);
                    rs = stat.executeQuery(sql);
                    while (rs.next()) {
                        sb.append("\"" + (rs.getString(1) == null ? "" : rs.getString(1)) + "\",");
                        sb.append("\"" + (rs.getString(2) == null ? "" : rs.getString(2)) + "\",");
                        sb.append("\"" + (rs.getString(3) == null ? "" : rs.getString(3)) + "\",");
                        sb.append("\"" + (rs.getString(4) == null ? "" : rs.getString(4).replaceAll("\"", "\"\"")) + "\",");
                        sb.append("\"" + (rs.getString(5) == null ? "" : rs.getString(5)) + "\",");
                        sb.append("\"" + (rs.getString(6) == null ? "" : rs.getString(6)) + "\",");
                        sb.append("\"" + (rs.getString(7) == null ? "" : rs.getString(7)) + "\",");
                        sb.append("\"" + (rs.getString(8) == null ? "" : rs.getString(8).trim()) + "\",");
                        sb.append("\"" + (rs.getString(9) == null ? "" : rs.getString(9)) + "\",");
                        sb.append("\"" + (rs.getString(10) == null ? "" : rs.getString(10)) + "\",");
                        sb.append("\"" + (rs.getString(11) == null ? "" : rs.getString(11)) + "\",");
                        sb.append("\"" + (rs.getString(12) == null ? "" : rs.getString(12)) + "\",");
                        sb.append("\"" + (rs.getString(13) == null ? "" : rs.getString(13)) + "\",");
                        sb.append("\"" + (rs.getString(14) == null ? "" : rs.getString(14)) + "\",");
                        sb.append("\"" + (rs.getString(15) == null ? "" : rs.getString(15)) + "\",");
                        sb.append("\"" + (rs.getString(16) == null ? "" : rs.getString(16)) + "\",");
                        sb.append("\"" + (rs.getString(17) == null ? "" : rs.getString(17)) + "\",");
                        sb.append("\"" + (rs.getString(18) == null ? "" : rs.getString(18))+"\"");
                        System.out.println(sb.toString());
                        sb.setLength(0);
                    }
                    long endTime = System.nanoTime();
                    System.out.println((endTime - startTime) / (1024 * 1024));
                } catch (Exception ex) {
                    ex.printStackTrace();
                } finally {
                    try {
                        rs.close();
                    } catch (Exception ex) {
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
            }
        }
    }
}
=======
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.commons.lab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 *
 * @author alexmu
 */
public class TestST {

    public static void main(String[] args) {
        Properties pros = new Properties();
        Connection conn = null;
        try {
            Class.forName("com.oscar.Driver");
            conn = DriverManager.getConnection("jdbc:oscar://10.168.17.1:2003/nodedb", "cluster", "cluster");
            Statement stat = conn.createStatement();
            stat.setFetchSize(1000);
            StringBuffer sb = new StringBuffer();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long step = 2 * 3600 * 1000;

            for (long startTimeMSec = 1355760000000L; startTimeMSec < 1356969600000L; startTimeMSec += step) {
                long endTimeMSec = startTimeMSec + step;
                String startTimestampStr = sdf.format(new Date(startTimeMSec));
                String endTimestampStr = sdf.format(new Date(endTimeMSec));
                System.out.println(startTimestampStr + "----" + endTimestampStr);
                long startTime = System.nanoTime();

                ResultSet rs = null;
                try {
                    String sql = "select \"UID\",sex,isreal,nick_name,rel_name,user_email,user_location,user_birth,unix_timestamp(create_at),fansnum,friendsnum,statusnum,favouritesnum,description,blog_url,photo_url,sp_type,unix_timestamp(input_time) from mb_user where input_time>='" + startTimestampStr + "' and input_time<'" + endTimestampStr + "'";
                    System.out.println(sql);
                    rs = stat.executeQuery(sql);
                    while (rs.next()) {
                        sb.append("\"" + (rs.getString(1) == null ? "" : rs.getString(1)) + "\",");
                        sb.append("\"" + (rs.getString(2) == null ? "" : rs.getString(2)) + "\",");
                        sb.append("\"" + (rs.getString(3) == null ? "" : rs.getString(3)) + "\",");
                        sb.append("\"" + (rs.getString(4) == null ? "" : rs.getString(4).replaceAll("\"", "\"\"")) + "\",");
                        sb.append("\"" + (rs.getString(5) == null ? "" : rs.getString(5)) + "\",");
                        sb.append("\"" + (rs.getString(6) == null ? "" : rs.getString(6)) + "\",");
                        sb.append("\"" + (rs.getString(7) == null ? "" : rs.getString(7)) + "\",");
                        sb.append("\"" + (rs.getString(8) == null ? "" : rs.getString(8).trim()) + "\",");
                        sb.append("\"" + (rs.getString(9) == null ? "" : rs.getString(9)) + "\",");
                        sb.append("\"" + (rs.getString(10) == null ? "" : rs.getString(10)) + "\",");
                        sb.append("\"" + (rs.getString(11) == null ? "" : rs.getString(11)) + "\",");
                        sb.append("\"" + (rs.getString(12) == null ? "" : rs.getString(12)) + "\",");
                        sb.append("\"" + (rs.getString(13) == null ? "" : rs.getString(13)) + "\",");
                        sb.append("\"" + (rs.getString(14) == null ? "" : rs.getString(14)) + "\",");
                        sb.append("\"" + (rs.getString(15) == null ? "" : rs.getString(15)) + "\",");
                        sb.append("\"" + (rs.getString(16) == null ? "" : rs.getString(16)) + "\",");
                        sb.append("\"" + (rs.getString(17) == null ? "" : rs.getString(17)) + "\",");
                        sb.append("\"" + (rs.getString(18) == null ? "" : rs.getString(18))+"\"");
                        System.out.println(sb.toString());
                        sb.setLength(0);
                    }
                    long endTime = System.nanoTime();
                    System.out.println((endTime - startTime) / (1024 * 1024));
                } catch (Exception ex) {
                    ex.printStackTrace();
                } finally {
                    try {
                        rs.close();
                    } catch (Exception ex) {
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
            }
        }
    }
}
>>>>>>> 24e5a68860f09b3e497aadc003941dbdbb6750b8
