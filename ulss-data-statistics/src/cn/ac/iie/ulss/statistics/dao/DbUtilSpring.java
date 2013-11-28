/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.statistics.dao;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 *
 * @author fenglei
 */
public class DbUtilSpring {

    public DbUtilSpring() {
    }

    //获取数据库连接
    public static Connection getConnection(JdbcInfo jdbcInfo) throws SQLException, SQLException {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName(jdbcInfo.getDriverClassName());
        ds.setUsername(jdbcInfo.getUsername());
        ds.setPassword(jdbcInfo.getPassword());
        ds.setUrl(jdbcInfo.getUrl());
        Connection con = ds.getConnection();
        return con;
    }

    //获取dataSource
    public static DataSource getDataSource(String dbCluster) {
        DriverManagerDataSource ds = null;
        try {
            String[] db = dbCluster.split("\\|");
            ds = new DriverManagerDataSource();
            ds.setDriverClassName(db[0]);
            ds.setUrl(db[1]);
            ds.setUsername(db[2]);
            ds.setPassword(db[3]);
        } catch (Exception e) {
            System.out.println(e);
        }
        return ds;
    }

    public static DataSource getDataSource(JdbcInfo jdbcInfo) {
        DriverManagerDataSource ds = null;
        try {
            //创建ds
            ds = new DriverManagerDataSource();
            ds.setDriverClassName(jdbcInfo.getDriverClassName());
            ds.setUsername(jdbcInfo.getUsername());
            ds.setPassword(jdbcInfo.getPassword());
            ds.setUrl(jdbcInfo.getUrl());
        } catch (Exception e) {
        }
        return ds;
    }

    //关闭数据库连接
    public void closeConnection(Connection con) {
        try {
            if (con != null) {
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
