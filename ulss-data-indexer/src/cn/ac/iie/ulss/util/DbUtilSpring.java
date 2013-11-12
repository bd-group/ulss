/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.util;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;
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
    public static DataSource getDataSource() {
        String cfg_file = "db.properties";
        DriverManagerDataSource ds = null;
        try {
            FileInputStream inStream = new FileInputStream(cfg_file);
            Properties oracle_cfg = new Properties();
            oracle_cfg.load(inStream);

            ds = new DriverManagerDataSource();
            ds.setDriverClassName(oracle_cfg.getProperty("jdbc.driverClass"));
            ds.setUsername(oracle_cfg.getProperty("jdbc.user"));
            ds.setPassword(oracle_cfg.getProperty("jdbc.password"));
            ds.setUrl(oracle_cfg.getProperty("jdbc.jdbcUrl"));
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
