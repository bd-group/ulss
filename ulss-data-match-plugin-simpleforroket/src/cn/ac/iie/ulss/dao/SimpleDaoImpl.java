/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.sql.PreparedStatement;
import javax.sql.DataSource;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;
import cn.ac.iie.ulss.struct.RowSetHolder;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.annotation.Transactional;
import cn.ac.iie.ulss.util.DbUtilSpring;
import cn.ac.iie.ulss.util.JdbcInfo;
import cn.ac.iie.ulss.struct.ColumnSetHolder;

/**
 *
 * @author fenglei
 */
public class SimpleDaoImpl implements SimpleDao {

    private JdbcTemplate jdbcTemplate;
    private static SimpleDaoImpl sigleDaoInstance;

    private SimpleDaoImpl() {
        jdbcTemplate = new JdbcTemplate();
        DataSource ds = DbUtilSpring.getDataSource();
        jdbcTemplate.setDataSource(ds);
    }

    public static SimpleDaoImpl getDaoInstance() {
        if (sigleDaoInstance == null) {
            sigleDaoInstance = new SimpleDaoImpl();
        }
        return sigleDaoInstance;
    }

    private SimpleDaoImpl(JdbcInfo jdbcInfo) {
        jdbcTemplate = new JdbcTemplate();
        DataSource ds = DbUtilSpring.getDataSource();
        jdbcTemplate.setDataSource(ds);

    }
    // ================ General Sql Operation =======.===========//

    @Transactional
    public void excuteSQL(String sql) {
        getJdbcTemplate().update(sql);
    }

    //如果有BLOB类型；取出来的blob为空值 (2008-11-12 测试通过)
    public RowResultSet queryForRowSet(String sql) {
        RowResultSet res = (RowResultSet) getJdbcTemplate().query(sql, new ResultSetExtractor() {
            public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
                RowResultSet myRs = new RowResultSet();

                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();

                RsMetaData myMeta = new RsMetaData(colCount);
                for (int i = 1; i <= colCount; i++) {
                    myMeta.setColName(meta.getColumnName(i), i - 1);
                    myMeta.setColType(meta.getColumnType(i), i - 1);
                    myMeta.setColTypeName(meta.getColumnTypeName(i), i - 1);
                }
                myRs.setMetaData(myMeta);

                RowSetHolder rowSet = new RowSetHolder();

                while (rs.next()) {
                    List record = rowSet.addNewRow();
                    for (int i = 0; i < colCount; i++) {
                        int type = meta.getColumnType(i + 1);
                        Object obj = getValue(rs, i + 1, type);
                        record.add(obj);
                    }
                }
                myRs.setRowSet(rowSet);
                return myRs;
            }
        });
        return res;

    }
//如果有BLOB类型；取出来的blob为空值 (2008-11-12 测试通过)

    public ColResultSet queryForColSet(String sql) {
//        setDataSource(sql);
        ColResultSet res = (ColResultSet) getJdbcTemplate().query(sql, new ResultSetExtractor() {
            public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
                ColResultSet myRs = new ColResultSet();

                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();

                RsMetaData myMeta = new RsMetaData(colCount);
                for (int i = 1; i <= colCount; i++) {
                    myMeta.setColName(meta.getColumnName(i), i - 1);
                    myMeta.setColType(meta.getColumnType(i), i - 1);
                    myMeta.setColTypeName(meta.getColumnTypeName(i), i - 1);
                }
                myRs.setMetaData(myMeta);

                ColumnSetHolder colSet = new ColumnSetHolder(colCount);
                while (rs.next()) {
                    for (int i = 0; i < colCount; i++) {
                        int type = meta.getColumnType(i + 1);
                        Object obj = getValue(rs, i + 1, type);
                        colSet.addObjectOfColumn(i, obj);
                    }
                }
                myRs.setColSet(colSet);
                return myRs;
            }
        });
//        RouterContextHolder.clearRouterType();
        return res;
    }
    //如果有BLOB类型；取出来的blob为空值 (2008-11-12 测试通过)

    public List queryForList(String sql) throws DataAccessException {
        RowResultSet res = this.queryForRowSet(sql);
        List list = res.getAllRowList();
        return list;

    }

    //获得列向量集合
    public List queryForColumnList(String sql) {
        ColResultSet res = this.queryForColSet(sql);
        List list = res.getAllColList();
        return list;

    }

    //(2008-11-13 测试Clob的读取通过,Clob的读取方式为getString/setString
    //idx start with 1
    private Object getValue(ResultSet rs, int idx, int type) throws SQLException {
        Object obj = null;
        switch (type) {
            case Types.NUMERIC:
                obj = Long.valueOf(rs.getLong(idx));
                break;
            case Types.TIMESTAMP:
                Timestamp ts = rs.getTimestamp(idx);
                if (ts != null) {
//                    obj = new java.util.Date(ts.getTime());
                    int len = ts.toString().length();
                    obj = ts.toString().substring(0, len - 2);
                }
                break;
            case Types.BLOB:
                obj = rs.getBlob(idx);          //预留字段总是返回null

                break;
            case Types.DATE:
                obj = rs.getString(idx);
            default:
                obj = rs.getString(idx);
        }
        return obj;
    }

    /**
     *
     * @
     * @param sql
     * @param args
     */
    public void batchExecuteSQL(String sql, final List<List> args) throws DataAccessException {
        getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {
            public int getBatchSize() {
                return (args.get(0)).size();
            }

            public void setValues(PreparedStatement ps, int i) throws SQLException {
                for (int j = 1; j <= args.size(); j++) {
                    ps.setObject(j, args.get(j - 1).get(i));
                }
            }
        });
    }

    /*
     *得到se的sequence
     *
     */
    public Long getNextSeID(Boolean isDefer) {
        String sql = "";
        if (isDefer == false) {
            sql = "select DBK_SEQ_SE_ID.nextval from dual";
        } else {
            sql = "select DBK_SEQ_DEFER_SE_ID.nextval from dual";
        }
        Long temp = getJdbcTemplate().queryForLong(sql);
        return temp;
    }
    /*
     *得到se的sequence
     *
     */

    public Long getNextSeIDCopy(Boolean isDefer) {
        String sql = "";
        if (isDefer == false) {
            sql = "select DBK_SEQ_SE_ID_COPY.nextval from dual";
        } else {
            sql = "select DBK_SEQ_DEFER_SE_ID_COPY.nextval from dual";
        }
        Long temp = getJdbcTemplate().queryForLong(sql);
        return temp;
    }

    public List<List> getNext2Seq(Boolean isDefer) {
        String sql = "";
        if (isDefer == false) {
            sql = "select DBK_SEQ_SE_ID.nextval,DBK_SEQ_SE_ID_COPY.nextval from dual";
        } else {
            sql = "select DBK_SEQ_DEFER_SE_ID.nextval,DBK_SEQ_DEFER_SE_ID_COPY.nextval from dual";
        }
        List<List> temp = queryForList(sql);
        return temp;
    }
    /*
     *得到se的sequence
     *
     */

    public Long getNextDeployIndexID() {
        String sql = "";

        sql = "select dbk_index_deploy_seq.nextval from dual";

        Long temp = getJdbcTemplate().queryForLong(sql);
        return temp;
    }

    public Long getNextMetaqID() {
        String sql = "";

        sql = "select metaq.nextval from dual";
        Long temp = getJdbcTemplate().queryForLong(sql);
        return temp;
    }

    public Long getDeferIndexerNum() {
        String sql = "select CUR_INDEXER_NUM from dbk_createdefer_indexer where rownum <2";
        Long temp = getJdbcTemplate().queryForLong(sql);
        if (temp == null || temp == 0) {
            temp = 1L;
        }
        return temp;
    }

    public void completeDeferIndexer(Long num) {
        if (num == null || num == 0) {
            num = 1L;
        }
        String sql = "update dbk_createdefer_indexer set CUR_INDEXER_NUM=" + num;
        getJdbcTemplate().update(sql);

    }

    /**
     * inserDate
     *
     * @param sql
     * @param args
     */
    public void insertDate(String sql, Object[] args) {
        getJdbcTemplate().update(sql, args);
    }

    /**
     * @return the jdbcTemplate
     */
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * @param jdbcTemplate the jdbcTemplate to set
     */
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}
