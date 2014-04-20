/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.struct;

import cn.ac.iie.ulss.indexer.worker.DataWriter;
import java.util.concurrent.Future;

/**
 *
 * @author liucuili
 */
public class PoolWriter {

    public DataWriter dataWriter;
    public Future result;

    public DataWriter getDataWriter() {
        return dataWriter;
    }

    public void setDataWriter(DataWriter dataWriter) {
        this.dataWriter = dataWriter;
    }

    public Future getResult() {
        return result;
    }

    public void setResult(Future result) {
        this.result = result;
    }
}
