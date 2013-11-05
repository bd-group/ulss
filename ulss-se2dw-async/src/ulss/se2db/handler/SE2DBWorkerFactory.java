/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.handler;

/**
 *
 * @author alexmu
 */
public class SE2DBWorkerFactory {

    public static SE2DBWorker getSE2DBWroker(String pDBType, TableSe2DBHandler pTableSe2DBHandler) throws Exception {
        SE2DBWorker se2DBWorker = null;

        if (pDBType.equals("st")) {
            se2DBWorker = new SE2STDBWorker(pTableSe2DBHandler);
        } else if (pDBType.equals("gbase")) {
            se2DBWorker = new SE2GBDBWorker(pTableSe2DBHandler);
        } else {
            throw new Exception("unknown ");
        }
        return se2DBWorker;
    }
}
