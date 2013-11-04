package cn.ac.ncic.impdatabg.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author AlexMu
 */
public class MessageProcess {

    private static Map<String, String> PhoneNumber2RegionCodeTable = null;
    private static final int MOBILEPHONE_NUMBER_LENGTH = 11;

    public static void main(String[] args) {
        String number = "01088440398";

        System.out.println(number.substring(0, 3));
    }

    public static void processMessage(String[] message) {
        //remove 86 or +86 or 12520 from ydz
        try {
            if (message[5].startsWith("86")) {
                message[5] = message[5].replaceFirst("86", "");
            } else if (message[5].startsWith("+86")) {
                message[5] = message[5].replaceFirst("\\+86", "");
            } else if (message[5].startsWith("12520")) {
                message[5] = message[5].replaceFirst("12520", "");
            }
        } catch (Exception ex) {
        }

        //remove 86 or +86 or 12520 from mddz
        try {
            if (message[6].startsWith("86")) {
                message[6] = message[6].replaceFirst("86", "");
            } else if (message[6].startsWith("+86")) {
                message[6] = message[6].replaceFirst("\\+86", "");
            } else if (message[6].startsWith("12520")) {
                message[6] = message[6].replaceFirst("12520", "");
            }
        } catch (Exception ex) {
        }

        //mark short message as  Internet type
        if (message[5].startsWith("106") || message[6].startsWith("106")) {
            if (message[10].length() < 3) {
                message[10] = "99";
            } else {
                message[10] = message[10].substring(0, 1) + "99";
            }
        }


        String regionCode;
        // get ydz_sd
        regionCode = getRegionCode(message[5]);

//        System.out.println("ydz_sd-->" + message[5] + ":" + regionCode);

        if (regionCode != null) {
            message[11] = regionCode;
        }

        // get mddz_sd
        regionCode = getRegionCode(message[6]);
//	System.out.println("mddz_sd-->" + message[5] + ":" + regionCode);

        if (regionCode != null) {
            message[12] = regionCode;
        }
    }

    private synchronized static boolean initPhoneNumber2RegionCodeTable() {

	if (PhoneNumber2RegionCodeTable != null){
		return true;
	}
        Connection dbConn = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");

            String dbUrl = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=dbnode1-vip)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=dbnode2-vip)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=dbnode3-vip)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=dbnode4-vip)(PORT=1521))(LOAD_BALANCE=yes)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=dbroker)))";
            String user = "dbk_user";
            String pwd = "dbk_pwd";

            dbConn = DriverManager.getConnection(dbUrl, user, pwd);

            Statement st = dbConn.createStatement();

            ResultSet rs = st.executeQuery("select tel,sd_id from tel_location");

            PhoneNumber2RegionCodeTable = new HashMap<String, String>();

            String tel = "";
            String sd_id = "";
            while (rs.next()) {
                tel = rs.getString("tel");
                sd_id = rs.getString("sd_id");
//		System.out.println(tel+":"+sd_id);
                PhoneNumber2RegionCodeTable.put(tel, sd_id);
            }

            rs.close();
            st.close();

//	    System.out.println("init PhoneNumber2RegionCodeTable successfully!"+PhoneNumber2RegionCodeTable.size());
        } catch (Exception ex) {
            if (PhoneNumber2RegionCodeTable != null) {
                PhoneNumber2RegionCodeTable.clear();
            }
            PhoneNumber2RegionCodeTable = null;
	    ex.printStackTrace();
            return false;
        } finally {
            try {
                if (dbConn != null) {
                    dbConn.close();
                }
            } catch (Exception ex) {
            }
            dbConn = null;
        }

        return true;
    }

    private static String getRegionCode(String phoneNumber) {

        if (PhoneNumber2RegionCodeTable == null) {
            if (!initPhoneNumber2RegionCodeTable()) {
                return null;
            }
        }

        String regionCode = null;
        int numberLength = phoneNumber.length();
        if (!phoneNumber.startsWith("10")) {
            //dispose common telephone number with regioncode as prefix,such as 01088440398
            if (phoneNumber.startsWith("0") && numberLength > 10) {
                regionCode = PhoneNumber2RegionCodeTable.get(phoneNumber.substring(0, 3)/*try the first three number*/);
                if (regionCode == null) {
                    regionCode = PhoneNumber2RegionCodeTable.get(phoneNumber.substring(0, 4)/*try the first four number*/);
                }
            } //dispose mobilephone number
            else if (numberLength > 10) {
                regionCode = PhoneNumber2RegionCodeTable.get(phoneNumber.substring(numberLength - MOBILEPHONE_NUMBER_LENGTH, phoneNumber.length() - 4));
            }
        }
        return regionCode;
    }
}
