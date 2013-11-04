/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author AlexMu
 */
public class DateService {

    public static Date getCurrentHour(String dateRegx) {
        return getHour(0, dateRegx);
    }

    public static Date getLastHour(String dateRegx) {
        return getHour(-1, dateRegx);
    }

    private static Date getHour(int amount, String dateRegx) {
        return getTime(Calendar.HOUR_OF_DAY, amount, dateRegx);
    }

    public static Date getTime(int field, int amount, String dateRegx) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(field, amount);

        SimpleDateFormat sdf = new SimpleDateFormat(dateRegx);
        Date date = null;
        try {
            date = sdf.parse(sdf.format(cal.getTime()));
        } catch (Exception ex) {
            return null;
        }
        return date;
    }

    public static String getTimeStr(Date date, String dateRegx) {
        SimpleDateFormat sdf = new SimpleDateFormat(dateRegx);
        return sdf.format(date);
    }

}
