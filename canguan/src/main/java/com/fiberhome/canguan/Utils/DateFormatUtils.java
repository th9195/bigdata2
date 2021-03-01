package com.fiberhome.canguan.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormatUtils {


    /**
     * 将Date 日期 转成 String 日期
     * @param date  Date日期
     * @param desFormat   目标字符串日期格式
     * @return
     */
    public static String dateToStr(Date date, String desFormat) {

        String dateStr = null;
        try{

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(desFormat);
            dateStr = simpleDateFormat.format(date);

        }catch ( Exception e){
            e.printStackTrace();
        }
        return dateStr;
    }


    /**
     *  将字符串 日期 转成 Date
     * @param srcFormat  字符串日期格式
     * @param dayStr     原字符串日期
     * @return
     */
    public static Date strToDate(String srcFormat, String dayStr) {
        Date date = null;
        try{
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(srcFormat);
            date = simpleDateFormat.parse(dayStr);

        }catch ( Exception e){
            e.printStackTrace();
        }
        return date;
    }



}
