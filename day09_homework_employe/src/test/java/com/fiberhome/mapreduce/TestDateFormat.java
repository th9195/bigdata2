package com.fiberhome.mapreduce;


import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDateFormat {

    @Test
    public void test() throws ParseException {
        String dateStr = "23-3月-90";

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM月-yy");
        Date date = simpleDateFormat.parse(dateStr);
        System.out.println(date);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String format = dateFormat.format(date);
        System.out.println(format);
    }



}
