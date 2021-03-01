package com.fiberhome.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Myudf  extends UDF {

    /**
     * udf 函数
     *  一进一出
     * @return
     */
    public Text evaluate( final Text line){
        // 需求： 首字母大写，其它小写；Smith

        if (null == line || "".equals(line.toString()) ) {
            return null;
        }


        String strLine = line.toString();

        String resultLine = strLine.substring(0,1).toUpperCase() +
                strLine.substring(1).toLowerCase();


        return new Text(resultLine);
    }

}
