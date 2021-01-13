package com.fiberhome.mapreduce.flowcount.reducer;

import com.fiberhome.mapreduce.flowcount.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        Long upPackageNum = 0L ;
        Long downPackageNum = 0L ;
        Long upPayLoad = 0L ;
        Long downPayLoad = 0L ;
        for (FlowBean flowBean : values) {
            upPackageNum += flowBean.getUpPackageNum();
            downPackageNum += flowBean.getDownPackageNum();
            upPayLoad += flowBean.getUpPayLoad();
            downPayLoad += flowBean .getDownPayLoad();
        }

        FlowBean flowBean = new FlowBean(upPackageNum, downPackageNum, upPayLoad, downPayLoad);

        context.write(key,flowBean);
    }
}
