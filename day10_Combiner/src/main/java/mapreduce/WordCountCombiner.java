package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text k2, Iterable<LongWritable> v2Arr, Context context) throws IOException, InterruptedException {
        // 1- 得到k3  ---> k3 == k2

        // 2- 得到v3 v2的长度就是V3
        long v = 0;
        for (LongWritable v2 : v2Arr) {
            v += v2.get();
        }
        LongWritable v3 = new LongWritable(v);
        // 3- 将k3 v3 写入上下文中
        context.write(k2,v3);


    }
}
