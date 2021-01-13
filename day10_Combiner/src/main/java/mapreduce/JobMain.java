package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

public class JobMain {

    // 将MR串成一个job 任务，提交给yarn执行
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path inputPath = new Path("hdfs://node1:8020/input/wordcount");
//        Path inputPath = new Path("file:///E:\\input\\wordcount");
        Path outputPath = new Path("hdfs://node1:8020/output/wordcount");
//        Path outputPath = new Path("file:///E:\\output\\wordcount");

        // 1- 创建一个job任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "combiner"); // 任务名称

        // 2- 指定job所在的jar包
        job.setJarByClass(JobMain.class);

        // 3- 指定源文件的读取文件方式类  和  源文件的读取路径
        job.setInputFormatClass(TextInputFormat.class); // 按照行读取
        // linux
        TextInputFormat.addInputPath(job,inputPath);
        // windows
        //TextInputFormat.addInputPath(job,inputPath));

        //4- 指定自定义的mapper类 和 K2 V2 类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 5- 指定自定义分区类
        // 6- 指定自定义分组类

        // 如果自定义了combiner, 需要设置combiner类；
        // job.setCombinerClass(WordCountCombiner.class);
        // 7- 指定自定义 的 Reducer类和 K3  V3 的类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 8- 指定输出方式类和结果输出路径
        job.setOutputFormatClass(TextOutputFormat.class);  // 输出到一个文本文件
        // Linux
        deletePathIfExists(outputPath);
        TextOutputFormat.setOutputPath(job,outputPath);
        // windows
        //TextOutputFormat.setOutputPath(job,outputPath));

        // 9- 将job 交给yarn运行
        boolean b = job.waitForCompletion(true);// true 表示可以看到任务的执行进度

        // 10-  退出进程
        System.exit(b?0:1);
    }


    /**
     * 判断改路径是否存在，如果存就删除
     * @param path
     */
    public static void deletePathIfExists(Path path){
        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            boolean exists = fileSystem.exists(path);
            if (exists ) {
                fileSystem.delete(path,true);
            }
        }catch ( Exception e){
            e.printStackTrace();
        }

    }
}
