package com.fiberhome.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.xerces.impl.io.UCSReader;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

public class DemoHDFS {

    // 获取FileSystem 方式一
    public void getFileSystem1(){

        try{
            // 1- 创建configuration 对象
            Configuration configuration = new Configuration();
            //指定我们使用的文件系统类型:
            configuration.set("fs.defaultFS","hdfs://node1:8020/");

            FileSystem fileSystem = FileSystem.get(configuration);
            System.out.println(fileSystem.getUri());

        }catch ( Exception e){
            e.printStackTrace();
        }

    }

    public void getFileSystem2(){
        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020/"), new Configuration());

            System.out.println(fileSystem.getUri());
        }catch ( Exception e){
            e.printStackTrace();
        }
    }


    // 遍历根目录的文件
    public void listFiles(){
        try{
            // 1- 获取Filesystem
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020/"), new Configuration());

            // 2- 获取RemoteIterator 得到所有的文件或者文件夹，
            // 第一个参数指定遍历的路径，
            // 第二个参数表示是否要递归遍历
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
            while (iterator.hasNext()){
                LocatedFileStatus next = iterator.next();
                Path path = next.getPath();
                long len = next.getLen();

                // 获取lock 信息
                BlockLocation[] blockLocations = next.getBlockLocations();

                // 数组的长度就是block的个数
                int  count  = blockLocations.length;
                for (BlockLocation blockLocation : blockLocations) {
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts) {
                        System.out.println(host);
                    }
                }
                System.out.println(path.toString() + "len == " + len   );
            }
        }catch ( Exception e){
            e.printStackTrace();
        }
    }


    // 创建一个文件夹
    public void mkdirs(){

        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            fileSystem.mkdirs(new Path("/day08/test/mkdir"));
            fileSystem.close();

        }catch ( Exception e){
            e.printStackTrace();
        }
    }


    // 删除一个文件夹
    public void deleteDirs(){

        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());

            fileSystem.delete(new Path("/day08"),true);

            fileSystem.close();

        }catch ( Exception e){
            e.printStackTrace();
        }
    }

    // 长传一个文件
    public void putFile(){

        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            fileSystem.copyFromLocalFile(new Path("E:\\hdfsFilesTest\\a.txt"),new Path("/day08/a.txt"));
            fileSystem.close();

        }catch ( Exception e){
            e.printStackTrace();
        }
    }

    // 删除一个文件
    public void deleteFile(){

        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());

            Path path = new Path("/day08/a.txt");
            // 1- 判断文件是否存在
            boolean exists = fileSystem.exists(path);
            // 2- 判断是不是文件
            boolean isFile = fileSystem.isFile(path);
            if (exists ) {
                fileSystem.delete(path,true);
            }
            fileSystem.close();

        }catch ( Exception e){
            e.printStackTrace();
        }
    }
    
    // 判断目录或文件是否存在  判断是不是文件
    public void exists_isFile(){
        
        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            boolean exists = fileSystem.exists(new Path("/day08"));
            System.out.println("dir is exists ? " + exists);

            boolean isfile = fileSystem.isFile(new Path("/day08/a.txt"));
            System.out.println("is File ? " + isfile);

            fileSystem.close();
        }catch ( Exception e){
            e.printStackTrace();
        }
        
    }

    // 下载文件1
    public void downloadFile1(){
        try{

            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());

            fileSystem.copyToLocalFile(new Path("/day08/a.txt"),new Path("E:\\hdfsFilesTest\\aa.txt"));

            fileSystem.close();

        }catch ( Exception e){
            e.printStackTrace();

        }
    }

    // 下载文件2
    public void downloadFile2(){
        try{

            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            FSDataInputStream inputStream = fileSystem.open(new Path("/day08/a.txt"));
            FileOutputStream outputStream = new FileOutputStream(new File("E:\\hdfsFilesTest\\aaa.txt"));
            int copy = IOUtils.copy(inputStream, outputStream);
            System.out.println(copy);
            IOUtils.closeQuietly(outputStream);
            IOUtils.closeQuietly(inputStream);
            fileSystem.close();

        }catch ( Exception e){
            e.printStackTrace();

        }
    }

    // 小文件合并到hdfsO
    public void mergeFiles(){
        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());

            FSDataOutputStream outputStream = fileSystem.create(new Path("/day08/bigFile.txt"));

            LocalFileSystem local = FileSystem.getLocal(new Configuration());
            FileStatus[] fileStatuses = local.listStatus(new Path("E:\\hdfsFilesTest\\"));

            for (FileStatus fileStatus : fileStatuses) {
                FSDataInputStream inputStream = local.open(fileStatus.getPath());
                IOUtils.copy(inputStream,outputStream);
                Path path = fileStatus.getPath();
                String content = path.toString() + "\r\n";
                outputStream.write(content.getBytes());
                IOUtils.closeQuietly(inputStream);
            }

            IOUtils.closeQuietly(outputStream);
            fileSystem.close();
        }catch ( Exception e){
            e.printStackTrace();
        }
    }


    // 小文件合并到hdfsO
    public void mergeAppendFiles(){
        try{
            Configuration configuration = new Configuration();
            configuration.setBoolean("dfs.support.append",true);
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),configuration );

            //FSDataOutputStream outputStream = fileSystem.create(new Path("/day08/bigFile.txt"),true);
            FSDataOutputStream outputStream = fileSystem.append(new Path("/day08/bigFile.txt"));

            LocalFileSystem local = FileSystem.getLocal(new Configuration());
            FileStatus[] fileStatuses = local.listStatus(new Path("E:\\hdfsFilesTest\\"));

            for (FileStatus fileStatus : fileStatuses) {
                FSDataInputStream inputStream = local.open(fileStatus.getPath());
                IOUtils.copy(inputStream,outputStream);
                Path path = fileStatus.getPath();
                String content = path.toString() + "\r\n";
                outputStream.write(content.getBytes());
                IOUtils.closeQuietly(inputStream);
            }

            IOUtils.closeQuietly(outputStream);
            fileSystem.close();
        }catch ( Exception e){
            e.printStackTrace();
        }
    }
}
