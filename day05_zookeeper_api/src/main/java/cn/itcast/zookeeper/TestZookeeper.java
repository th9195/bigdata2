package cn.itcast.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZookeeper {
    private RetryPolicy retryPolicy  = null;
//    String connectionStr = "192.168.88.161:2181,192.168.88.162:2181,192.168.88.163:2181";
    String connectionStr = "node1:2181,node2:2181,node3:2181";

    CuratorFramework client = null;

    @Before
    public void init(){
        retryPolicy = new ExponentialBackoffRetry(1000, 1);
        client = CuratorFrameworkFactory.newClient(connectionStr, retryPolicy);
        client.start();
    }


    @After
    public void close(){
        client.close();
    }


    @Test
    public void deleteZnode() throws Exception {
//        client.delete().forPath("/app1");
        client.delete().deletingChildrenIfNeeded().forPath("/app1");
    }

    @Test
    public void setZnode() throws Exception {
        client.setData().forPath("/app1","app1".getBytes());
    }

    @Test
    public void createZnode() throws Exception {
        client.
                create().
                creatingParentsIfNeeded().
                withMode(CreateMode.PERSISTENT).
                forPath("/app1/app1-1/app1-1-1","hello".getBytes());
    }


    @Test
    public void createTempZnode() throws Exception {

        //1:定制一个重试策略
	    /*
            param1: 重试的间隔时间
            param2: 重试的最大次数
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);


        //2:获取一个客户端对象
        /*
           param1:要连接的Zookeeper服务器列表
           param2:会话的超时时间
           param3:链接超时时间
           param4:重试策略
         */
        String connectionStr = "node1:2181,node2:2181,192.168.88.163:2181";

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, retryPolicy);

        // 3- 开启客户端
        client.start();


        // 4- 创建节点
        /**
         * PERSISTENT               永久节点
         * PERSISTENT_SEQUENTIAL    永久序列化节点
         * EPHEMERAL                临时节点
         * EPHEMERAL_SEQUENTIAL     临时序列化节点
         */
        client.
                create().
                creatingParentsIfNeeded().
                withMode(CreateMode.EPHEMERAL).
                forPath("/temp222_java","hello".getBytes());

        // 5- 关闭客户端
        // Thread.sleep(Integer.MAX_VALUE);
        client.close();

    }


    @Test
    public void demoCreateZnode() throws Exception {

        //1:定制一个重试策略
	/*
		param1: 重试的间隔时间
		param2: 重试的最大次数
	 */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);


        //2:获取一个客户端对象
	/*
	   param1:要连接的Zookeeper服务器列表
	   param2:会话的超时时间
	   param3:链接超时时间
	   param4:重试策略
	 */
        String connectionStr = "192.168.88.161:2181,192.168.88.162:2181,192.168.88.163:2181";

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, retryPolicy);

        // 3- 开启客户端
        client.start();


        // 4- 创建节点
        /**
         * PERSISTENT(
         * PERSISTENT_SEQUENTIAL(2, false, true),
         * EPHEMERAL(1, true, false),
         * EPHEMERAL_SEQUENTIAL(3, true, true);
         */
        client.
                create().
                creatingParentsIfNeeded().
                withMode(CreateMode.PERSISTENT).
                forPath("/app_java","hello".getBytes());

        // 5- 关闭客户端
        client.close();

    }


    @Test
    public void watchZnode() throws Exception {
        TreeCache treeCache = new TreeCache(client, "/app1");

        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {



                switch (treeCacheEvent.getType()){
                    case NODE_ADDED:
                        System.out.println("增加");


                        break;
                    case NODE_REMOVED:
                        System.out.println("删除");
                        break;
                    case NODE_UPDATED:
                        System.out.println("修改");
                        break;
                }

                ChildData childData = treeCacheEvent.getData();
                if (null != childData){
                    System.out.println("data == " + new String(childData.getData()));
                    System.out.println("path == " + childData.getPath());
                }

            }
        });

        // 开启监听
        treeCache.start();

        // 挂起
        Thread.sleep(Integer.MAX_VALUE);

    }

}
