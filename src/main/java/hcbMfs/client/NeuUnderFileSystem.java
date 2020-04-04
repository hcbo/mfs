
package hcbMfs.client;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaFuture;


import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * n
 */

public class NeuUnderFileSystem  {

  public static final String NEU_SCHEME = "neu://";

  public CuratorFramework client;

  AdminClient adminClient ;

  KafkaProducer<String, byte[]> producer ;

  String rootPath ; // china

  Properties properties = new Properties();


  /**
   * Constructs a new {@link NeuUnderFileSystem}.
   *
   * @param conf UFS configuration
   */
  public NeuUnderFileSystem(URI uri, Configuration conf) {


      MfsFileSystem.LOG.error("NeuUnderFileSystem 构造方法开始");
      String uriStr =  uri.toString();
      RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
      client = CuratorFrameworkFactory.builder()
            .connectString("kafka:2181")
            .retryPolicy(retryPolicy)
            .sessionTimeoutMs(6000)
            .connectionTimeoutMs(3000)
//            .namespace("fileSize1")
            .build();
      client.start();
      this.rootPath = getRootPath(uri);
      try {
          if(null == client.checkExists().forPath("/"+rootPath)){
              //写入rootPath 元信息到zookeeper
              PathInfo pathInfo = new PathInfo(true,rootPath,System.currentTimeMillis());

              byte[] input = SerializationUtils.serialize(pathInfo);
              try {
                  client.create()
                          .creatingParentContainersIfNeeded()
                          .forPath("/"+rootPath, input);
              } catch (Exception e) {
                  e.printStackTrace();
              }
          }
      } catch (Exception e) {
          e.printStackTrace();
      }



      //kafka的property
      properties.put("bootstrap.servers","192.168.225.6:9092");
      properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
      properties.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
      properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
      properties.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
      properties.put("enable.auto.commit",true);
      // todo group id 变化起来?
      properties.put("group.id","test10");
      properties.put("auto.offset.reset","earliest");
      properties.put("acks", "-1");
      properties.put("retries", 3);
      properties.put("buffer.memory", 33554432);


      adminClient = AdminClient.create(properties);

      try{
          MfsFileSystem.LOG.error(" system 线程上下文加载器 "+ Thread.currentThread().getContextClassLoader());
          producer = new KafkaProducer<String, byte[]>(properties);
      }catch (Exception e){

          MfsFileSystem.LOG.error(e.getMessage());
      }


      initTopicPartitions(adminClient,rootPath);

      MfsFileSystem.LOG.error("NeuUnderFileSystem 构造方法执行完毕");
  }

    private void initTopicPartitions(AdminClient adminClient, String rootPath) {
        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        //将包含metadata的topic消息设置为70days
        NewTopic newTopic1 = new NewTopic(rootPath, 1, (short)1);
        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms","6048000000");
        newTopic1 = newTopic1.configs(configs);
        //将包含metadata的topic消息设置为70days
        newTopics.add(newTopic1);

        NewTopic newTopic2 = new NewTopic(rootPath+"_commits", 1, (short)1);
        newTopics.add(newTopic2);

        NewTopic newTopic3 = new NewTopic(rootPath+"_offsets", 1, (short)1);
        newTopics.add(newTopic3);

        NewTopic newTopic4 = new NewTopic(rootPath+"_sources", 10, (short)1);
        newTopics.add(newTopic4);

        NewTopic newTopic5 = new NewTopic(rootPath+"_state_0", 200, (short)1);
        newTopics.add(newTopic5);

        // 假设3个state的应用
        NewTopic newTopic6 = new NewTopic(rootPath+"_state_1", 200, (short)1);
        newTopics.add(newTopic6);
        NewTopic newTopic7 = new NewTopic(rootPath+"_state_2", 200, (short)1);
        newTopics.add(newTopic7);

        try {
            if(null == client.checkExists().forPath("/brokers/topics/"+rootPath)){
                CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
                // 确保topic创建成功
                KafkaFuture kafkaFuture = createTopicsResult.all();
                try {
                    kafkaFuture.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String getRootPath(URI uri) {
      //mfs://localhost:8888/china
        String fullPath = uri.toString();
        return fullPath.substring(fullPath.lastIndexOf('/')+1,fullPath.length());
    }


    public String getUnderFSType() {
      MfsFileSystem.LOG.error("getUnderFSType() 执行");
      return "neu";
  }


  public void close() throws IOException {

  }


  public OutputStream create(String path) throws IOException {
      MfsFileSystem.LOG.error("create()方法执行 path="+path);
      KafkaProducer<String, byte[]> produc = null;
      try{
          MfsFileSystem.LOG.error("System.getSecurityManager():"+System.getSecurityManager());
          MfsFileSystem.LOG.error("加载器: "+Thread.currentThread().getContextClassLoader());
          MfsFileSystem.LOG.error("该System类加载器: "+this.getClass().getClassLoader());
//          Thread.currentThread().setContextClassLoader(null);
          produc = new KafkaProducer<String, byte[]>(properties);
      }catch (Exception e){
          MfsFileSystem.LOG.error("异常"+e.getMessage()+ "\n--------");
          StringWriter sw = new StringWriter();
          e.printStackTrace(new PrintWriter(sw));
          MfsFileSystem.LOG.error("异常栈: "+sw.toString());
      }

    return new NeuFileOutputStream(client,stripPath(path),produc);
  }


  public boolean deleteDirectory(String path) throws IOException {
      MfsFileSystem.LOG.error("deleteDirectory()方法执行 path="+path);
    return true;
  }


  public boolean deleteFile(String path) throws IOException {
      MfsFileSystem.LOG.error("deleteFile()方法执行 path="+path);
//      String underPath = stripPath(path);
//      if(isFile(underPath)){
//          try {
//              client.delete()
//                      .guaranteed()      //删除失败，则客户端持续删除，直到节点删除为止
//                      .deletingChildrenIfNeeded()   //删除相关子节点
//                      .withVersion(-1)    //无视版本，直接删除
//                      .forPath(underPath);
//          } catch (Exception e) {
//              e.printStackTrace();
//          }
//      }
      return true;
  }


  public boolean exists(String path) throws IOException {
      MfsFileSystem.LOG.error("exists()方法执行 path="+path);
      String underPath = path;
      if(path.contains(MfsFileSystem.FS_SEAWEED_DEFAULT_PORT+"")){
          underPath = stripPath(path);
      }
        try {
            return null != client.checkExists().forPath(underPath);
        } catch (Exception e) {
            MfsFileSystem.LOG.error(e.getMessage());
            e.printStackTrace();
        }
        return false;
  }

  public long getBlockSizeByte(String path) throws IOException {
      MfsFileSystem.LOG.error("getBlockSizeByte()方法执行 path="+path);
    String underPath = stripPath(path);
    if(exists(underPath)){
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath(underPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        return pathInfo.fileInfo.contentLength;

    }
    else {
        return 100;
    }
  }

  public FileStatus getDirectoryStatus(String path) throws IOException {
      MfsFileSystem.LOG.error("getDirectoryStatus()方法执行 path="+path);
      String underPath = stripPath(path);
      if(exists(underPath)){
          byte[] output = new byte[0];
          try {
              output = client.getData().forPath(underPath);
          } catch (Exception e) {
              e.printStackTrace();
          }
          PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
          return new FileStatus(100,pathInfo.isDirectory, 100,100,100,new Path(path));
      }else {
          return null;
      }
  }


//  public List<String> getFileLocations(String path) throws IOException {
//      MfsFileSystem.LOG.error("getFileLocations()方法执行 path="+path);
//    List<String> ret = new ArrayList<>();
//    ret.add(NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC, mUfsConf));
//    return ret;
//  }

//
//  public List<String> getFileLocations(String path, FileLocationOptions options)
//      throws IOException {
//    return getFileLocations(path);
//  }


//  public UfsFileStatus getFileStatus(String path) throws IOException {
//      MfsFileSystem.LOG.error("getFileStatus()方法执行 path="+path);
//    String underPath = stripPath(path);
//    if(isFile(underPath)){
//        byte[] output = new byte[0];
//        try {
//            output = client.getData().forPath(underPath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
//        return new UfsFileStatus(pathInfo.name,pathInfo.fileInfo.contentHash,
//                pathInfo.fileInfo.contentLength,pathInfo.lastModified,
//                pathInfo.owner,pathInfo.group,pathInfo.mode);
//    }else {
//        return null;
//    }
//  }

//  @Override
//  public long getSpace(String path, SpaceType type) throws IOException {
//      MfsFileSystem.LOG.error("getSpace()方法执行 path="+path);
//    if(type.getValue()==0){
//      return 249849593856L;
//    }else if (type.getValue()==2){
//      return 105187893248L;
//    }else {
//      return 100000000000L;
//    }
//  }

//  @Override
//  public UfsStatus getStatus(String path) throws IOException {
//      MfsFileSystem.LOG.error("getStatus()方法执行 path="+path);
//      String underPath = stripPath(path);
//      if(exists(underPath)){
//          byte[] output = new byte[0];
//          try {
//              output = client.getData().forPath(underPath);
//          } catch (Exception e) {
//              e.printStackTrace();
//          }
//          PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
//
//          if(isFile(underPath)){
//              return new UfsFileStatus(pathInfo.name,pathInfo.fileInfo.contentHash,
//                      pathInfo.fileInfo.contentLength,pathInfo.lastModified,
//                      pathInfo.owner,pathInfo.group,pathInfo.mode);
//          }else {
//              return new UfsDirectoryStatus(pathInfo.name,pathInfo.owner,
//                      pathInfo.group,pathInfo.mode,pathInfo.lastModified);
//          }
//      }else {
//          return null;
//      }
//
//
//  }


  public boolean isDirectory(String path) throws IOException {
      MfsFileSystem.LOG.error("isDirectory()方法执行 path="+path);
    String underPath = stripPath(path);
    if(exists(path)){
      return !isFile(path);
    }else {
      return false;
    }
  }


  public boolean isFile(String path) throws IOException {
      MfsFileSystem.LOG.error("isFile()方法执行 path="+path);
    String underPath = stripPath(path);
    if(underPath.contains(".alluxio_ufs_blocks")){
      return false;
    }else if (exists(path)){
      // 读元信息
      byte[] output = new byte[0];
      try {
        output = client.getData().forPath(underPath);
      } catch (Exception e) {
        e.printStackTrace();
        MfsFileSystem.LOG.error(e.getMessage());
      }
      PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
      return !pathInfo.isDirectory;
    }else {
      return false;
    }
  }


  public FileStatus[] listStatus(String path) throws IOException {
      MfsFileSystem.LOG.error("listStatus()方法执行 path="+path);
    String underPath = stripDirPath(path);
    // 根据zk 获取子节点 getchildlen
    List<String> children = null;
    try {
       children = client.getChildren().forPath(underPath);
    } catch (Exception e) {
      e.printStackTrace();
    }
    FileStatus[] rtn = new FileStatus[children.size()];
    if(children != null && children.size() != 0){

      int i = 0;
      for(String child:children){
        String childPath = underPath+"/"+child;
          FileStatus retStatus;
        // 取元信息出来
        byte[] output = new byte[0];
        try {
          output = client.getData().forPath(childPath);
        } catch (Exception e) {
          e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        FsPermission permission = FsPermission.createImmutable((short)777);
        retStatus = new FileStatus(pathInfo.fileInfo.contentLength,pathInfo.isDirectory,
                1,512,pathInfo.lastModified,0,permission,null,
                null,null,new Path(childPath));

        rtn[i++] = retStatus;
      }
      return rtn;
    }else {
        return rtn;
    }

  }

  /**
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/offsets
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/commits
   */

  public boolean mkdirs(String path ) throws IOException {
      MfsFileSystem.LOG.error("mkdirs()方法执行 path="+path);
      //    // 传入的一定是目录的路径
    String underPath = stripDirPath(path);
    if(exists(underPath)){
      return false;
    }else {
      // save to zookeeper
      PathInfo pathInfo = new PathInfo();
      pathInfo.name = underPath;
      pathInfo.isDirectory = true;
      byte[] input = SerializationUtils.serialize(pathInfo);
      try {
        client.create()
                .creatingParentContainersIfNeeded()
                .forPath(underPath, input);
      } catch (Exception e) {
        e.printStackTrace();
      }

      // create topic and partition
//      initTopicPartition(underPath);
      return true;
    }

  }

    private String stripDirPath(String path) {
        String divSign = MfsFileSystem.FS_SEAWEED_DEFAULT_PORT+"";
        int begin = path.indexOf(divSign)+divSign.length();

        return path.substring(begin,path.length());
    }

    /**
   * /china
   * /china/offsets
   * /china/sources --
   * /china/sources/0
   * /china/state --
   * /china/state/0 --
   * /china/state/0/0
   * /china/commits
   */

    private void initTopicPartition(String underPath){
        //去掉第一个/
        underPath = underPath.substring(1,underPath.length());
        String topicName = getTopicName(underPath);
        if(topicName!= null){
            //创建topic
            NewTopic newTopic = new NewTopic(topicName, 1, (short)1);

            //将包含metadata的topic消息设置为70days
            if(!topicName.contains("_")){
                Map<String, String> configs = new HashMap<>();
                configs.put("retention.ms","6048000000");
                newTopic = newTopic.configs(configs);
            }

            List<NewTopic> newTopics = new ArrayList<NewTopic>();
            newTopics.add(newTopic);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
            // 确保topic创建成功
            KafkaFuture kafkaFuture = createTopicsResult.all();
            try {
              kafkaFuture.get();
            } catch (InterruptedException e) {
              e.printStackTrace();
            } catch (ExecutionException e) {
              e.printStackTrace();
            }
        }
        int partitionNo = getPartionNo(underPath);
        if(partitionNo != 0){
            topicName = setTopicName(underPath);
            // 增加partition
            int oldPartitions = producer.partitionsFor(topicName).size();
            if(partitionNo < oldPartitions){
              // no-op
            }else {
              // 增加到partitionNO+1,因为partitionNO是从0开始
              Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
              newPartitionsMap.put(topicName,NewPartitions.increaseTo(partitionNo+1));
              CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(newPartitionsMap);
              // 确保partition增加成功
              KafkaFuture kafkaFuture = createPartitionsResult.all();
              try {
                kafkaFuture.get();
              } catch (InterruptedException e) {
                e.printStackTrace();
              } catch (ExecutionException e) {
                e.printStackTrace();
              }
            }
        }
    }

    private String setTopicName(String underPath) {
        String topicName = null;
        if(underPath.contains("sources")){
            topicName = "sources";
        }else {
           // china/state/0/1
            topicName = underPath.substring(0,underPath.lastIndexOf("/")).replace('/','_');
        }
        return topicName;
    }

    private int getPartionNo(String underPath) {
        int partitonNo = 0;
        if(underPath.contains("sources")|| underPath.contains("state")){
            String partitonNoStr = underPath.substring(underPath.lastIndexOf("/")+1,underPath.length());
            partitonNo = Integer.parseInt(partitonNoStr);
        }
        return partitonNo;
    }

    private String getTopicName(String underPath) {
        String topicName = null;
        if(!underPath.contains("/")||underPath.endsWith("offsets")||
        underPath.endsWith("commits")){
            topicName = underPath.replace("/","_");
        }else if(underPath.endsWith("sources/0")){
            topicName = "sources";
        }else if(underPath.contains("state")&&underPath.endsWith("0")){
            // state/*/0
            topicName = underPath.substring(0,underPath.lastIndexOf("/")).
                    replace("/","_");
        }
        return topicName;
    }

//  private void initTopicPartition(String underPath) {
//    //将mount的路径取消掉
//    String realPath = underPath.substring(1,underPath.length());
//
//
//    // 什么都不做 checkpoint_streaming1/state
//    if(realPath.endsWith("/state")){
//      return;
//    }
//
//    // 创建topic
//    // checkpoint_streaming1  checkpoint_streaming1/offsets  checkpoint_streaming1/commits
//    // checkpoint_streaming1/state/0  checkpoint_streaming1/sources
//    else if(realPath.endsWith("/offsets")||realPath.endsWith("/commits")||
//            realPath.endsWith("/sources")||!realPath.contains("/")||
//            realPath.matches(".*(/state/(\\d){1,5})$")){
//            // 创建topic
//            String topicName = underPath.replace("/","_").substring(1,underPath.length());
//            NewTopic newTopic = new NewTopic(topicName, 1, (short)1);
//
//            //将包含metadata的topic消息设置为70days
//            if(!realPath.contains("/")){
//                Map<String, String> configs = new HashMap<>();
//                configs.put("retention.ms","6048000000");
//                newTopic = newTopic.configs(configs);
//            }
//
//            List<NewTopic> newTopics = new ArrayList<NewTopic>();
//            newTopics.add(newTopic);
//            CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
//            // 确保topic创建成功
//            KafkaFuture kafkaFuture = createTopicsResult.all();
//            try {
//              kafkaFuture.get();
//            } catch (InterruptedException e) {
//              e.printStackTrace();
//            } catch (ExecutionException e) {
//              e.printStackTrace();
//            }
//
//    }
//
//    // 增加partition
//    // checkpoint_streaming1/sources/0  checkpoint_streaming1/state/0/0
//    else if(realPath.matches(".*(/sources/(\\d){1,5})$")||
//            realPath.matches(".*(/state/(\\d){1,5})/(\\d){1,5}$")){
//            String topicDir = underPath.substring(0,underPath.lastIndexOf("/"));
//            String topicName = topicDir.replace("/","_").substring(1,topicDir.length());
//            int oldPartitions = producer.partitionsFor(topicName).size();
//            int partitionNo = Integer.parseInt(realPath.substring(realPath.lastIndexOf("/")+1,realPath.length()));
//            if(partitionNo < oldPartitions){
//              // no-op
//            }else {
//              // 增加到partitionNO+1,因为partitionNO是从0开始
//              Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
//              newPartitionsMap.put(topicName,NewPartitions.increaseTo(partitionNo+1));
//              CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(newPartitionsMap);
//              // 确保partition增加成功
//              KafkaFuture kafkaFuture = createPartitionsResult.all();
//              try {
//                kafkaFuture.get();
//              } catch (InterruptedException e) {
//                e.printStackTrace();
//              } catch (ExecutionException e) {
//                e.printStackTrace();
//              }
//        }
//    }
//    else {
//      //todo 增加其他的文件夹判断?
//      return;
//    }
//  }





  public InputStream open(String path) throws IOException {


      try {
          if(null == client.checkExists().forPath(stripDirPath(path))){
              throw new FileNotFoundException("read non-exist file " + path);
          }
      } catch (Exception e) {
          throw new FileNotFoundException("read non-exist file " + path);
      }


      MfsFileSystem.LOG.error("open()方法执行 path="+path);
      KafkaConsumer<String, byte[]> consum = null;
      try{
          MfsFileSystem.LOG.error("加载器: "+Thread.currentThread().getContextClassLoader());
//          Thread.currentThread().setContextClassLoader(null);
          consum = new KafkaConsumer<String, byte[]>(properties);
          return new NeuFileInputStream(client,stripDirPath(path),consum);
      }catch (Exception e){

//          MfsFileSystem.LOG.error("异常"+e.toString());
          return null;
      }


  }


  public boolean renameDirectory(String src, String dst) throws IOException {
      MfsFileSystem.LOG.error("renameDirectory()方法执行 src="+src+" dst"+dst);
    return true;
  }


  public boolean renameFile(String src, String dst) throws IOException {
      dst = stripPath(src);
      MfsFileSystem.LOG.error("renameFile()方法执行 src="+src+" dst"+dst);
      byte[] output = new byte[0];
      try {
          output = client.getData().forPath(dst);
      } catch (Exception e) {
          e.printStackTrace();
      }
      PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
      pathInfo.fileInfo.hasRenamed = true;
      byte[] input = SerializationUtils.serialize(pathInfo);
      try {
          client.setData().forPath(dst,input);
      } catch (Exception e) {
          e.printStackTrace();
      }


      return true;
  }

//  @Override
//  public void setOwner(String path, String user, String group) throws IOException {
//
//  }
//
//  @Override
//  public void setMode(String path, short mode) throws IOException {
//
//  }
//
//  @Override
//  public void connectFromMaster(String hostname) throws IOException {
//  }
//
//  @Override
//  public void connectFromWorker(String hostname) throws IOException {
//  }
//
//  @Override
//  public boolean supportsFlush() throws IOException {
//    return false;
//  }
//
//  @Override
//  public void cleanup() {}

  /**
   * Sleep and strip scheme from path.
   *
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
//    MfsFileSystem.LOG.debug("Sleeping for configured interval");
//    SleepUtils.sleepMs(mUfsConf.getMs(NeuUnderFileSystemPropertyKey.NEU_UFS_SLEEP));
    String divSign = MfsFileSystem.FS_SEAWEED_DEFAULT_PORT+"";
    int begin = path.indexOf(divSign)+divSign.length();
    int end = path.lastIndexOf("/");
    String dirPath = path.substring(begin,end+1);
    String tmpFileName = path.substring(end+2,path.length());
    String fileName = tmpFileName.substring(0,tmpFileName.indexOf('.'));
    if(path.contains("state")){
        String postfix = "";
        postfix = tmpFileName.substring(tmpFileName.indexOf('.')+1,tmpFileName.length());
        postfix = postfix.substring(0,postfix.indexOf('.'));
        fileName+="."+postfix;
    }

    return dirPath+fileName;
  }


    public FileStatus getFileStatus(Path path) {
      String curPath = stripDirPath(path.toString());
        try {
            if(null == client.checkExists().forPath(curPath)){
                return null;
            }else {
                byte[] output = new byte[0];
                output = client.getData().forPath(curPath);
                PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
                if(pathInfo.isDirectory){
                    return new FileStatus();
                }else {
                    if(!pathInfo.fileInfo.hasRenamed){
                        return null;
                    }else {
                        return new FileStatus();
                    }
                }
            }
        } catch (Exception e) {
            MfsFileSystem.LOG.error("Neu getFileStatus "+"path:"+path+" "+e.toString());
        }
        return null;
    }


}
