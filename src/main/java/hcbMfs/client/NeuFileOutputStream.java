package hcbMfs.client;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NeuFileOutputStream extends OutputStream {
    final int BYTE_BUFFER_SIZE = 1024;
    byte[] byteBuffer = new byte[BYTE_BUFFER_SIZE];
    int pointer;

    PathInfo pathInfo;

    CuratorFramework client;

    Producer<String, byte[]> producer;


    public NeuFileOutputStream(CuratorFramework zkclient,String path,Producer<String, byte[]> produc) {
        MfsFileSystem.LOG.error("NeuFileOutputStream构造方法调用");
        this.client = zkclient;
        pathInfo = new PathInfo();
        pathInfo.name = path;
        pathInfo.isDirectory = false;
        if(client == null){

            String zkServers = PropertyUtils.getZkServers();

            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
            this.client = CuratorFrameworkFactory.builder()
                    .connectString(zkServers)
                    .retryPolicy(retryPolicy)
                    .sessionTimeoutMs(6000)
                    .connectionTimeoutMs(3000)
//                    .namespace("fileSize1")
                    .build();
            this.client.start();
        }

        this.producer = produc;

        MfsFileSystem.LOG.error("NeuFileOutputStream构造方法调用结束");
    }

    @Override
    public void write(int b) throws IOException {
//        NeuUnderFileSystem.LOG.error("NeuFileOutputStream.write()调用");
        byteBuffer[pointer] = (byte) b;
        pointer++;
    }


    @Override
    public void close() throws IOException {
        MfsFileSystem.LOG.error("NeuFileOutputStream.close()调用:"+"pathInfo.name"+pathInfo.name);
        if(pointer == 0) {
            MfsFileSystem.LOG.error("NeuFileOutputStream.close()提前结束");
            return;
        }
        // 写入kafka
        String[] topicPartition = getTopicPatition(pathInfo.name);

        MfsFileSystem.LOG.error("file data to kafka time start path " + pathInfo.name + " " + System.currentTimeMillis());
        int parCount = Integer.parseInt(PropertyUtils.getSourcesAndStatePartitionNum());
        ProducerRecord record =
                new ProducerRecord(topicPartition[0],Integer.parseInt(topicPartition[1]) % parCount,
                        pathInfo.name, Arrays.copyOf(byteBuffer,pointer));

        MfsFileSystem.LOG.error("file data to kafka pointer " + pointer);

        Future<RecordMetadata> future = producer.send(record);
        // 下边这句代码必须有,会刷新缓存到主题.
        producer.flush();
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
//        producer.close();
        MfsFileSystem.LOG.error("file data to kafka time stop path " + pathInfo.name + " " + System.currentTimeMillis());


        MfsFileSystem.LOG.error("file metadata to zk time start path " + pathInfo.name + " " +  System.currentTimeMillis());

        // 写元信息
        pathInfo.fileInfo.hasRenamed = false;
        pathInfo.fileInfo.offset = recordMetadata.offset();
        MfsFileSystem.LOG.error("file metadata to zk offset " + pathInfo.fileInfo.offset);
        pathInfo.fileInfo.contentLength = pointer;
        pathInfo.lastModified = System.currentTimeMillis();
        pathInfo.fileInfo.contentHash = "";
        byte[] input = SerializationUtils.serialize(pathInfo);
        try {
            if(client.checkExists().forPath(pathInfo.name) == null){
                client.create()
                        .creatingParentContainersIfNeeded()
                        .forPath(pathInfo.name, input);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        MfsFileSystem.LOG.error("file metadata to zk time stop path " + pathInfo.name + " " + System.currentTimeMillis());

        MfsFileSystem.LOG.error("NeuFileOutputStream.close()调用完毕");

    }



    /**
     * filePath的几种形式
     *  /china/commits/12
     *  /china/metadata
     *  /china/offsets/18
     *  /china/sources/0/0
     *  /china/state/0/0/.5.delta.5a88bcdc-c3b4-4ac4-b89e-089fd0648bf7.TID11.tmp
     *  /china/state/0/0/17.delta
     *  /china/state/0/0/16.snapshot
     *
     * @param filePath
     * @return
     */
    private String[] getTopicPatition(String filePath) {
        String[] tps= new String[2];
        String[] subPaths = filePath.split("/");
        int len = subPaths.length;

        if (subPaths[len-1].contains("delta")||subPaths[len-1].contains("snapshot")){
            tps[1] = subPaths[len-2];
            tps[0] = toTopicName(subPaths,len-2);
        }else if(subPaths[len-1].contains("metadata")){
            tps[1] = Integer.toString(0);
            tps[0] = toTopicName(subPaths,len-1);
        }else if(subPaths[len-2].equals("commits")||subPaths[len-2].equals("offsets")){
            tps[1] = Integer.toString(0);
            tps[0] = toTopicName(subPaths,len-1);
        }else if(subPaths[len-3].equals("sources")){
            tps[1] = subPaths[len-2];
            tps[0] = toTopicName(subPaths,len-2);
        }
        return tps;
    }


    // kafka的主题名不能带/
    private String toTopicName(String[] subPaths,int end){
        String topicName = "";
        for (int i = 1; i < end; i++) {
            topicName = topicName+"_"+subPaths[i];
        }

        return topicName.substring(1,topicName.length());
    }


}