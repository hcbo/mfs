package hcbMfs.client;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class NeuFileInputStream extends FSInputStream {
    byte[] byteBuffer ;
    int pointer;


    CuratorFramework client;

    KafkaConsumer<String,byte[]> consumer;

    public NeuFileInputStream(CuratorFramework zkclient, String path,KafkaConsumer<String,byte[]> consum) {
        MfsFileSystem.LOG.error("NeuFileInputStream.构造函数调用");
        this.client = zkclient;
        this.consumer = consum;

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


        MfsFileSystem.LOG.error("file metadata from zk time start path " + path + " " +  System.currentTimeMillis());
        // get offset from zookeeper
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        long offset = pathInfo.fileInfo.offset;
        MfsFileSystem.LOG.error("file metadata from zk time stop path " + path + " " + System.currentTimeMillis());


        // get message from kafka
        MfsFileSystem.LOG.error("file data from kafka time start path " + path + " " + System.currentTimeMillis());
        String[] tp = getTopicPatition(pathInfo.name);
        int parCount = Integer.parseInt(PropertyUtils.getSourcesAndStatePartitionNum());
        TopicPartition topicPartition = new TopicPartition(tp[0], Integer.parseInt(tp[1]) % parCount);
        List topicPartitionList = new ArrayList<TopicPartition>();
        topicPartitionList.add(topicPartition);
        consumer.assign(topicPartitionList);
        consumer.seek(topicPartition,offset);
        ConsumerRecords<String, byte[]> records;
        while (true) {
            records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            if(!records.isEmpty()){
                break;
            }
        }
        Iterator<ConsumerRecord<String, byte[]>> iterator =records.iterator();
        ConsumerRecord<String, byte[]> record = iterator.next();
        byteBuffer = record.value();
        // solved org.apache.kafka.common.KafkaException: Failed to construct kafka consumer
        consum.close();
        MfsFileSystem.LOG.error("file data from kafka time stop path " + path + " " + System.currentTimeMillis());
        MfsFileSystem.LOG.error("NeuFileInputStream.构造函数调用结束"+ " "+byteBuffer.length);
    }

    @Override
    public int read() throws IOException {
        if(pointer < byteBuffer.length){
            int res = (int)byteBuffer[pointer];
            pointer++;
            return res&(0xff);
        }
        return -1;
    }

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

    @Override
    public void seek(long pos) throws IOException {

    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
}
