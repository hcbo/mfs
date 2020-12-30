import hcbMfs.client.PathInfo;
import hcbMfs.client.PropertyUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import hcbMfs.client.MfsFileSystem;

public class AppTest {



    @Test
    public void app8(){
        String zkServers = PropertyUtils.getZkServers();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zkServers)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(3000)
//                    .namespace("fileSize1")
                .build();
        client.start();
        String path = "/checkRoot/state/0/0/1.delta";
        // get offset from zookeeper
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        long offset = pathInfo.fileInfo.offset;
        System.out.println();
    }
}
