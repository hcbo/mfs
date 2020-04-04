import org.junit.Test;
import hcbMfs.client.SeaweedFileSystem;

public class AppTest {


    @Test
    public void app(){
        System.out.println("hello test");
    }
//    @Test
//    public void app2(){
//        ZkClient zkClient = new ZkClient("kafka:2181", 5000);
//        zkClient.createPersistent("/fileSize1/zkclientTest1/hcb",true);
//        zkClient.writeData("/fileSize1/zkclientTest1/hcb",new StringBuffer("hcb2030"));
//        StringBuffer sb = (StringBuffer)zkClient.readData("/fileSize1/zkclientTest1/hcb");
//        System.out.println(sb);
//
//    }

    @Test
    public void app3(){
        String path = stripPath("mfs://localhost:8888/china/state/0/0/17.snapshot");
        System.out.println(path);
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

    @Test
    public void app4(){
        String[] strings = getTopicPatition("/china/state/0/0/1.delta");
        System.out.println();
    }
    private String stripPath(String path) {
//    SeaweedFileSystem.LOG.debug("Sleeping for configured interval");
//    SleepUtils.sleepMs(mUfsConf.getMs(NeuUnderFileSystemPropertyKey.NEU_UFS_SLEEP));
        String divSign = SeaweedFileSystem.FS_SEAWEED_DEFAULT_PORT+"";
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
    private String stripDirPath(String path) {
        String divSign = SeaweedFileSystem.FS_SEAWEED_DEFAULT_PORT+"";
        int begin = path.indexOf(divSign)+divSign.length();

        return path.substring(begin,path.length());
    }

    @Test
    public void app5(){

        System.out.println
                (stripDirPath("mfs://localhost:8888/china/offsets/0"));
    }
    private String[] getTopicPatitionInputStream(String filePath) {
        String[] tps= new String[2];
        String[] subPaths = filePath.split("/");
        int len = subPaths.length;

        if (subPaths[len-1].contains("delta")||subPaths[len-1].contains("snapshot")){
            tps[1] = subPaths[len-2];
            tps[0] = toTopicNameInputStream(subPaths,len-2);
        }else if(subPaths[len-1].contains("metadata")){
            tps[1] = Integer.toString(0);
            tps[0] = toTopicNameInputStream(subPaths,len-1);
        }else if(subPaths[len-2].equals("commits")||subPaths[len-2].equals("offsets")){
            tps[1] = Integer.toString(0);
            tps[0] = toTopicNameInputStream(subPaths,len-1);
        }else if(subPaths[len-3].equals("sources")){
            tps[1] = subPaths[len-2];
            tps[0] = toTopicNameInputStream(subPaths,len-2);
        }
        return tps;
    }
    private String toTopicNameInputStream(String[] subPaths,int end){
        String topicName = "";
        for (int i = 1; i < end; i++) {
            topicName = topicName+"_"+subPaths[i];
        }

        return topicName.substring(1,topicName.length());
    }

    @Test
    public void app6(){
        String[] strings = getTopicPatitionInputStream("/china/offsets/0");
        System.out.println();
    }
}
