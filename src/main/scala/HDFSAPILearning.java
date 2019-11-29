import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSAPILearning {

    public static final String HDFS_PATH = "hdfs://hadoop:8020";
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void init() throws URISyntaxException, IOException {
        System.out.println("HDFS init");
        configuration = new Configuration();
        fileSystem = fileSystem.get(new URI(HDFS_PATH), configuration);
    }

    @After
    public void tearDown() {
        fileSystem = null;
        configuration = null;
        System.out.println("HDFS tearDown");
    }

    /**
     * 创建目录
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/abc.txt"));
        outputStream.write("helloworkd".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/abc.txt");
        Path newPath = new Path("/hdfsapi/test/new.txt");
        System.out.println(fileSystem.rename(oldPath, newPath));
    }

    /**
     * 上传本地文件到HDFS
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path src = new Path("/hdfsapi/test/abc.txt");
        Path dist = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dist);
    }

    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFile() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfs/test"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            String permission = fileStatus.getPermission().toString(); //权限
            short replication = fileStatus.getReplication(); //副本系数
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();//路径
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
        }

        Path src = new Path("/hdfsapi/test/abc.txt");
        Path dist = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dist);
    }

    /**
     * 查看文件快信息
     * @throws IOException
     */
    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/b.txt"));
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation : fileBlockLocations){
            for (String host :blockLocation.getHosts()){
                System.out.println(host);
            }
        }
    }


}
