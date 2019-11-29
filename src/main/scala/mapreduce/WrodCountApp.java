package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class WrodCountApp {

    /**
     * @param KEYIN    →k1 表示每一行的起始位置（偏移量offset）
     * @param VALUEIN  →v1 表示每一行的文本内容
     * @param KEYOUT   →k2 表示每一行中的每个单词
     * @param VALUEOUT →v2 表示每一行中的每个单词的出现次数，固定值为1
     */
    public static class MyMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(IntWritable key, Text value, Mapper<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] splited = value.toString().split(" ");
            for (String word : splited) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    /**
     * @param KEYIN    →k2 表示每一行中的每个单词
     * @param VALUEIN  →v2 表示每一行中的每个单词的出现次数，固定值为1
     * @param KEYOUT   →k3 表示每一行中的每个单词
     * @param VALUEOUT →v3 表示每一行中的每个单词的出现次数之和
     */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,  Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = "hdfs://hadoop:8020/wc";
        String OUT_PATH = "hdfs://hadoop:8020/outputwc";

        Configuration configuration = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), configuration);

        if (fileSystem.exists(new Path(OUT_PATH))) {
            fileSystem.delete(new Path(OUT_PATH), true);
        }
        Job job = Job.getInstance(configuration, "WordCountApp");

        // run jar class
        job.setJarByClass(WrodCountApp.class);

        // 设置map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置input format
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(INPUT_PATH);
        FileInputFormat.setInputPaths(job, inputPath);

        // 设置output format
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outPath = new Path(OUT_PATH);
        FileOutputFormat.setOutputPath(job, outPath);
        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
