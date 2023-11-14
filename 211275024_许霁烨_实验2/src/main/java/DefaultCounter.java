import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DefaultCounter {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        //<key value, type(key_out), type(value_out)>
        // 定义一个IntWritable对象,value为1,表示计数(Default or not)
        private final static IntWritable one = new IntWritable(1);
        // 定义Text对象,用于存储分类标签
        private Text word = new Text();

        // Map任务
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 将输入的Text转为字符串数组,以空格分割
            String[] data = value.toString().split(",");
            // 检查最后一个元素,TARGET列的值
            if (data[data.length-1].equals("1")) {
                // TARGET为1表示违约,将word设置为"Default"
                word.set("Default(1)");
            }
            else if (data[data.length-1].equals("0")){
                // 否则为非违约,设置为"Non-Default"
                word.set("Non-Default(0)");
            }

            // 输出map结果,Text为分类标签,IntWritable为计数1
            context.write(word, one);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("Non-Default(0)")) {
                // 统计key为0的数量
                int sum0 = 0;
                for (IntWritable val : values) {
                    sum0 += val.get();
                }
                result.set(sum0);
                context.write(new Text("Non-Default(0)"), result);

            } else if (key.toString().equals("Default(1)")) {
                // 统计key为1的数量
                int sum1 = 0;
                for (IntWritable val : values) {
                    sum1 += val.get();
                }
                result.set(sum1);
                context.write(new Text("Default(1)"), result);

            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "default counter");
        job.setJarByClass(DefaultCounter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //set the addr of input and output
        String[] addr_args = new String[]{"/exp2/input","/exp2/Task1_output"}; /* 直接设置输入参数 */
        //blow is setting the address of input and output
        FileInputFormat.addInputPath(job, new Path(addr_args[0]));
        FileOutputFormat.setOutputPath(job, new Path(addr_args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}