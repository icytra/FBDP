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
//
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import sun.reflect.generics.tree.Tree;
//
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;


public class Weekday {

    public static class WeekdayMapper extends Mapper<Object, Text, Text, IntWritable> {
        //<key value, type(key_out), type(value_out)>
        // 定义一个IntWritable对象,value为1,表示计数(Default or not)
        private final static IntWritable one = new IntWritable(1);
        // 定义Text对象,用于存储分类标签
        private Text word = new Text();
        //
        private static final Log LOG = LogFactory.getLog(Map.class);
        // Map任务
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 将输入的Text转为字符串数组,以空格分割
            String[] data = value.toString().split(",");
            if (data[25].equals("MONDAY")) {
                word.set("MONDAY");
                context.write(word, one);
            } else if (data[25].equals("TUESDAY")) {
                word.set("TUESDAY");
                context.write(word, one);
            } else if (data[25].equals("WEDNESDAY")) {
                word.set("WEDNESDAY");
                context.write(word, one);
            } else if (data[25].equals("THURSDAY")) {
                word.set("THURSDAY");
                context.write(word, one);
            } else if (data[25].equals("FRIDAY")) {
                word.set("FRIDAY");
                context.write(word, one);
            } else if (data[25].equals("SATURDAY")) {
                word.set("SATURDAY");
                context.write(word, one);
            } else if (data[25].equals("SUNDAY")) {
                word.set("SUNDAY");
                context.write(word, one);
            }
            //LOG.info("Map output - key: " + word + ", value: " + one);
            // 输出map结果,Text为分类标签,IntWritable为计数1


        }
    }

    public static class WeekdayReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();
        private Map<Integer, Text> resultMap = new TreeMap<>(Collections.reverseOrder() );
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = sum(values);
            if (!key.equals("WEEKDAY_APPR_PROCESS_START")){
                result.set(sum);
                resultMap.put(sum, new Text(key));
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 迭代排序后的条目并将其写入上下文
            for (Map.Entry<Integer, Text> entry : resultMap.entrySet()) {
                result.set(entry.getKey());
                context.write(entry.getValue(), result);
            }
        }
        private int sum(Iterable<IntWritable> values) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            return sum;
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "Weekday");
        job.setJarByClass(WeekdayMapper.class);
        job.setMapperClass(WeekdayMapper.class);
        job.setCombinerClass(WeekdayReducer.class);
        job.setReducerClass(WeekdayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //
        //set the addr of input and output
        String[] addr_args = new String[]{"/exp2/input/application_data.csv","/exp2/Task2_output"}; /* 直接设置输入参数 */
        //blow is setting the address of input and output
        FileInputFormat.addInputPath(job, new Path(addr_args[0]));
        FileOutputFormat.setOutputPath(job, new Path(addr_args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}