// 导入必要的库
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;

import java.util.Random;
import java.util.PriorityQueue;
import java.util.Comparator;
//import Pair
import javafx.util.Pair;



public class KNN {
    public static class SplitMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static int TRAIN_PERCENT = 80;
        private boolean skipFirst = true;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] elements = line.split(",");

            List<IntWritable> list = new ArrayList<>();
            if (skipFirst) {
                skipFirst = false;
                return;
            }
            // 取特征元素
            for (int i = 0; i < 9; i++) {
                list.add(new IntWritable(Integer.parseInt(elements[i])));
            }

            list.add(new IntWritable(Integer.parseInt(elements[elements.length - 1])));

            Random random = new Random();
            int randomNumber = random.nextInt(100);

            // 根据随机数判断是否为训练集数据
            if (randomNumber < TRAIN_PERCENT) {
                context.write(new Text("train"), listToString(list));
            } else {
                context.write(new Text("test"), listToString(list));
            }
        }

        private Text listToString(List<IntWritable> list) {
            StringBuilder sb = new StringBuilder();
            int size = list.size();
            for (int i = 0; i < size; i++) {
                sb.append(list.get(i).get());
                if (i < size - 1) {
                    sb.append(",");
                }
            }
            return new Text(sb.toString());
        }
    }
    public static class KNNReducer extends Reducer<Text, Text, Text, Text> {
        private static final int K = 5;
        private List<Pair<Integer, Integer>> predictionResults = new ArrayList<>();
        private List<List<Integer>> trainingData = new ArrayList<>(); // 存储训练集数据
        private List<List<Integer>> testingData = new ArrayList<>(); // 存储测试集数据

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 将数据根据key的值分别添加到训练集或测试集中
            if (key.toString().equals("train")) {
                for (Text value : values) {
                    List<Integer> data = parseData(value.toString());
                    trainingData.add(data);
                }
            } else if (key.toString().equals("test")) {
                for (Text value : values) {
                    List<Integer> data = parseData(value.toString());
                    testingData.add(data);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 在cleanup方法中实现KNN算法，根据训练集数据进行预测
            //输出trainingData的长度
            context.write(new Text("trainingData size: "), new Text(String.valueOf(trainingData.size())));
            context.write(new Text("testingData size: "), new Text(String.valueOf(testingData.size())));
            context.write(new Text("single data size: "), new Text(String.valueOf(trainingData.get(0).size())));

            // 进行预测并输出结果
            performPrediction(context);

            // 输出最终的预测结果列表
            //for (Pair<Integer, Integer> result : predictionResults) {
            //    context.write(new Text("Prediction: " + result.getKey()), new Text("Actual Value: " + result.getValue()));
            //}
            // 计算准确度并输出
            double accuracy = calculateAccuracy(predictionResults);
            context.write(new Text("Accuracy: "), new Text(String.valueOf(accuracy)));
        }

        private double calculateAccuracy(List<Pair<Integer, Integer>> predictions) {
            int correctPredictions = 0;
            int totalPredictions = predictions.size();

            for (Pair<Integer, Integer> prediction : predictions) {
                int predictedValue = prediction.getKey();
                int actualValue = prediction.getValue();

                if (predictedValue == actualValue) {
                    correctPredictions++;
                }
            }

            double accuracy = (double) correctPredictions / totalPredictions * 100;
            return accuracy;
        }
        private void performPrediction(Context context) {
            // 创建一个优先级队列，按距离从小到大排序
            PriorityQueue<Pair<Double, Integer>> minDistanceQueue = new PriorityQueue<>(K, Comparator.comparing(Pair::getKey));
            int count_pred = 0;
            // 遍历testingData，并计算与每个trainingData之间的欧式距离
            for (List<Integer> testData : testingData) {
                // 清空优先级队列
                minDistanceQueue.clear();

                // 计算与每个trainingData的欧式距离，并将距离和最后一个值加入优先级队列
                for (List<Integer> trainData : trainingData) {
                    double distance = calculateEuclideanDistance(testData, trainData);
                    int lastValue = trainData.get(trainData.size() - 1);
                    minDistanceQueue.offer(new Pair<>(distance, lastValue));

                    // 如果队列中的元素数量超过k个，则移除距离最大的元素
                    if (minDistanceQueue.size() > K) {
                        minDistanceQueue.poll();
                    }
                }

                // 统计优先级队列中最后一个值为1的数量
                int count = 0;
                for (Pair<Double, Integer> pair : minDistanceQueue) {
                    if (pair.getValue() == 1) {
                        count++;
                    }
                }

                // 记录预测结果
                int prediction = count > K / 2 ? 1 : 0;
                int actualValue = testData.get(testData.size() - 1); // 假设真实值在数据集的最后一个位置
                Pair<Integer, Integer> resultPair = new Pair<>(prediction, actualValue);
                predictionResults.add(resultPair);
                //打印是预测的第几条
                count_pred++;
                if (count_pred % 100 == 0){
                    System.out.println("----------------------------" + count_pred);
                }
            }
        }

        private double calculateEuclideanDistance(List<Integer> data1, List<Integer> data2) {
            if (data1.size() != data2.size()) {
                throw new IllegalArgumentException("Data sizes do not match");
            }

            double sum = 0.0;
            for (int i = 0; i < data1.size()-1; i++) {
                int diff = data1.get(i) - data2.get(i);
                sum += Math.pow(diff, 2);
            }

            return Math.sqrt(sum);
        }

        private List<Integer> parseData(String value) {
            // 解析字符串并将其转换为适当的数据类型
            List<Integer> data = new ArrayList<>();
            String[] elements = value.split(",");
            for (String element : elements) {
                data.add(Integer.parseInt(element));
            }
            return data;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "train-test-split");

        job.setJarByClass(KNN.class);
        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(KNNReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出路径
        FileInputFormat.addInputPath(job, new Path("/exp2/input/application_data_preprocessed.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/exp2/Task3_output"));


        // 等待任务完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
