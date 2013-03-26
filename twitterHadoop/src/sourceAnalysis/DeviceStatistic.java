package sourceAnalysis;

/**
 * twitterHadoop  TopicAnalysis.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-3-26 下午11:56:30
 * email: gh.chen@siat.ac.cn
 *
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.lang.Character;
 

public class DeviceStatistic {
 

    public static class Map extends

            Mapper<LongWritable, Text, Text, IntWritable> { 

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        //public void map(LongWritable key, Text value, Context context)
    	public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, 
                Reporter reporter)
                throws IOException, InterruptedException {

            // 将输入的纯文本文件的数据转化成String
            String line = value.toString(); 

            // 将输入的数据首先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\r\n"); 

            // 分别对每一行进行处理
            while (tokenizerArticle.hasMoreElements()) {

                // 每行按;划分
            	String twitt=tokenizerArticle.toString();
            	String[] twittArray=twitt.split(";");
            	if(twittArray.length<6) return;
            	String tags=twittArray[5];
            	if(tags.equals(null)) {
            		return;}
            	String tag;
            	if(tags.contains("iPhone")){
            		tag="iPhone";
            	}else if(tags.contains("iOS")){
            		tag="Android";
            	}else if(tags.contains("Android")){
            		tag="Android";
            	}else if(tags.contains("BlackBerry")){
            		tag="BlackBerry";
            	}else if(tags.contains("web")){
            		tag="web";
            	}else
            		tag="others";
            	
            	word.set(tag);
            	output.collect(word, one);
            	}
             }
        }
 


    public static class Reduce extends

            Reducer<Text, IntWritable, Text, IntWritable> {

        // 实现reduce函数
    	public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output,Reporter reporter)
                throws IOException, InterruptedException {
        	IntWritable linenum = new IntWritable(1);

        	int sum = 0;
            while (values.hasNext()) {
              sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));

            }           

        }

 

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // 这句话很关键
        conf.set("mapred.job.tracker", "ccrfox10:9001"); 
        String[] ioArgs = new String[] { "in", "out" };
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: mostActive <in> <out>");
            System.exit(2);

        }

 

        Job job = new Job(conf, "mostActive desc");

        job.setJarByClass(DeviceStatistic.class);

 

        // 设置Map、Combine和Reduce处理类

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

 

        // 设置输出类型

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 

        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现

        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class); 

        // 设置输入和输出目录

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

   
    public static boolean isNumeric(String str)
    {
    Pattern pattern = Pattern.compile("[0-9]*");
    Matcher isNum = pattern.matcher(str);
    if( !isNum.matches() )
    {
    return false;
    }
    return true;
    } 
    }
