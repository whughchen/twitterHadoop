/**
 * twitterHadoop  TopicAnalysis.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-3-26 下午10:56:30
 * email: gh.chen@siat.ac.cn
 *
 */
package topicAnalysis;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.lang.Character;
 

public class mostHotTopic {
 

    public static class Map extends

            Mapper<LongWritable, Text, Text, IntWritable> { 

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
    	//public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException, InterruptedException {

            // 将输入的纯文本文件的数据转化成String
            String line = value.toString(); 
            	String[] twittArray=line.split(";");
            	if(twittArray.length<8) return;
            	String tags=twittArray[7].replace("[", " ");
            	tags=tags.replace("]", " ");
            	tags=tags.trim();
            	String[] tmp=tags.split(",");
            	int start,end;
            	String tag;
            	for(int i=0;i<tmp.length;i++){
            		if(tmp[i].equals(null)) 
            			break;
            	start=tmp[i].indexOf("'");
            	end=tmp[i].indexOf("}");
            	if(start<0 || end<0) return;
            	tag=tmp[i].substring(start+1, end-1);
            	
            	word.set(tag);
            	//output.collect(word, one);
            	context.write(word, one);
            	}
             }

    }

 

    public static class Reduce extends

            Reducer<Text, IntWritable, Text, IntWritable> {

        // 实现reduce函数                  
    	public void reduce(Text key, Iterable<IntWritable> values,Context context)
                throws IOException, InterruptedException {
        	Integer linenum=0;

        	int sum = 0;
        	for (IntWritable val : values) {
                sum += val.get();
               }
        	//String keyStr=linenum.toString()+","+key.toString();
        
            context.write(key ,new IntWritable(sum));

            }           

        }

 

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();


        String[] ioArgs = new String[] { "examples/2013-03-25-18", "examples/mostHotTopic" };
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: mostActive <in> <out>");
            System.exit(2);

        }

 

        Job job = new Job(conf, "mostHotTopic desc");

        job.setJarByClass(mostHotTopic.class);

 

        // 设置Map、Combine和Reduce处理类

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

 
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        
        
        

        // 设置输出类型
        //job.setOutputKeyClass(LongWritable.class);
     
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 


        job.setInputFormatClass(TextInputFormat.class);
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
