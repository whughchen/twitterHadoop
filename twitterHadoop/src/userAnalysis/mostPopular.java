/**
 * twitterHadoop  UserAnalysis.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-3-26 下午1:56:30
 * email: gh.chen@siat.ac.cn
 */

/**
 * twitterHadoop  UserAnalysis.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-3-26 下午1:56:30
 * email: gh.chen@siat.ac.cn
 *
 */
package userAnalysis;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.lang.Character;
 

public class mostPopular {
 

    public static class Map extends

            Mapper<LongWritable, Text, Text, IntWritable> { 

        // 实现map函数
        public void map(LongWritable key, Text value, Context context)

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
            	if(twittArray.length<5) return;
            	String user=twittArray[4].replace("[", " ");
            	user=user.replace("]", " ");
            	user=user.trim();
            	
            	String[] tmp=user.split(",");
            	String userName;
         	
            	//int userFollowerCnt;
            	int userStatusCnt;
            	if(tmp.length<6) return;
            	if(tmp[1].equals(null)){
            		return;}
            	else {userName=tmp[1];}
            	
//            	if( mostPopular.isNumeric(tmp[5])){
//            	  userFollowerCnt=Integer.parseInt(tmp[5]);
//            	}
//            	else return;
            	
            	if( mostPopular.isNumeric(tmp[4])){
            		userStatusCnt=Integer.parseInt(tmp[4]);
            	}
            	else return;

                Text name = new Text(userName);
                // 输出姓名和粉丝数
                context.write(name, new IntWritable(userStatusCnt));

            }
        }
    }

 

    public static class Reduce extends

            Reducer<Text, IntWritable, Text, IntWritable> {

        // 实现reduce函数
        public void reduce(Text key, Iterable<IntWritable> values,

                Context context) throws IOException, InterruptedException {
        	IntWritable linenum = new IntWritable(1);

            Iterator<IntWritable> iterator = values.iterator();

            while (iterator.hasNext()) {
            	 context.write(key, new IntWritable(Integer.parseInt(iterator.toString()  )));
            } 
//            Text ran_Key;
//            for(IntWritable tmp : values){
//            	ran_Key=new Text(key+"_"+linenum);
//            	context.write(ran_Key, tmp);  
//            	linenum = new IntWritable(linenum.get()+1);

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

 

        Job job = new Job(conf, "mostPopular desc");

        job.setJarByClass(mostPopular.class);

 

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
