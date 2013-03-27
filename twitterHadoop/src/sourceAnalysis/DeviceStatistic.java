package sourceAnalysis;

/**
 * twitterHadoop  DeviceAnalysis.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-3-26 下午11:56:30
 * email: gh.chen@siat.ac.cn
 *
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobconf_005fhistory_jsp;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 

public class DeviceStatistic {
 

    public static class Map  extends  Mapper<LongWritable,Text , Text, IntWritable> { 
    	 //private final IntPair inkey = new IntPair();
       	public void map( LongWritable key , Text value,Context context)
                throws IOException, InterruptedException {

            // 将输入的纯文本文件的数据转化成String
            String line = value.toString(); 
            
            String[] twittArray = line.split(";");
            if(twittArray.length<6)
            	return;
               String tags=twittArray[5];
            	if(tags.equals(null)) {
            		return;}
            	String tag = "";
            	if(tags.contains("iPhone")){
            		tag="iPhone";
            	}else if(tags.contains("iOS")){
            		tag="iPhone";
            	}else if(tags.contains("Android")){
            		tag="Android";
            	}else if(tags.contains("BlackBerry")){
            		tag="BlackBerry";
            	}else if(tags.contains("web")){
            		tag="web";
            	}else
            		tag="others";
            	
            	context.write(new Text(tag), new IntWritable(1));
            	}
             }
        
 


    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	Integer linenum=0;
    	 private Text result = new Text();
    	public void reduce(Text key, Iterable<IntWritable> values,Context context)
                //OutputCollector<Text, IntWritable> output,Reporter reporter)
                throws IOException, InterruptedException 
          {
    		

        	int sum = 0;
        	for (IntWritable val : values) {
                sum += val.get();
              }
        	
        	//result.set((++linenum).toString()+","+key);
        	
            context.write(key, new IntWritable(sum));
           
            }           

        }

 

    public static void main(String[] args) throws Exception {
    	
        Configuration conf = new Configuration();

        String[] ioArgs = new String[] { "examples\\2013-03-25-18", "examples/deviceAnalusis" };
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: mostActive <in> <out>");
            System.exit(2);

        }
        
        
        String outDirTemp="examples/deviceAnalusis";        
        Job job = new Job(conf, "mostActive desc");
        job.setJarByClass(DeviceStatistic.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setGroupingComparatorClass(FirstGroupingComparator.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); 
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
        
        JobConf jobConf=new JobConf(DeviceStatistic.class);        
        jobConf.setOutputKeyClass(KeyComparator.class);
        jobConf.setOutputKeyComparatorClass(FirstGroupingComparator.class);
        jobConf.setOutputValueGroupingComparator(IntWritableDecreasingComparator.class);
        

        JobClient.runJob(jobConf);
      
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    public static class FirstPartitioner extends Partitioner<IntPair,IntWritable>{
        @Override
        public int getPartition(IntPair key, IntWritable value, 
                                int numPartitions) {
          return Math.abs(key.getFirst() * 127) % numPartitions;
        }
      }
    
    
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
          return -super.compare(a, b);
        }
        
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    
    
    public static class FirstGroupingComparator implements RawComparator<IntPair> {
    	@Override
    	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    		return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, 
    				b2, s2, Integer.SIZE/8);
    	}

    	@Override
    	public int compare(IntPair o1, IntPair o2) {
    		int l = o1.getFirst();
    		int r = o2.getFirst();
    		return l == r ? 0 : (l < r ? -1 : 1);
    	}
    }
    
    
    public static class IntPair 
    implements WritableComparable<IntPair> {
    	//private int first = 0;
    	private int first = 0;
    	private int second = 0;

    	/**
    	 * Set the left and right values.
    	 */
    	public void set(int left, int right) {
    		first = left;
    		second = right;
    	}
    	public int getFirst() {
    		return first;
    	}
    	public int getSecond() {
    		return second;
    	}
    	/**
    	 * Read the two integers. 
    	 * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
    	 */
    	@Override
    	public void readFields(DataInput in) throws IOException {
    		first = in.readInt() + Integer.MIN_VALUE;
    		second = in.readInt() + Integer.MIN_VALUE;
    	}
    	@Override
    	public void write(DataOutput out) throws IOException {
    		out.writeInt(first - Integer.MIN_VALUE);
    		out.writeInt(second - Integer.MIN_VALUE);
    	}
    	@Override
    	public int hashCode() {
    		return first * 157 + second;
    	}
    	@Override
    	public boolean equals(Object right) {
    		if (right instanceof IntPair) {
    			IntPair r = (IntPair) right;
    			return r.first == first && r.second == second;
    		} else {
    			return false;
    		}
    	}
    	/** A Comparator that compares serialized IntPair. */ 
    	public static class Comparator extends WritableComparator {
    		public Comparator() {
    			super(IntPair.class);
    		}

    		public int compare(byte[] b1, int s1, int l1,
    				byte[] b2, int s2, int l2) {
    			return compareBytes(b1, s1, l1, b2, s2, l2);
    		}
    	}

    	static {                                        // register this comparator
    		WritableComparator.define(IntPair.class, new Comparator());
    	}

    	@Override
    	public int compareTo(IntPair o) {
    		if (first != o.first) {
    			return first < o.first ? -1 : 1;
    		} else if (second != o.second) {
    			return second < o.second ? -1 : 1;
    		} else {
    			return 0;
    		}
    	}
    }


   

    }
