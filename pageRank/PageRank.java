package cs3390;


import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class PageRank {
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Job1-FirstMapper");
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);// mapper key type
		job.setOutputValueClass(Text.class);// mapper value type 
		job.setMapperClass(FirstMapper.class); 
		job.setReducerClass(FirstReducer.class); //
		job.setNumReduceTasks(10);
        job.setInputFormatClass(TextInputFormat.class); // type of input 
    	job.setOutputFormatClass(TextOutputFormat.class); // type of output 
    	FileInputFormat.addInputPath(job, new Path(args[0]));  //  give /user/input   cause it has /user/input/Matrix /user/input/vecotr 
    	FileSystem fs = FileSystem.get(conf);
    	fs.delete(new Path(args[1]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	boolean success = job.waitForCompletion(true);
  
    	
    	if (success) {
    		Job job2 = new Job(conf);
    		job2.setJobName("Job2-SecondMapper");
    		job2.setJarByClass(PageRank.class);
   
    		job2.setOutputKeyClass(Text.class);// mapper key type
    		job2.setOutputValueClass(Text.class);// mapper value type 
    		
    		job2.setMapperClass(SecondMapper.class); 
    		job2.setReducerClass(SecondReducer.class);
    		
            job2.setInputFormatClass(TextInputFormat.class); // type of input 
        	job2.setOutputFormatClass(TextOutputFormat.class); // type of output 
            
        	FileInputFormat.addInputPath(job2, new Path(args[1]));  //  /user/input/
        	fs.delete(new Path(args[2]));
        	FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        	success = job2.waitForCompletion(true);
    	}
    	
	}
}
