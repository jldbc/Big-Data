//package cs3390;
//
//
//import java.io.IOException;
//import java.util.*;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import cs3390.XmlInputFormat;
//
//public class WikiParser {
//    
//	
//	//------------------------------------------------------------
//	public static void main(String[] args) throws Exception {
//		
//		
//		Configuration conf = new Configuration();
//		 	conf.set("xmlinput.start", "<page>");
//		    conf.set("xmlinput.end", "</page>");
//		   
//		Job job = new Job(conf);
//		job.setJobName("Job 1 - Wiki Parser ");
//		job.setJarByClass(WikiParser.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//     
//		job.setMapperClass(WikipediaMapper.class);
//	    //MultipleOutputs.addNamedOutput(job, "pageid", TextOutputFormat.class,	Text.class, Text.class);
//
//        job.setNumReduceTasks(0);
//        job.setInputFormatClass(XmlInputFormat.class);
//    	job.setOutputFormatClass(TextOutputFormat.class);
//        
//    	FileInputFormat.addInputPath(job, new Path(args[0]));  
//    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        
//    	job.waitForCompletion(true);
// }
//        
//}