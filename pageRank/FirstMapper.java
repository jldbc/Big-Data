package cs3390;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {   // see if all this input stuff is fine 

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Text outputKey = new Text();
		Text outputValue = new Text();

		//This line below gives us the entire line in a Matrix/Vector file
		String s = value.toString(); 
		List<String> slist = new ArrayList<String>(Arrays.asList(s.split("\t")));
		int i;
		int k;
		//If the file first value is a Matrix line
		if(slist.get(0).contains("M")){ // M i k val 
		
			outputKey.set(new Text(slist.get(2).trim()));
			outputValue.set(new Text("M" + "\t" + slist.get(1).trim() + "\t" + slist.get(3).trim() ));
			context.write(outputKey, outputValue);
		}
		// if file is vector
		else{       // v k val 
			
			outputKey.set(new Text(slist.get(1).trim()));
			outputValue.set("V" + "\t" + slist.get(2).trim() );
			context.write(outputKey,  outputValue);
		}
	}
}
