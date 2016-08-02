package cs3390;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SecondReducer extends Reducer <LongWritable, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//M  R  C  Val
		//V  R  Val
		//get matrix and float vals
		
		Double sum  = 0.;
		for(Iterator it = values.iterator(); it.hasNext();){
			String entry  = it.next().toString();
			Double val = Double.parseDouble(entry.trim());
			sum = sum + val;
		}
		context.write(new Text(),   new Text("V\t" + key.toString() + "\t" + String.valueOf(sum)));
	}
}
