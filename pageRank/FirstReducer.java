package cs3390;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class FirstReducer extends Reducer <Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//M  R  C  Val
		//V  R  Val
		//get matrix and float vals 
		Double vVal = 0.;
		ArrayList<String> mList = new ArrayList<String>();
		for(Iterator it = values.iterator(); it.hasNext();){
			
			String parts[] = it.next().toString().split("\t");
			
			if(parts[0].contains("M")) { // m i val 
				mList.add(parts[1].trim() + "\t" + parts[2].trim());   //make sure that's all that's in the matrix
			}
			else{ // v val 
				vVal = Double.parseDouble(parts[1].trim());
			}
		}
		
		for(int i = 0; i < mList.size() ;  i++) {
			String parts[] = mList.get(i).split("\t");
			String rowID = parts[0];
			Double val = Double.parseDouble(parts[1]);
			context.write(new Text(rowID.trim()) , new Text(String.valueOf(val*vVal)));
		}
	}
}

