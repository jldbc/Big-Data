package cs3390;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps over Wikipedia xml format and output all document having the category listed in the input category
 * file
 * 
 */
public class WikipediaMapper extends Mapper<LongWritable, Text, Text, Text> {

  //private MultipleOutputs mos;
  public static Set <String> links = new HashSet(); 
  private static final Logger log = LoggerFactory.getLogger(WikipediaMapper.class);

  private static final String START_DOC = "<text xml:space=\"preserve\">";

  private static final String END_DOC = "</text>";

  private static final Pattern TITLE = Pattern.compile("<title>(.*)<\\/title>");

  private static final Pattern LINKS = Pattern.compile("[[.*]]");
  
  private static final String REDIRECT = "<redirect />";

  private long id;
  private int increment;

  
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    String content = value.toString();
    
    if (content.contains(REDIRECT)) {
      return;
    }
    String document;
    String title;
    try {
      
      document = getDocument(content);  /* XML doc as a string */ 
      
      /* Step 1 - extract XML page title */
      title = getTitle(content).replaceAll(" ", "_");		/* XML page title */
      
      
      /* Step 3 - extract links */
      ArrayList<String> links = getLinks(document);
      id += increment;
      context.write(new Text(title +","+id), new Text());
      for (java.util.Iterator<String> it = links.iterator(); it.hasNext();) {
    	  String link = it.next();
    	  if (!links.contains(link)) {
    		  context.write(new Text(title), new Text(links.size() + "," + link));
    		  //mos.write("pageid", new Text(title), new Text(), new Text() );
    	  }
      }
      
      
      
    } catch (RuntimeException e) {
        // TODO: reporter.getCounter("Wikipedia", "Parse errors").increment(1);
    	e.printStackTrace();
    	log.error(e.getMessage());
      return;
    }

  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException  {
    super.setup(context);
    //mos = new MultipleOutputs(context);
    id = context.getTaskAttemptID().getTaskID().getId();
    increment = context.getConfiguration().getInt("mapred.map.tasks", 0);
    if (increment == 0) {
        throw new IllegalArgumentException("mapred.map.tasks is zero");
    }
  }
  public void cleanup(Context context) throws IOException, InterruptedException {
	  //mos.close();
   }

  private static String getDocument(String xml) {
    int start = xml.indexOf(START_DOC) + START_DOC.length();
    int end = xml.indexOf(END_DOC, start);
    return xml.substring(start, end);
  }

  private static String getTitle(CharSequence xml) {
    Matcher m = TITLE.matcher(xml);
    return m.find() ? m.group(1) : "";
  }
  
  private static ArrayList<String> getLinks(String xml) {
	  ArrayList <String> links = new ArrayList<String>();
	  Matcher m = LINKS.matcher(xml);
	  while(m.find()) {
		  String link = m.group(0) ;
		  if (!link.startsWith("file:") && !link.contains("category:")) {
			  links.add(link.split("\\|")[0]); 
		  }
	  }
	  return links;
  }
  
}
