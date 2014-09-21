import org.json.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;


public class Pagerank{
	
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		 private Text page_links = new Text();
		 	
		 public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String json = value.toString();
	        String links = "1.0";
	        
	        //parse json 
         	JSONObject jobj;
			try {
				jobj = new JSONObject(json);
	         	JSONArray jarr = jobj.getJSONObject("content").getJSONArray("links");
	         	for(int i = 0; i < jarr.length(); i++) {
	         		JSONObject jo = jarr.getJSONObject(i);
	         		String type = jo.getString("type");
	         		String u = jo.getString("href");
	         		
	             	if(type.compareTo("a") == 0) {
	             		links += "\t" + u;	
	         		}
	         	}	         	
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			page_links.set(links);			
			output.collect(key, page_links);
		 }
	}
	
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			output.collect(key, values.next());
		}
	}
	
		
	public static void main(String[] args) throws Exception{
		
		/**
		 * Configuration of map reduce
		 */
		JobConf conf = new JobConf(Pagerank.class);
		conf.setJobName("pagerank");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		
	}
}