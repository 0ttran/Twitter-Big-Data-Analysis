package org.myorg;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class HashtagCount {

  public static class Map extends
			Mapper<Object, Text, Text, LongWritable> {

		final static Pattern TAG_PATTERN = Pattern.compile("Coords:.*$");
		private final static LongWritable ONE = new LongWritable(1L);
		private Text word = new Text();
		
		
		public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = TAG_PATTERN.matcher(value.toString());
            while (matcher.find()) {
            	String found = matcher.group();
            	String coord=found.substring(7,8);
            	
            	String a=found.substring(7,8);
            	if(a.equals("["))
            	{
            		int j=9;
            		while(a.equals("["))
            		{
            			
            			if((found.substring(j,j+1)).equals("]"))
            			{
            				a="]";
            			}
            			else
            			{
            				j++;
            			}
            		}
            		coord=found.substring(8,j);
            		String[] coord_arr = coord.split(" ");
            		coord_arr[0] = coord_arr[0].substring(0, coord_arr[0].length()-1);
            		int longitude = (int)Float.parseFloat(coord_arr[1]);
            		int latitude = (int)Float.parseFloat(coord_arr[0]);

	            	String cleaned = found.replaceFirst(".*Hashtags:", "");
	            	String polished = cleaned.split("\",\"")[0];
	
	            	String s = polished;
	            	if (polished.startsWith("\\u")) {
	            		s = StringEscapeUtils.unescapeJava(polished);
	            	}
					
					String[] elements=s.split(" ");
				
					for(int i=0;i<elements.length;i++)
					{
						
					//	System.out.println(elements[i]);
						if(!elements[i].equals(""))
						{
			            	word.set(longitude +" " + latitude + " " + elements[i]);
			            	context.write(word, ONE);
						}
					}
	            }
            }
		}

	}

	public static class Reduce extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "hashtag count");
		job.setJarByClass(HashtagCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}   