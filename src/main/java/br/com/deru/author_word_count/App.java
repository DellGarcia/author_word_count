package br.com.deru.author_word_count;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App 
{
	
	public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            
            set(texts);
        }
    }
	
	public static class Map extends Mapper<LongWritable, Text, Text, ArrayWritable> {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String[] lineParts = value.toString().split(":::");
			
			String[] authors = lineParts[1].split("::");
			StringTokenizer tokenizer = new StringTokenizer(lineParts[2]);
			
			List<String> words = new ArrayList<String>();
			
			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken().replaceAll("[^0-9|^a-z|^A-Z]", ""));
				words.add(value.toString());
			}
			
			for (String author: authors) {
				String[] array = new String[words.size()];
				
				for(int i = 0; i < words.size(); i++) {
					array[i] = words.get(i);
 				}
				
				context.write(new Text(author), new TextArrayWritable(array));
			}
		}
		
	}

	public static class Reduce extends Reducer<Text, TextArrayWritable, Text, Text> {
		
		private final HashSet<String> stop_words = new HashSet<String>(Arrays.asList(StopWords.STOP_WORDS));
		
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			HashMap<Text, Integer> word_count = new HashMap<Text, Integer>();
			
			for (ArrayWritable array: values) {
				for (Writable value : array.get()) {
					Text word = (Text) value;
					
					if(!stop_words.contains(word.toString().toLowerCase())) {
						if(!word_count.containsKey(word)) {
							word_count.put(word, 1);
						} else {
							word_count.put(word, word_count.get(word) + 1);
						}
					}
				}
			}
			
			StringBuilder result = new StringBuilder("/t || ");

			for (Text k : word_count.keySet()) {
				result.append(String.format("%s: %d, ", k.toString(), word_count.get(k)));
			}
			
			context.write(key, new Text(result.toString()));
		}
		
	}
	
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
    	
    	Configuration conf = new Configuration();
    	
    	Job job = Job.getInstance(conf, "Word Count");
    	
    	job.setJarByClass(App.class);
    	job.setMapperClass(Map.class);
    	job.setReducerClass(Reduce.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(TextArrayWritable.class);
    	
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	Path outputPath = new Path(args[1]);
    	
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	outputPath.getFileSystem(conf).delete(outputPath, true);
    	
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
