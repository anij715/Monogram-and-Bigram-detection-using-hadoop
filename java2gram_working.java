import java.io.IOException;
import java.util.*;
	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class BigramCount {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	     private final static IntWritable one = new IntWritable(1);
	     private Text word = new Text();
	
	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       String line = value.toString();
	       String prev = null;
	       StringTokenizer tokenizer = new StringTokenizer(line.toLowerCase()," \t.!?:()[]“”,*‘’/<>'&-;|0123456789");
	       while (tokenizer.hasMoreTokens()) {
	       	String cur = tokenizer.nextToken();
	       	if (prev != null) {
	          word.set(prev + " " + cur);
	          output.collect(word, one);
	        }
       		prev = cur;
	       }
	     }
	   }
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       int sum = 0;
	       while (values.hasNext()) {
	         sum += values.next().get();
	       }
	       if (sum>=10) {
	       	output.collect(key, new IntWritable(sum));
	       	}
	     }
	   }

	public static class Map2 extends MapReduceBase implements Mapper<Text, Text, IntWritable, Text> {
	     private IntWritable frequency = new IntWritable();
	
	     public void map(Text key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	       
	       int newVal = Integer.parseInt(value.toString());
	       frequency.set(newVal);
	       output.collect(frequency, key);
	     }
	   }

	public static class Reduce2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
	     private Text word = new Text();

	     public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		     while (values.hasNext()) {
		     	word = values.next();
		     	output.collect(key, word);
		     }	
	        
	     }
	   }
	
	public static void main(String[] args) throws Exception {
	     

	     JobConf conf1 = new JobConf(BigramCount.class);
	     conf1.setJobName("wordcount");
	
	     conf1.setOutputKeyClass(Text.class);
	     conf1.setOutputValueClass(IntWritable.class);
	
	     conf1.setMapperClass(Map.class);
	     conf1.setCombinerClass(Reduce.class);
	     conf1.setReducerClass(Reduce.class);
	
	     conf1.setInputFormat(TextInputFormat.class);
	     conf1.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf1, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

	     JobClient.runJob(conf1);

	     JobConf conf2 = new JobConf(BigramCount.class);
	     conf2.setJobName("wordcountsorting");
	
	     conf2.setOutputKeyClass(IntWritable.class);
	     conf2.setOutputValueClass(Text.class);
	
	     conf2.setMapperClass(Map2.class);
	     conf2.setCombinerClass(Reduce2.class);
	     conf2.setReducerClass(Reduce2.class);
	
	     conf2.setInputFormat(KeyValueTextInputFormat.class);
	     conf2.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf2, new Path(args[1]));
	     FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
	     
	     JobClient.runJob(conf2);
	   }
	}