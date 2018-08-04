import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			threashold = context.getConfiguration().getInt("threashold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			if(count < threashold) {
				return;
			}
			//this is --> cool = 20
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length-1; i++) {
				sb.append(words[i]).append(" ");
			}


			//what is the outputkey?
			//what is the outputvalue?
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length-1]+"="+count;
			
			//write key-value to reducer?
			if(outputKey != null && outputKey.length() > 0) {
				context.write(new Text(outputKey), new Text(outputValue));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			TreeMap<Integer,List<String>> tm = new TreeMap<>(Collections.reverseOrder());
			for(Text cur: values) {
				String[] pair = cur.toString().trim().split("=");
				int count = Integer.parseInt(pair[1].trim());
				if(tm.containsKey(count)) {
					tm.get(count).add(pair[0].trim());
				} else {
					List<String> list = new ArrayList<>();
					list.add(pair[0].trim());
					tm.put(count, list);
				}
			}
			Iterator<Integer> iterator = tm.keySet().iterator();
			for(int i = 0;i < n && iterator.hasNext();) {
				int count = iterator.next();
				List<String> list = tm.get(count);
				for(String str: list) {
					context.write(new DBOutputWritable(key.toString(), str, count), NullWritable.get());
					i++;
				}
			}


		}
	}
}
