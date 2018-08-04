import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			//how to get n-gram from command line?
			noGram = context.getConfiguration().getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			line = line.trim().toLowerCase();
			//how to remove useless elements?
			line = line.replaceAll("[^a-z]", " ");
			//how to separate word by space?
			String[] words = line.split("\\s+");
			//how to build n-gram based on array of words?
			if(words.length< 2) {
				return;
			}

			StringBuilder sb;
			for(int i = 0; i < words.length-1; i++) {
				sb = new StringBuilder();
				sb.append(words[i]);
				for(int j = i + 1; j < words.length && j < (noGram - i); j++) {
					sb.append(" ");
					sb.append(words[j]);
					context.write(new Text(sb.toString()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//how to sum up the total count for each n-gram?
			int sum = 0;
			 for(IntWritable n : values) {
			 	sum += n.get();
			 }
			 context.write(key, new IntWritable(sum));
		}
	}

}