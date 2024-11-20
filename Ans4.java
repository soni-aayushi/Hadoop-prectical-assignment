import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class Ans4 {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                int length = token.length();
                if (length >= 4) {
                    if (length == 5) {
                        outputKey.set("Token length 5");
                        context.write(outputKey, one);
                    } else {
                        outputKey.set("Token length >= 4");
                        context.write(outputKey, one);
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

 
    public static void main(String[] args) throws Exception {
    	 Path input = new Path(args[0]);
         Path output = new Path(args[1]);
          
         
         Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "WordCount with Average");
         
        
         job.setJarByClass(Ans4.class);
         
         job.setMapperClass(Map.class);
         job.setReducerClass(Reduce.class);
         
     
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);  
         
    
         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(TextOutputFormat.class);
         
         
         job.setNumReduceTasks(1);
         
    
         FileInputFormat.setInputPaths(job, input);
         FileOutputFormat.setOutputPath(job, output);
         
       
         System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
