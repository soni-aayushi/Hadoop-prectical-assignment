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

public class Ans6 {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text userId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       
            String[] fields = value.toString().split(",");
            if (fields.length > 0) {
                String userIdStr = fields[1].trim();
                userId.set(userIdStr);
                context.write(userId, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
         
        
         job.setJarByClass(Ans6.class);
         
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
