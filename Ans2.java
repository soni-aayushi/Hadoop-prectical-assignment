import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Ans2 {


    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable temperature = new IntWritable();
        private Text year = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\\s+");
            
            if (fields.length == 2) {
                String yearStr = fields[0];
                int temp = Integer.parseInt(fields[1]);

                year.set(yearStr);
                temperature.set(temp);
                
                context.write(year, temperature); 
            }
        }
    }

   
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int minTemp = Integer.MAX_VALUE;
            for (IntWritable val : values) {
                minTemp = Math.min(minTemp, val.get());
            }
            result.set(minTemp);

            context.write(key, result);  
        }
    }

 
    public static void main(String[] args) throws Exception {
    	
    	Path input = new Path(args[0]);
        Path output = new Path(args[1]);
         
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Minimum Temperature");
   
      
        job.setJarByClass(Ans2.class);

      
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
