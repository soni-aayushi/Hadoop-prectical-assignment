import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Ans7 {
    public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length > 2) {
                String genres = fields[2];
                if (genres.contains("Comedy")) {
                    context.write(NullWritable.get(), new Text(line));
                }
            }
        }
    }

    public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(NullWritable.get(), val);
            }
        }
    }

   
    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("DocumentaryMoviesCount");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length > 3) {
                String genres = fields[2];
                String year = fields[3];
                if (genres.contains("Documentary") && year.contains("1995")) {
                    context.write(word, one);
                }
            }
        }
    }

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    
    public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("MissingGenresCount");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length > 2) {
                String genres = fields[2];
                if (genres == null || genres.trim().isEmpty()) {
                    context.write(word, one);
                }
            }
        }
    }

    public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    
    public static class Map3 extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length > 1) {
                String title = fields[1];
                if (title.contains("Gold")) {
                    context.write(NullWritable.get(), new Text(title));
                }
            }
        }
    }

    public static class Reduce3 extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(NullWritable.get(), val);
            }
        }
    }

   
    public static class Map4 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("DramaRomanticMoviesCount");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length > 2) {
                String genres = fields[2];
                if (genres.contains("Drama") && genres.contains("Romance")) {
                    context.write(word, one);
                }
            }
        }
    }

    public static class Reduce4 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

   
        Job job1 = Job.getInstance(conf, "Comedy Movies");
        job1.setJarByClass(Ans7.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_job1"));

      
        Job job2 = Job.getInstance(conf, "Documentary Movies 1995");
        job2.setJarByClass(Ans7.class);
        job2.setMapperClass(Map1.class);
        job2.setReducerClass(Reduce1.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_job2"));

    
        Job job3 = Job.getInstance(conf, "Missing Genres");
        job3.setJarByClass(Ans7.class);
        job3.setMapperClass(Map2.class);
        job3.setReducerClass(Reduce2.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "_job3"));

   
        Job job4 = Job.getInstance(conf, "Gold in Title");
        job4.setJarByClass(Ans7.class);
        job4.setMapperClass(Map3.class);
        job4.setReducerClass(Reduce3.class);
        job4.setOutputKeyClass(NullWritable.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job4, new Path(args[0]));
        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "_job4"));

    
        Job job5 = Job.getInstance(conf, "Drama and Romantic Movies");
        job5.setJarByClass(Ans7.class);
        job5.setMapperClass(Map4.class);
        job5.setReducerClass(Reduce4.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        job5.setInputFormatClass(TextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job5, new Path(args[0]));
        FileOutputFormat.setOutputPath(job5, new Path(args[1] + "_job5"));

      
        boolean success = job1.waitForCompletion(true) &&
                          job2.waitForCompletion(true) &&
                          job3.waitForCompletion(true) &&
                          job4.waitForCompletion(true) &&
                          job5.waitForCompletion(true);

        System.exit(success ? 0 : 1);
    }
}
