import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.MalformedURLException;
import java.net.URL;

import java.io.IOException;

public class MainClass extends Configured implements Tool {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("\t");

            if (parts.length == 2) {
                try {
                    URL url = new URL(parts[1]);
                    context.write(new Text(url.getHost() + "\t" + parts[0]), one);
                } catch (MalformedURLException err) {
                    context.getCounter("COMMON_COUNTERS", "SpoiledUrls").increment(1);
                }
            } else {
                context.getCounter("COMMON_COUNTERS", "SpoiledRecords").increment(1);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        @Override
        protected void reduce(Text word, Iterable<LongWritable> nums, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable i: nums) {
                sum += i.get();
            }
            context.write(new Text(word), new LongWritable(sum));
        }
    }

    private Job getJobConf1(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(MainClass.class);
        job.setJobName("<host, query> -> count");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output, "out1"));

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    private Job getJobConf2(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(MainClass.class);
        job.setJobName("<host, popular_query> -> count");

        FileInputFormat.addInputPath(job, new Path(output, "out1"));
        FileOutputFormat.setOutputPath(job, new Path(output, "out2"));

        job.setMapperClass(SecondMapper.class);
        job.setReducerClass(SecondReducer.class);

        job.setPartitionerClass(PartitionerByHost.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setMapOutputKeyClass(HostQCount.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(HostQCount.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }

    public int run(String[] args) throws Exception {
        Job job1 = getJobConf1(args[0], args[1]);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        Job job2 = getJobConf2(args[0], args[1]);
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new MainClass(), args);
        System.exit(ret);
    }
}
