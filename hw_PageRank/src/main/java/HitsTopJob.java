import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HitsTopJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1], args[2]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public class HitsTopMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        @Override
        protected void map(LongWritable offset, Text data, Context context)
                throws IOException, InterruptedException
        {
            String thisUrl;
            double a;
            double h;

            String[] allData = data.toString().split("\t");
            int i = 0;
            thisUrl = allData[i]; i += 1;
            a = -Double.valueOf(allData[i]); i += 1;
            h = -Double.valueOf(allData[i]);

            Configuration conf = context.getConfiguration();
            String sortBy = conf.get("sortBy");
            if (sortBy.equals("h")) {
                context.write(new DoubleWritable(h), new Text(thisUrl));
            } else if (sortBy.equals("a")) {
                context.write(new DoubleWritable(a), new Text(thisUrl));
            } else {
                System.out.println("Invalid sort-by parameter " + sortBy);
                throw new RuntimeException("Invalid sort-by parameter " + sortBy);
            }
        }
    }

    public static class HitsTopReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        protected void reduce(DoubleWritable score, Iterable<Text> urls, Context context) throws IOException, InterruptedException {
            for (Text url : urls) {
                context.write(score, url);
            }
        }
    }

    public static Job GetJobConf(Configuration conf, String input, String outDir, String sortBy) throws IOException {
        conf.set("sortBy", sortBy);
        Job job = Job.getInstance(conf);
        job.setJarByClass(HitsTopJob.class);
        job.setJobName(HitsTopJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(HitsTopMapper.class);
        job.setReducerClass(HitsTopReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HitsTopJob(), args);
        System.exit(exitCode);
    }
}
