import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class BuildGraph extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1], args[2]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public class GraphBuilderMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable offset, Text data, Context context)
                throws IOException, InterruptedException
        {
            String line = data.toString();
            String[] idContent = line.split("\t");
            int id = Integer.valueOf(idContent[0]);
            String content = idContent[1];

            LinkedList<String> outgoingLinksList = LinksExtractor.extract(content);

            // creating list of full outgoing URLs
            StringBuilder sb = new StringBuilder();
            sb.append("L");
            for (String l : outgoingLinksList) {
                sb.append(l);
                sb.append("\t");
            }

            context.write(new IntWritable(id), new Text(sb.toString()));
        }
    }

    static public class UrlsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable offset, Text data, Context context)
                throws IOException, InterruptedException
        {
            String line = data.toString();
            String[] idUrl = line.split("\t");
            int id = Integer.valueOf(idUrl[0]);
            String url = idUrl[1];

            context.write(new IntWritable(id), new Text("U" + url));
        }
    }

    public static class GraphBuilderReducer extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        protected void reduce(IntWritable id, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String base = null;
            List<String> targets = null;
            for (Text v : values) {
                String val = v.toString();
                char opcode = val.charAt(0);
                String content = val.substring(1);
                if (opcode == 'U') {
                    base = content;
                } else if (opcode == 'L') {
                    targets = Arrays.asList(content.split("\t"));
                } else {
                    System.out.println("Unknown prefix " + opcode);
                    throw new RuntimeException("Unknown prefix " + opcode);
                }
            }
            targets = LinksExtractor.merge(targets, base);
            StringBuilder sb = new StringBuilder();
            for (String t : targets) {
                sb.append(t);
                sb.append("\t");
            }
            context.write(new Text(base), new Text(sb.toString()));
        }
    }

    public static Job GetJobConf(Configuration conf, String input, String urls, String outDir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(BuildGraph.class);
        job.setJobName(BuildGraph.class.getCanonicalName());

        Path inputPath = new Path(input);
        Path urlsPath = new Path(urls);
        Path output = new Path(outDir);

        MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, GraphBuilderMapper.class);
        MultipleInputs.addInputPath(job, urlsPath, TextInputFormat.class, UrlsMapper.class);
        FileOutputFormat.setOutputPath(job, output);


        job.setReducerClass(GraphBuilderReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BuildGraph(), args);
        System.exit(exitCode);
    }
}
