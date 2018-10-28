import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class HitsIterJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public class HitsIterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text data, Context context)
                throws IOException, InterruptedException
        {
            String thisUrl;
            double a;
            double h;
            LinkedList<String> to = new LinkedList<>();
            LinkedList<String> from = new LinkedList<>();

            String[] allData = data.toString().split("\t");
            int i = 0;
            thisUrl = allData[i]; i += 1;
            a = Double.valueOf(allData[i]); i += 1;
            h = Double.valueOf(allData[i]); i += 1;
            int nTo = Integer.valueOf(allData[i]); i += 1;
            for (int k = 0; k < nTo; k += 1) {
                to.add(allData[i]); i += 1;
            }
            int nFrom = Integer.valueOf(allData[i]); i += 1;
            for (int k = 0; k < nFrom; k += 1) {
                from.add(allData[i]); i += 1;
            }

            for (String l : to) {
                context.write(new Text(l), new Text("a" + h));
                context.write(new Text(thisUrl), new Text('>' + l));
            }
            for (String l : from) {
                context.write(new Text(l), new Text("h" + a));
                context.write(new Text(thisUrl), new Text('<' + l));
            }
        }
    }

    public static class HitsIterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text url, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LinkedList<String> from = new LinkedList<>();
            LinkedList<String> to = new LinkedList<>();
            double a = 0;
            double h = 0;

            for (Text _val : values) {
                String val = _val.toString();
                String content = val.substring(1);
                char opcode = val.charAt(0);
                switch (opcode) {
                    case '>':
                        to.add(content);
                        break;
                    case '<':
                        from.add(content);
                        break;
                    case 'a':
                        a += Double.valueOf(content);
                        break;
                    case 'h':
                        h += Double.valueOf(content);
                        break;
                }
            }

            StringConstructor sc = new StringConstructor();
            sc.writeGeneric(a);
            sc.writeGeneric(h);
            sc.writeGenericIterable(to);
            sc.writeGenericIterable(from);
            context.write(url, new Text(sc.toString()));
        }
    }

    public static Job GetJobConf(Configuration conf, String input, String outDir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(HitsIterJob.class);
        job.setJobName(HitsIterJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(HitsIterMapper.class);
        job.setReducerClass(HitsIterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static void main(String[] args) throws Exception {

        // parameters <working directory> <start iteration> <iterations' count>
        String workdir = args[1];
        String input;
        int exitCode = 1;
        int nIter = Integer.valueOf(args[3]);
        int startIter = Integer.valueOf(args[2]);
        if (startIter == 1) {
            input = Paths.get(workdir, "init").toString();
        } else {
            input = Paths.get(workdir, "iter" + (startIter - 1)).toString();
            System.out.println(input);
        }

        for (int i = startIter; i <= startIter + nIter; i += 1) {
            System.out.println(">>>Iteration " + i);
            String output = Paths.get(workdir, "iter" + i).toString();
            exitCode = ToolRunner.run(new HitsIterJob(), new String[] {input, output});
            input = output;
        }

        System.exit(exitCode);
    }
}
