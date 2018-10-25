import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondMapper extends Mapper<LongWritable, Text, HostQCount, Text> {
    private final static LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        String[] parts = record.split("\t");
        String query = parts[1];
        String host = parts[0];
        int qcount = Integer.parseInt(parts[2]);
        context.write(new HostQCount(new Text(host), new LongWritable(qcount)), new Text(query));
    }
}
