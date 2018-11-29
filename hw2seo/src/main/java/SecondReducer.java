import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondReducer extends Reducer<HostQCount, Text, Text, LongWritable> {
    private int numberClicksMin = 1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        numberClicksMin = config.getInt("seo.minclicks", numberClicksMin);
    }

    @Override
    protected void reduce(HostQCount key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        String best_query = values.iterator().next().toString();
        LongWritable max = key.getCount();
        if (max.get() >= numberClicksMin) {
            context.write(new Text(key.getHost() + "\t" + best_query), max);
        }
        
    }
}
