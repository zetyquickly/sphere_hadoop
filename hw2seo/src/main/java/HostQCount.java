import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class HostQCount implements WritableComparable<HostQCount> {
    private Text host;
    private LongWritable qcount;

    public HostQCount() {
        this.host = new Text();
        this.qcount = new LongWritable();
    }

    public HostQCount(String host, int qcount) {
        this.host = new Text(host);
        this.qcount = new LongWritable(qcount);
    }

    public HostQCount(Text host, LongWritable qcount) {
        this.host = host;
        this.qcount = qcount;
    }

    public Text getHost() {
        return host;
    }

    public LongWritable getCount() {
        return qcount;
    }

    public int compareTo(@Nonnull HostQCount that) {
        int compareResult = this.host.compareTo(that.host);
        return (compareResult == 0) ? -this.qcount.compareTo(that.qcount) : compareResult;
    }

    @Override
    public int hashCode() {
        int result = (host != null) ? host.hashCode() : 0;
        result = 139 * result;
        return result;
    }

    @Override
    public String toString() {
        return host + "\t" + qcount.toString();
    }

    public void write(DataOutput out) throws IOException {
        host.write(out);
        qcount.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        host.readFields(in);
        qcount.readFields(in);
    }
}
