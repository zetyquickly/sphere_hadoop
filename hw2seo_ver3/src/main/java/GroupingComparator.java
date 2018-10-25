import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    GroupingComparator() {
        super(HostQCount.class, true);
    }

    public int compare(WritableComparable value1, WritableComparable value2) {
        Text host1 = ((HostQCount) value1).getHost();
        Text host2 = ((HostQCount) value2).getHost();
        return host1.compareTo(host2);
    }
}
