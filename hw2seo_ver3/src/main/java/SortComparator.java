import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {
    SortComparator() {
        super(HostQCount.class, true);
    }

    @Override
    public int compare(WritableComparable value1, WritableComparable value2) {
        return ((HostQCount) value1).compareTo((HostQCount) value2);
    }
}
