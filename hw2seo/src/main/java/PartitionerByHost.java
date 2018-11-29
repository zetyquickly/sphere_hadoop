import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerByHost extends Partitioner<HostQCount, Text> {
    @Override
    public int getPartition(HostQCount key, Text val, int num_partitions) {
        if (key.getHost().toString().length() == 0) {
            return 0;
        }

        float first_letter_code = key.getHost().charAt(0);
        if (first_letter_code < 'a') {
            return 0;
        }
        if (first_letter_code > 'z') {
            return num_partitions - 1;
        }

        return (int) ((first_letter_code - 'a') / ('z' - 'a' + 1) * num_partitions); //+1: to prevent returning more then num_partitions-1

    }
}