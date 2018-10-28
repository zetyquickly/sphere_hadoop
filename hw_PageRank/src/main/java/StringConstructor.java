import java.util.Collection;


public class StringConstructor {
    private StringBuilder stringBuilder = new StringBuilder();

    public <T> void writeGeneric(T value) {
        if (stringBuilder.length() > 0) {
            stringBuilder.append("\t");
        }
        stringBuilder.append(value);
    }

    public <T> void writeGenericIterable(Collection<? extends T> values) {
        writeGeneric(values.size());
        for (T e : values) {
            writeGeneric(e);
        }
    }

    public String toString() {
        return stringBuilder.toString();
    }

}
