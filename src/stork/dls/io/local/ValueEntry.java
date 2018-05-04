package stork.dls.io.local;
import com.sleepycat.persist.model.*;

@Entity
public class ValueEntry {
    @PrimaryKey
    CompositeKey key;

    String metadataString;
    ValueEntry() {}// for bindings
    ValueEntry(CompositeKey key, String metadataString) {
        this.key = key;
        this.metadataString = metadataString;
    }
    @Override
    public String toString() {
        return "ValueEntry: (" + key + "," + metadataString + ")";
    }
}
