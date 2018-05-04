package stork.dls.io.local;
import com.sleepycat.persist.model.*;


@Persistent
public class CompositeKey {
    @KeyField(1)
    String serverName;
    @KeyField(2)
    String pathEntry;
    CompositeKey() {}// for bindings
    CompositeKey(String serverName, String pathEntry) {
        this.serverName = serverName;
        this.pathEntry = pathEntry;
    }
    CompositeKey(CompositeKey obj) {
        this.serverName = obj.serverName;
        this.pathEntry = obj.pathEntry;
    }
    @Override
    public String toString() {
        return "CacheEntryKey: (" + serverName + "," + pathEntry + ")";
    }
}
