package vm.java.io.files;

import java.io.File;
import java.util.List;
import java.util.Optional;

public interface FileSystem {

    List<FileCollection> topNCollectionsBySize(int n);

    Optional<FileCollection> topCollectionsBySize();

    void addFile(File file);

    void addFile(File file, String tag);

    FileCollection createCollection(String tag);
}