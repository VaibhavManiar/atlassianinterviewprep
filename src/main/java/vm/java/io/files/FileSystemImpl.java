package vm.java.io.files;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FileSystemImpl implements FileSystem {
    private final Map<String, FileCollection> collectionStore;
    private final Set<FileCollection> sortedCollectionStore;
    private final static String DEFAULT_FILE_COLLECTION = "_default_";

    public FileSystemImpl() {
        this.collectionStore = new ConcurrentHashMap<>();
        this.sortedCollectionStore = new TreeSet<>((col1, col2) -> {
            if (col1.equals(col2)) return 0;
            long size1 = col1.getSize();
            long size2 = col2.getSize();
            if (size2 == size1) {
                return Long.compare(col1.getLastUpdatedAt(), col2.getLastUpdatedAt());
            }
            return Long.compare(size2, size1);
        });
    }

    @Override
    public List<FileCollection> topNCollectionsBySize(int n) {
        if (n <= 0) return new ArrayList<>();
        return this.sortedCollectionStore.stream().limit(n).toList();
    }

    @Override
    public Optional<FileCollection> topCollectionsBySize() {
        List<FileCollection> collections = topNCollectionsBySize(1);
        if (collections.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(collections.get(0));
    }

    @Override
    public void addFile(File file) {
        this.addFile(file, DEFAULT_FILE_COLLECTION);
    }

    @Override
    public void addFile(File file, String tag) {
        this.collectionStore.computeIfAbsent(tag, FileCollection::new).addFile(file);
    }

    @Override
    public FileCollection createCollection(String tag) {
        return Optional.ofNullable(this.collectionStore.get(tag)).orElseGet(() -> {
            FileCollection collection = new FileCollection(tag);
            this.collectionStore.put(tag, collection);
            this.sortedCollectionStore.add(collection);
            return collection;
        });
    }
}
