package vm.java.io.files;

import java.io.File;
import java.util.*;

public class FileCollection {
    private final String tag;
    private final List<File> files;
    private final Map<String, FileCollection> collections;
    private final long size;
    private final long lastUpdatedAt;

    public FileCollection(String tag) {
        this.tag = tag;
        this.files = new ArrayList<>();
        this.collections = new HashMap<>();
        this.size = totalFileSize();
        this.lastUpdatedAt = System.currentTimeMillis();
    }

    private long totalFileSize() {
        return files.stream().map(File::length).mapToLong(len -> len).sum();
    }

    public void addFile(File file) {
        this.files.add(file);
    }

    public void addCollection(FileCollection collection) {
        this.collections.put(collection.tag, collection);
    }

    public String getTag() {
        return tag;
    }

    public List<File> getFiles() {
        return files;
    }

    public Optional<FileCollection> getCollection(String tag) {
        return Optional.ofNullable(collections.get(tag));
    }

    public long getSize() {
        return size;
    }

    public long getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileCollection that)) return false;

        return tag.equals(that.tag);
    }

    @Override
    public int hashCode() {
        return tag.hashCode();
    }
}
