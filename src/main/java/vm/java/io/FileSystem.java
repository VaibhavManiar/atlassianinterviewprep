package vm.java.io;

import java.io.*;
import java.lang.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class FileSystem {
	
	private final Map<String, Collection> tagVsCollectionMap;
	private final Set<Collection> reverseSortedCollectionBySize;

	public FileSystem() {
		this.reverseSortedCollectionBySize =  new TreeSet<>((c1, c2) -> {
			if (c1.equals(c2)) return 0;

			if(c1.size == c2.size) {
				return Long.compare(c2.lastUpdatedAt(), c1.lastUpdatedAt());
			}
			return Long.compare(c2.size, c1.size);
		});
		this.tagVsCollectionMap = new ConcurrentHashMap<>();
	}

	public long getCollectionSize(String tag) {
		Collection collection = tagVsCollectionMap.get(tag);
		if(collection != null) {
			return collection.size();
		}
		return -1;
	}

	public List<Collection> getTopNCollections(int n) {
		reverseSortedCollectionBySize.forEach(collection -> System.out.println(collection.getTag()));
		return reverseSortedCollectionBySize.stream().limit(n).collect(Collectors.toList());
	}

	public List<Collection> getEmptyCollections() {
		List<Collection> emptyCollections = new ArrayList<>();
		for(Collection collection : tagVsCollectionMap.values()) {
			if(collection.getFiles().isEmpty()) {
				emptyCollections.add(collection);
			}
		}
		return emptyCollections;
	}

	public void addFile(File file, Set<String> tags) {
		tags.forEach(tag -> {
			Collection collection = this.tagVsCollectionMap.get(tag);
			if(collection != null) {
				collection.addFile(file);
				this.reverseSortedCollectionBySize.remove(collection);
				this.reverseSortedCollectionBySize.add(collection);
			} else {
				Collection newCollection = new Collection(tag);
				newCollection.addFile(file);
				System.out.println("Creating new collection : " + newCollection.getTag());
				this.tagVsCollectionMap.put(tag, newCollection);
				this.reverseSortedCollectionBySize.add(newCollection);
			}
		});
	}

	public void createCollection(String tag) {
		Collection newCollection = new Collection(tag);
		this.tagVsCollectionMap.put(tag, newCollection);
		this.reverseSortedCollectionBySize.add(newCollection);
	}

	public void addCollection(Collection parentCollection, Collection newCollection) {
		if(!this.tagVsCollectionMap.containsKey(newCollection.getTag())) {
			this.tagVsCollectionMap.put(newCollection.getTag(), newCollection);
		}
		parentCollection.addCollection(newCollection);
		this.reverseSortedCollectionBySize.add(newCollection);
	}

	public static class Collection {
		private final List<File> files = new ArrayList<>();
		private long size;
		private final String tag;
		private final Map<String, Collection> collections = new HashMap<>();
		private long timestamp;

		public Collection(String tag) {
			this.tag = tag;
			this.timestamp = System.currentTimeMillis();
		}

		public void addFile(File file) {
			this.files.add(file);
			this.size += file.length();
			this.timestamp = System.currentTimeMillis();
		}

		public long size() {
			return this.size;
		}

		public List<File> getFiles() {
			return files;
		}

		public Map<String, Collection> getCollections() {
			return this.collections;
		}

		public void addCollection(Collection collection) {
			this.collections.put(collection.getTag(), collection);
			this.size += collection.size();
			this.timestamp = System.currentTimeMillis();
		}

		public String getTag() {
			return this.tag;
		}

		public long lastUpdatedAt() {
			return this.timestamp;
		}

		@Override
		public boolean equals(Object o) {
			if(o == this)
				return true;

			if(o instanceof Collection) {
				Collection that = (Collection) o;
				return this.tag.equals(that.getTag());
			} 
			return false;
		}

		@Override
		public int hashCode() {
			return this.tag.hashCode();
		}
	}

	public static void main(String[] args) {
		FileSystem fileSystem = new FileSystem();
		String filePath = "/Users/vaibhav.m/Documents/code/atlassianinterviewprep/src/main/resources/";
		// Test 1
		Set<String> tags1 = new HashSet<>();
		tags1.add("Important"); 
		fileSystem.addFile(createAndWriteFile(filePath + "dummy1.txt", filePath + "dummy1.txt"), tags1);
		fileSystem.addFile(createAndWriteFile(filePath + "dummy2.txt", filePath + "dummy2.txt"), tags1);
		fileSystem.addFile(createAndWriteFile(filePath + "dummy3.txt", filePath + "dummy3.txt"), tags1);
		fileSystem.addFile(createAndWriteFile(filePath + "dummy4.txt", filePath + "dummy4.txt"), tags1);


		try {
			Thread.sleep(10);
		} catch (Exception ignore) {}

		Set<String> tags2 = new HashSet<>();
		tags2.add("Dummy");

		fileSystem.addFile(createAndWriteFile(filePath + "dummy5.txt", filePath + "dummy5.txt"), tags2);
		fileSystem.addFile(createAndWriteFile(filePath + "dummy6.txt", filePath + "dummy6.txt"), tags2);
		fileSystem.addFile(new File(filePath + "dummy7.txt"), tags2);
		fileSystem.addFile(new File(filePath + "dummy8.txt"), tags2);

		System.out.println("Top 2 collections");
		fileSystem.getTopNCollections(2).forEach(col -> System.out.println(col.getTag() + " , "));
		
	}

	private static File createAndWriteFile(String path, String content) {
		File file = new File(path);
		try(FileWriter fileWriter = new FileWriter(path)) {
			file.createNewFile();
			fileWriter.write(content);
			System.out.println("Successfully wrote to the file : " + path);
		} catch (Exception e) {
			System.err.println("An error occurred.");
			e.printStackTrace();
		}

		return file;
	}
}