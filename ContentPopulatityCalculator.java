// Create Class Popular Content Calculator Engine
// TreeSet <Content>

import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.stream.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ContentPopulatityCalculator {

	private final Set<Content> sortedContents;
	private final Map<Long, Content> contentMap;

	public ContentPopulatityCalculator() {
		this.sortedContents = new TreeSet<>((c1, c2) -> {
			if(c1.getPopularity() == c2.getPopularity()) {
				return Long.compare(c2.getTimestamp(), c1.getTimestamp());
			}
			return Integer.compare(c2.getPopularity(), c1.getPopularity());
		});
		this.contentMap = new ConcurrentHashMap<>();
	}

	public List<Content> getMostPopularContent(int n) {
		return sortedContents.stream().limit(n).collect(Collectors.toList());
	}

	public Optional<Content> getMostPopularContent() {
		List<Content> list = this.getMostPopularContent(1);
		if(list.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(list.get(0));
	}
	
	public void consume(Stream<ContentAction> actionStream) {
		actionStream.forEach(action -> {
			Content content = contentMap.get(action.getContentId());
			if(content != null) {
				if(ContentAction.Type.INCREASE == action.getType()) {
					content.increasePopularity(action.getCount());
				} else if(ContentAction.Type.DECREASE == action.getType()) {
					content.decreasePopularity(action.getCount());
				}
				sortedContents.add(content);
			}
		});
	}

	public static class Content {
		private long id;
		private long timestamp;
		private AtomicInteger popularity;

		public long getId() {
			return this.id;
		}

		public long getTimestamp() {
			return this.timestamp;
		}

		public int getPopularity() {
			return this.popularity.get();
		}

		public void increasePopularity(int count) {
			this.popularity.set(this.popularity.get() + count);
			this.timestamp = System.currentTimeMillis();
		}

		public void decreasePopularity(int count) {
			int newCount = this.popularity.get() - count;
			this.popularity.set(newCount < 0 ? 0: newCount);
			this.timestamp = System.currentTimeMillis();
		}
	}

	public static class ContentAction {
		private final long contentId;
		private final Type type;
		private final int count;

		public ContentAction(long contentId, Type type, int count) {
			this.contentId = contentId;
			this.type = type;
			this.count = count;
		}

		public ContentAction(long contentId, Type type) {
			this.contentId = contentId;
			this.type = type;
			this.count = 1;
		}


		public enum Type {
			INCREASE, DECREASE;
		}

		public long getContentId() {
			return this.contentId;
		}

		public Type getType() {
			return this.type;
		}

		public int getCount() {
			return this.count;
		}
	}
} 
