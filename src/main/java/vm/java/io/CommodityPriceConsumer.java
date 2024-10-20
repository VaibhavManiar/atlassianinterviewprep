package vm.java.io;

import java.lang.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class CommodityPriceConsumer {
    // <CommodityId vs <Timestamp vs CommodityPrice>>
    private final Map<Long, Map<Long, CommodityPriceChangeEvent>> priceStore;

    // <CommodityId vs CommodityPrice>
    private final Map<Long, CommodityPriceChangeEvent> commodityMaxPrice;

    public CommodityPriceConsumer() {
        this.priceStore = new ConcurrentHashMap<>();
        this.commodityMaxPrice = new ConcurrentHashMap<>();
    }

    public void consume(Stream<CommodityPriceChangeEvent> commodityPriceChangeEventStream) {
        commodityPriceChangeEventStream.forEach(commodityPriceChangeEvent -> {
            // Create TimeSeries Data
            this.priceStore.computeIfAbsent(commodityPriceChangeEvent.commodityId, k -> new TreeMap<>((t1, t2) -> Long.compare(t2, t1)));
            this.priceStore.get(commodityPriceChangeEvent.getCommodityId()).put(commodityPriceChangeEvent.getTimestamp(), commodityPriceChangeEvent);

            // Calculate Max
            CommodityPriceChangeEvent maxCommodityPriceChangeEvent = this.commodityMaxPrice.get(commodityPriceChangeEvent.getCommodityId());
            if (maxCommodityPriceChangeEvent == null) {
                this.commodityMaxPrice.put(commodityPriceChangeEvent.getCommodityId(), maxCommodityPriceChangeEvent);
            } else if (maxCommodityPriceChangeEvent.getPrice() < commodityPriceChangeEvent.getPrice()) {
                this.commodityMaxPrice.put(commodityPriceChangeEvent.getCommodityId(), commodityPriceChangeEvent);
            }
        });
    }

    public Optional<Double> getMaxPrice(long commodityId) {
        return Optional.ofNullable(commodityMaxPrice.get(commodityId)).map(CommodityPriceChangeEvent::getPrice);
    }

    public static class CommodityPriceChangeEvent {

        private final long commodityId;
        private final double price;
        private final long timestamp;

        public CommodityPriceChangeEvent(long commodityId, double price) {
            this.commodityId = commodityId;
            this.price = price;
            this.timestamp = System.currentTimeMillis();
        }

        public long getCommodityId() {
            return this.commodityId;
        }

        public double getPrice() {
            return this.price;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

    }

	/*
	Need more clarity on this:
	public getCommodityPrice(long timestamp, int checkpoint) {
		return Optional.ofNullable(commodityMaxPrice.get(commodityId));
	}*/

    public static void main(String[] args) {
    }
}

