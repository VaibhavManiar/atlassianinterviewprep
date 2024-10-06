import java.io.*;
import java.lang.*;
import java.util.*;
import java.util.concurrent.*;

public class CommodityPriceConsumer {
	private Map<Long, Map<Long, CommodityPrice>> priceStore;
	private Map<Long, CommodityPrice> commodityMaxPrice;

	public CommodityPriceConsumer() {
		this.priceStore = new ConcurrentHashMap<>();
		this.commodityMaxPrice = new ConcurrentHashMap<>();
	}

	public void consume(CommodityPrice commodityPrice) {

		// Create Timeseries Data
		Map<Long, CommodityPrice> prices = this.priceStore.getOrDefault(commodityPrice.getCommodityId(), new TreeMap<>((t1, t2) -> Long.compare(t2, t1)));
		prices.put(commodityPrice.getTimestamp(), commodityPrice);
		
		// Calculate Max
		CommodityPrice maxCommodityPrice = this.commodityMaxPrice.get(commodityPrice.getCommodityId());
		if(maxCommodityPrice == null) {
			this.commodityMaxPrice.put(commodityPrice.getCommodityId(), maxCommodityPrice);
			return;
		}
		if(maxCommodityPrice.getPrice() < commodityPrice.getPrice()) {
			this.commodityMaxPrice.put(commodityPrice.getCommodityId(), commodityPrice);
		}
	}

	public Optional<Double> getMaxPrice(long commodityId) {
		return Optional.ofNullable(commodityMaxPrice.get(commodityId)).map(commodityPrice -> commodityPrice.getPrice());
	}

	/*
	Need more clarity on this:
	public getCommodityPrice(long timestamp, int checkpoint) {
		return Optional.ofNullable(commodityMaxPrice.get(commodityId));
	}*/

	public static void main(String[] args) {}
}

class CommodityPrice {

	private final long commodityId;
	private final double price;
	private final long timestamp;

	public CommodityPrice (long commodityId, double price) {
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
