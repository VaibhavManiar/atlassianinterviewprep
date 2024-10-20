package vm.java.io.costexplorer;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionServiceImpl implements SubscriptionService {

    // Customer V/S Subscriptions
    private final Map<Long, List<CustomerProductSubscription>> customerSubscriptionHistory;

    public SubscriptionServiceImpl() {
        this.customerSubscriptionHistory = new ConcurrentHashMap<>();
    }

    @Override
    public void subscibe(long customerId, Subscription subscription, LocalDate startDate) {
        CustomerProductSubscription customerProductSubscription = new CustomerProductSubscription(customerId, startDate, subscription);
        this.customerSubscriptionHistory.computeIfAbsent(customerId, k -> new ArrayList<>()).add(customerProductSubscription);
    }

    @Override
    public void unsubscribe(long customerId, long productId) {
        this.customerSubscriptionHistory.get(customerId).stream()
                .filter(CustomerProductSubscription::isEnabled)
                .filter(customerProductSubscription -> customerProductSubscription.getProductId() == productId)
                .findFirst()
                .ifPresent(CustomerProductSubscription::disable);
    }

    @Override
    public List<CustomerProductSubscription> getCustomerSubscriptions(Long customerId) {
        return this.customerSubscriptionHistory.getOrDefault(customerId, new ArrayList<>());
    }
}
