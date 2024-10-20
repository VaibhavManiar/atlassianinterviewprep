package vm.java.io.costexplorer.practice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionServiceImpl implements SubscriptionService {

    private final Map<Long, List<CustomerSubscription>> customerSubscriptions;

    public SubscriptionServiceImpl() {
        this.customerSubscriptions = new ConcurrentHashMap<>();
    }

    @Override
    public void subscribe(long customerId, Subscription subscription) {
        CustomerSubscription customerSubscription = new CustomerSubscription(customerId, subscription);
        customerSubscriptions.computeIfAbsent(customerId, k-> new ArrayList<>()).add(customerSubscription);
    }

    @Override
    public List<CustomerSubscription> getCustomerSubscriptions(long customerId) {
        return customerSubscriptions.getOrDefault(customerId, List.of());
    }
}
