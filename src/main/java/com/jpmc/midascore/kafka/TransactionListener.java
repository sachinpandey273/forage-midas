package com.jpmc.midascore.kafka;

import org.springframework.stereotype.Component;
import org.springframework.kafka.annotation.KafkaListener;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;

@Component
public class TransactionListener {

    private static final Logger log = LoggerFactory.getLogger(TransactionListener.class);

    private final AtomicInteger counter = new AtomicInteger(0);
    private final List<Float> firstFourAmounts = new ArrayList<>();

    @KafkaListener(
            topics = "${midas.transactions.topic:midas-transactions}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void receive(Transaction tx) {
        log.info("Received transaction: {}", tx);

        if (counter.get() < 4) {
            float amount = tx.getAmount(); // exact getter
            firstFourAmounts.add(amount);

            int n = counter.incrementAndGet();
            log.info("Captured amount #{} = {}", n, amount);

            if (n == 4) {
                // Printed to console so you can read it from test output
                System.out.println("FIRST_FOUR_AMOUNTS: " + firstFourAmounts);
                log.info("First four transaction amounts: {}", firstFourAmounts);
            }
        }
    }
}
