package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class DatabaseConduit {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConduit.class);
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveService incentiveService;

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository,
            IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveService = incentiveService;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            Incentive incentive = incentiveService.getIncentive(transaction);
            float incentiveAmount = incentive.getAmount();

            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            userRepository.save(sender);
            userRepository.save(recipient);

            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(),
                    incentiveAmount);
            transactionRepository.save(record);

            if (sender.getName().equals("wilbur")) {
                logger.info("wilbur balance: {}", sender.getBalance());
            }
            if (recipient.getName().equals("wilbur")) {
                logger.info("wilbur balance: {}", recipient.getBalance());
            }
        }
    }

}
