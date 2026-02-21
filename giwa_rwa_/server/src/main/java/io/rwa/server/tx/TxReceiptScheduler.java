package io.rwa.server.tx;

import io.rwa.server.config.SharedConstantsLoader;
import io.rwa.server.web3.Web3FunctionService;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class TxReceiptScheduler {

    private static final Logger log = LoggerFactory.getLogger(TxReceiptScheduler.class);

    private final OutboxTxRepository outboxTxRepository;
    private final Web3FunctionService web3FunctionService;
    private final SharedConstantsLoader sharedConstantsLoader;

    public TxReceiptScheduler(
        OutboxTxRepository outboxTxRepository,
        Web3FunctionService web3FunctionService,
        SharedConstantsLoader sharedConstantsLoader
    ) {
        this.outboxTxRepository = outboxTxRepository;
        this.web3FunctionService = web3FunctionService;
        this.sharedConstantsLoader = sharedConstantsLoader;
    }

    @Scheduled(fixedDelayString = "${rwa.tx.polling-interval-ms:5000}")
    public void updateTxStatuses() {
        List<OutboxTxEntity> sent = outboxTxRepository.findByStatus(OutboxTxStatus.SENT.name());
        if (sent.isEmpty()) {
            return;
        }

        BigInteger latestBlock = web3FunctionService.latestBlockNumber();
        int requiredConfirmations = sharedConstantsLoader.getConfirmations();

        for (OutboxTxEntity tx : sent) {
            if (tx.getTxHash() == null || tx.getTxHash().isBlank()) {
                continue;
            }
            try {
                web3FunctionService.getReceipt(tx.getTxHash()).ifPresent(receipt -> {
                    boolean success = "0x1".equalsIgnoreCase(receipt.getStatus()) || receipt.isStatusOK();
                    if (!success) {
                        tx.setStatus(OutboxTxStatus.FAILED.name());
                        tx.setLastError("Transaction reverted");
                        tx.setUpdatedAt(Instant.now());
                        outboxTxRepository.save(tx);
                        return;
                    }

                    BigInteger blockNumber = receipt.getBlockNumber();
                    BigInteger confirmations = latestBlock.subtract(blockNumber).add(BigInteger.ONE);
                    if (confirmations.compareTo(BigInteger.valueOf(requiredConfirmations)) >= 0) {
                        tx.setStatus(OutboxTxStatus.MINED.name());
                        tx.setUpdatedAt(Instant.now());
                        outboxTxRepository.save(tx);
                    }
                });
            } catch (Exception e) {
                log.warn("Failed to process receipt for {}", tx.getTxHash(), e);
            }
        }
    }
}
