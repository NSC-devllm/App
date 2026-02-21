package io.rwa.server.tx;

import com.fasterxml.jackson.databind.JsonNode;
import io.rwa.server.common.ApiException;
import io.rwa.server.config.RwaProperties;
import io.rwa.server.web3.SendRawResult;
import io.rwa.server.web3.Web3FunctionService;
import jakarta.transaction.Transactional;
import java.math.BigInteger;
import java.time.Instant;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.web3j.abi.datatypes.Function;

@Service
public class TxOrchestratorService {

    private final OutboxTxRepository outboxTxRepository;
    private final NonceLockManager nonceLockManager;
    private final Web3FunctionService web3FunctionService;
    private final RwaProperties properties;

    public TxOrchestratorService(
        OutboxTxRepository outboxTxRepository,
        NonceLockManager nonceLockManager,
        Web3FunctionService web3FunctionService,
        RwaProperties properties
    ) {
        this.outboxTxRepository = outboxTxRepository;
        this.nonceLockManager = nonceLockManager;
        this.web3FunctionService = web3FunctionService;
        this.properties = properties;
    }

    @Transactional
    public OutboxTxEntity submitContractTx(
        String requestId,
        String fromAddress,
        String privateKeyHex,
        String toAddress,
        Function function,
        String txType,
        JsonNode payload
    ) {
        Optional<OutboxTxEntity> existing = outboxTxRepository.findByRequestId(requestId);
        if (existing.isPresent()) {
            return existing.get();
        }

        OutboxTxEntity tx = new OutboxTxEntity();
        tx.setOutboxId(UUID.randomUUID());
        tx.setRequestId(requestId);
        tx.setFromAddress(fromAddress.toLowerCase());
        tx.setToAddress(toAddress == null ? null : toAddress.toLowerCase());
        tx.setStatus(OutboxTxStatus.CREATED.name());
        tx.setTxType(txType);
        tx.setPayload(payload == null ? null : payload.toString());
        tx.setCreatedAt(Instant.now());
        tx.setUpdatedAt(Instant.now());
        outboxTxRepository.save(tx);

        return nonceLockManager.withAddressLock(fromAddress, () -> {
            try {
                BigInteger nonce = web3FunctionService.getPendingNonce(fromAddress);
                BigInteger gasPrice = web3FunctionService.getGasPrice();
                BigInteger gasLimit = BigInteger.valueOf(properties.getTx().getDefaultGasLimit());

                tx.setNonce(nonce.longValue());
                tx.setRawTx(web3FunctionService.signFunctionTransaction(
                    privateKeyHex,
                    toAddress,
                    function,
                    nonce,
                    gasPrice,
                    gasLimit
                ));
                tx.setStatus(OutboxTxStatus.SIGNED.name());
                tx.setUpdatedAt(Instant.now());
                outboxTxRepository.save(tx);

                SendRawResult sendResult = web3FunctionService.sendSignedRawTransaction(tx.getRawTx());
                tx.setTxHash(sendResult.txHash());
                tx.setStatus(OutboxTxStatus.SENT.name());
                tx.setUpdatedAt(Instant.now());
                outboxTxRepository.save(tx);
                return tx;
            } catch (Exception e) {
                tx.setStatus(OutboxTxStatus.FAILED.name());
                tx.setLastError(e.getMessage());
                tx.setUpdatedAt(Instant.now());
                outboxTxRepository.save(tx);
                if (e instanceof ApiException apiException) {
                    throw apiException;
                }
                throw new ApiException(HttpStatus.BAD_GATEWAY, "Failed to submit tx: " + e.getMessage());
            }
        });
    }

    public Optional<OutboxTxEntity> findByOutboxId(UUID outboxId) {
        return outboxTxRepository.findById(outboxId);
    }

    public Optional<OutboxTxEntity> findByHash(String txHash) {
        return outboxTxRepository.findByTxHash(txHash);
    }

    @Transactional
    public void markMined(UUID outboxId) {
        OutboxTxEntity tx = outboxTxRepository.findById(outboxId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Outbox tx not found"));
        tx.setStatus(OutboxTxStatus.MINED.name());
        tx.setUpdatedAt(Instant.now());
        outboxTxRepository.save(tx);
    }

    @Transactional
    public void markFailed(UUID outboxId, String error) {
        OutboxTxEntity tx = outboxTxRepository.findById(outboxId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Outbox tx not found"));
        tx.setStatus(OutboxTxStatus.FAILED.name());
        tx.setLastError(error);
        tx.setUpdatedAt(Instant.now());
        outboxTxRepository.save(tx);
    }

    public OutboxTxEntity waitForMined(UUID outboxId, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            OutboxTxEntity tx = findByOutboxId(outboxId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Outbox tx not found"));
            if (OutboxTxStatus.MINED.name().equals(tx.getStatus())) {
                return tx;
            }
            if (OutboxTxStatus.FAILED.name().equals(tx.getStatus())) {
                throw new ApiException(HttpStatus.BAD_GATEWAY, "Chain tx failed: " + tx.getLastError());
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Interrupted while waiting tx mining");
            }
        }
        throw new ApiException(HttpStatus.ACCEPTED, "Tx not mined yet");
    }
}
