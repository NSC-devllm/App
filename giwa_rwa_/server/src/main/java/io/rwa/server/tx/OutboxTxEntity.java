package io.rwa.server.tx;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_tx")
public class OutboxTxEntity {

    @Id
    @Column(name = "outbox_id", nullable = false)
    private UUID outboxId;

    @Column(name = "request_id", nullable = false, unique = true)
    private String requestId;

    @Column(name = "from_address", nullable = false)
    private String fromAddress;

    @Column(name = "to_address")
    private String toAddress;

    @Column(name = "nonce")
    private Long nonce;

    @Column(name = "tx_hash")
    private String txHash;

    @Column(name = "raw_tx")
    private String rawTx;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "tx_type", nullable = false)
    private String txType;

    @Column(name = "payload", columnDefinition = "jsonb")
    private String payload;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public UUID getOutboxId() {
        return outboxId;
    }

    public void setOutboxId(UUID outboxId) {
        this.outboxId = outboxId;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getFromAddress() {
        return fromAddress;
    }

    public void setFromAddress(String fromAddress) {
        this.fromAddress = fromAddress;
    }

    public String getToAddress() {
        return toAddress;
    }

    public void setToAddress(String toAddress) {
        this.toAddress = toAddress;
    }

    public Long getNonce() {
        return nonce;
    }

    public void setNonce(Long nonce) {
        this.nonce = nonce;
    }

    public String getTxHash() {
        return txHash;
    }

    public void setTxHash(String txHash) {
        this.txHash = txHash;
    }

    public String getRawTx() {
        return rawTx;
    }

    public void setRawTx(String rawTx) {
        this.rawTx = rawTx;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTxType() {
        return txType;
    }

    public void setTxType(String txType) {
        this.txType = txType;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
