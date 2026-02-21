package io.rwa.server.wallet;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "wallets")
public class WalletEntity {

    @Id
    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "address", nullable = false, unique = true, length = 42)
    private String address;

    @Lob
    @Column(name = "encrypted_privkey", nullable = false)
    private byte[] encryptedPrivkey;

    @Column(name = "enc_version", nullable = false)
    private int encVersion;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public byte[] getEncryptedPrivkey() {
        return encryptedPrivkey;
    }

    public void setEncryptedPrivkey(byte[] encryptedPrivkey) {
        this.encryptedPrivkey = encryptedPrivkey;
    }

    public int getEncVersion() {
        return encVersion;
    }

    public void setEncVersion(int encVersion) {
        this.encVersion = encVersion;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
}
