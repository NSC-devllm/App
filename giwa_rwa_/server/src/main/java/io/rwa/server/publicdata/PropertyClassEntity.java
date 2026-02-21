package io.rwa.server.publicdata;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigInteger;
import java.time.Instant;

@Entity
@Table(name = "classes")
public class PropertyClassEntity {

    @Id
    @Column(name = "class_id", nullable = false)
    private String classId;

    @Column(name = "kapt_code", nullable = false)
    private String kaptCode;

    @Column(name = "class_key", nullable = false)
    private String classKey;

    @Column(name = "unit_count", nullable = false)
    private int unitCount;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "doc_hash")
    private String docHash;

    @Column(name = "base_token_id", precision = 78, scale = 0)
    private BigInteger baseTokenId;

    @Column(name = "issued_at")
    private Instant issuedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public String getClassId() {
        return classId;
    }

    public void setClassId(String classId) {
        this.classId = classId;
    }

    public String getKaptCode() {
        return kaptCode;
    }

    public void setKaptCode(String kaptCode) {
        this.kaptCode = kaptCode;
    }

    public String getClassKey() {
        return classKey;
    }

    public void setClassKey(String classKey) {
        this.classKey = classKey;
    }

    public int getUnitCount() {
        return unitCount;
    }

    public void setUnitCount(int unitCount) {
        this.unitCount = unitCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDocHash() {
        return docHash;
    }

    public void setDocHash(String docHash) {
        this.docHash = docHash;
    }

    public BigInteger getBaseTokenId() {
        return baseTokenId;
    }

    public void setBaseTokenId(BigInteger baseTokenId) {
        this.baseTokenId = baseTokenId;
    }

    public Instant getIssuedAt() {
        return issuedAt;
    }

    public void setIssuedAt(Instant issuedAt) {
        this.issuedAt = issuedAt;
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
