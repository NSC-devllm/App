package io.rwa.server.publicdata;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigInteger;

@Entity
@Table(name = "units")
public class UnitEntity {

    @Id
    @Column(name = "unit_id", nullable = false)
    private String unitId;

    @Column(name = "class_id", nullable = false)
    private String classId;

    @Column(name = "unit_no", nullable = false)
    private int unitNo;

    @Column(name = "token_id", precision = 78, scale = 0)
    private BigInteger tokenId;

    @Column(name = "status", nullable = false)
    private String status;

    public String getUnitId() {
        return unitId;
    }

    public void setUnitId(String unitId) {
        this.unitId = unitId;
    }

    public String getClassId() {
        return classId;
    }

    public void setClassId(String classId) {
        this.classId = classId;
    }

    public int getUnitNo() {
        return unitNo;
    }

    public void setUnitNo(int unitNo) {
        this.unitNo = unitNo;
    }

    public BigInteger getTokenId() {
        return tokenId;
    }

    public void setTokenId(BigInteger tokenId) {
        this.tokenId = tokenId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
