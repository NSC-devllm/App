package io.rwa.server.publicdata;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "complexes")
public class ComplexEntity {

    @Id
    @Column(name = "kapt_code", nullable = false)
    private String kaptCode;

    @Column(name = "kapt_name")
    private String kaptName;

    @Column(name = "kapt_addr")
    private String kaptAddr;

    @Column(name = "doro_juso")
    private String doroJuso;

    @Column(name = "ho_cnt")
    private Integer hoCnt;

    @Column(name = "raw_json", columnDefinition = "jsonb", nullable = false)
    private String rawJson;

    @Column(name = "fetched_at", nullable = false)
    private Instant fetchedAt;

    public String getKaptCode() {
        return kaptCode;
    }

    public void setKaptCode(String kaptCode) {
        this.kaptCode = kaptCode;
    }

    public String getKaptName() {
        return kaptName;
    }

    public void setKaptName(String kaptName) {
        this.kaptName = kaptName;
    }

    public String getKaptAddr() {
        return kaptAddr;
    }

    public void setKaptAddr(String kaptAddr) {
        this.kaptAddr = kaptAddr;
    }

    public String getDoroJuso() {
        return doroJuso;
    }

    public void setDoroJuso(String doroJuso) {
        this.doroJuso = doroJuso;
    }

    public Integer getHoCnt() {
        return hoCnt;
    }

    public void setHoCnt(Integer hoCnt) {
        this.hoCnt = hoCnt;
    }

    public String getRawJson() {
        return rawJson;
    }

    public void setRawJson(String rawJson) {
        this.rawJson = rawJson;
    }

    public Instant getFetchedAt() {
        return fetchedAt;
    }

    public void setFetchedAt(Instant fetchedAt) {
        this.fetchedAt = fetchedAt;
    }
}
