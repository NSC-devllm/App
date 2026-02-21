package io.rwa.server.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "rwa")
public class RwaProperties {

    private String sharedDirPath;
    private String giwaRpcUrl;
    private long giwaChainId;
    private String masterKeyBase64;
    private String issuerPrivateKey;
    private String treasuryPrivateKey;
    private String adminApiToken;
    private long nonceLockTimeoutMs = 5000L;
    private final Tx tx = new Tx();
    private final Outbox outbox = new Outbox();

    public String getSharedDirPath() {
        return sharedDirPath;
    }

    public void setSharedDirPath(String sharedDirPath) {
        this.sharedDirPath = sharedDirPath;
    }

    public String getGiwaRpcUrl() {
        return giwaRpcUrl;
    }

    public void setGiwaRpcUrl(String giwaRpcUrl) {
        this.giwaRpcUrl = giwaRpcUrl;
    }

    public long getGiwaChainId() {
        return giwaChainId;
    }

    public void setGiwaChainId(long giwaChainId) {
        this.giwaChainId = giwaChainId;
    }

    public String getMasterKeyBase64() {
        return masterKeyBase64;
    }

    public void setMasterKeyBase64(String masterKeyBase64) {
        this.masterKeyBase64 = masterKeyBase64;
    }

    public String getIssuerPrivateKey() {
        return issuerPrivateKey;
    }

    public void setIssuerPrivateKey(String issuerPrivateKey) {
        this.issuerPrivateKey = issuerPrivateKey;
    }

    public String getTreasuryPrivateKey() {
        return treasuryPrivateKey;
    }

    public void setTreasuryPrivateKey(String treasuryPrivateKey) {
        this.treasuryPrivateKey = treasuryPrivateKey;
    }

    public String getAdminApiToken() {
        return adminApiToken;
    }

    public void setAdminApiToken(String adminApiToken) {
        this.adminApiToken = adminApiToken;
    }

    public long getNonceLockTimeoutMs() {
        return nonceLockTimeoutMs;
    }

    public void setNonceLockTimeoutMs(long nonceLockTimeoutMs) {
        this.nonceLockTimeoutMs = nonceLockTimeoutMs;
    }

    public Tx getTx() {
        return tx;
    }

    public Outbox getOutbox() {
        return outbox;
    }

    public static class Tx {
        private long confirmations = 12L;
        private long pollingIntervalMs = 5000L;
        private long defaultGasLimit = 800000L;

        public long getConfirmations() {
            return confirmations;
        }

        public void setConfirmations(long confirmations) {
            this.confirmations = confirmations;
        }

        public long getPollingIntervalMs() {
            return pollingIntervalMs;
        }

        public void setPollingIntervalMs(long pollingIntervalMs) {
            this.pollingIntervalMs = pollingIntervalMs;
        }

        public long getDefaultGasLimit() {
            return defaultGasLimit;
        }

        public void setDefaultGasLimit(long defaultGasLimit) {
            this.defaultGasLimit = defaultGasLimit;
        }
    }

    public static class Outbox {
        private boolean kafkaEnabled = true;
        private String defaultTopic = "server.domain.events";
        private int retryMinAgeMinutes = 10;
        private int maxAttempts = 30;
        private long retryIntervalMs = 60000L;
        private int lockTtlSeconds = 300;
        private int claimBatchSize = 100;
        private int backoffSeconds = 60;
        private final PublisherExecutor publisherExecutor = new PublisherExecutor();

        public boolean isKafkaEnabled() {
            return kafkaEnabled;
        }

        public void setKafkaEnabled(boolean kafkaEnabled) {
            this.kafkaEnabled = kafkaEnabled;
        }

        public String getDefaultTopic() {
            return defaultTopic;
        }

        public void setDefaultTopic(String defaultTopic) {
            this.defaultTopic = defaultTopic;
        }

        public int getRetryMinAgeMinutes() {
            return retryMinAgeMinutes;
        }

        public void setRetryMinAgeMinutes(int retryMinAgeMinutes) {
            this.retryMinAgeMinutes = retryMinAgeMinutes;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getRetryIntervalMs() {
            return retryIntervalMs;
        }

        public void setRetryIntervalMs(long retryIntervalMs) {
            this.retryIntervalMs = retryIntervalMs;
        }

        public int getLockTtlSeconds() {
            return lockTtlSeconds;
        }

        public void setLockTtlSeconds(int lockTtlSeconds) {
            this.lockTtlSeconds = lockTtlSeconds;
        }

        public int getClaimBatchSize() {
            return claimBatchSize;
        }

        public void setClaimBatchSize(int claimBatchSize) {
            this.claimBatchSize = claimBatchSize;
        }

        public int getBackoffSeconds() {
            return backoffSeconds;
        }

        public void setBackoffSeconds(int backoffSeconds) {
            this.backoffSeconds = backoffSeconds;
        }

        public PublisherExecutor getPublisherExecutor() {
            return publisherExecutor;
        }
    }

    public static class PublisherExecutor {
        private int corePoolSize = 4;
        private int maxPoolSize = 8;
        private int queueCapacity = 200;

        public int getCorePoolSize() {
            return corePoolSize;
        }

        public void setCorePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }

        public int getMaxPoolSize() {
            return maxPoolSize;
        }

        public void setMaxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }
    }
}
