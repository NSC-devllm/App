graph TD
    %% --- 스타일 정의 (색상 테마) ---
    classDef orange fill:#fff2cc,stroke:#d6b656,stroke-width:2px;
    classDef green fill:#d5e8d4,stroke:#82b366,stroke-width:2px;
    classDef blue fill:#dae8fc,stroke:#6c8ebf,stroke-width:2px;
    classDef plain fill:#ffffff,stroke:#333333,stroke-width:1px;
    classDef dashed fill:none,stroke:#666,stroke-dasharray: 5 5;

    %% 1. Public Internet Zone
    subgraph PublicZone ["Public Internet Zone"]
        direction TB
        UserClient[User Client]:::orange
        AdminOps[Admin / Ops]:::orange
    end

    %% 2. Azure Cloud - Single VNet
    subgraph AzureCloud ["Azure Cloud - Single VNet"]
        direction TB
        style AzureCloud fill:#f9f9f9,stroke:#666,stroke-width:1px

        %% Subnet: Perimeter
        subgraph SubnetPerimeter ["Subnet: Perimeter"]
            style SubnetPerimeter fill:#d5e8d4,stroke:#82b366
            AppGw[App Gateway + WAF]:::green
        end

        %% Subnet: Ops
        subgraph SubnetOps ["Subnet: Ops"]
            style SubnetOps fill:#dae8fc,stroke:#6c8ebf
            Bastion[Bastion]:::blue
        end

        %% Subnet: App - AKS
        subgraph SubnetApp ["Subnet: App - AKS"]
            style SubnetApp fill:#d5e8d4,stroke:#82b366
            
            subgraph PythonGroup ["Python - FastAPI"]
                style PythonGroup fill:#fff,stroke:#666
                CryptoSvc[Crypto Svc]:::plain
            end
            
            subgraph JavaGroup ["Java Spring Boot"]
                style JavaGroup fill:#fff,stroke:#666
                AccountSvc[Account Svc]:::plain
                CommerceSvc[Commerce Svc]:::plain
            end
        end

        %% Subnet: Messaging
        subgraph SubnetMessaging ["Subnet: Messaging"]
            style SubnetMessaging fill:#dae8fc,stroke:#6c8ebf
            EventHubs[Event Hubs (Kafka)]:::blue
        end

        %% Subnet: Data
        subgraph SubnetData ["Subnet: Data"]
            style SubnetData fill:#d5e8d4,stroke:#82b366
            ConfLedger[Confidential Ledger]:::green
            SQLDB[SQL Database]:::green
            PostgreSQL[PostgreSQL]:::green
        end

        %% Subnet: Egress
        subgraph SubnetEgress ["Subnet: Egress"]
            style SubnetEgress fill:#d5e8d4,stroke:#82b366
            Firewall[Firewall]:::green
        end

        %% Subnet: Analytics
        subgraph SubnetAnalytics ["Subnet: Analytics"]
            style SubnetAnalytics fill:#d5e8d4,stroke:#82b366
            Databricks[Databricks]:::blue
            ADLS[ADLS Gen2]:::green
        end

        %% Subnet: Security
        subgraph SubnetSecurity ["Subnet: Security"]
            style SubnetSecurity fill:#d5e8d4,stroke:#82b366
            ACR[Container Registry]:::blue
            KeyVault[Key Vault]:::green
            DNS[Private DNS Zone]:::blue
        end
    end

    %% 3. Monitoring & External
    subgraph Monitoring ["Monitoring - PaaS"]
        style Monitoring fill:#dae8fc,stroke:#6c8ebf
        AppInsights[App Insights]:::plain
        LogAnalytics[Log Analytics]:::plain
    end

    ExternalAPI[External API]:::orange

    %% --- 연결선 (Flows) ---

    %% Ingress & Access
    UserClient -->|HTTPS| AppGw
    AdminOps -->|SSH| Bastion
    
    %% App Gateway Routing
    AppGw -->|Crypto| CryptoSvc
    AppGw -->|Account| AccountSvc
    AppGw -->|Commerce| CommerceSvc

    %% Bastion Access
    Bastion -.->|Access| CryptoSvc
    Bastion -.->|Access| AccountSvc
    Bastion -.->|Access| CommerceSvc

    %% Service to Service (Logic)
    CryptoSvc -->|REST API| AccountSvc

    %% Service -> Messaging (Publish)
    CryptoSvc -->|Publish| EventHubs
    AccountSvc -->|Publish| EventHubs
    CommerceSvc -->|Publish| EventHubs

    %% Messaging -> Service (Subscribe)
    EventHubs -.->|Subscribe| CryptoSvc
    EventHubs -.->|Subscribe| AccountSvc
    EventHubs -.->|Subscribe| CommerceSvc

    %% Service -> Data
    CryptoSvc -->|REST API| ConfLedger
    AccountSvc -->|Private EP| SQLDB
    CommerceSvc -->|Private EP| PostgreSQL

    %% Service -> Egress
    CryptoSvc -->|Outbound| Firewall
    AccountSvc -->|Outbound| Firewall
    CommerceSvc -->|Outbound| Firewall
    Firewall -->|Filtered| ExternalAPI

    %% Service -> Security (Secrets & Pull)
    CryptoSvc -.->|Pull| ACR
    AccountSvc -.->|Pull| ACR
    CommerceSvc -.->|Pull| ACR
    
    CryptoSvc -.->|Secrets| KeyVault
    AccountSvc -.->|Secrets| KeyVault
    CommerceSvc -.->|Secrets| KeyVault

    %% Analytics Flow
    EventHubs -->|Streaming| Databricks
    SQLDB -->|CDC| Databricks
    Databricks -->|R/W| ADLS
    Databricks -.->|Secrets| KeyVault

    %% Logs & Monitoring (Dotted Lines)
    AppGw -.->|WAF Logs| LogAnalytics
    AppGw -.->|Logs| AppInsights
    Firewall -.->|Net Logs| LogAnalytics
    Databricks -.->|Job Logs| LogAnalytics
    ExternalAPI -.->|Logs| LogAnalytics