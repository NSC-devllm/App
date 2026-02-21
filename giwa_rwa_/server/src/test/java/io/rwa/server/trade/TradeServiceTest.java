package io.rwa.server.trade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import io.rwa.server.config.SharedConstantsLoader;
import io.rwa.server.publicdata.UnitRepository;
import io.rwa.server.query.ReadModelQueryService;
import io.rwa.server.tx.OutboxTxEntity;
import io.rwa.server.tx.TxOrchestratorService;
import io.rwa.server.wallet.WalletService;
import io.rwa.server.web3.ContractGatewayService;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class TradeServiceTest {

    @Mock
    private WalletService walletService;

    @Mock
    private UnitRepository unitRepository;

    @Mock
    private ReadModelQueryService readModelQueryService;

    @Mock
    private ContractGatewayService contractGatewayService;

    @Mock
    private TxOrchestratorService txOrchestratorService;

    @Mock
    private SharedConstantsLoader sharedConstantsLoader;

    private TradeService tradeService;

    @BeforeEach
    void setUp() {
        tradeService = new TradeService(
            walletService,
            unitRepository,
            readModelQueryService,
            contractGatewayService,
            txOrchestratorService,
            sharedConstantsLoader,
            new ObjectMapper()
        );
    }

    @Test
    @DisplayName("list는 seller 승인 미완료 시 승인 tx 후 list tx를 생성한다")
    void listShouldSubmitApprovalAndListTxWhenSellerHasNoApproval() {
        // given: seller가 아직 market approval을 주지 않은 list 요청 상황을 준비한다.
        UUID sellerUserId = UUID.randomUUID();
        TradeListRequest request = new TradeListRequest(
            sellerUserId,
            "101",
            null,
            BigInteger.valueOf(3),
            BigInteger.valueOf(12)
        );
        String idempotencyKey = "list-key";

        when(walletService.getAddress(sellerUserId)).thenReturn("0xseller");
        when(walletService.decryptUserPrivateKey(sellerUserId)).thenReturn("seller-priv");
        when(contractGatewayService.marketAddress()).thenReturn("0xmarket");
        when(contractGatewayService.propertyShareAddress()).thenReturn("0xshare");
        when(contractGatewayService.isApprovedForAll("0xseller", "0xmarket")).thenReturn(false);

        OutboxTxEntity approvalTx = outboxTx("d6c86809-7fd4-4ffd-ac0d-9af79f3768f6");
        OutboxTxEntity listTx = outboxTx("4f9faaf2-e4a3-4877-8ca0-8d31e6308623");
        when(txOrchestratorService.submitContractTx(anyString(), anyString(), anyString(), anyString(), any(), anyString(), any()))
            .thenReturn(approvalTx, listTx);

        // when: list 거래 오케스트레이션을 실행한다.
        TradeResult result = tradeService.list(request, idempotencyKey);

        // then: approval tx 후 list tx가 생성되고 결과 값이 요청과 일치한다.
        assertThat(result.outboxIds()).containsExactly(approvalTx.getOutboxId(), listTx.getOutboxId());
        assertThat(result.tokenId()).isEqualTo(new BigInteger("101"));
        assertThat(result.amount()).isEqualTo(request.amount());
        assertThat(result.unitPrice()).isEqualTo(request.unitPrice());

        ArgumentCaptor<String> txTypeCaptor = ArgumentCaptor.forClass(String.class);
        verify(txOrchestratorService, times(2)).submitContractTx(
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            any(),
            txTypeCaptor.capture(),
            any()
        );
        assertThat(txTypeCaptor.getAllValues()).containsExactly("TRADE_SET_APPROVAL_FOR_ALL", "TRADE_LIST");
    }

    @Test
    @DisplayName("list는 tokenId와 unitId가 모두 없으면 400 예외를 던진다")
    void listShouldFailWhenTokenIdAndUnitIdAreBothMissing() {
        // given: tokenId/unitId가 모두 없는 잘못된 list 요청을 준비한다.
        UUID sellerUserId = UUID.randomUUID();
        TradeListRequest request = new TradeListRequest(
            sellerUserId,
            null,
            null,
            BigInteger.ONE,
            BigInteger.ONE
        );
        when(walletService.getAddress(sellerUserId)).thenReturn("0xseller");
        when(walletService.decryptUserPrivateKey(sellerUserId)).thenReturn("seller-priv");

        // when: list 요청을 처리한다.
        // then: BAD_REQUEST 예외로 필수 식별자 누락을 반환한다.
        assertThatThrownBy(() -> tradeService.list(request, "missing-token"))
            .isInstanceOfSatisfying(ApiException.class, ex -> {
                assertThat(ex.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
                assertThat(ex.getMessage()).contains("tokenId or unitId is required");
            });
    }

    @Test
    @DisplayName("buy는 체인 listing 상태가 ACTIVE가 아니면 409 예외를 던진다")
    void buyShouldThrowConflictWhenChainListingIsNotActive() {
        // given: 체인상의 listing 상태가 ACTIVE가 아닌 buy 요청을 준비한다.
        UUID buyerUserId = UUID.randomUUID();
        TradeBuyRequest request = new TradeBuyRequest(
            buyerUserId,
            BigInteger.valueOf(99),
            BigInteger.valueOf(2)
        );
        when(walletService.getAddress(buyerUserId)).thenReturn("0xbuyer");
        when(walletService.decryptUserPrivateKey(buyerUserId)).thenReturn("buyer-priv");
        when(readModelQueryService.listingById("99")).thenReturn(Map.of("listing_status", "ACTIVE", "price", "15"));
        when(contractGatewayService.getMarketListing(BigInteger.valueOf(99))).thenReturn(
            new ContractGatewayService.MarketListing(
                "0xseller",
                "0xshare",
                BigInteger.TEN,
                "0xmusd",
                BigInteger.valueOf(15),
                BigInteger.valueOf(10),
                BigInteger.valueOf(10),
                2
            )
        );

        // when: buy 요청을 처리한다.
        // then: listing 비활성 상태로 CONFLICT 예외가 발생한다.
        assertThatThrownBy(() -> tradeService.buy(request, "buy-inactive"))
            .isInstanceOfSatisfying(ApiException.class, ex -> {
                assertThat(ex.getStatus()).isEqualTo(HttpStatus.CONFLICT);
                assertThat(ex.getMessage()).contains("Listing is not ACTIVE");
            });
    }

    @Test
    @DisplayName("buy는 allowance 부족 시 approve tx와 buy tx를 순서대로 생성한다")
    void buyShouldSubmitApproveAndBuyWhenAllowanceIsInsufficient() {
        // given: allowance가 부족한 ACTIVE listing buy 시나리오를 준비한다.
        UUID buyerUserId = UUID.randomUUID();
        BigInteger listingId = BigInteger.valueOf(44);
        BigInteger amount = BigInteger.valueOf(3);
        TradeBuyRequest request = new TradeBuyRequest(buyerUserId, listingId, amount);

        when(walletService.getAddress(buyerUserId)).thenReturn("0xbuyer");
        when(walletService.decryptUserPrivateKey(buyerUserId)).thenReturn("buyer-priv");
        when(readModelQueryService.listingById("44")).thenReturn(Map.of(
            "listing_status", "ACTIVE",
            "price", new BigDecimal("2")
        ));

        when(contractGatewayService.marketAddress()).thenReturn("0xmarket");
        when(contractGatewayService.mockUsdAddress()).thenReturn("0xmusd");
        when(contractGatewayService.propertyShareAddress()).thenReturn("0xshare");
        when(contractGatewayService.getMarketListing(listingId)).thenReturn(
            new ContractGatewayService.MarketListing(
                "0xseller",
                "0xshare",
                BigInteger.valueOf(501),
                "0xmusd",
                BigInteger.valueOf(9999),
                BigInteger.valueOf(100),
                BigInteger.valueOf(100),
                1
            )
        );
        when(sharedConstantsLoader.getShareScale()).thenReturn(BigInteger.ONE);
        when(contractGatewayService.allowance("0xbuyer", "0xmarket")).thenReturn(BigInteger.ZERO);

        OutboxTxEntity approveTx = outboxTx("0dc73698-950f-478a-a99f-c80d50206f06");
        OutboxTxEntity buyTx = outboxTx("62e2398e-5330-4ece-a62e-f83ac09833f0");
        when(txOrchestratorService.submitContractTx(anyString(), anyString(), anyString(), anyString(), any(), anyString(), any()))
            .thenReturn(approveTx, buyTx);

        // when: buy 거래 오케스트레이션을 실행한다.
        TradeResult result = tradeService.buy(request, "buy-key");

        // then: approve tx와 buy tx가 순서대로 생성되고 비용 계산값이 검증된다.
        assertThat(result.outboxIds()).containsExactly(approveTx.getOutboxId(), buyTx.getOutboxId());
        assertThat(result.listingId()).isEqualTo(listingId);
        assertThat(result.tokenId()).isEqualTo(BigInteger.valueOf(501));
        assertThat(result.unitPrice()).isEqualTo(BigInteger.valueOf(2));
        assertThat(result.cost()).isEqualTo(BigInteger.valueOf(6));

        ArgumentCaptor<String> txTypeCaptor = ArgumentCaptor.forClass(String.class);
        verify(txOrchestratorService, times(2)).submitContractTx(
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            any(),
            txTypeCaptor.capture(),
            any()
        );
        assertThat(txTypeCaptor.getAllValues()).containsExactly("TRADE_BUY_APPROVE", "TRADE_BUY");

        verify(contractGatewayService).allowance(eq("0xbuyer"), eq("0xmarket"));
    }

    private OutboxTxEntity outboxTx(String uuid) {
        OutboxTxEntity entity = new OutboxTxEntity();
        entity.setOutboxId(UUID.fromString(uuid));
        return entity;
    }
}
