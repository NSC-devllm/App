import { expect } from "chai";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { network } from "hardhat";

type TxLike = {
  hash: string;
  gasPrice?: bigint | null;
  wait: () => Promise<{
    gasUsed: bigint;
    gasPrice?: bigint | null;
  } | null>;
};

type GasEntry = {
  category: "deploy" | "tx";
  action: string;
  txHash: string;
  gasUsed: bigint;
  gasPriceWei: bigint;
  txFeeWei: bigint;
  sampleFeesWei: Record<string, bigint>;
};

describe("Gas report", () => {
  it("measures deployment + core function gas and writes logs", async () => {
    const connection = await network.connect();
    const { ethers, networkName } = connection;
    const [deployer, issuer, treasury, seller, buyer, buyer2] = await ethers.getSigners();
    const shareScale = ethers.parseEther("1");

    const sampleGasPricesGwei = ["0.1", "1", "5"];
    const sampleGasPricesWei = new Map<string, bigint>();
    const sampleTotalsWei: Record<string, bigint> = {};
    for (const gwei of sampleGasPricesGwei) {
      sampleGasPricesWei.set(gwei, ethers.parseUnits(gwei, "gwei"));
      sampleTotalsWei[gwei] = 0n;
    }

    const entries: GasEntry[] = [];

    const formatEth = (wei: bigint): string => {
      const raw = ethers.formatEther(wei);
      if (!raw.includes(".")) {
        return `${raw} ETH`;
      }

      const [whole, fraction] = raw.split(".");
      const compactFraction = fraction.slice(0, 8).replace(/0+$/, "");
      if (compactFraction.length === 0) {
        return `${whole} ETH`;
      }
      return `${whole}.${compactFraction} ETH`;
    };

    const recordTx = async (category: "deploy" | "tx", action: string, txPromise: Promise<TxLike>) => {
      const tx = await txPromise;
      const receipt = await tx.wait();
      if (!receipt) {
        throw new Error(`Missing receipt for ${action}`);
      }

      const gasUsed = receipt.gasUsed;
      const gasPriceWei = receipt.gasPrice ?? tx.gasPrice ?? 0n;
      const txFeeWei = gasUsed * gasPriceWei;

      const sampleFeesWei: Record<string, bigint> = {};
      for (const [gwei, priceWei] of sampleGasPricesWei.entries()) {
        const feeWei = gasUsed * priceWei;
        sampleFeesWei[gwei] = feeWei;
        sampleTotalsWei[gwei] += feeWei;
      }

      entries.push({
        category,
        action,
        txHash: tx.hash,
        gasUsed,
        gasPriceWei,
        txFeeWei,
        sampleFeesWei
      });
    };

    const deployAndTrack = async (name: string, args: unknown[] = []) => {
      const contract = await ethers.deployContract(name, args);
      const deploymentTx = contract.deploymentTransaction();
      if (!deploymentTx) {
        throw new Error(`Missing deployment transaction for ${name}`);
      }

      await recordTx("deploy", `Deploy ${name}`, Promise.resolve(deploymentTx as unknown as TxLike));
      await contract.waitForDeployment();
      return contract;
    };

    const usd = await deployAndTrack("MockUSD");
    const registry = await deployAndTrack("PropertyRegistry");
    const share = await deployAndTrack("PropertyShare1155", ["ipfs://rwa/property-share"]);
    const tokenizer = await deployAndTrack("PropertyTokenizer", [await registry.getAddress(), await share.getAddress()]);
    const market = await deployAndTrack("FixedPriceMarketDvP");

    const classDirect = ethers.id("CLASS_DIRECT");
    await recordTx(
      "tx",
      "PropertyRegistry.registerClass(classDirect,docHash,2,7001,issuedAt)",
      registry.registerClass(classDirect, ethers.id("DOC_DIRECT_V1"), 2, 7001, 1_700_000_000)
    );
    await recordTx("tx", "PropertyRegistry.setStatus(classDirect,PAUSED)", registry.setStatus(classDirect, 2));
    await recordTx(
      "tx",
      "PropertyRegistry.updateDocHash(classDirect,newDocHash)",
      registry.updateDocHash(classDirect, ethers.id("DOC_DIRECT_V2"))
    );

    const sellerTokenId = 9001n;
    const sellerMintAmount = 3n * shareScale;
    await recordTx("tx", "PropertyShare1155.mintBatch(seller,[id],[3*SHARE_SCALE])", share.mintBatch(seller.address, [sellerTokenId], [sellerMintAmount]));
    await recordTx(
      "tx",
      "PropertyShare1155.burn(seller,id,SHARE_SCALE)",
      share.connect(seller).burn(seller.address, sellerTokenId, shareScale)
    );
    await recordTx(
      "tx",
      "PropertyShare1155.setBaseURI(ipfs://updated/property-share)",
      share.setBaseURI("ipfs://updated/property-share")
    );

    await recordTx("tx", "MockUSD.mint(buyer,1000)", usd.mint(buyer.address, ethers.parseEther("1000")));

    await recordTx(
      "tx",
      "PropertyRegistry.transferOwnership(tokenizer)",
      registry.transferOwnership(await tokenizer.getAddress())
    );
    await recordTx("tx", "PropertyShare1155.transferOwnership(tokenizer)", share.transferOwnership(await tokenizer.getAddress()));

    const classTokenized = ethers.id("CLASS_TOKENIZED");
    await recordTx(
      "tx",
      "PropertyTokenizer.reserveAndRegisterClass(classTokenized,docHash,3,issuedAt)",
      tokenizer.reserveAndRegisterClass(classTokenized, ethers.id("DOC_TOKENIZED_V1"), 3, 1_700_000_001)
    );
    await recordTx(
      "tx",
      "PropertyTokenizer.mintUnits(classTokenized,0,3,treasury)",
      tokenizer.mintUnits(classTokenized, 0, 3, treasury.address)
    );

    await recordTx(
      "tx",
      "PropertyShare1155.setApprovalForAll(market,true) by seller",
      share.connect(seller).setApprovalForAll(await market.getAddress(), true)
    );
    await recordTx(
      "tx",
      "FixedPriceMarketDvP.list(share,id,usd,2*SHARE_SCALE,20)",
      market
        .connect(seller)
        .list(await share.getAddress(), sellerTokenId, await usd.getAddress(), 2n * shareScale, ethers.parseEther("20"))
    );
    await recordTx(
      "tx",
      "MockUSD.approve(market,1000) by buyer",
      usd.connect(buyer).approve(await market.getAddress(), ethers.parseEther("1000"))
    );
    await recordTx("tx", "FixedPriceMarketDvP.buy(listingId=1,SHARE_SCALE)", market.connect(buyer).buy(1n, shareScale));
    await recordTx("tx", "FixedPriceMarketDvP.cancel(listingId=1)", market.connect(seller).cancel(1n));

    expect(await share.balanceOf(treasury.address, 1n)).to.equal(shareScale);
    expect(await share.balanceOf(buyer.address, sellerTokenId)).to.equal(shareScale);
    expect((await market.listings(1n)).status).to.equal(2n);

    let totalGasUsed = 0n;
    let totalFeeWei = 0n;
    for (const entry of entries) {
      totalGasUsed += entry.gasUsed;
      totalFeeWei += entry.txFeeWei;
    }

    const reportEntries = entries.map((entry) => {
      const sampleFeesWeiAsString: Record<string, string> = {};
      const sampleFeesEth: Record<string, string> = {};
      for (const [gwei, feeWei] of Object.entries(entry.sampleFeesWei)) {
        sampleFeesWeiAsString[gwei] = feeWei.toString();
        sampleFeesEth[gwei] = formatEth(feeWei);
      }

      return {
        category: entry.category,
        action: entry.action,
        txHash: entry.txHash,
        gasUsed: entry.gasUsed.toString(),
        gasPriceWei: entry.gasPriceWei.toString(),
        txFeeWei: entry.txFeeWei.toString(),
        txFeeEth: formatEth(entry.txFeeWei),
        sampleFeesWei: sampleFeesWeiAsString,
        sampleFeesEth
      };
    });

    const totalSampleFeesWei: Record<string, string> = {};
    const totalSampleFeesEth: Record<string, string> = {};
    for (const [gwei, feeWei] of Object.entries(sampleTotalsWei)) {
      totalSampleFeesWei[gwei] = feeWei.toString();
      totalSampleFeesEth[gwei] = formatEth(feeWei);
    }

    const report = {
      generatedAt: new Date().toISOString(),
      network: networkName,
      note: "Local Hardhat gas profile. Real GIWA Sepolia fee can differ due to live base fee and L2/L1 data fee dynamics.",
      sampleGasPricesGwei,
      totals: {
        transactionCount: entries.length,
        totalGasUsed: totalGasUsed.toString(),
        totalFeeWei: totalFeeWei.toString(),
        totalFeeEth: formatEth(totalFeeWei),
        totalSampleFeesWei,
        totalSampleFeesEth
      },
      entries: reportEntries
    };

    const markdownLines: string[] = [];
    markdownLines.push("# Hardhat Local Gas Report");
    markdownLines.push("");
    markdownLines.push(`- Generated at: ${report.generatedAt}`);
    markdownLines.push(`- Network: ${report.network}`);
    markdownLines.push(`- Transactions measured: ${entries.length}`);
    markdownLines.push(`- Total gas used: ${totalGasUsed.toString()}`);
    markdownLines.push(`- Total fee (local gas price): ${formatEth(totalFeeWei)}`);
    markdownLines.push(`- ${report.note}`);
    markdownLines.push("");
    markdownLines.push("| Category | Action | Gas Used | Fee (local) | Fee @0.1 gwei | Fee @1 gwei | Fee @5 gwei |");
    markdownLines.push("| --- | --- | ---: | ---: | ---: | ---: | ---: |");
    for (const entry of entries) {
      markdownLines.push(
        `| ${entry.category} | ${entry.action} | ${entry.gasUsed.toString()} | ${formatEth(entry.txFeeWei)} | ${formatEth(entry.sampleFeesWei["0.1"])} | ${formatEth(entry.sampleFeesWei["1"])} | ${formatEth(entry.sampleFeesWei["5"])} |`
      );
    }
    markdownLines.push("");
    markdownLines.push("## Totals at Sample Gas Prices");
    markdownLines.push("");
    markdownLines.push("| Gas Price | Total Fee |");
    markdownLines.push("| --- | ---: |");
    for (const gwei of sampleGasPricesGwei) {
      markdownLines.push(`| ${gwei} gwei | ${formatEth(sampleTotalsWei[gwei])} |`);
    }
    markdownLines.push("");

    const reportDir = path.join(process.cwd(), "logs", "gas");
    await mkdir(reportDir, { recursive: true });

    const jsonPath = path.join(reportDir, "hardhat-gas-report.json");
    const markdownPath = path.join(reportDir, "hardhat-gas-report.md");

    await writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
    await writeFile(markdownPath, `${markdownLines.join("\n")}\n`, "utf8");

    expect(entries.length).to.be.greaterThan(0);
    console.log(`Gas report written to:\n- ${jsonPath}\n- ${markdownPath}`);
  });
});
