import { expect } from "chai";
import { network } from "hardhat";

describe("RWA suite", () => {
  async function deployCore() {
    const { ethers } = await network.connect();
    const [deployer, issuer, treasury, seller, buyer, buyer2] = await ethers.getSigners();

    const usd = await ethers.deployContract("MockUSD");
    await usd.waitForDeployment();

    const registry = await ethers.deployContract("PropertyRegistry");
    await registry.waitForDeployment();

    const share = await ethers.deployContract("PropertyShare1155", ["ipfs://rwa/property-share"]);
    await share.waitForDeployment();

    const tokenizer = await ethers.deployContract("PropertyTokenizer", [
      await registry.getAddress(),
      await share.getAddress()
    ]);
    await tokenizer.waitForDeployment();

    const market = await ethers.deployContract("FixedPriceMarketDvP");
    await market.waitForDeployment();

    return {
      ethers,
      deployer,
      issuer,
      treasury,
      seller,
      buyer,
      buyer2,
      usd,
      registry,
      share,
      tokenizer,
      market
    };
  }

  describe("Off-chain compliance model", () => {
    it("allows listing and buying without on-chain allowlist checks", async () => {
      const { ethers, share, usd, market, seller, buyer } = await deployCore();

      const shareScale = ethers.parseEther("1");
      const tokenId = 1001n;
      const unitPrice = ethers.parseEther("10");

      await share.mintBatch(seller.address, [tokenId], [shareScale]);
      await share.connect(seller).setApprovalForAll(await market.getAddress(), true);

      await market
        .connect(seller)
        .list(await share.getAddress(), tokenId, await usd.getAddress(), shareScale, unitPrice);

      await usd.mint(buyer.address, ethers.parseEther("100"));
      await usd.connect(buyer).approve(await market.getAddress(), ethers.parseEther("100"));
      await market.connect(buyer).buy(1n, shareScale);

      expect(await share.balanceOf(buyer.address, tokenId)).to.equal(shareScale);
      expect(await usd.balanceOf(seller.address)).to.equal(unitPrice);
    });

    it("allows direct wallet-to-wallet transfers", async () => {
      const { ethers, share, seller, buyer } = await deployCore();

      const shareScale = ethers.parseEther("1");
      const tokenId = 1002n;

      await share.mintBatch(seller.address, [tokenId], [shareScale]);
      await share.connect(seller).safeTransferFrom(seller.address, buyer.address, tokenId, shareScale, "0x");

      expect(await share.balanceOf(seller.address, tokenId)).to.equal(0n);
      expect(await share.balanceOf(buyer.address, tokenId)).to.equal(shareScale);
    });
  });

  describe("DvP atomicity", () => {
    it("buy reverts on insufficient ERC20 allowance and transfers nothing", async () => {
      const { ethers, share, usd, market, seller, buyer } = await deployCore();

      const shareScale = ethers.parseEther("1");
      const tokenId = 3003n;
      const listAmount = 2n * shareScale;
      const unitPrice = ethers.parseEther("50");
      const buyAmount = shareScale;
      const expectedCost = ethers.parseEther("50");

      await share.mintBatch(seller.address, [tokenId], [listAmount]);
      await share.connect(seller).setApprovalForAll(await market.getAddress(), true);
      await market.connect(seller).list(await share.getAddress(), tokenId, await usd.getAddress(), listAmount, unitPrice);

      await usd.mint(buyer.address, ethers.parseEther("200"));
      await usd.connect(buyer).approve(await market.getAddress(), expectedCost - 1n);

      await expect(market.connect(buyer).buy(1n, buyAmount)).to.be.revert(ethers);

      expect(await usd.balanceOf(seller.address)).to.equal(0n);
      expect(await usd.balanceOf(buyer.address)).to.equal(ethers.parseEther("200"));
      expect(await share.balanceOf(buyer.address, tokenId)).to.equal(0n);
      expect(await share.balanceOf(await market.getAddress(), tokenId)).to.equal(listAmount);

      const listing = await market.listings(1n);
      expect(listing.remainingAmount).to.equal(listAmount);
      expect(listing.status).to.equal(1n);
    });

    it("buy reverts when escrow is insufficient and no ERC20 transfer is persisted", async () => {
      const { ethers, deployer, share, usd, market, seller, buyer } = await deployCore();

      const shareScale = ethers.parseEther("1");
      const tokenId = 3004n;
      const listAmount = 2n * shareScale;
      const unitPrice = ethers.parseEther("25");

      await share.mintBatch(seller.address, [tokenId], [listAmount]);
      await share.connect(seller).setApprovalForAll(await market.getAddress(), true);
      await market.connect(seller).list(await share.getAddress(), tokenId, await usd.getAddress(), listAmount, unitPrice);

      await share.connect(deployer).burn(await market.getAddress(), tokenId, listAmount);

      await usd.mint(buyer.address, ethers.parseEther("100"));
      await usd.connect(buyer).approve(await market.getAddress(), ethers.parseEther("100"));

      await expect(market.connect(buyer).buy(1n, shareScale)).to.be.revert(ethers);

      expect(await usd.balanceOf(seller.address)).to.equal(0n);
      expect(await usd.balanceOf(buyer.address)).to.equal(ethers.parseEther("100"));
      expect(await share.balanceOf(buyer.address, tokenId)).to.equal(0n);
      expect(await share.balanceOf(await market.getAddress(), tokenId)).to.equal(0n);

      const listing = await market.listings(1n);
      expect(listing.remainingAmount).to.equal(listAmount);
      expect(listing.status).to.equal(1n);
    });
  });

  describe("Listing lifecycle", () => {
    it("supports partial fills and tracks remainingAmount", async () => {
      const { ethers, share, usd, market, seller, buyer, buyer2 } = await deployCore();

      const shareScale = ethers.parseEther("1");
      const tokenId = 4001n;
      const listAmount = 3n * shareScale;
      const unitPrice = ethers.parseEther("30");

      await share.mintBatch(seller.address, [tokenId], [listAmount]);
      await share.connect(seller).setApprovalForAll(await market.getAddress(), true);
      await market.connect(seller).list(await share.getAddress(), tokenId, await usd.getAddress(), listAmount, unitPrice);

      await usd.mint(buyer.address, ethers.parseEther("500"));
      await usd.mint(buyer2.address, ethers.parseEther("500"));
      await usd.connect(buyer).approve(await market.getAddress(), ethers.parseEther("500"));
      await usd.connect(buyer2).approve(await market.getAddress(), ethers.parseEther("500"));

      await market.connect(buyer).buy(1n, shareScale);

      let listing = await market.listings(1n);
      expect(listing.remainingAmount).to.equal(2n * shareScale);
      expect(listing.status).to.equal(1n);

      await market.connect(buyer2).buy(1n, 2n * shareScale);

      listing = await market.listings(1n);
      expect(listing.remainingAmount).to.equal(0n);
      expect(listing.status).to.equal(3n);

      expect(await usd.balanceOf(seller.address)).to.equal(ethers.parseEther("90"));
      expect(await share.balanceOf(buyer.address, tokenId)).to.equal(shareScale);
      expect(await share.balanceOf(buyer2.address, tokenId)).to.equal(2n * shareScale);
    });

    it("cancel returns remaining shares to seller", async () => {
      const { ethers, share, usd, market, seller, buyer } = await deployCore();

      const shareScale = ethers.parseEther("1");
      const tokenId = 4002n;
      const listAmount = 3n * shareScale;
      const unitPrice = ethers.parseEther("12");

      await share.mintBatch(seller.address, [tokenId], [listAmount]);
      await share.connect(seller).setApprovalForAll(await market.getAddress(), true);
      await market.connect(seller).list(await share.getAddress(), tokenId, await usd.getAddress(), listAmount, unitPrice);

      await usd.mint(buyer.address, ethers.parseEther("100"));
      await usd.connect(buyer).approve(await market.getAddress(), ethers.parseEther("100"));
      await market.connect(buyer).buy(1n, shareScale);

      await market.connect(seller).cancel(1n);

      const listing = await market.listings(1n);
      expect(listing.remainingAmount).to.equal(0n);
      expect(listing.status).to.equal(2n);

      expect(await share.balanceOf(seller.address, tokenId)).to.equal(2n * shareScale);
      expect(await share.balanceOf(await market.getAddress(), tokenId)).to.equal(0n);
    });
  });

  describe("Tokenizer", () => {
    it("allocates ranges and mintUnits mints expected token IDs", async () => {
      const { ethers, deployer, treasury, registry, share, tokenizer } = await deployCore();

      const shareScale = ethers.parseEther("1");
      const classA = ethers.id("CLASS_A");
      const classB = ethers.id("CLASS_B");

      await registry.connect(deployer).transferOwnership(await tokenizer.getAddress());
      await share.connect(deployer).transferOwnership(await tokenizer.getAddress());

      await expect(
        tokenizer.connect(deployer).reserveAndRegisterClass(classA, ethers.id("DOC_A"), 3, 1_700_000_000)
      )
        .to.emit(tokenizer, "ClassReserved")
        .withArgs(classA, 1n, 3n);

      await expect(
        tokenizer.connect(deployer).reserveAndRegisterClass(classB, ethers.id("DOC_B"), 2, 1_700_000_001)
      )
        .to.emit(tokenizer, "ClassReserved")
        .withArgs(classB, 4n, 2n);

      expect(await tokenizer.nextBaseTokenId()).to.equal(6n);

      await expect(tokenizer.connect(deployer).mintUnits(classA, 1, 2, treasury.address))
        .to.emit(tokenizer, "UnitsMinted")
        .withArgs(classA, 1n, 2n, treasury.address);

      expect(await share.balanceOf(treasury.address, 1n)).to.equal(0n);
      expect(await share.balanceOf(treasury.address, 2n)).to.equal(shareScale);
      expect(await share.balanceOf(treasury.address, 3n)).to.equal(shareScale);

      const classAInfo = await registry.getClass(classA);
      expect(classAInfo.baseTokenId).to.equal(1n);
      expect(classAInfo.unitCount).to.equal(3n);
      expect(classAInfo.status).to.equal(1n);
    });

    it("reverts when trying to mint an already-minted unit ID", async () => {
      const { ethers, deployer, treasury, registry, share, tokenizer } = await deployCore();

      const classA = ethers.id("CLASS_DUPLICATE_MINT");

      await registry.connect(deployer).transferOwnership(await tokenizer.getAddress());
      await share.connect(deployer).transferOwnership(await tokenizer.getAddress());

      await tokenizer
        .connect(deployer)
        .reserveAndRegisterClass(classA, ethers.id("DOC_DUPLICATE_MINT"), 2, 1_700_000_010);

      await tokenizer.connect(deployer).mintUnits(classA, 0, 1, treasury.address);

      await expect(tokenizer.connect(deployer).mintUnits(classA, 0, 1, treasury.address))
        .to.be.revertedWithCustomError(tokenizer, "TokenAlreadyMinted")
        .withArgs(1n);
    });
  });

  describe("PropertyRegistry", () => {
    it("rejects unknown status values", async () => {
      const { ethers, registry } = await deployCore();

      const classId = ethers.id("CLASS_STATUS_GUARD");
      await registry.registerClass(classId, ethers.id("DOC_STATUS_GUARD"), 1, 5000, 1_700_000_020);

      await expect(registry.setStatus(classId, 99))
        .to.be.revertedWithCustomError(registry, "InvalidStatus")
        .withArgs(99);
    });
  });
});
