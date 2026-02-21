import { buildModule } from "@nomicfoundation/hardhat-ignition/modules";

const RwaModule = buildModule("RwaModule", (m) => {
  const envBaseURI = process.env.PROPERTY_SHARE_BASE_URI?.trim();
  const baseURI = m.getParameter("baseURI", envBaseURI && envBaseURI.length > 0 ? envBaseURI : "ipfs://rwa/property-share");

  const mockUsd = m.contract("MockUSD", [], { id: "MockUSD" });
  const registry = m.contract("PropertyRegistry", [], { id: "PropertyRegistry", after: [mockUsd] });
  const share = m.contract("PropertyShare1155", [baseURI], {
    id: "PropertyShare1155",
    after: [registry]
  });
  const tokenizer = m.contract("PropertyTokenizer", [registry, share], {
    id: "PropertyTokenizer",
    after: [share]
  });
  const market = m.contract("FixedPriceMarketDvP", [], {
    id: "FixedPriceMarketDvP",
    after: [tokenizer]
  });

  return {
    mockUsd,
    registry,
    share,
    tokenizer,
    market
  };
});

export default RwaModule;
