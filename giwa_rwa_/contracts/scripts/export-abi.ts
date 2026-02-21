import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { artifacts } from "hardhat";

const CONTRACTS = [
  "MockUSD",
  "PropertyRegistry",
  "PropertyShare1155",
  "PropertyTokenizer",
  "FixedPriceMarketDvP"
];

async function main(): Promise<void> {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const outputDir = path.resolve(__dirname, "..", "..", "shared", "abi");
  fs.mkdirSync(outputDir, { recursive: true });

  const legacyKycAbiPath = path.join(outputDir, "KYCRegistry.json");
  if (fs.existsSync(legacyKycAbiPath)) {
    fs.rmSync(legacyKycAbiPath);
    console.log(`Removed legacy ABI: ${legacyKycAbiPath}`);
  }

  for (const contractName of CONTRACTS) {
    const artifact = await artifacts.readArtifact(contractName);
    const payload = {
      contractName,
      abi: artifact.abi
    };

    const outputPath = path.join(outputDir, `${contractName}.json`);
    fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2));
    console.log(`Wrote ABI: ${outputPath}`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
