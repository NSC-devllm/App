import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { network } from "hardhat";

type AddressBook = Record<string, string>;

function pickAddress(addresses: AddressBook, suffix: string): string {
  const entry = Object.entries(addresses).find(([key]) => key.endsWith(`#${suffix}`));
  if (!entry) {
    throw new Error(`Missing address for ${suffix} in Ignition deployment`);
  }
  return entry[1];
}

async function main(): Promise<void> {
  const connection = await network.connect();
  const { ethers } = connection;
  const { chainId } = await ethers.provider.getNetwork();
  const chainIdNumber = Number(chainId);
  const networkName = connection.networkName;
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const rootDir = path.resolve(__dirname, "..");

  const ignitionAddressesPath = path.join(
    rootDir,
    "ignition",
    "deployments",
    `chain-${chainIdNumber}`,
    "deployed_addresses.json"
  );

  if (!fs.existsSync(ignitionAddressesPath)) {
    throw new Error(`Ignition deployment not found: ${ignitionAddressesPath}`);
  }

  const rawAddresses = fs.readFileSync(ignitionAddressesPath, "utf8");
  const addresses: AddressBook = JSON.parse(rawAddresses);

  const deployment = {
    network: networkName,
    chainId: chainIdNumber,
    deployedAt: new Date().toISOString(),
    contracts: {
      MockUSD: pickAddress(addresses, "MockUSD"),
      PropertyRegistry: pickAddress(addresses, "PropertyRegistry"),
      PropertyShare1155: pickAddress(addresses, "PropertyShare1155"),
      PropertyTokenizer: pickAddress(addresses, "PropertyTokenizer"),
      FixedPriceMarketDvP: pickAddress(addresses, "FixedPriceMarketDvP")
    }
  };

  const outputFileName = networkName === "giwaSepolia" ? "giwa-sepolia.json" : `${networkName}.json`;

  const deploymentsDir = path.join(rootDir, "deployments");
  fs.mkdirSync(deploymentsDir, { recursive: true });

  const localOutputPath = path.join(deploymentsDir, outputFileName);
  fs.writeFileSync(localOutputPath, JSON.stringify(deployment, null, 2));

  const sharedOutputPath = path.resolve(rootDir, "..", "shared", "deployments", outputFileName);
  fs.mkdirSync(path.dirname(sharedOutputPath), { recursive: true });
  fs.writeFileSync(sharedOutputPath, JSON.stringify(deployment, null, 2));

  console.log(`Wrote deployment artifact: ${localOutputPath}`);
  console.log(`Wrote shared deployment artifact: ${sharedOutputPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
