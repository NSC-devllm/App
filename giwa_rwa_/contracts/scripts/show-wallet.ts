import fs from "node:fs";
import path from "node:path";

import { Wallet } from "ethers";

type KeystoreShape = {
  address?: string;
};

async function main() {
  const walletName = process.env.WALLET_NAME?.trim();
  const password = process.env.WALLET_PASSWORD;
  const printPrivateKey = process.env.PRINT_PRIVATE_KEY === "1";

  if (!walletName) {
    throw new Error("Set WALLET_NAME env var (e.g. WALLET_NAME=treasury).");
  }
  if (!password) {
    throw new Error("Set WALLET_PASSWORD env var.");
  }

  const keystorePath =
    process.env.KEYSTORE_PATH?.trim() ||
    path.resolve(process.cwd(), "../shared/wallets", `${walletName}.keystore.json`);

  if (!fs.existsSync(keystorePath)) {
    throw new Error(`Keystore file not found: ${keystorePath}`);
  }

  const keystoreRaw = fs.readFileSync(keystorePath, "utf8");
  const parsed = JSON.parse(keystoreRaw) as KeystoreShape;
  const wallet = await Wallet.fromEncryptedJson(keystoreRaw, password);

  console.log(`name: ${walletName}`);
  console.log(`keystore: ${keystorePath}`);
  console.log(`address(from keystore): ${parsed.address ? `0x${parsed.address}` : "(missing)"}`);
  console.log(`address(from decrypt):  ${wallet.address}`);

  if (wallet.mnemonic?.phrase) {
    console.log(`mnemonic: ${wallet.mnemonic.phrase}`);
  } else {
    console.log("mnemonic: (not included in this keystore)");
  }

  if (printPrivateKey) {
    console.log(`privateKey: ${wallet.privateKey}`);
  } else {
    console.log("privateKey: (hidden)  -> set PRINT_PRIVATE_KEY=1 to print once");
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
