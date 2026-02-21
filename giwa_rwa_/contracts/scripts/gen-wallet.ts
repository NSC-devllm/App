// contracts/scripts/gen-wallet.ts
import { Wallet } from "ethers";
import fs from "fs";
import path from "path";

async function main() {
  // Hardhat 3's `run` task doesn't forward extra positional args to scripts.
  // Use WALLET_NAME env var as the primary way to set output filename.
  const name = process.env.WALLET_NAME?.trim() || "wallet";
  const password = process.env.WALLET_PASSWORD;
  if (!password) {
    throw new Error("Set WALLET_PASSWORD env var (for keystore encryption).");
  }

  const wallet = Wallet.createRandom();
  const keystoreJson = await wallet.encrypt(password);

  // 모노레포라면 shared/wallets 쪽으로 저장 추천
  const outDir = path.resolve(process.cwd(), "../shared/wallets");
  fs.mkdirSync(outDir, { recursive: true });

  const outPath = path.join(outDir, `${name}.keystore.json`);
  fs.writeFileSync(outPath, keystoreJson, "utf8");

  console.log(`name: ${name}`);
  console.log(`address: ${wallet.address}`);
  console.log(`keystore: ${outPath}`);
  console.log(`mnemonic: ${wallet.mnemonic?.phrase}`);

  if (process.env.PRINT_PRIVATE_KEY === "1") {
    console.log(`privateKey: ${wallet.privateKey}`);
  } else {
    console.log(`privateKey: (hidden)  -> set PRINT_PRIVATE_KEY=1 to print once`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
