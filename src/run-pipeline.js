/**
 * src/run-pipeline.js
 *
 * Runs the whole pipeline in order:
 * 1) ingest-active-jobs.js
 * 2) ingest-linkedin.js
 * 3) build-inbox.js
 * 4) curate-inbox-openai.js
 *
 * Then copies the resulting inbox_latest.json into web/public/ so the app uses it.
 */

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

function runNode(script) {
  console.log(`\n=== Running: node ${script} ===`);
  const res = spawnSync(process.execPath, [script], {
    stdio: "inherit",
    env: process.env,
  });
  if (res.status !== 0) {
    throw new Error(`Script failed: ${script} (exit ${res.status})`);
  }
}

function copyInboxToWebPublic() {
  const from = path.join(process.cwd(), "data", "inbox", "inbox_latest.json");
  const to = path.join(process.cwd(), "web", "public", "inbox_latest.json");

  if (!fs.existsSync(from)) {
    throw new Error(`Missing expected file: ${from}`);
  }

  fs.mkdirSync(path.dirname(to), { recursive: true });
  fs.copyFileSync(from, to);
  console.log(`\nCopied:\n  ${from}\n→ ${to}`);
}

async function main() {
  runNode("src/ingest-active-jobs.js");
  runNode("src/ingest-linkedin.js");
  runNode("src/build-inbox.js");
  runNode("src/curate-inbox-openai.js");

  // After curation has written ignored decisions, rebuild so ignored items disappear from inbox_latest.json
  runNode("src/build-inbox.js");

  copyInboxToWebPublic();

  console.log("\n✅ Pipeline complete.");
}

main().catch((e) => {
  console.error("\n❌ Pipeline failed:", e.message || e);
  process.exit(1);
});
