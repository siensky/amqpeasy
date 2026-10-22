import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const artifacts = join(root, ".artifacts");
await mkdir(artifacts, { recursive: true });
function run(command, args, cwd = root, capture = false) {
  const result = spawnSync(command, args, {
    cwd, env: process.env, encoding: "utf8", stdio: capture ? "pipe" : "inherit", timeout: 120000,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    if (capture) console.error(result.stdout, result.stderr);
    throw new Error(`${command} failed with status ${result.status}`);
  }
  return result.stdout;
}

const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
let tarball = process.env.PACKAGE_TARBALL;
if (!tarball) {
  const [packed] = JSON.parse(run("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", artifacts], root, true));
  assert.equal(packed.version, manifest.version);
  assert.ok(packed.files.some(file => file.path === "dist/public.js"));
  assert.ok(packed.files.some(file => file.path === "dist/public.d.ts"));
  assert.ok(packed.files.some(file => file.path === "dist/error.js"));
  for (const file of packed.files) {
    assert.ok(/^(dist\/|package\.json$|README(?:\.|$)|LICEN[CS]E(?:\.|$))/.test(file.path), `Unexpected package file: ${file.path}`);
  }
  tarball = join(artifacts, packed.filename);
} else {
  tarball = resolve(tarball);
}

const temporaryProject = await mkdtemp(join(tmpdir(), "amqpeasy-package-"));
try {
  await writeFile(join(temporaryProject, "package.json"), JSON.stringify({ private: true, type: "module" }));
  run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false", tarball], temporaryProject);
  for (const name of ["consumer.mjs", "consumer.ts"]) {
    await copyFile(join(root, "tests/package", name), join(temporaryProject, name));
  }
  await writeFile(join(temporaryProject, "tsconfig.json"), JSON.stringify({
    compilerOptions: {
      target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", strict: true,
      noEmit: true, skipLibCheck: false, types: [],
    },
    include: ["consumer.ts"],
  }));
  run(join(root, "node_modules/.bin/tsc"), ["-p", join(temporaryProject, "tsconfig.json")], temporaryProject);
  run(process.execPath, ["consumer.mjs"], temporaryProject);
  run("bun", ["consumer.mjs"], temporaryProject);
  console.log(`Verified package: ${tarball}`);
} finally {
  await rm(temporaryProject, { recursive: true, force: true });
}
