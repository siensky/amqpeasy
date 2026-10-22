if (!process.env.RABBITMQ_URL) {
  throw new Error("Set RABBITMQ_URL to a dedicated test broker before running integration tests.");
}
const child = Bun.spawn(["bun", "test", "tests/integration"], {
  stdin: "inherit", stdout: "inherit", stderr: "inherit", env: process.env,
});
process.exit(await child.exited);
export {};
