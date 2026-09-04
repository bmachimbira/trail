// @ts-check
import { defineConfig } from "astro/config";
import node from "@astrojs/node";

// Host header validation + CSRF origin checks trust these domains (any port).
// Add public hostnames via the ALLOWED_HOSTS env var (comma-separated).
const allowedHosts = ["localhost", "127.0.0.1", ...(process.env.ALLOWED_HOSTS ?? "").split(",")]
  .map((hostname) => hostname.trim())
  .filter(Boolean)
  .map((hostname) => ({ hostname }));

export default defineConfig({
  output: "server",
  adapter: node({ mode: "standalone" }),
  server: { port: 4321 },
  security: {
    allowedDomains: allowedHosts,
  },
});
