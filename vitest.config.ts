import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Test files share one database, so they must run one after another.
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // Temporary: remove as soon as the first real tests exist (step 2).
    passWithNoTests: true,
  },
});
