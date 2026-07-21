// Playwright config for the passwordless-auth end-to-end suite.
// The repo had playwright as a devDependency but no config and no specs; this
// is the first harness. Servers are started by e2e/run_e2e.sh, not here, so the
// same processes can be reused across desktop and mobile projects.
const { defineConfig, devices } = require('@playwright/test');

module.exports = defineConfig({
  testDir: require('path').join(__dirname, 'e2e'),
  testMatch: '**/*.spec.js',
  timeout: 45_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: process.env.E2E_BASE_URL || 'http://127.0.0.1:3099',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    { name: 'desktop', use: { ...devices['Desktop Chrome'] } },
    { name: 'mobile', use: { ...devices['Pixel 5'] } },
  ],
});
