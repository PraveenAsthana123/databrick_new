const { test, expect } = require('@playwright/test');

/**
 * Visual Flow Tests — Screenshot capture for each page
 *
 * Captures screenshots of every major page for visual regression tracking.
 * Screenshots saved to: e2e/screenshots/
 */

const ALL_PAGES = [
  { nav: 'Dashboard', name: 'dashboard' },
  { nav: 'Medallion', name: 'medallion' },
  { nav: 'Landing Zone', name: 'landing-zone' },
  { nav: 'Ingestion', name: 'ingestion' },
  { nav: 'Modeling', name: 'modeling' },
  { nav: 'Unity Catalog', name: 'unity-catalog' },
  { nav: 'Visualization', name: 'visualization' },
  { nav: 'ELT', name: 'elt-operations' },
  { nav: 'Data Testing', name: 'data-testing' },
  { nav: 'Pipeline', name: 'pipelines' },
  { nav: 'XAI', name: 'xai' },
  { nav: 'RAG', name: 'rag' },
  { nav: 'Security', name: 'security' },
  { nav: 'Terraform', name: 'terraform' },
  { nav: 'Settings', name: 'settings' },
];

test.describe('Visual Flow — Page Screenshots', () => {
  for (const pg of ALL_PAGES) {
    test(`capture ${pg.name} page`, async ({ page }) => {
      await page.goto('/');
      await page.waitForSelector('.sidebar');

      const navItem = page.locator('.sidebar-item').filter({ hasText: pg.nav }).first();
      if ((await navItem.count()) > 0) {
        await navItem.click();
        await page.waitForTimeout(1000);
      }

      await page.screenshot({
        path: `e2e/screenshots/${pg.name}.png`,
        fullPage: true,
      });
    });
  }
});

test.describe('Visual Flow — Interaction Screenshots', () => {
  test('capture sidebar collapsed state', async ({ page }) => {
    await page.goto('/');
    await page.locator('.topbar-toggle').click();
    await page.waitForTimeout(500);
    await page.screenshot({
      path: 'e2e/screenshots/sidebar-collapsed.png',
      fullPage: true,
    });
  });

  test('capture mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.goto('/');
    await page.waitForTimeout(500);
    await page.screenshot({
      path: 'e2e/screenshots/mobile-view.png',
      fullPage: true,
    });
  });

  test('capture tablet viewport', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForTimeout(500);
    await page.screenshot({
      path: 'e2e/screenshots/tablet-view.png',
      fullPage: true,
    });
  });
});
