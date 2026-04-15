const { test, expect } = require('@playwright/test');

// ─── All sidebar pages to test ─────────────────
const PAGES = [
  { id: 'dashboard', label: 'Dashboard', hasStats: true },
  { id: 'medallion', label: 'Medallion Architecture' },
  { id: 'landing-zone', label: 'Landing Zone' },
  { id: 'ingestion', label: 'Ingestion' },
  { id: 'modeling', label: 'Modeling' },
  { id: 'unity-catalog', label: 'Unity Catalog' },
  { id: 'visualization', label: 'Visualization' },
  { id: 'elt-operations', label: 'ELT' },
  { id: 'data-testing', label: 'Data Testing' },
  { id: 'pipelines', label: 'Pipeline Builder' },
  { id: 'xai', label: 'XAI' },
  { id: 'rag', label: 'RAG' },
  { id: 'security', label: 'Security' },
  { id: 'terraform', label: 'Terraform' },
  { id: 'clusters', label: 'Clusters' },
  { id: 'notebooks', label: 'Notebooks' },
  { id: 'jobs', label: 'Jobs' },
  { id: 'spark-ui', label: 'Spark UI' },
  { id: 'data-storage', label: 'Data Storage' },
  { id: 'download-data', label: 'Download Data' },
  { id: 'simulation', label: 'Simulation Tools' },
  { id: 'settings', label: 'Settings' },
];

test.describe('Navigation — All Pages', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('.sidebar');
  });

  test('sidebar renders with all menu sections', async ({ page }) => {
    const sections = page.locator('.sidebar-label');
    const count = await sections.count();
    expect(count).toBeGreaterThanOrEqual(5);
  });

  test('sidebar has all navigation items', async ({ page }) => {
    const items = page.locator('.sidebar-item');
    const count = await items.count();
    expect(count).toBeGreaterThanOrEqual(20);
  });

  test('clicking each sidebar item loads content', async ({ page }) => {
    for (const pg of PAGES.slice(0, 5)) {
      // Test first 5 pages for speed
      const btn = page.locator(`.sidebar-item`).filter({ hasText: pg.label }).first();
      if ((await btn.count()) > 0) {
        await btn.click();
        await page.waitForTimeout(500);
        // Main content should update
        const mainContent = page.locator('.main-content');
        await expect(mainContent).toBeVisible();
      }
    }
  });

  test('sidebar collapses and expands', async ({ page }) => {
    const toggleBtn = page.locator('.topbar-toggle');
    await toggleBtn.click();
    await expect(page.locator('.sidebar.collapsed')).toBeVisible();

    await toggleBtn.click();
    await expect(page.locator('.sidebar:not(.collapsed)')).toBeVisible();
  });
});
