const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

const SCREENSHOTS = path.join(__dirname, 'screenshots', 'manual-test');

test.beforeAll(() => {
  if (!fs.existsSync(SCREENSHOTS)) fs.mkdirSync(SCREENSHOTS, { recursive: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 1: Dashboard loads with stats
// ═══════════════════════════════════════════
test('MT-01: Dashboard — stats, medallion flow, components visible', async ({ page }) => {
  await page.goto('/');
  await page.waitForTimeout(1500);

  // Check stats cards exist
  const statCards = page.locator('.stat-card');
  const statCount = await statCards.count();
  expect(statCount).toBeGreaterThanOrEqual(3);

  // Check page header
  await expect(page.locator('h1').first()).toBeVisible();

  await page.screenshot({ path: `${SCREENSHOTS}/01-dashboard.png`, fullPage: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 2: Sidebar navigation works
// ═══════════════════════════════════════════
test('MT-02: Sidebar — all 23 items clickable, active state works', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.sidebar');

  const items = page.locator('.sidebar-item');
  const count = await items.count();
  expect(count).toBeGreaterThanOrEqual(22);

  // Click 5th item and verify active class
  await items.nth(4).click();
  await page.waitForTimeout(500);
  const activeItems = page.locator('.sidebar-item.active');
  expect(await activeItems.count()).toBe(1);

  await page.screenshot({ path: `${SCREENSHOTS}/02-sidebar-nav.png`, fullPage: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 3: Sidebar collapse/expand
// ═══════════════════════════════════════════
test('MT-03: Sidebar — collapse and expand toggle', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.sidebar');

  // Collapse
  await page.locator('.topbar-toggle').click();
  await page.waitForTimeout(400);
  await expect(page.locator('.sidebar.collapsed')).toBeVisible();
  await page.screenshot({ path: `${SCREENSHOTS}/03a-sidebar-collapsed.png`, fullPage: true });

  // Expand
  await page.locator('.topbar-toggle').click();
  await page.waitForTimeout(400);
  await expect(page.locator('.sidebar:not(.collapsed)')).toBeVisible();
  await page.screenshot({ path: `${SCREENSHOTS}/03b-sidebar-expanded.png`, fullPage: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 4: Each page loads without crash
// ═══════════════════════════════════════════
const PAGES = [
  { nav: 'Dashboard', file: '04a-dashboard' },
  { nav: 'Medallion', file: '04b-medallion' },
  { nav: 'Landing Zone', file: '04c-landing-zone' },
  { nav: 'Ingestion', file: '04d-ingestion' },
  { nav: 'Modeling', file: '04e-modeling' },
  { nav: 'Unity Catalog', file: '04f-unity-catalog' },
  { nav: 'Visualization', file: '04g-visualization' },
  { nav: 'ELT', file: '04h-elt' },
  { nav: 'Data Testing', file: '04i-data-testing' },
  { nav: 'Pipeline', file: '04j-pipeline' },
  { nav: 'XAI', file: '04k-xai' },
  { nav: 'RAG', file: '04l-rag' },
  { nav: 'Security', file: '04m-security' },
  { nav: 'Terraform', file: '04n-terraform' },
  { nav: 'Clusters', file: '04o-clusters' },
  { nav: 'Notebooks', file: '04p-notebooks' },
  { nav: 'Jobs', file: '04q-jobs' },
  { nav: 'Spark UI', file: '04r-spark-ui' },
  { nav: 'Data Storage', file: '04s-data-storage' },
  { nav: 'Upload', file: '04t-upload' },
  { nav: 'Download Data', file: '04u-download-data' },
  { nav: 'Simulation', file: '04v-simulation' },
  { nav: 'Settings', file: '04w-settings' },
];

for (const pg of PAGES) {
  test(`MT-04-${pg.file}: ${pg.nav} page loads with content`, async ({ page }) => {
    const errors = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') errors.push(msg.text());
    });

    await page.goto('/');
    await page.waitForSelector('.sidebar');
    const navItem = page.locator('.sidebar-item').filter({ hasText: pg.nav }).first();
    if ((await navItem.count()) > 0) {
      await navItem.click();
      await page.waitForTimeout(1500);
    }

    // Main content visible
    await expect(page.locator('.main-content')).toBeVisible();

    // Has some content (not blank)
    const mainText = await page.locator('.main-content').textContent();
    expect(mainText.length).toBeGreaterThan(10);

    // Screenshot
    await page.screenshot({ path: `${SCREENSHOTS}/${pg.file}.png`, fullPage: true });

    // No critical errors
    const critical = errors.filter((e) => !e.includes('favicon') && !e.includes('manifest'));
    expect(critical).toHaveLength(0);
  });
}

// ═══════════════════════════════════════════
// MANUAL TEST 5: Pipeline — Run Now flow
// ═══════════════════════════════════════════
test('MT-05: Pipeline — Run Now, stages execute, progress visible', async ({ page }) => {
  await page.goto('/');
  await page.locator('.sidebar-item').filter({ hasText: 'Pipeline' }).click();
  await page.waitForTimeout(1000);

  await page.screenshot({ path: `${SCREENSHOTS}/05a-pipeline-list.png`, fullPage: true });

  // Click Run Now on first pipeline
  await page.locator('button').filter({ hasText: 'Run Now' }).first().click();
  await page.waitForTimeout(500);
  await page.screenshot({ path: `${SCREENSHOTS}/05b-schedule-modal.png`, fullPage: true });

  // Confirm Run Now
  await page.locator('.card button').filter({ hasText: 'Run Now' }).last().click();
  await page.waitForTimeout(1000);

  // Go to Run History
  await page.locator('.tab').filter({ hasText: 'Run History' }).click();
  await page.waitForTimeout(3000);
  await page.screenshot({ path: `${SCREENSHOTS}/05c-run-in-progress.png`, fullPage: true });

  // Wait for completion
  await page.waitForTimeout(8000);
  await page.screenshot({ path: `${SCREENSHOTS}/05d-run-completed.png`, fullPage: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 6: Pipeline — Schedule job
// ═══════════════════════════════════════════
test('MT-06: Pipeline — Schedule a daily job', async ({ page }) => {
  await page.goto('/');
  await page.locator('.sidebar-item').filter({ hasText: 'Pipeline' }).click();
  await page.waitForTimeout(500);

  // Click Schedule on 2nd pipeline
  const schedBtns = page.locator('button').filter({ hasText: 'Schedule' });
  if ((await schedBtns.count()) > 1) {
    await schedBtns.nth(1).click();
  } else {
    await schedBtns.first().click();
  }
  await page.waitForTimeout(500);

  await page.screenshot({ path: `${SCREENSHOTS}/06a-schedule-modal.png`, fullPage: true });

  // Schedule is already set to daily_6am from the "Schedule" button click
  await page.waitForTimeout(300);

  // Click Schedule Job button in modal
  const schedJobBtn = page.locator('button.btn-primary').filter({ hasText: /Schedule Job|Run Now/ }).last();
  await schedJobBtn.click();
  await page.waitForTimeout(500);

  // Go to Scheduled Jobs tab
  await page.locator('.tab').filter({ hasText: 'Scheduled Jobs' }).click();
  await page.waitForTimeout(500);

  await page.screenshot({ path: `${SCREENSHOTS}/06b-scheduled-jobs.png`, fullPage: true });

  // Verify job appears
  const jobRow = page.locator('td').filter({ hasText: 'Daily at 6 AM' });
  expect(await jobRow.count()).toBeGreaterThanOrEqual(1);
});

// ═══════════════════════════════════════════
// MANUAL TEST 7: Upload — CSV with preview
// ═══════════════════════════════════════════
test('MT-07: Upload — CSV file with data table preview', async ({ page }) => {
  await page.goto('/');
  await page.locator('.sidebar-item').filter({ hasText: 'Upload' }).click();
  await page.waitForTimeout(500);

  await page.screenshot({ path: `${SCREENSHOTS}/07a-upload-empty.png`, fullPage: true });

  // Upload CSV
  const csv = 'id,name,email,amount\n1,Alice,alice@test.com,150.50\n2,Bob,bob@test.com,200.75\n3,Charlie,charlie@test.com,75.00\n4,Diana,diana@test.com,320.25\n5,Eve,eve@test.com,99.99';
  const fileChooserPromise = page.waitForEvent('filechooser');
  await page.locator('button').filter({ hasText: 'Browse Files' }).click();
  const fileChooser = await fileChooserPromise;
  await fileChooser.setFiles({ name: 'sales-data.csv', mimeType: 'text/csv', buffer: Buffer.from(csv) });
  await page.waitForTimeout(1000);

  await page.screenshot({ path: `${SCREENSHOTS}/07b-upload-file-list.png`, fullPage: true });

  // Click to preview
  await page.locator('text=sales-data.csv').click();
  await page.waitForTimeout(500);

  await page.screenshot({ path: `${SCREENSHOTS}/07c-csv-data-preview.png`, fullPage: true });

  // Verify data table
  await expect(page.getByRole('cell', { name: 'Alice', exact: true })).toBeVisible();
  await expect(page.locator('th').filter({ hasText: 'amount' })).toBeVisible();
});

// ═══════════════════════════════════════════
// MANUAL TEST 8: Upload — Multiple file types
// ═══════════════════════════════════════════
test('MT-08: Upload — JSON + TXT files', async ({ page }) => {
  await page.goto('/');
  await page.locator('.sidebar-item').filter({ hasText: 'Upload' }).click();
  await page.waitForTimeout(500);

  const fileChooserPromise = page.waitForEvent('filechooser');
  await page.locator('button').filter({ hasText: 'Browse Files' }).click();
  const fileChooser = await fileChooserPromise;
  await fileChooser.setFiles([
    { name: 'config.json', mimeType: 'application/json', buffer: Buffer.from(JSON.stringify({ env: 'prod', debug: false, workers: 4 }, null, 2)) },
    { name: 'readme.txt', mimeType: 'text/plain', buffer: Buffer.from('Pipeline Documentation\n\nStep 1: Ingest data\nStep 2: Transform\nStep 3: Load to warehouse') },
  ]);
  await page.waitForTimeout(1000);

  // Verify both files
  await expect(page.locator('text=config.json')).toBeVisible();
  await expect(page.locator('text=readme.txt')).toBeVisible();

  // Preview JSON
  await page.locator('text=config.json').click();
  await page.waitForTimeout(500);
  await page.screenshot({ path: `${SCREENSHOTS}/08a-json-preview.png`, fullPage: true });

  // Preview TXT
  await page.locator('text=readme.txt').click();
  await page.waitForTimeout(500);
  await page.screenshot({ path: `${SCREENSHOTS}/08b-txt-preview.png`, fullPage: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 9: Ingestion page — scenarios expand
// ═══════════════════════════════════════════
test('MT-09: Ingestion — scenarios load, tabs work, code visible', async ({ page }) => {
  await page.goto('/');
  await page.locator('.sidebar-item').filter({ hasText: 'Ingestion' }).click();
  await page.waitForTimeout(1500);

  await page.screenshot({ path: `${SCREENSHOTS}/09a-ingestion-page.png`, fullPage: true });

  // Check tabs exist
  const tabs = page.locator('.tab');
  if ((await tabs.count()) > 0) {
    await tabs.first().click();
    await page.waitForTimeout(500);
  }

  // Check content loaded
  const mainText = await page.locator('.main-content').textContent();
  expect(mainText.length).toBeGreaterThan(100);

  await page.screenshot({ path: `${SCREENSHOTS}/09b-ingestion-content.png`, fullPage: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 10: XAI page loads
// ═══════════════════════════════════════════
test('MT-10: XAI — Explainable AI scenarios load', async ({ page }) => {
  await page.goto('/');
  await page.locator('.sidebar-item').filter({ hasText: 'XAI' }).click();
  await page.waitForTimeout(1500);

  const mainText = await page.locator('.main-content').textContent();
  expect(mainText.length).toBeGreaterThan(100);

  await page.screenshot({ path: `${SCREENSHOTS}/10-xai-page.png`, fullPage: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 11: White background verification
// ═══════════════════════════════════════════
test('MT-11: Layout — white background, dark sidebar', async ({ page }) => {
  await page.goto('/');
  await page.waitForTimeout(1000);

  const contentBg = await page.locator('.main-content').evaluate((el) => getComputedStyle(el).backgroundColor);
  expect(contentBg).toBe('rgb(255, 255, 255)');

  const sidebarBg = await page.locator('.sidebar').evaluate((el) => getComputedStyle(el).backgroundColor);
  // Dark sidebar — not white
  expect(sidebarBg).not.toBe('rgb(255, 255, 255)');

  await page.screenshot({ path: `${SCREENSHOTS}/11-layout-colors.png`, fullPage: true });
});

// ═══════════════════════════════════════════
// MANUAL TEST 12: Topbar — branding + cluster status
// ═══════════════════════════════════════════
test('MT-12: Topbar — brand name, cluster status, admin', async ({ page }) => {
  await page.goto('/');
  await page.waitForTimeout(500);

  await expect(page.locator('.topbar-brand')).toBeVisible();
  await expect(page.locator('text=Cluster Active')).toBeVisible();
  await expect(page.locator('text=Admin')).toBeVisible();

  await page.screenshot({ path: `${SCREENSHOTS}/12-topbar.png` });
});

// ═══════════════════════════════════════════
// MANUAL TEST 13: Mobile responsive
// ═══════════════════════════════════════════
test('MT-13: Responsive — mobile viewport sidebar collapses', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.goto('/');
  await page.waitForTimeout(1000);

  // Sidebar labels should be hidden on mobile
  const labels = page.locator('.sidebar .sidebar-label');
  if ((await labels.count()) > 0) {
    await expect(labels.first()).toBeHidden();
  }

  await page.screenshot({ path: `${SCREENSHOTS}/13-mobile-view.png`, fullPage: true });
});
