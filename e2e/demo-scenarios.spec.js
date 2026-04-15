const { test, expect } = require('@playwright/test');

/**
 * Demo Scenarios — Visual Flow Tests
 *
 * These tests simulate real user journeys through the application.
 * Each scenario represents a complete workflow.
 */

test.describe('Demo Scenario 1: Data Engineer Workflow', () => {
  test('complete ingestion to visualization flow', async ({ page }) => {
    await page.goto('/');

    // Step 1: Start at Dashboard
    await expect(page.locator('.main-content')).toBeVisible();
    await expect(page.locator('.topbar-brand')).toContainText('Databricks');

    // Step 2: Navigate to Ingestion
    await page.locator('.sidebar-item').filter({ hasText: 'Ingestion' }).click();
    await page.waitForTimeout(500);
    const mainContent = page.locator('.main-content');
    await expect(mainContent).toBeVisible();

    // Step 3: Navigate to Modeling
    await page.locator('.sidebar-item').filter({ hasText: 'Modeling' }).click();
    await page.waitForTimeout(500);
    await expect(mainContent).toBeVisible();

    // Step 4: Check Visualization
    await page.locator('.sidebar-item').filter({ hasText: 'Visualization' }).click();
    await page.waitForTimeout(500);
    await expect(mainContent).toBeVisible();
  });
});

test.describe('Demo Scenario 2: ML Engineer Workflow', () => {
  test('XAI and RAG exploration', async ({ page }) => {
    await page.goto('/');

    // Step 1: Navigate to XAI
    await page.locator('.sidebar-item').filter({ hasText: 'XAI' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();

    // Step 2: Navigate to RAG
    await page.locator('.sidebar-item').filter({ hasText: 'RAG' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();

    // Step 3: Check Modeling
    await page.locator('.sidebar-item').filter({ hasText: 'Modeling' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();
  });
});

test.describe('Demo Scenario 3: DevOps / Governance Workflow', () => {
  test('security, terraform, and pipeline check', async ({ page }) => {
    await page.goto('/');

    // Step 1: Security & Governance
    await page.locator('.sidebar-item').filter({ hasText: 'Security' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();

    // Step 2: Terraform / Azure / Snowflake
    await page.locator('.sidebar-item').filter({ hasText: 'Terraform' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();

    // Step 3: Pipeline Builder
    await page.locator('.sidebar-item').filter({ hasText: 'Pipeline' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();
  });
});

test.describe('Demo Scenario 4: Data Testing Workflow', () => {
  test('testing, ELT, and Unity Catalog flow', async ({ page }) => {
    await page.goto('/');

    // Step 1: Data Testing
    await page.locator('.sidebar-item').filter({ hasText: 'Data Testing' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();

    // Step 2: ELT Operations
    await page.locator('.sidebar-item').filter({ hasText: 'ELT' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();

    // Step 3: Unity Catalog
    await page.locator('.sidebar-item').filter({ hasText: 'Unity Catalog' }).click();
    await page.waitForTimeout(500);
    await expect(page.locator('.main-content')).toBeVisible();
  });
});

test.describe('Visual & Layout Checks', () => {
  test('white background on content area', async ({ page }) => {
    await page.goto('/');
    const bg = await page.locator('.main-content').evaluate((el) => {
      return getComputedStyle(el).backgroundColor;
    });
    // Should be white (#ffffff = rgb(255, 255, 255))
    expect(bg).toBe('rgb(255, 255, 255)');
  });

  test('sidebar is on the left, content on the right', async ({ page }) => {
    await page.goto('/');
    const sidebarBox = await page.locator('.sidebar').boundingBox();
    const contentBox = await page.locator('.main-content').boundingBox();

    expect(sidebarBox.x).toBeLessThan(contentBox.x);
  });

  test('topbar is fixed at the top', async ({ page }) => {
    await page.goto('/');
    const topbar = await page.locator('.topbar').boundingBox();
    expect(topbar.y).toBe(0);
  });

  test('no console errors on page load', async ({ page }) => {
    const errors = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') errors.push(msg.text());
    });
    await page.goto('/');
    await page.waitForTimeout(2000);
    expect(errors).toHaveLength(0);
  });

  test('responsive — sidebar collapses on small viewport', async ({ page }) => {
    await page.setViewportSize({ width: 600, height: 800 });
    await page.goto('/');
    // On mobile, sidebar labels should be hidden
    const labels = page.locator('.sidebar .sidebar-label');
    const firstLabel = labels.first();
    if ((await firstLabel.count()) > 0) {
      await expect(firstLabel).toBeHidden();
    }
  });
});
