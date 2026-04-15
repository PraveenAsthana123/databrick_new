/**
 * ErrorTracker — Comprehensive frontend error tracking
 *
 * Tracks: Console errors, DOM issues, CSS problems, routing errors,
 *         network failures, performance issues, unhandled rejections
 *
 * Usage:
 *   import { errorTracker } from '../utils/errorTracker';
 *   errorTracker.init();           // Call once in index.js
 *   errorTracker.getReport();      // Get all tracked errors
 *   errorTracker.clear();          // Clear error log
 */

const ERROR_TYPES = {
  CONSOLE: 'console',
  DOM: 'dom',
  CSS: 'css',
  ROUTING: 'routing',
  NETWORK: 'network',
  RENDER: 'render',
  PERFORMANCE: 'performance',
  PORT: 'port',
  UNHANDLED: 'unhandled',
  HTML: 'html',
};

class ErrorTracker {
  constructor() {
    this.errors = [];
    this.warnings = [];
    this.initialized = false;
    this.maxErrors = 100;
  }

  // ─── Initialize all tracking ────────────────
  init() {
    if (this.initialized) return;
    this.initialized = true;

    this._trackConsoleErrors();
    this._trackUnhandledErrors();
    this._trackUnhandledRejections();
    this._trackNetworkErrors();
    this._trackPerformance();

    // Run DOM/CSS/HTML checks after page loads
    if (typeof window !== 'undefined') {
      window.addEventListener('load', () => {
        setTimeout(() => {
          this._checkDOMIssues();
          this._checkCSSIssues();
          this._checkHTMLIssues();
          this._checkAccessibility();
        }, 1000);
      });
    }
  }

  // ─── Console Error Tracking ─────────────────
  _trackConsoleErrors() {
    if (typeof window === 'undefined') return;

    const originalError = window.console.error;
    const originalWarn = window.console.warn;
    const self = this;

    window.console.error = function (...args) {
      self._log(ERROR_TYPES.CONSOLE, 'error', args.map(String).join(' '));
      originalError.apply(window.console, args);
    };

    window.console.warn = function (...args) {
      self._logWarning(ERROR_TYPES.CONSOLE, args.map(String).join(' '));
      originalWarn.apply(window.console, args);
    };
  }

  // ─── Unhandled JS Errors ────────────────────
  _trackUnhandledErrors() {
    if (typeof window === 'undefined') return;

    window.addEventListener('error', (event) => {
      this._log(ERROR_TYPES.UNHANDLED, 'error', event.message, {
        filename: event.filename,
        lineno: event.lineno,
        colno: event.colno,
        stack: event.error?.stack,
      });
    });
  }

  // ─── Unhandled Promise Rejections ───────────
  _trackUnhandledRejections() {
    if (typeof window === 'undefined') return;

    window.addEventListener('unhandledrejection', (event) => {
      const reason = event.reason;
      this._log(ERROR_TYPES.UNHANDLED, 'rejection', String(reason?.message || reason), {
        stack: reason?.stack,
      });
    });
  }

  // ─── Network Error Tracking ─────────────────
  _trackNetworkErrors() {
    if (typeof window === 'undefined') return;

    const originalFetch = window.fetch;
    const self = this;

    window.fetch = function (...args) {
      const url = typeof args[0] === 'string' ? args[0] : args[0]?.url || '';

      return originalFetch.apply(window, args).then(
        (response) => {
          if (!response.ok) {
            self._log(
              ERROR_TYPES.NETWORK,
              'http_error',
              `${response.status} ${response.statusText}`,
              {
                url,
                status: response.status,
              }
            );
          }
          return response;
        },
        (error) => {
          if (error.name === 'AbortError') {
            self._log(ERROR_TYPES.NETWORK, 'timeout', `Request timed out: ${url}`, { url });
          } else if (
            error.message?.includes('ECONNREFUSED') ||
            error.message?.includes('Failed to fetch')
          ) {
            self._log(ERROR_TYPES.PORT, 'connection_refused', `Cannot connect: ${url}`, {
              url,
              hint: 'Check if server is running. Try: lsof -i :<port>',
            });
          } else {
            self._log(ERROR_TYPES.NETWORK, 'fetch_error', error.message, { url });
          }
          throw error;
        }
      );
    };
  }

  // ─── Performance Tracking ───────────────────
  _trackPerformance() {
    if (typeof window === 'undefined' || !window.PerformanceObserver) return;

    // Track long tasks (>50ms)
    try {
      const longTaskObserver = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          if (entry.duration > 100) {
            this._logWarning(
              ERROR_TYPES.PERFORMANCE,
              `Long task: ${Math.round(entry.duration)}ms`,
              {
                duration: entry.duration,
                startTime: entry.startTime,
              }
            );
          }
        }
      });
      longTaskObserver.observe({ entryTypes: ['longtask'] });
    } catch {
      // longtask not supported in all browsers
    }

    // Track layout shifts (CLS)
    try {
      const clsObserver = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          if (entry.value > 0.1) {
            this._logWarning(
              ERROR_TYPES.CSS,
              `Layout shift detected: CLS=${entry.value.toFixed(3)}`,
              {
                value: entry.value,
              }
            );
          }
        }
      });
      clsObserver.observe({ entryTypes: ['layout-shift'] });
    } catch {
      // layout-shift not supported in all browsers
    }
  }

  // ─── DOM Issue Detection ────────────────────
  _checkDOMIssues() {
    if (typeof document === 'undefined') return;

    // Check for empty links
    const emptyLinks = document.querySelectorAll('a:not([href]), a[href=""], a[href="#"]');
    if (emptyLinks.length > 0) {
      this._logWarning(ERROR_TYPES.DOM, `${emptyLinks.length} empty/broken links found`);
    }

    // Check for images without alt
    const imgsNoAlt = document.querySelectorAll('img:not([alt])');
    if (imgsNoAlt.length > 0) {
      this._logWarning(ERROR_TYPES.HTML, `${imgsNoAlt.length} images missing alt attribute`);
    }

    // Check for buttons without accessible name
    const buttons = document.querySelectorAll('button');
    let emptyButtons = 0;
    buttons.forEach((btn) => {
      if (
        !btn.textContent?.trim() &&
        !btn.getAttribute('aria-label') &&
        !btn.getAttribute('title')
      ) {
        emptyButtons++;
      }
    });
    if (emptyButtons > 0) {
      this._logWarning(ERROR_TYPES.DOM, `${emptyButtons} buttons without accessible name`);
    }

    // Check for duplicate IDs
    const allIds = document.querySelectorAll('[id]');
    const idMap = {};
    allIds.forEach((el) => {
      const id = el.getAttribute('id');
      if (id) {
        idMap[id] = (idMap[id] || 0) + 1;
      }
    });
    const dupes = Object.entries(idMap).filter(([, count]) => count > 1);
    if (dupes.length > 0) {
      this._logWarning(ERROR_TYPES.DOM, `Duplicate IDs: ${dupes.map(([id]) => id).join(', ')}`);
    }

    // Check for deeply nested elements (>15 levels)
    const deepCheck = (el, depth) => {
      if (depth > 15) {
        this._logWarning(ERROR_TYPES.DOM, `Deeply nested element (${depth} levels): ${el.tagName}`);
        return;
      }
      Array.from(el.children).forEach((child) => deepCheck(child, depth + 1));
    };
    deepCheck(document.body, 0);
  }

  // ─── CSS Issue Detection ────────────────────
  _checkCSSIssues() {
    if (typeof document === 'undefined') return;

    // Check for inline styles (should use classes)
    const inlineStyled = document.querySelectorAll('[style]');
    if (inlineStyled.length > 5) {
      this._logWarning(
        ERROR_TYPES.CSS,
        `${inlineStyled.length} elements with inline styles — prefer CSS classes`
      );
    }

    // Check for overflow issues
    const body = document.body;
    if (body.scrollWidth > body.clientWidth) {
      this._logWarning(
        ERROR_TYPES.CSS,
        'Horizontal overflow detected — content wider than viewport'
      );
    }

    // Check for missing CSS variables
    const root = getComputedStyle(document.documentElement);
    const requiredVars = ['--primary', '--bg-page', '--text-primary', '--border'];
    requiredVars.forEach((v) => {
      if (!root.getPropertyValue(v).trim()) {
        this._log(ERROR_TYPES.CSS, 'missing_var', `CSS variable ${v} is not defined`);
      }
    });

    // Check z-index sanity
    const allElements = document.querySelectorAll('*');
    let maxZ = 0;
    allElements.forEach((el) => {
      const z = parseInt(getComputedStyle(el).zIndex, 10);
      if (z > maxZ) maxZ = z;
    });
    if (maxZ > 10000) {
      this._logWarning(
        ERROR_TYPES.CSS,
        `High z-index detected: ${maxZ} — consider using a z-index scale`
      );
    }
  }

  // ─── HTML Validation ────────────────────────
  _checkHTMLIssues() {
    if (typeof document === 'undefined') return;

    // Check meta tags
    if (!document.querySelector('meta[name="viewport"]')) {
      this._logWarning(ERROR_TYPES.HTML, 'Missing viewport meta tag — mobile display may break');
    }

    if (
      !document.querySelector('meta[charset]') &&
      !document.querySelector('meta[http-equiv="Content-Type"]')
    ) {
      this._logWarning(ERROR_TYPES.HTML, 'Missing charset meta tag');
    }

    // Check title
    if (!document.title || document.title === 'React App') {
      this._logWarning(ERROR_TYPES.HTML, `Page title is "${document.title}" — update for SEO`);
    }

    // Check for deprecated elements
    const deprecated = document.querySelectorAll('center, font, marquee, blink, frame, frameset');
    if (deprecated.length > 0) {
      this._logWarning(ERROR_TYPES.HTML, `${deprecated.length} deprecated HTML elements found`);
    }

    // Check form inputs have labels
    const inputs = document.querySelectorAll('input:not([type="hidden"]):not([type="submit"])');
    let unlabeled = 0;
    inputs.forEach((input) => {
      const id = input.getAttribute('id');
      if (!id || !document.querySelector(`label[for="${id}"]`)) {
        if (!input.getAttribute('aria-label') && !input.closest('label')) {
          unlabeled++;
        }
      }
    });
    if (unlabeled > 0) {
      this._logWarning(ERROR_TYPES.HTML, `${unlabeled} form inputs without labels`);
    }
  }

  // ─── Accessibility Check ────────────────────
  _checkAccessibility() {
    if (typeof document === 'undefined') return;

    // Check color contrast on key elements
    const textElements = document.querySelectorAll('h1, h2, h3, p, span, a, button, label');
    let lowContrast = 0;
    textElements.forEach((el) => {
      const style = getComputedStyle(el);
      const color = style.color;
      const bg = style.backgroundColor;
      if (color === bg && color !== 'rgba(0, 0, 0, 0)') {
        lowContrast++;
      }
    });
    if (lowContrast > 0) {
      this._logWarning(ERROR_TYPES.HTML, `${lowContrast} elements with same text/background color`);
    }

    // Check for keyboard-accessible interactive elements
    const interactive = document.querySelectorAll('div[onclick], span[onclick]');
    if (interactive.length > 0) {
      this._logWarning(
        ERROR_TYPES.DOM,
        `${interactive.length} non-semantic clickable elements — use <button> instead`
      );
    }
  }

  // ─── Routing Error Detection ────────────────
  trackRouteChange(path, status) {
    if (status === 'not_found') {
      this._log(ERROR_TYPES.ROUTING, '404', `Route not found: ${path}`, { path });
    } else if (status === 'error') {
      this._log(ERROR_TYPES.ROUTING, 'route_error', `Error loading route: ${path}`, { path });
    }
  }

  // ─── Internal Logging ───────────────────────
  _log(type, subtype, message, meta) {
    if (this.errors.length >= this.maxErrors) {
      this.errors.shift(); // Remove oldest
    }
    this.errors.push({
      timestamp: new Date().toISOString(),
      type,
      subtype,
      message,
      meta: meta || {},
    });
  }

  _logWarning(type, message, meta) {
    if (this.warnings.length >= this.maxErrors) {
      this.warnings.shift();
    }
    this.warnings.push({
      timestamp: new Date().toISOString(),
      type,
      message,
      meta: meta || {},
    });
  }

  // ─── Public API ─────────────────────────────

  getErrors() {
    return [...this.errors];
  }

  getWarnings() {
    return [...this.warnings];
  }

  getReport() {
    return {
      timestamp: new Date().toISOString(),
      totalErrors: this.errors.length,
      totalWarnings: this.warnings.length,
      errors: this.getErrors(),
      warnings: this.getWarnings(),
      byType: this._groupByType(),
    };
  }

  _groupByType() {
    const grouped = {};
    [...this.errors, ...this.warnings].forEach((item) => {
      const type = item.type;
      if (!grouped[type]) grouped[type] = [];
      grouped[type].push(item);
    });
    return grouped;
  }

  getSummary() {
    const report = this.getReport();
    return {
      timestamp: report.timestamp,
      errors: report.totalErrors,
      warnings: report.totalWarnings,
      console: (report.byType.console || []).length,
      dom: (report.byType.dom || []).length,
      css: (report.byType.css || []).length,
      html: (report.byType.html || []).length,
      network: (report.byType.network || []).length,
      routing: (report.byType.routing || []).length,
      port: (report.byType.port || []).length,
      performance: (report.byType.performance || []).length,
      render: (report.byType.render || []).length,
    };
  }

  clear() {
    this.errors = [];
    this.warnings = [];
  }
}

// Singleton instance
const errorTracker = new ErrorTracker();

export { errorTracker, ERROR_TYPES };
export default errorTracker;
