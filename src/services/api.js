/**
 * Centralized API client — all external requests go through here
 *
 * Features:
 *   - Timeout on every request (default 10s)
 *   - AbortController for cancellation
 *   - Consistent error handling
 *   - No hardcoded URLs
 */

const API_BASE = process.env.REACT_APP_API_URL || '';
const DEFAULT_TIMEOUT = 10000;

class ApiError extends Error {
  constructor(message, status, code) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
  }
}

async function request(url, options = {}) {
  const timeout = options.timeout || DEFAULT_TIMEOUT;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new ApiError(
        body.detail || `HTTP ${response.status}`,
        response.status,
        body.error_code
      );
    }

    return response.json();
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === 'AbortError') {
      throw new ApiError('Request timed out', 408, 'TIMEOUT');
    }
    if (error instanceof ApiError) throw error;
    throw new ApiError(error.message || 'Network error', 0, 'NETWORK');
  }
}

// CRUD helpers for internal API
export const api = {
  get: (path, opts) => request(`${API_BASE}${path}`, opts),
  post: (path, data, opts) =>
    request(`${API_BASE}${path}`, { method: 'POST', body: JSON.stringify(data), ...opts }),
  put: (path, data, opts) =>
    request(`${API_BASE}${path}`, { method: 'PUT', body: JSON.stringify(data), ...opts }),
  delete: (path, opts) => request(`${API_BASE}${path}`, { method: 'DELETE', ...opts }),
};

// External API helper (no base URL prefix)
export const external = {
  get: (url, opts) => request(url, opts),
};

export { ApiError };
export default api;
