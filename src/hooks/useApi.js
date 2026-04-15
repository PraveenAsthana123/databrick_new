/**
 * useApi — Custom hook for data fetching with loading/error/empty states
 *
 * Usage:
 *   const { data, loading, error, refetch } = useApi('/api/v1/items');
 */

import { useState, useEffect, useCallback } from 'react';
import api from '../services/api';

export function useApi(endpoint, options = {}) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const { immediate = true } = options;

  const fetchData = useCallback(async () => {
    let cancelled = false;
    try {
      setLoading(true);
      setError(null);
      const result = await api.get(endpoint);
      if (!cancelled) setData(result);
    } catch (err) {
      if (!cancelled) setError(err.message || 'Failed to fetch data');
    } finally {
      if (!cancelled) setLoading(false);
    }
    return () => {
      cancelled = true;
    };
  }, [endpoint]);

  useEffect(() => {
    let cleanup;
    if (immediate) {
      cleanup = fetchData();
    }
    return () => {
      if (cleanup && typeof cleanup.then === 'function') {
        cleanup.then((fn) => fn && fn());
      }
    };
  }, [fetchData, immediate]);

  return { data, loading, error, refetch: fetchData };
}

export default useApi;
