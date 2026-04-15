import { searchPatents, getGooglePatentsUrl, getUSPTOSearchUrl } from '../research';

describe('Research Service — Patent Portals', () => {
  test('searchPatents returns all 5 portal URLs', () => {
    const results = searchPatents('machine learning');
    expect(results.google).toBeDefined();
    expect(results.google.source).toBe('google_patents');
    expect(results.google.searchUrl).toContain('patents.google.com');

    expect(results.uspto).toBeDefined();
    expect(results.uspto.source).toBe('uspto');

    expect(results.lens).toBeDefined();
    expect(results.lens.source).toBe('lens');

    expect(results.epo).toBeDefined();
    expect(results.epo.source).toBe('epo');

    expect(results.wipo).toBeDefined();
    expect(results.wipo.source).toBe('wipo');
  });

  test('getGooglePatentsUrl encodes query correctly', () => {
    const result = getGooglePatentsUrl('neural network');
    expect(result.searchUrl).toContain('neural%20network');
    expect(result.source).toBe('google_patents');
  });

  test('getUSPTOSearchUrl returns valid URL', () => {
    const result = getUSPTOSearchUrl('data pipeline');
    expect(result.searchUrl).toContain('patft.uspto.gov');
    expect(result.source).toBe('uspto');
  });

  test('searchPatents with special characters', () => {
    const results = searchPatents('AI & ML: "deep learning"');
    expect(results.google.searchUrl).toBeDefined();
    expect(results.lens.searchUrl).toBeDefined();
  });
});
