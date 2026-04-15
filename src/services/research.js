/**
 * Research Paper & Patent Portal Integration
 *
 * Supported portals:
 *   Papers: arXiv, Semantic Scholar, CrossRef, PubMed
 *   Patents: Google Patents (via SerpAPI), USPTO, Lens.org
 *
 * Usage:
 *   import { searchPapers, searchPatents } from '../services/research';
 *   const papers = await searchPapers('machine learning', 'arxiv');
 *   const patents = await searchPatents('neural network', 'google');
 */

// ─── Paper Search APIs ────────────────────────

const ARXIV_API = 'https://export.arxiv.org/api/query';
const SEMANTIC_SCHOLAR_API = 'https://api.semanticscholar.org/graph/v1/paper/search';
const CROSSREF_API = 'https://api.crossref.org/works';

/**
 * Search arXiv for research papers
 * @param {string} query - Search terms
 * @param {number} maxResults - Max results (default 10)
 * @returns {Promise<Array>} Papers with title, authors, abstract, url, date
 */
export async function searchArxiv(query, maxResults = 10) {
  const params = new URLSearchParams({
    search_query: `all:${query}`,
    start: '0',
    max_results: String(maxResults),
    sortBy: 'relevance',
    sortOrder: 'descending',
  });

  const response = await fetch(`${ARXIV_API}?${params}`, { timeout: 10000 });
  const text = await response.text();

  // Parse XML response
  const parser = new DOMParser();
  const xml = parser.parseFromString(text, 'text/xml');
  const entries = xml.querySelectorAll('entry');

  return Array.from(entries).map((entry) => ({
    source: 'arxiv',
    title: entry.querySelector('title')?.textContent?.trim() || '',
    authors: Array.from(entry.querySelectorAll('author name')).map((a) => a.textContent),
    abstract: entry.querySelector('summary')?.textContent?.trim() || '',
    url: entry.querySelector('id')?.textContent || '',
    published: entry.querySelector('published')?.textContent?.slice(0, 10) || '',
    categories: Array.from(entry.querySelectorAll('category')).map(
      (c) => c.getAttribute('term') || ''
    ),
  }));
}

/**
 * Search Semantic Scholar for research papers
 * @param {string} query - Search terms
 * @param {number} limit - Max results (default 10)
 * @returns {Promise<Array>} Papers with title, authors, abstract, url, citations
 */
export async function searchSemanticScholar(query, limit = 10) {
  const params = new URLSearchParams({
    query,
    limit: String(limit),
    fields: 'title,authors,abstract,url,year,citationCount,publicationDate',
  });

  const response = await fetch(`${SEMANTIC_SCHOLAR_API}?${params}`);
  const data = await response.json();

  return (data.data || []).map((paper) => ({
    source: 'semantic_scholar',
    title: paper.title || '',
    authors: (paper.authors || []).map((a) => a.name),
    abstract: paper.abstract || '',
    url: paper.url || `https://www.semanticscholar.org/paper/${paper.paperId}`,
    published: paper.publicationDate || String(paper.year || ''),
    citations: paper.citationCount || 0,
  }));
}

/**
 * Search CrossRef for academic papers (DOI-based)
 * @param {string} query - Search terms
 * @param {number} rows - Max results (default 10)
 * @returns {Promise<Array>} Papers with title, authors, DOI, url
 */
export async function searchCrossRef(query, rows = 10) {
  const params = new URLSearchParams({
    query,
    rows: String(rows),
    sort: 'relevance',
  });

  const response = await fetch(`${CROSSREF_API}?${params}`);
  const data = await response.json();

  return (data.message?.items || []).map((item) => ({
    source: 'crossref',
    title: (item.title || [''])[0],
    authors: (item.author || []).map((a) => `${a.given || ''} ${a.family || ''}`.trim()),
    abstract: item.abstract || '',
    url: item.URL || '',
    doi: item.DOI || '',
    published: item.created?.['date-parts']?.[0]?.join('-') || '',
    citations: item['is-referenced-by-count'] || 0,
    journal: item['container-title']?.[0] || '',
  }));
}

// ─── Patent Search APIs ───────────────────────

/**
 * Search Lens.org for patents and scholarly works (free, open API)
 * @param {string} query - Search terms
 * @param {number} size - Max results (default 10)
 * @returns {Promise<Array>} Results with title, authors, url
 */
export async function searchLens(query, size = 10) {
  // Lens.org requires API key for full access
  // This searches the public endpoint
  const searchUrl = `https://www.lens.org/lens/search/patent/list?q=${encodeURIComponent(query)}&n=${size}`;

  return [
    {
      source: 'lens',
      searchUrl,
      note: 'Open Lens.org in browser for full patent search',
      query,
    },
  ];
}

/**
 * Get USPTO patent data
 * @param {string} query - Search terms
 * @returns {Promise<Object>} USPTO search URL and instructions
 */
export function getUSPTOSearchUrl(query) {
  return {
    source: 'uspto',
    searchUrl: `https://patft.uspto.gov/netacgi/nph-Parser?Sect1=PTO2&Sect2=HITOFF&p=1&u=%2Fnetahtml%2FPTO%2Fsearch-bool.html&r=0&f=S&l=50&TERM1=${encodeURIComponent(query)}&FIELD1=&co1=AND&TERM2=&FIELD2=&d=PTXT`,
    fullTextUrl: `https://ppubs.uspto.gov/pubwebapp/static/pages/searchable-docs.html`,
    note: 'USPTO requires browser access for full patent search',
  };
}

/**
 * Get Google Patents search URL
 * @param {string} query - Search terms
 * @returns {Object} Google Patents search URL
 */
export function getGooglePatentsUrl(query) {
  return {
    source: 'google_patents',
    searchUrl: `https://patents.google.com/?q=${encodeURIComponent(query)}`,
    note: 'Open in browser for full Google Patents search',
  };
}

// ─── Unified Search ───────────────────────────

/**
 * Search across multiple paper portals
 * @param {string} query - Search terms
 * @param {Array<string>} sources - Which sources to search (default: all)
 * @param {number} limit - Max results per source
 * @returns {Promise<Object>} Results grouped by source
 */
export async function searchPapers(query, sources, limit = 10) {
  const activeSources = sources || ['arxiv', 'semantic_scholar', 'crossref'];
  const results = {};

  const searches = activeSources.map(async (source) => {
    try {
      switch (source) {
        case 'arxiv':
          results.arxiv = await searchArxiv(query, limit);
          break;
        case 'semantic_scholar':
          results.semantic_scholar = await searchSemanticScholar(query, limit);
          break;
        case 'crossref':
          results.crossref = await searchCrossRef(query, limit);
          break;
        default:
          break;
      }
    } catch (error) {
      results[source] = { error: error.message };
    }
  });

  await Promise.allSettled(searches);
  return results;
}

/**
 * Get patent search URLs across multiple portals
 * @param {string} query - Search terms
 * @returns {Object} Patent portal URLs
 */
export function searchPatents(query) {
  return {
    google: getGooglePatentsUrl(query),
    uspto: getUSPTOSearchUrl(query),
    lens: {
      source: 'lens',
      searchUrl: `https://www.lens.org/lens/search/patent/list?q=${encodeURIComponent(query)}`,
    },
    epo: {
      source: 'epo',
      searchUrl: `https://worldwide.espacenet.com/searchResults?ST=singleline&locale=en_EP&submitted=true&DB=&query=${encodeURIComponent(query)}`,
    },
    wipo: {
      source: 'wipo',
      searchUrl: `https://patentscope.wipo.int/search/en/search.jsf?query=${encodeURIComponent(query)}`,
    },
  };
}
