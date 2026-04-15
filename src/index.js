import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import { errorTracker } from './utils/errorTracker';

// Initialize error tracking (console, DOM, CSS, network, performance)
if (process.env.NODE_ENV === 'development') {
  errorTracker.init();
  // Expose to browser console for debugging: window.__errors.getSummary()
  window.__errors = errorTracker;
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

reportWebVitals();
