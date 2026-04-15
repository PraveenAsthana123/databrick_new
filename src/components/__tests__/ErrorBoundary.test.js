import { render, screen } from '@testing-library/react';
import ErrorBoundary from '../common/ErrorBoundary';

// Component that throws an error
function BrokenComponent() {
  throw new Error('Test error');
}

function WorkingComponent() {
  return <div>Working fine</div>;
}

describe('ErrorBoundary', () => {
  // Suppress console.error for expected errors
  const originalError = console.error; // eslint-disable-line no-console
  beforeAll(() => {
    console.error = jest.fn(); // eslint-disable-line no-console
  });
  afterAll(() => {
    console.error = originalError; // eslint-disable-line no-console
  });

  test('renders children when no error', () => {
    render(
      <ErrorBoundary>
        <WorkingComponent />
      </ErrorBoundary>
    );
    expect(screen.getByText('Working fine')).toBeInTheDocument();
  });

  test('catches errors and shows fallback', () => {
    render(
      <ErrorBoundary>
        <BrokenComponent />
      </ErrorBoundary>
    );
    expect(screen.getByText(/Something went wrong/i)).toBeInTheDocument();
    expect(screen.getByText('Test error')).toBeInTheDocument();
  });

  test('shows Try Again button', () => {
    render(
      <ErrorBoundary>
        <BrokenComponent />
      </ErrorBoundary>
    );
    expect(screen.getByRole('button', { name: /Try Again/i })).toBeInTheDocument();
  });
});
