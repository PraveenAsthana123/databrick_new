import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import App from './App';

describe('App', () => {
  test('renders without crashing', () => {
    render(<App />);
  });

  test('renders topbar with branding', () => {
    render(<App />);
    const elements = screen.getAllByText(/Databricks PySpark Environment/i);
    expect(elements.length).toBeGreaterThanOrEqual(1);
  });

  test('renders sidebar navigation', () => {
    render(<App />);
    expect(screen.getByText(/Dashboard/i)).toBeInTheDocument();
  });

  test('renders cluster status', () => {
    render(<App />);
    expect(screen.getByText(/Cluster Active/i)).toBeInTheDocument();
  });

  test('sidebar toggle button exists', () => {
    render(<App />);
    const toggleBtn = screen.getByRole('button', { name: /☰/i });
    expect(toggleBtn).toBeInTheDocument();
  });

  test('sidebar collapses when toggle clicked', async () => {
    const { container } = render(<App />);
    const toggleBtn = screen.getByRole('button', { name: /☰/i });
    await userEvent.click(toggleBtn);
    // eslint-disable-next-line testing-library/no-node-access
    const sidebar = container.querySelector('.sidebar');
    expect(sidebar).toHaveClass('collapsed');
  });

  test('default page is Dashboard', () => {
    render(<App />);
    expect(screen.getByText(/Dashboard/i)).toBeInTheDocument();
  });
});
