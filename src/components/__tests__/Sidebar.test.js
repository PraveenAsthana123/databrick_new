import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import Sidebar from '../Sidebar';

describe('Sidebar', () => {
  const mockNavigate = jest.fn();

  beforeEach(() => {
    mockNavigate.mockClear();
  });

  test('renders all menu sections', () => {
    render(<Sidebar activePage="dashboard" onNavigate={mockNavigate} collapsed={false} />);
    expect(screen.getByText(/Overview/i)).toBeInTheDocument();
    expect(screen.getByText(/Infrastructure/i)).toBeInTheDocument();
    expect(screen.getByText(/System/i)).toBeInTheDocument();
  });

  test('renders navigation items', () => {
    render(<Sidebar activePage="dashboard" onNavigate={mockNavigate} collapsed={false} />);
    expect(screen.getByText(/Dashboard/i)).toBeInTheDocument();
    expect(screen.getByText(/Settings/i)).toBeInTheDocument();
  });

  test('highlights active page', () => {
    render(<Sidebar activePage="dashboard" onNavigate={mockNavigate} collapsed={false} />);
    const dashboardBtn = screen.getByTitle('Dashboard');
    expect(dashboardBtn).toHaveClass('active');
  });

  test('calls onNavigate when item clicked', async () => {
    render(<Sidebar activePage="dashboard" onNavigate={mockNavigate} collapsed={false} />);
    const settingsBtn = screen.getByTitle('Settings');
    await userEvent.click(settingsBtn);
    expect(mockNavigate).toHaveBeenCalledWith('settings');
  });

  test('renders collapsed state', () => {
    const { container } = render(
      <Sidebar activePage="dashboard" onNavigate={mockNavigate} collapsed={true} />
    );
    // eslint-disable-next-line testing-library/no-node-access
    const sidebar = container.querySelector('.sidebar');
    expect(sidebar).toHaveClass('collapsed');
  });

  test('has at least 20 navigation items', () => {
    render(<Sidebar activePage="dashboard" onNavigate={mockNavigate} collapsed={false} />);
    const items = screen.getAllByRole('button');
    expect(items.length).toBeGreaterThanOrEqual(20);
  });
});
