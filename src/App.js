import { useState, lazy, Suspense } from 'react';
import './App.css';
import Sidebar from './components/Sidebar';

// Lazy load all pages for performance
const Dashboard = lazy(() => import('./pages/Dashboard'));
const Medallion = lazy(() => import('./pages/Medallion'));
const LandingZone = lazy(() => import('./pages/LandingZone'));
const Ingestion = lazy(() => import('./pages/Ingestion'));
const BatchIngestion = lazy(() => import('./pages/ingestion/BatchIngestion'));
const BatchPipelines = lazy(() => import('./pages/ingestion/BatchPipelines'));
const GovernanceScenarios = lazy(() => import('./pages/governance/GovernanceScenarios'));
const ETLScenarios = lazy(() => import('./pages/transformation/ETLScenarios'));
const ELTScenarios = lazy(() => import('./pages/transformation/ELTScenarios'));
const StreamIngestion = lazy(() => import('./pages/ingestion/StreamIngestion'));
const SecurityPIIScenarios = lazy(() => import('./pages/security/SecurityPIIScenarios'));
const BronzeOperations = lazy(() => import('./pages/medallion/BronzeOperations'));
const DataArchitectChallenges = lazy(() => import('./pages/architect/DataArchitectChallenges'));
const SilverOperations = lazy(() => import('./pages/medallion/SilverOperations'));
const GoldOperations = lazy(() => import('./pages/medallion/GoldOperations'));
const Modeling = lazy(() => import('./pages/Modeling'));
const UnityCatalog = lazy(() => import('./pages/UnityCatalog'));
const Visualization = lazy(() => import('./pages/Visualization'));
const ELTOperations = lazy(() => import('./pages/ELTOperations'));
const PipelineBuilder = lazy(() => import('./pages/PipelineBuilder'));
const DataTesting = lazy(() => import('./pages/DataTesting'));
const SecurityGovernance = lazy(() => import('./pages/SecurityGovernance'));
const XAI = lazy(() => import('./pages/XAI'));
const RAGIntegration = lazy(() => import('./pages/RAGIntegration'));
const TerraformAzure = lazy(() => import('./pages/TerraformAzure'));
const Clusters = lazy(() => import('./pages/Clusters'));
const Notebooks = lazy(() => import('./pages/Notebooks'));
const Jobs = lazy(() => import('./pages/Jobs'));
const SparkUI = lazy(() => import('./pages/SparkUI'));
const DataStorage = lazy(() => import('./pages/DataStorage'));
const UploadDocs = lazy(() => import('./pages/UploadDocs'));
const DownloadData = lazy(() => import('./pages/DownloadData'));
const SimulationTools = lazy(() => import('./pages/SimulationTools'));
const Settings = lazy(() => import('./pages/Settings'));

function App() {
  const [activePage, setActivePage] = useState('dashboard');
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  const renderPage = () => {
    switch (activePage) {
      case 'dashboard':
        return <Dashboard />;
      case 'medallion':
        return <Medallion />;
      case 'bronze-ops':
        return <BronzeOperations />;
      case 'silver-ops':
        return <SilverOperations />;
      case 'gold-ops':
        return <GoldOperations />;
      case 'landing-zone':
        return <LandingZone />;
      case 'ingestion-batch':
        return <BatchIngestion />;
      case 'batch-pipelines':
        return <BatchPipelines />;
      case 'stream-scenarios':
        return <StreamIngestion />;
      case 'ingestion-stream':
        return <Ingestion filter="stream" />;
      case 'ingestion-all':
        return <Ingestion filter="all" />;
      case 'elt-scenarios':
        return <ELTScenarios />;
      case 'elt-operations':
        return <ELTOperations filter="elt" />;
      case 'etl-scenarios':
        return <ETLScenarios />;
      case 'etl-operations':
        return <ELTOperations filter="etl" />;
      case 'modeling':
        return <Modeling />;
      case 'unity-catalog':
        return <UnityCatalog />;
      case 'visualization':
        return <Visualization />;
      case 'pipelines':
        return <PipelineBuilder />;
      case 'data-testing':
        return <DataTesting />;
      case 'governance-scenarios':
        return <GovernanceScenarios />;
      case 'security-pii':
        return <SecurityPIIScenarios />;
      case 'security':
        return <SecurityGovernance />;
      case 'xai':
        return <XAI />;
      case 'rag':
        return <RAGIntegration />;
      case 'terraform':
        return <TerraformAzure />;
      case 'clusters':
        return <Clusters />;
      case 'notebooks':
        return <Notebooks />;
      case 'jobs':
        return <Jobs />;
      case 'spark-ui':
        return <SparkUI />;
      case 'data-storage':
        return <DataStorage />;
      case 'upload-docs':
        return <UploadDocs />;
      case 'download-data':
        return <DownloadData />;
      case 'simulation':
        return <SimulationTools />;
      case 'architect-challenges':
        return <DataArchitectChallenges />;
      case 'settings':
        return <Settings />;
      default:
        return <Dashboard />;
    }
  };

  return (
    <div className="app">
      <header className="topbar">
        <button className="topbar-toggle" onClick={() => setSidebarCollapsed(!sidebarCollapsed)}>
          ☰
        </button>
        <div className="topbar-brand">
          <span className="brand-icon">◆</span> Databricks PySpark Environment
        </div>
        <div className="topbar-actions">
          <span className="cluster-status">
            <span className="status-dot running"></span> Cluster Active
          </span>
          <span className="topbar-user">⚙ Admin</span>
        </div>
      </header>
      <div className="app-body">
        <Sidebar activePage={activePage} onNavigate={setActivePage} collapsed={sidebarCollapsed} />
        <main className={`main-content ${sidebarCollapsed ? 'expanded' : ''}`}>
          <Suspense
            fallback={<div style={{ padding: '2rem', textAlign: 'center' }}>Loading...</div>}
          >
            {renderPage()}
          </Suspense>
        </main>
      </div>
    </div>
  );
}

export default App;
