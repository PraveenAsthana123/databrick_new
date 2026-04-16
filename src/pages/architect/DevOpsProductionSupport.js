import React from 'react';
import ProductionSupportLibrary from '../../components/architect/ProductionSupportLibrary';

const LEVELS = {
  L1: [
    {
      role: 'Support Analyst',
      issueType: 'Build Failure',
      description: 'Pipeline build failed',
      scenario: 'GitHub Actions failed',
      impact: 'Deployment blocked',
      rootCause: 'Syntax error',
      resolution: 'Re-run build',
      tools: 'GitHub Actions',
    },
    {
      role: 'Support Analyst',
      issueType: 'Deployment Failure',
      description: 'Code not deployed',
      scenario: 'Job not updated in Databricks',
      impact: 'Delay',
      rootCause: 'Pipeline issue',
      resolution: 'Retry deployment',
      tools: 'CI/CD tool',
    },
    {
      role: 'Support Analyst',
      issueType: 'Job Not Triggered',
      description: 'Scheduled job not running',
      scenario: 'Daily job skipped',
      impact: 'Data delay',
      rootCause: 'Scheduler issue',
      resolution: 'Trigger manually',
      tools: 'Databricks Jobs',
    },
    {
      role: 'Support Analyst',
      issueType: 'Environment Issue',
      description: 'Wrong environment used',
      scenario: 'Dev code in Prod',
      impact: 'Risk',
      rootCause: 'Misconfiguration',
      resolution: 'Correct env config',
      tools: 'Pipeline config',
    },
    {
      role: 'Support Analyst',
      issueType: 'Alert Triggered',
      description: 'CI/CD alert received',
      scenario: 'Failure notification',
      impact: 'Needs action',
      rootCause: 'Unknown',
      resolution: 'Escalate to L2',
      tools: 'Monitoring',
    },
  ],
  L2: [
    {
      role: 'DevOps Engineer',
      issueType: 'Config Error',
      description: 'Wrong pipeline config',
      scenario: 'Wrong path / variable',
      impact: 'Deployment failure',
      rootCause: 'Misconfiguration',
      resolution: 'Fix config',
      tools: 'YAML',
    },
    {
      role: 'DevOps Engineer',
      issueType: 'Dependency Issue',
      description: 'Missing library',
      scenario: 'Job fails at runtime',
      impact: 'Pipeline break',
      rootCause: 'Missing dependency',
      resolution: 'Add dependency',
      tools: 'Requirements',
    },
    {
      role: 'DevOps Engineer',
      issueType: 'Version Conflict',
      description: 'Wrong package version',
      scenario: 'Works in Dev, fails in Prod',
      impact: 'Instability',
      rootCause: 'Version mismatch',
      resolution: 'Fix versioning',
      tools: 'Package mgmt',
    },
    {
      role: 'DevOps Engineer',
      issueType: 'Secret Issue',
      description: 'Secret not accessible',
      scenario: 'API key missing',
      impact: 'Pipeline failure',
      rootCause: 'Vault issue',
      resolution: 'Fix secret access',
      tools: 'Key Vault',
    },
    {
      role: 'DevOps Engineer',
      issueType: 'Permission Issue',
      description: 'Pipeline lacks access',
      scenario: 'Cannot deploy job',
      impact: 'Delay',
      rootCause: 'IAM issue',
      resolution: 'Grant permissions',
      tools: 'IAM',
    },
  ],
  L3: [
    {
      role: 'Senior DevOps',
      issueType: 'CI/CD Design Issue',
      description: 'Pipeline not scalable',
      scenario: 'Multiple pipelines failing',
      impact: 'Delay',
      rootCause: 'Poor design',
      resolution: 'Redesign pipeline',
      tools: 'CI/CD',
    },
    {
      role: 'Senior DevOps',
      issueType: 'Rollback Failure',
      description: 'Cannot rollback deployment',
      scenario: 'Bad release in Prod',
      impact: 'Downtime',
      rootCause: 'No rollback strategy',
      resolution: 'Implement rollback',
      tools: 'Git',
    },
    {
      role: 'Senior DevOps',
      issueType: 'Environment Drift',
      description: 'Dev/QA/Prod mismatch',
      scenario: 'Works in Dev only',
      impact: 'Risk',
      rootCause: 'Config inconsistency',
      resolution: 'Standardize env',
      tools: 'Terraform',
    },
    {
      role: 'Senior DevOps',
      issueType: 'Artifact Issue',
      description: 'Wrong artifact deployed',
      scenario: 'Old version deployed',
      impact: 'Incorrect results',
      rootCause: 'Artifact mismatch',
      resolution: 'Fix artifact mgmt',
      tools: 'Repo',
    },
    {
      role: 'Senior DevOps',
      issueType: 'Parallel Deployment Issue',
      description: 'Multiple deployments conflict',
      scenario: 'Job override',
      impact: 'Instability',
      rootCause: 'No locking',
      resolution: 'Add deployment control',
      tools: 'CI/CD',
    },
  ],
  L4: [
    {
      role: 'Architect',
      issueType: 'No CI/CD Strategy',
      description: 'Manual deployments',
      scenario: 'Frequent errors',
      impact: 'Risk',
      rootCause: 'No automation',
      resolution: 'Implement CI/CD model',
      tools: 'GitHub Actions',
    },
    {
      role: 'Architect',
      issueType: 'No Environment Strategy',
      description: 'No Dev/QA/Prod separation',
      scenario: 'Testing in Prod',
      impact: 'Risk',
      rootCause: 'No env design',
      resolution: 'Define env architecture',
      tools: 'Terraform',
    },
    {
      role: 'Architect',
      issueType: 'No Version Control Strategy',
      description: 'No code versioning',
      scenario: 'Code conflicts',
      impact: 'Chaos',
      rootCause: 'No Git governance',
      resolution: 'Implement Git model',
      tools: 'Git',
    },
    {
      role: 'Architect',
      issueType: 'No Release Management',
      description: 'No structured releases',
      scenario: 'Frequent failures',
      impact: 'Instability',
      rootCause: 'No release plan',
      resolution: 'Release framework',
      tools: 'DevOps',
    },
    {
      role: 'Architect',
      issueType: 'No IaC Strategy',
      description: 'Infra manually managed',
      scenario: 'Inconsistent infra',
      impact: 'Risk',
      rootCause: 'No automation',
      resolution: 'Implement IaC',
      tools: 'Terraform',
    },
  ],
};

const FLOW = `Build/Deploy Issue (L1)
  ↓
Pipeline Debug (L2)
  ↓
Pipeline Design Fix (L3)
  ↓
DevOps Architecture Redesign (L4)`;

export default function DevOpsProductionSupport() {
  return (
    <ProductionSupportLibrary
      pageTitle="DevOps / CI-CD — Production Support (L1 → L4)"
      pageSubtitle="L1-L4 support playbook for CI/CD pipelines, Databricks Repos/Jobs deployment, IaC (Terraform), release management."
      coverage="CI/CD pipelines (GitHub Actions / Azure DevOps) · Databricks Repos / Jobs deployment · IaC (Terraform) · Release management"
      levels={LEVELS}
      csvName="devops-production-support.csv"
      flow={FLOW}
    />
  );
}
