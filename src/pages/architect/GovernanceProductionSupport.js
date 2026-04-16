import React from 'react';
import ProductionSupportLibrary from '../../components/architect/ProductionSupportLibrary';

const LEVELS = {
  L1: [
    {
      role: 'Support Analyst',
      issueType: 'Access Denied',
      description: 'User cannot access table',
      scenario: 'Analyst blocked from dataset',
      impact: 'Work delay',
      rootCause: 'Missing permission',
      resolution: 'Grant access (as per policy)',
      tools: 'Unity Catalog',
    },
    {
      role: 'Support Analyst',
      issueType: 'Unauthorized Access Alert',
      description: 'Alert triggered for suspicious access',
      scenario: 'Multiple login attempts',
      impact: 'Security risk',
      rootCause: 'Unknown',
      resolution: 'Escalate to L2',
      tools: 'SIEM',
    },
    {
      role: 'Support Analyst',
      issueType: 'Missing Table/View',
      description: 'User cannot see dataset',
      scenario: 'Table not visible in catalog',
      impact: 'Confusion',
      rootCause: 'Metadata issue',
      resolution: 'Refresh metadata',
      tools: 'UC',
    },
    {
      role: 'Support Analyst',
      issueType: 'Data Not Visible',
      description: 'Data exists but not accessible',
      scenario: 'RLS/CLS blocking data',
      impact: 'User frustration',
      rootCause: 'Policy restriction',
      resolution: 'Validate policy',
      tools: 'UC',
    },
    {
      role: 'Support Analyst',
      issueType: 'Audit Alert',
      description: 'Audit log shows anomaly',
      scenario: 'Unusual access pattern',
      impact: 'Risk',
      rootCause: 'Unknown',
      resolution: 'Escalate to L2',
      tools: 'Audit logs',
    },
  ],
  L2: [
    {
      role: 'Data Engineer',
      issueType: 'RBAC Misconfiguration',
      description: 'Wrong role assignment',
      scenario: 'User sees restricted data',
      impact: 'Security breach',
      rootCause: 'Incorrect role mapping',
      resolution: 'Fix RBAC roles',
      tools: 'Unity Catalog',
    },
    {
      role: 'Data Engineer',
      issueType: 'Row-Level Security Issue',
      description: 'Incorrect filtering',
      scenario: 'Manager sees all regions',
      impact: 'Compliance issue',
      rootCause: 'Wrong RLS logic',
      resolution: 'Fix RLS rule',
      tools: 'UC',
    },
    {
      role: 'Data Engineer',
      issueType: 'Column Masking Issue',
      description: 'Sensitive data visible',
      scenario: 'Salary column exposed',
      impact: 'Privacy risk',
      rootCause: 'Masking not applied',
      resolution: 'Apply masking policy',
      tools: 'UC',
    },
    {
      role: 'Data Engineer',
      issueType: 'Data Classification Missing',
      description: 'PII not tagged',
      scenario: 'Sensitive data unprotected',
      impact: 'Compliance risk',
      rootCause: 'No classification',
      resolution: 'Tag data properly',
      tools: 'Collibra',
    },
    {
      role: 'Data Engineer',
      issueType: 'Lineage Missing',
      description: 'Cannot trace data source',
      scenario: 'Debug failure',
      impact: 'Delay',
      rootCause: 'Lineage not enabled',
      resolution: 'Enable lineage',
      tools: 'UC',
    },
  ],
  L3: [
    {
      role: 'Senior Engineer',
      issueType: 'Policy Conflict',
      description: 'Multiple policies conflict',
      scenario: 'Access denied unexpectedly',
      impact: 'Business disruption',
      rootCause: 'Overlapping rules',
      resolution: 'Resolve policy hierarchy',
      tools: 'UC',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Audit Failure',
      description: 'Missing audit logs',
      scenario: 'Cannot track access',
      impact: 'Compliance failure',
      rootCause: 'Logging disabled',
      resolution: 'Enable audit logging',
      tools: 'SIEM',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Data Leakage',
      description: 'Sensitive data exposed',
      scenario: 'PII in reports',
      impact: 'Major breach',
      rootCause: 'Weak governance',
      resolution: 'Apply masking/encryption',
      tools: 'UC + KMS',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Metadata Corruption',
      description: 'Wrong metadata',
      scenario: 'Table definitions incorrect',
      impact: 'Confusion',
      rootCause: 'Catalog issue',
      resolution: 'Fix metadata',
      tools: 'Catalog tools',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Access Escalation Issue',
      description: 'Privilege escalation',
      scenario: 'User gains higher access',
      impact: 'Security risk',
      rootCause: 'IAM misconfig',
      resolution: 'Fix IAM policies',
      tools: 'IAM tools',
    },
  ],
  L4: [
    {
      role: 'Architect',
      issueType: 'No Governance Framework',
      description: 'No enterprise governance model',
      scenario: 'Teams define own rules',
      impact: 'Chaos',
      rootCause: 'Missing framework',
      resolution: 'Define governance model',
      tools: 'UC + GRC',
    },
    {
      role: 'Architect',
      issueType: 'No Data Ownership',
      description: 'No data owner defined',
      scenario: 'Issues unresolved',
      impact: 'Accountability gap',
      rootCause: 'No ownership model',
      resolution: 'Define data ownership',
      tools: 'Governance model',
    },
    {
      role: 'Architect',
      issueType: 'Multi-Cloud Governance Gap',
      description: 'Different rules across cloud',
      scenario: 'Azure vs AWS mismatch',
      impact: 'Compliance risk',
      rootCause: 'No unified governance',
      resolution: 'Central governance',
      tools: 'GRC',
    },
    {
      role: 'Architect',
      issueType: 'No Data Catalog Strategy',
      description: 'Metadata not centralized',
      scenario: 'Data discovery failure',
      impact: 'Low adoption',
      rootCause: 'No catalog',
      resolution: 'Implement catalog',
      tools: 'Collibra',
    },
    {
      role: 'Architect',
      issueType: 'Compliance Misalignment',
      description: 'Not aligned with regulations',
      scenario: 'GDPR/PII violation',
      impact: 'Legal risk',
      rootCause: 'No compliance mapping',
      resolution: 'Implement compliance framework',
      tools: 'ISO/NIST',
    },
  ],
};

const FLOW = `Access Issue / Alert (L1)
  ↓
Policy Investigation (L2)
  ↓
Security / Compliance Fix (L3)
  ↓
Governance Architecture Redesign (L4)`;

export default function GovernanceProductionSupport() {
  return (
    <ProductionSupportLibrary
      pageTitle="Data Governance — Production Support (L1 → L4)"
      pageSubtitle="L1-L4 support playbook for Unity Catalog, RBAC/ABAC, lineage, audit, compliance, PII handling."
      coverage="Unity Catalog · RBAC / ABAC · Data lineage, audit, compliance · PII / sensitive data handling"
      levels={LEVELS}
      csvName="governance-production-support.csv"
      flow={FLOW}
    />
  );
}
