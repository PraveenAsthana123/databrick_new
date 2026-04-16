import React from 'react';
import ProductionSupportLibrary from '../../components/architect/ProductionSupportLibrary';

const LEVELS = {
  L1: [
    {
      role: 'Support Analyst',
      issueType: 'Login Failure',
      description: 'User cannot log in',
      scenario: 'Analyst blocked from Databricks',
      impact: 'Work delay',
      rootCause: 'Credential issue',
      resolution: 'Reset credentials',
      tools: 'IAM / Azure AD',
    },
    {
      role: 'Support Analyst',
      issueType: 'Access Denied',
      description: 'User cannot access resource',
      scenario: 'Table access blocked',
      impact: 'Delay',
      rootCause: 'Missing permission',
      resolution: 'Request access',
      tools: 'Unity Catalog',
    },
    {
      role: 'Support Analyst',
      issueType: 'Suspicious Login Alert',
      description: 'Login from unknown location',
      scenario: 'Multiple login attempts',
      impact: 'Security risk',
      rootCause: 'Unknown',
      resolution: 'Escalate to L2',
      tools: 'SIEM',
    },
    {
      role: 'Support Analyst',
      issueType: 'Expired Token',
      description: 'Session expired',
      scenario: 'API call fails',
      impact: 'Pipeline issue',
      rootCause: 'Token expiry',
      resolution: 'Renew token',
      tools: 'Auth service',
    },
    {
      role: 'Support Analyst',
      issueType: 'Basic Alert',
      description: 'Security alert triggered',
      scenario: 'Alert from monitoring system',
      impact: 'Risk',
      rootCause: 'Unknown',
      resolution: 'Escalate',
      tools: 'Monitoring tools',
    },
  ],
  L2: [
    {
      role: 'Security Engineer',
      issueType: 'IAM Misconfiguration',
      description: 'Incorrect roles assigned',
      scenario: 'User has excessive access',
      impact: 'Security risk',
      rootCause: 'Wrong role mapping',
      resolution: 'Fix IAM roles',
      tools: 'IAM',
    },
    {
      role: 'Security Engineer',
      issueType: 'Secret Exposure',
      description: 'Credentials exposed in code',
      scenario: 'API key visible in notebook',
      impact: 'Breach risk',
      rootCause: 'Poor secret handling',
      resolution: 'Move to vault',
      tools: 'Key Vault',
    },
    {
      role: 'Security Engineer',
      issueType: 'Network Access Issue',
      description: 'Unauthorized network access',
      scenario: 'Public endpoint exposed',
      impact: 'Risk',
      rootCause: 'No network restriction',
      resolution: 'Apply private endpoints',
      tools: 'VNet',
    },
    {
      role: 'Security Engineer',
      issueType: 'Encryption Missing',
      description: 'Data not encrypted',
      scenario: 'Raw data exposed',
      impact: 'Compliance risk',
      rootCause: 'No encryption config',
      resolution: 'Enable encryption',
      tools: 'KMS',
    },
    {
      role: 'Security Engineer',
      issueType: 'API Security Issue',
      description: 'API not secured',
      scenario: 'Open endpoint access',
      impact: 'Risk',
      rootCause: 'No auth control',
      resolution: 'Secure API',
      tools: 'API Gateway',
    },
  ],
  L3: [
    {
      role: 'Security Engineer',
      issueType: 'Data Breach',
      description: 'Sensitive data exposed',
      scenario: 'PII leaked in logs',
      impact: 'Major risk',
      rootCause: 'Weak masking',
      resolution: 'Mask/tokenize data',
      tools: 'Unity Catalog',
    },
    {
      role: 'Security Engineer',
      issueType: 'Privilege Escalation',
      description: 'User gains admin rights',
      scenario: 'Unauthorized admin access',
      impact: 'Severe breach',
      rootCause: 'IAM flaw',
      resolution: 'Fix privilege model',
      tools: 'IAM',
    },
    {
      role: 'Security Engineer',
      issueType: 'Insider Threat',
      description: 'Internal misuse of data',
      scenario: 'Employee downloads data',
      impact: 'Compliance risk',
      rootCause: 'Weak monitoring',
      resolution: 'Monitor + restrict',
      tools: 'SIEM',
    },
    {
      role: 'Security Engineer',
      issueType: 'Injection Attack',
      description: 'Malicious input exploited',
      scenario: 'SQL injection / prompt injection',
      impact: 'Risk',
      rootCause: 'No input validation',
      resolution: 'Sanitize inputs',
      tools: 'Security filters',
    },
    {
      role: 'Security Engineer',
      issueType: 'Audit Failure',
      description: 'Missing logs',
      scenario: 'Cannot track access',
      impact: 'Compliance failure',
      rootCause: 'Logging disabled',
      resolution: 'Enable audit logging',
      tools: 'SIEM',
    },
  ],
  L4: [
    {
      role: 'Architect',
      issueType: 'No Zero Trust Model',
      description: 'Open access system',
      scenario: 'Internal network trusted blindly',
      impact: 'High risk',
      rootCause: 'Legacy design',
      resolution: 'Implement zero trust',
      tools: 'IAM + network',
    },
    {
      role: 'Architect',
      issueType: 'Weak Security Architecture',
      description: 'Fragmented security controls',
      scenario: 'Multiple tools unmanaged',
      impact: 'Risk',
      rootCause: 'No unified design',
      resolution: 'Central security architecture',
      tools: 'Security framework',
    },
    {
      role: 'Architect',
      issueType: 'No Secrets Management Strategy',
      description: 'Secrets scattered',
      scenario: 'Keys leaked across systems',
      impact: 'Breach risk',
      rootCause: 'No vault usage',
      resolution: 'Central secrets mgmt',
      tools: 'Key Vault',
    },
    {
      role: 'Architect',
      issueType: 'No Data Protection Strategy',
      description: 'Sensitive data unprotected',
      scenario: 'PII exposed',
      impact: 'Compliance risk',
      rootCause: 'No data classification',
      resolution: 'Data protection model',
      tools: 'UC + DLP',
    },
    {
      role: 'Architect',
      issueType: 'Compliance Gap',
      description: 'Not aligned with regulations',
      scenario: 'GDPR violation',
      impact: 'Legal penalty',
      rootCause: 'No compliance framework',
      resolution: 'Implement compliance model',
      tools: 'ISO / NIST',
    },
  ],
};

const FLOW = `Security Alert / Access Issue (L1)
  ↓
Configuration Investigation (L2)
  ↓
Threat Mitigation (L3)
  ↓
Security Architecture Redesign (L4)`;

export default function SecurityProductionSupport() {
  return (
    <ProductionSupportLibrary
      pageTitle="Security — Production Support (L1 → L4)"
      pageSubtitle="L1-L4 support playbook for IAM, Unity Catalog security, encryption, secrets, network, PII, compliance, zero trust."
      coverage="IAM (Azure AD / AWS IAM) · Unity Catalog security · Encryption, secrets, network · PII / compliance / zero trust"
      levels={LEVELS}
      csvName="security-production-support.csv"
      flow={FLOW}
    />
  );
}
