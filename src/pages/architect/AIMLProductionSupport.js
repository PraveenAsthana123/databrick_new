import React from 'react';
import ProductionSupportLibrary from '../../components/architect/ProductionSupportLibrary';

const LEVELS = {
  L1: [
    {
      role: 'Support Analyst',
      issueType: 'API Failure',
      description: 'AI API not responding',
      scenario: 'Chatbot down',
      impact: 'Business disruption',
      rootCause: 'Service issue',
      resolution: 'Restart service',
      tools: 'API Gateway',
    },
    {
      role: 'Support Analyst',
      issueType: 'Slow Response',
      description: 'LLM taking too long',
      scenario: 'Chatbot delay',
      impact: 'Poor UX',
      rootCause: 'High latency',
      resolution: 'Restart / scale',
      tools: 'Monitoring',
    },
    {
      role: 'Support Analyst',
      issueType: 'Wrong Output',
      description: 'User complains incorrect answer',
      scenario: 'Chatbot hallucination',
      impact: 'Trust issue',
      rootCause: 'Unknown',
      resolution: 'Escalate to L2',
      tools: 'Logs',
    },
    {
      role: 'Support Analyst',
      issueType: 'Pipeline Failure',
      description: 'RAG pipeline not working',
      scenario: 'Retrieval not triggered',
      impact: 'No response',
      rootCause: 'Job failure',
      resolution: 'Restart pipeline',
      tools: 'Jobs',
    },
    {
      role: 'Support Analyst',
      issueType: 'Alert Triggered',
      description: 'Model monitoring alert',
      scenario: 'Accuracy drop alert',
      impact: 'Risk',
      rootCause: 'Unknown',
      resolution: 'Escalate',
      tools: 'Monitoring',
    },
  ],
  L2: [
    {
      role: 'ML Engineer',
      issueType: 'Retrieval Failure',
      description: 'Wrong documents retrieved',
      scenario: 'RAG returns irrelevant docs',
      impact: 'Wrong answer',
      rootCause: 'Poor embedding',
      resolution: 'Improve retrieval',
      tools: 'Vector DB',
    },
    {
      role: 'ML Engineer',
      issueType: 'Prompt Issue',
      description: 'Poor prompt design',
      scenario: 'Inconsistent responses',
      impact: 'Low quality',
      rootCause: 'Weak prompt',
      resolution: 'Optimize prompt',
      tools: 'Prompt tuning',
    },
    {
      role: 'ML Engineer',
      issueType: 'Data Issue',
      description: 'Training data problem',
      scenario: 'Model biased output',
      impact: 'Risk',
      rootCause: 'Poor dataset',
      resolution: 'Clean data',
      tools: 'Data pipeline',
    },
    {
      role: 'ML Engineer',
      issueType: 'Token Limit Issue',
      description: 'Input truncated',
      scenario: 'Missing context',
      impact: 'Wrong output',
      rootCause: 'Token overflow',
      resolution: 'Chunking strategy',
      tools: 'LLM tools',
    },
    {
      role: 'ML Engineer',
      issueType: 'API Rate Limit',
      description: 'Too many requests',
      scenario: 'API throttling',
      impact: 'Delay',
      rootCause: 'No rate control',
      resolution: 'Implement throttling',
      tools: 'API mgmt',
    },
  ],
  L3: [
    {
      role: 'Senior ML Engineer',
      issueType: 'Model Drift',
      description: 'Model accuracy drops',
      scenario: 'Predictions degrade over time',
      impact: 'Business risk',
      rootCause: 'Data drift',
      resolution: 'Retrain model',
      tools: 'MLflow',
    },
    {
      role: 'Senior ML Engineer',
      issueType: 'Hallucination',
      description: 'LLM generates false info',
      scenario: 'Wrong medical advice',
      impact: 'High risk',
      rootCause: 'No grounding',
      resolution: 'Improve RAG + guardrails',
      tools: 'RAG',
    },
    {
      role: 'Senior ML Engineer',
      issueType: 'Embedding Issue',
      description: 'Poor similarity search',
      scenario: 'Wrong retrieval',
      impact: 'Wrong answers',
      rootCause: 'Bad embeddings',
      resolution: 'Rebuild embeddings',
      tools: 'Vector DB',
    },
    {
      role: 'Senior ML Engineer',
      issueType: 'Performance Issue',
      description: 'High latency inference',
      scenario: 'Slow chatbot',
      impact: 'UX issue',
      rootCause: 'Large model',
      resolution: 'Optimize model',
      tools: 'GPU / caching',
    },
    {
      role: 'Senior ML Engineer',
      issueType: 'Evaluation Gap',
      description: 'No model evaluation',
      scenario: 'Cannot measure quality',
      impact: 'Risk',
      rootCause: 'No metrics',
      resolution: 'Add evaluation framework',
      tools: 'MLflow',
    },
  ],
  L4: [
    {
      role: 'AI Architect',
      issueType: 'No RAG Architecture',
      description: 'Direct LLM use',
      scenario: 'Hallucinations',
      impact: 'Trust loss',
      rootCause: 'No retrieval layer',
      resolution: 'Implement RAG',
      tools: 'LangChain',
    },
    {
      role: 'AI Architect',
      issueType: 'No Guardrails',
      description: 'Unsafe outputs',
      scenario: 'Harmful responses',
      impact: 'Compliance risk',
      rootCause: 'No safety layer',
      resolution: 'Add guardrails',
      tools: 'Moderation APIs',
    },
    {
      role: 'AI Architect',
      issueType: 'No LLMOps',
      description: 'No monitoring/versioning',
      scenario: 'Model instability',
      impact: 'Risk',
      rootCause: 'No lifecycle mgmt',
      resolution: 'Implement LLMOps',
      tools: 'MLflow',
    },
    {
      role: 'AI Architect',
      issueType: 'No Evaluation Strategy',
      description: 'No quality tracking',
      scenario: 'Unknown accuracy',
      impact: 'Risk',
      rootCause: 'No metrics',
      resolution: 'Evaluation framework',
      tools: 'AI tools',
    },
    {
      role: 'AI Architect',
      issueType: 'No Responsible AI',
      description: 'Bias/unethical outputs',
      scenario: 'Legal risk',
      impact: 'Compliance issue',
      rootCause: 'No governance',
      resolution: 'Responsible AI framework',
      tools: 'ISO/NIST',
    },
  ],
};

const FLOW = `User / Alert Issue (L1)
  ↓
Pipeline / Prompt / Data Fix (L2)
  ↓
Model Optimization / Evaluation (L3)
  ↓
AI Architecture / Governance Design (L4)`;

export default function AIMLProductionSupport() {
  return (
    <ProductionSupportLibrary
      pageTitle="AI / ML / GenAI — Production Support (L1 → L4)"
      pageSubtitle="L1-L4 support playbook for RAG pipelines, ML models, LLMOps/MLOps, guardrails, evaluation, drift."
      coverage="RAG pipelines (retrieval + LLM) · ML models (training + inference) · LLMOps / MLOps · Guardrails, evaluation, drift"
      levels={LEVELS}
      csvName="ai-ml-production-support.csv"
      flow={FLOW}
    />
  );
}
