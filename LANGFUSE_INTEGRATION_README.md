# Langfuse Integration for NLP-to-SQL Project

This document explains how Langfuse observability has been integrated into your NLP-to-SQL application to provide comprehensive monitoring and analytics for your AI operations.

## What is Langfuse?

Langfuse is an open-source observability and analytics platform designed specifically for Large Language Model (LLM) applications. It provides:

- **Tracing**: Track the complete execution flow of your AI operations
- **Monitoring**: Real-time visibility into your LLM usage and performance
- **Analytics**: Detailed insights into costs, latency, and quality metrics
- **Debugging**: Identify and resolve issues in your AI workflows
- **Evaluation**: Score and evaluate your AI outputs for quality assurance

## Integration Overview

The Langfuse integration has been added to the following key components:

### 1. Core Configuration (`langfuse_config.py`)
- **LangfuseManager**: Centralized management of Langfuse initialization
- **Callback Handlers**: LangChain integration for automatic tracing
- **Helper Functions**: Simplified API for creating traces and observations
- **Graceful Degradation**: The system continues to work even if Langfuse is not configured

### 2. AI Engine Components
- **SmartSQLGenerator**: All major methods are now instrumented with Langfuse tracing
- **ConsistentAIEngine**: Template-based and AI-powered SQL generation is tracked
- **LangChain Integration**: Automatic tracing of all LLM calls through callback handlers

### 3. API Endpoints
- **Query Processing**: Main query endpoint creates traces with user context
- **Session Management**: All AI operations are linked to user sessions
- **Error Tracking**: Failed operations are captured for debugging

## Setup Instructions

### 1. Environment Variables

Add the following environment variables to your `.env` file:

```bash
# Langfuse Configuration
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_HOST=https://cloud.langfuse.com  # Optional, defaults to cloud.langfuse.com
```

### 2. Langfuse Account Setup

1. **Sign up for Langfuse**:
   - Visit [https://cloud.langfuse.com](https://cloud.langfuse.com)
   - Create a free account
   - Create a new project

2. **Get your API Keys**:
   - Go to Project Settings
   - Copy your Public Key and Secret Key
   - Add them to your `.env` file

### 3. Install Dependencies

The required dependencies have been added to `requirements.txt`:

```bash
# Install new dependencies
pip install langfuse>=2.7.0 langfuse-langchain>=2.7.0

# Or install all requirements
pip install -r requirements.txt
```

## Features Implemented

### 1. Automatic Tracing
- **SQL Generation**: Every SQL generation request is traced
- **Query Execution**: Database query execution is monitored
- **Error Handling**: Failed operations are captured with error details
- **Multi-Query Analysis**: Complex analysis workflows are fully traced

### 2. User Context
- **User Identification**: All traces include user information
- **Session Tracking**: Operations are grouped by user sessions
- **Workspace Context**: Database workspace information is included

### 3. Performance Monitoring
- **Execution Time**: Track how long operations take
- **Token Usage**: Monitor LLM token consumption (where available)
- **Success Rates**: Track the success/failure rates of operations

### 4. Quality Metrics
- **Confidence Scores**: SQL generation confidence levels are tracked
- **Source Attribution**: Whether queries came from templates or AI
- **Auto-Fix Attempts**: Track how often SQL queries need fixing

## Usage Examples

### 1. Viewing Traces
After running queries, you can view traces in the Langfuse dashboard:
- Go to your Langfuse project dashboard
- View the "Traces" section
- Click on individual traces to see detailed execution flows

### 2. Analyzing Performance
- **Dashboard**: View overall system performance metrics
- **Sessions**: Analyze user session patterns
- **Errors**: Identify common failure points
- **Costs**: Track LLM usage costs

### 3. Quality Evaluation
- **Scoring**: Add custom scores to evaluate SQL quality
- **Feedback**: Collect user feedback on generated queries
- **A/B Testing**: Compare different prompt templates

## Observability Architecture

```
User Query → FastAPI Endpoint → SmartSQLGenerator → LangChain + Gemini
     ↓              ↓                   ↓                    ↓
   Trace         Trace              Trace               Trace
  Created      Enhanced           Enhanced            Enhanced
     ↓              ↓                   ↓                    ↓
                 Langfuse Dashboard (Monitoring & Analytics)
```

## Key Metrics Tracked

1. **Request Metrics**:
   - Total queries processed
   - Success/failure rates
   - Average response times
   - User activity patterns

2. **AI Metrics**:
   - SQL generation accuracy
   - Template vs AI usage
   - Auto-fix success rates
   - Confidence score distributions

3. **Performance Metrics**:
   - Database query execution times
   - LLM response times
   - Memory usage patterns
   - Error frequencies

## Troubleshooting

### 1. Langfuse Not Working
If Langfuse is not configured or fails to initialize:
- The system will continue to work normally
- A warning will be logged
- No traces will be sent to Langfuse

### 2. Missing Environment Variables
```bash
# Check if environment variables are set
echo $LANGFUSE_SECRET_KEY
echo $LANGFUSE_PUBLIC_KEY
```

### 3. Network Issues
If Langfuse cloud is unreachable:
- Consider using a self-hosted Langfuse instance
- Set `LANGFUSE_HOST` to your self-hosted URL

### 4. Performance Impact
Langfuse tracing has minimal performance impact:
- Traces are sent asynchronously
- Failed trace uploads don't affect the main application
- Network requests are batched for efficiency

## Advanced Configuration

### 1. Custom Scoring
Add custom evaluation metrics:

```python
from langfuse_config import langfuse_manager

# Score a trace based on SQL quality
langfuse_manager.score_trace(
    trace_id="trace_id",
    name="sql_quality",
    value=0.85,
    comment="Well-structured query with proper joins"
)
```

### 2. Custom Metadata
Add additional context to traces:

```python
trace = create_langfuse_trace(
    name="custom_operation",
    user_id=user_id,
    session_id=session_id,
    database_type="postgresql",
    schema_complexity="medium",
    query_type="analytical"
)
```

### 3. Self-Hosted Langfuse
For production environments, consider self-hosting:

```bash
# Set custom host
LANGFUSE_HOST=https://your-langfuse-instance.com
```

## Best Practices

1. **Environment Separation**:
   - Use different Langfuse projects for dev/staging/prod
   - Keep API keys secure and separate per environment

2. **Data Privacy**:
   - Be mindful of sensitive data in traces
   - Use Langfuse's data retention settings
   - Consider data anonymization for PII

3. **Monitoring**:
   - Set up alerts for high error rates
   - Monitor performance degradation
   - Track cost metrics regularly

4. **Team Collaboration**:
   - Share Langfuse project access with team members
   - Use Langfuse's commenting features for collaboration
   - Create custom dashboards for different stakeholders

## Support and Resources

- **Langfuse Documentation**: [https://langfuse.com/docs](https://langfuse.com/docs)
- **GitHub Repository**: [https://github.com/langfuse/langfuse](https://github.com/langfuse/langfuse)
- **Discord Community**: [https://discord.gg/7NXusRtqYU](https://discord.gg/7NXusRtqYU)
- **Langfuse Cookbook**: Examples and best practices

---

The Langfuse integration provides comprehensive observability for your NLP-to-SQL application, helping you monitor performance, debug issues, and continuously improve your AI system's quality and reliability. 