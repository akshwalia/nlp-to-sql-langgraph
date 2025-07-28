# Complete Deployment Guide: AWS + Vercel (Free Tier)

## Overview
This guide will help you deploy your NLP-to-SQL LangGraph application using:
- **Frontend**: Vercel (Free tier)
- **Backend**: AWS Lambda + API Gateway (Free tier)
- **Database**: AWS RDS PostgreSQL (Free tier) or Supabase
- **Vector Store**: Pinecone (Free tier) or AWS-hosted ChromaDB
- **File Storage**: AWS S3 (Free tier)

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Database Setup](#database-setup)
3. [Backend Deployment (AWS)](#backend-deployment-aws)
4. [Frontend Deployment (Vercel)](#frontend-deployment-vercel)
5. [Environment Configuration](#environment-configuration)
6. [Domain & SSL Setup](#domain--ssl-setup)
7. [Monitoring & Logs](#monitoring--logs)
8. [Cost Management](#cost-management)
9. [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Accounts (All Free)
- AWS Account (12-month free tier)
- Vercel Account (free tier)
- GitHub Account (for code repository)
- Supabase Account (optional, for managed PostgreSQL)
- Pinecone Account (optional, for vector store)

### Required Tools
```bash
# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Install Vercel CLI
npm install -g vercel

# Install Serverless Framework (for AWS deployment)
npm install -g serverless
```

## Database Setup

### Option 1: AWS RDS PostgreSQL (Free Tier)

#### Step 1: Create RDS Instance
```bash
# Create subnet group
aws rds create-db-subnet-group \
    --db-subnet-group-name nlp-sql-subnet-group \
    --db-subnet-group-description "Subnet group for NLP-SQL database" \
    --subnet-ids subnet-12345 subnet-67890  # Replace with your subnet IDs

# Create security group
aws ec2 create-security-group \
    --group-name nlp-sql-db-sg \
    --description "Security group for NLP-SQL database"

# Add inbound rule for PostgreSQL
aws ec2 authorize-security-group-ingress \
    --group-name nlp-sql-db-sg \
    --protocol tcp \
    --port 5432 \
    --cidr 0.0.0.0/0

# Create RDS instance
aws rds create-db-instance \
    --db-instance-identifier nlp-sql-db \
    --db-instance-class db.t3.micro \
    --engine postgres \
    --engine-version 14.9 \
    --allocated-storage 20 \
    --storage-type gp2 \
    --db-name pbtest \
    --master-username postgres \
    --master-user-password YourSecurePassword123 \
    --vpc-security-group-ids sg-12345 \
    --db-subnet-group-name nlp-sql-subnet-group \
    --backup-retention-period 7 \
    --storage-encrypted
```

#### Step 2: Import Your Data
```bash
# Get RDS endpoint
aws rds describe-db-instances --db-instance-identifier nlp-sql-db

# Connect and import data
psql -h your-rds-endpoint.amazonaws.com -U postgres -d pbtest
# Import your IT_Professional_Services table
```

### Option 2: Supabase (Recommended for Simplicity)

1. Go to [supabase.com](https://supabase.com) and create account
2. Create new project
3. Note your database URL, anon key, and service key
4. Import your data using Supabase SQL editor

## Backend Deployment (AWS)

### Step 1: Prepare Backend for Serverless

Create `serverless.yml` in project root:
```yaml
service: nlp-sql-backend

provider:
  name: aws
  runtime: python3.9
  region: us-east-1
  memorySize: 1024
  timeout: 30
  environment:
    DATABASE_URL: ${env:DATABASE_URL}
    OPENAI_API_KEY: ${env:OPENAI_API_KEY}
    GEMINI_API_KEY: ${env:GEMINI_API_KEY}
    LANGFUSE_SECRET_KEY: ${env:LANGFUSE_SECRET_KEY}
    LANGFUSE_PUBLIC_KEY: ${env:LANGFUSE_PUBLIC_KEY}
    LANGFUSE_HOST: ${env:LANGFUSE_HOST}
    JWT_SECRET_KEY: ${env:JWT_SECRET_KEY}
    PINECONE_API_KEY: ${env:PINECONE_API_KEY}
    PINECONE_ENVIRONMENT: ${env:PINECONE_ENVIRONMENT}

functions:
  api:
    handler: src.api.lambda_handler.handler
    events:
      - http:
          path: /{proxy+}
          method: ANY
          cors: true
      - http:
          path: /
          method: ANY
          cors: true

plugins:
  - serverless-python-requirements

custom:
  pythonRequirements:
    dockerizePip: true
    slim: true
    strip: false
```

### Step 2: Create Lambda Handler

Create `src/api/lambda_handler.py`:
```python
from mangum import Mangum
from src.api.main import app

handler = Mangum(app, lifespan="off")
```

### Step 3: Update Requirements

Add to `requirements.txt`:
```txt
mangum==0.17.0
```

### Step 4: Install Serverless Plugin
```bash
npm install serverless-python-requirements
```

### Step 5: Deploy to AWS
```bash
# Configure AWS credentials
aws configure

# Deploy
serverless deploy

# Note the API Gateway URL from output
```

### Step 6: Setup S3 for File Storage (Optional)
```bash
# Create S3 bucket
aws s3 mb s3://nlp-sql-files-unique-name

# Configure CORS
aws s3api put-bucket-cors \
    --bucket nlp-sql-files-unique-name \
    --cors-configuration file://cors.json
```

Create `cors.json`:
```json
{
    "CORSRules": [
        {
            "AllowedOrigins": ["*"],
            "AllowedMethods": ["GET", "POST", "PUT", "DELETE"],
            "AllowedHeaders": ["*"],
            "MaxAgeSeconds": 3000
        }
    ]
}
```

## Frontend Deployment (Vercel)

### Step 1: Prepare Frontend

Update `frontend/next.config.js`:
```javascript
/** @type {import('next').NextConfig} */
const nextConfig = {
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL,
  },
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: `${process.env.NEXT_PUBLIC_API_URL}/:path*`,
      },
    ]
  },
}

module.exports = nextConfig
```

### Step 2: Deploy to Vercel
```bash
cd frontend

# Login to Vercel
vercel login

# Deploy
vercel

# Follow prompts:
# - Link to existing project? No
# - Project name: nlp-sql-frontend
# - Directory: ./
# - Override settings? No

# Set environment variables
vercel env add NEXT_PUBLIC_API_URL production
# Enter your AWS API Gateway URL

# Redeploy with environment variables
vercel --prod
```

### Step 3: Configure Custom Domain (Optional)
```bash
# Add domain in Vercel dashboard or via CLI
vercel domains add yourdomain.com
```

## Environment Configuration

### AWS Lambda Environment Variables
Set these in AWS Lambda console or via Serverless:

```bash
# Database
DATABASE_URL=postgresql://username:password@host:5432/database

# AI Services
OPENAI_API_KEY=your_openai_key
GEMINI_API_KEY=your_gemini_key

# Vector Store (choose one)
PINECONE_API_KEY=your_pinecone_key
PINECONE_ENVIRONMENT=your_pinecone_env
# OR
CHROMA_PERSIST_DIRECTORY=/tmp/chroma_db

# Observability
LANGFUSE_SECRET_KEY=your_langfuse_secret
LANGFUSE_PUBLIC_KEY=your_langfuse_public
LANGFUSE_HOST=https://cloud.langfuse.com

# Security
JWT_SECRET_KEY=your_jwt_secret_256_bit
```

### Vercel Environment Variables
```bash
# API endpoint
NEXT_PUBLIC_API_URL=https://your-lambda-url.amazonaws.com/dev

# Optional: Analytics
NEXT_PUBLIC_VERCEL_ANALYTICS=true
```

## Domain & SSL Setup

### Vercel Domain
1. Go to Vercel Dashboard → Project → Settings → Domains
2. Add your custom domain
3. Update DNS records as instructed
4. SSL is automatic with Vercel

### AWS API Gateway Custom Domain
```bash
# Create certificate in ACM (must be in us-east-1 for CloudFront)
aws acm request-certificate \
    --domain-name api.yourdomain.com \
    --validation-method DNS \
    --region us-east-1

# Create custom domain in API Gateway
aws apigatewayv2 create-domain-name \
    --domain-name api.yourdomain.com \
    --domain-name-configurations CertificateArn=arn:aws:acm:us-east-1:123:certificate/abc
```

## Monitoring & Logs

### AWS CloudWatch
```bash
# View Lambda logs
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/nlp-sql-backend

# Stream logs
aws logs tail /aws/lambda/nlp-sql-backend-dev-api --follow
```

### Vercel Analytics
1. Enable in Vercel Dashboard → Project → Analytics
2. Monitor performance and usage

### Application Monitoring
- Use Langfuse for LLM call tracing
- Set up CloudWatch alarms for errors
- Monitor RDS performance metrics

## Cost Management

### AWS Free Tier Limits
- **Lambda**: 1M requests/month, 400,000 GB-seconds compute
- **API Gateway**: 1M requests/month
- **RDS**: 750 hours/month t3.micro, 20GB storage
- **S3**: 5GB storage, 20,000 GET requests
- **CloudWatch**: 10 custom metrics, 1M API requests

### Vercel Free Tier Limits
- **Bandwidth**: 100GB/month
- **Builds**: 6,000 minutes/month
- **Serverless Functions**: 100GB-hrs/month

### Cost Optimization Tips
```bash
# Set up billing alerts
aws budgets create-budget \
    --account-id 123456789012 \
    --budget file://budget.json

# Monitor usage
aws ce get-cost-and-usage \
    --time-period Start=2024-01-01,End=2024-01-31 \
    --granularity MONTHLY \
    --metrics BlendedCost
```

## Troubleshooting

### Common Issues

#### 1. Lambda Cold Start Timeout
```python
# In lambda_handler.py, add warmup
import json

def handler(event, context):
    # Warmup request
    if event.get('source') == 'serverless-plugin-warmup':
        return {'statusCode': 200, 'body': json.dumps('Lambda is warm!')}
    
    return Mangum(app, lifespan="off")(event, context)
```

#### 2. Database Connection Issues
```python
# Update connection pooling in database config
DATABASE_CONFIG = {
    "max_connections": 1,  # Lambda limitation
    "pool_timeout": 30,
    "pool_recycle": -1,
    "pool_pre_ping": True
}
```

#### 3. CORS Issues
```python
# Ensure CORS is properly configured in FastAPI
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-vercel-domain.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

#### 4. Large Response Size
```python
# Implement response compression
from fastapi.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1000)
```

### Performance Optimization

#### 1. Lambda Layer for Dependencies
```bash
# Create layer for heavy dependencies
mkdir python
pip install -r requirements.txt -t python/
zip -r dependencies.zip python/

# Upload layer
aws lambda publish-layer-version \
    --layer-name nlp-sql-dependencies \
    --zip-file fileb://dependencies.zip \
    --compatible-runtimes python3.9
```

#### 2. Enable CloudFront for API
```yaml
# Add to serverless.yml
resources:
  Resources:
    CloudFrontDistribution:
      Type: AWS::CloudFront::Distribution
      Properties:
        DistributionConfig:
          Origins:
            - DomainName: 
                Fn::Join:
                  - ""
                  - - Ref: ApiGatewayRestApi
                    - ".execute-api.us-east-1.amazonaws.com"
              Id: ApiGateway
              CustomOriginConfig:
                HTTPPort: 443
                OriginProtocolPolicy: https-only
          Enabled: true
          DefaultCacheBehavior:
            TargetOriginId: ApiGateway
            ViewerProtocolPolicy: redirect-to-https
            CachePolicyId: 4135ea2d-6df8-44a3-9df3-4b5a84be39ad  # CachingDisabled
```

## Security Checklist

- [ ] Enable AWS CloudTrail for audit logs
- [ ] Use AWS Secrets Manager for sensitive data
- [ ] Implement rate limiting in API Gateway
- [ ] Enable WAF for additional protection
- [ ] Use environment-specific JWT secrets
- [ ] Enable RDS encryption at rest
- [ ] Implement proper RBAC in application
- [ ] Use HTTPS everywhere
- [ ] Enable Vercel Security Headers
- [ ] Implement CSP headers

## Backup & Recovery

### Database Backup
```bash
# Enable automated RDS backups (already enabled in creation script)
aws rds modify-db-instance \
    --db-instance-identifier nlp-sql-db \
    --backup-retention-period 7 \
    --apply-immediately
```

### Code Backup
- Code is backed up in GitHub
- Vercel automatically deploys from GitHub
- AWS Lambda code can be exported

## Scaling Considerations

When you exceed free tier:
1. **Lambda**: Consider AWS Fargate or ECS
2. **Database**: Use RDS Multi-AZ or Aurora Serverless
3. **Vector Store**: Migrate to Pinecone or AWS OpenSearch
4. **CDN**: CloudFront for global distribution
5. **Load Balancing**: Application Load Balancer

## Support Resources

- **AWS Support**: Basic support included
- **Vercel Support**: Community forums
- **Documentation**: AWS docs, Vercel docs
- **Monitoring**: CloudWatch, Vercel Analytics
- **Cost Calculator**: AWS Pricing Calculator

---

## Quick Start Commands Summary

```bash
# 1. Deploy Backend
serverless deploy

# 2. Deploy Frontend
cd frontend && vercel --prod

# 3. Set Environment Variables
vercel env add NEXT_PUBLIC_API_URL production

# 4. Monitor
aws logs tail /aws/lambda/nlp-sql-backend-dev-api --follow
```

Total setup time: ~2-3 hours
Monthly cost on free tier: $0 (within limits) 