# AWS EC2 and VPC LLM Recommendations - Implementation Guide

## Overview

This implementation adds AI-powered cost optimization recommendations for AWS EC2 instances and VPC resources to the FinOps platform.

## New Files Added

### SQL Schema Files

1. **`sql/bronze_ec2_metrics.sql`** - Bronze layer for raw EC2 CloudWatch metrics
   - Tables: `bronze_ec2_instance_metrics`
   - Metrics: CPU, Network, Disk I/O, etc.

2. **`sql/silver_ec2_metrics.sql`** - Silver layer for cleaned EC2 metrics
   - Tables: `silver_ec2_metrics`
   - Deduplication and data cleaning

3. **`sql/gold_ec2_metrics.sql`** - Gold layer for EC2 analytics
   - Dimension tables: `dim_ec2_instance`, `dim_ec2_metric`, `dim_time_hour_ec2`
   - Fact table: `fact_ec2_metrics`
   - View: `gold_ec2_fact_metrics`

4. **`sql/bronze_vpc_metrics.sql`** - Bronze layer for VPC metrics
   - Tables: `bronze_vpc_metrics`
   - Supports VPC, NAT Gateway, VPN, VPC Endpoint metrics

5. **`sql/silver_vpc_metrics.sql`** - Silver layer for VPC metrics
   - Tables: `silver_vpc_metrics`

6. **`sql/gold_vpc_metrics.sql`** - Gold layer for VPC analytics
   - Dimension tables: `dim_vpc_resource`, `dim_vpc_metric`, `dim_time_hour_vpc`
   - Fact table: `fact_vpc_metrics`
   - View: `gold_vpc_fact_metrics`

### Python Integration Files

7. **`llm_ec2_vpc_integration.py`** - Main LLM integration for EC2 and VPC
   - EC2 analysis functions
   - VPC analysis functions
   - LLM prompt generation
   - Recommendation extraction

### Modified Files

8. **`app/api/v1/endpoints/llm.py`** - Updated AWS endpoint to support EC2 and VPC
   - Added routing logic for resource types: s3, ec2, vpc
   - Integrated `llm_ec2_vpc_integration` module

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Frontend (React/Next.js)                    │
│  POST /llm/aws/{project_id}                                     │
│  Body: { resource_type: "ec2"|"vpc", start_date, end_date }    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Backend API (FastAPI)                           │
│  app/api/v1/endpoints/llm.py                                    │
│  - Route to EC2/VPC handler based on resource_type              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│           llm_ec2_vpc_integration.py                            │
│  ┌─────────────────┐        ┌─────────────────┐                │
│  │  EC2 Analysis   │        │  VPC Analysis   │                │
│  ├─────────────────┤        ├─────────────────┤                │
│  │ fetch_ec2_      │        │ fetch_vpc_      │                │
│  │ utilization_    │        │ utilization_    │                │
│  │ data()          │        │ data()          │                │
│  │                 │        │                 │                │
│  │ generate_ec2_   │        │ generate_vpc_   │                │
│  │ prompt()        │        │ prompt()        │                │
│  │                 │        │                 │                │
│  │ get_ec2_        │        │ get_vpc_        │                │
│  │ recommendation_ │        │ recommendation_ │                │
│  │ single()        │        │ single()        │                │
│  └─────────────────┘        └─────────────────┘                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              PostgreSQL Database                                 │
│  ┌───────────────────┐    ┌───────────────────┐                │
│  │ fact_ec2_metrics  │    │ fact_vpc_metrics  │                │
│  │ - instance_id     │    │ - resource_id     │                │
│  │ - metric_name     │    │ - vpc_id          │                │
│  │ - value           │    │ - metric_name     │                │
│  │ - timestamp       │    │ - value           │                │
│  └───────────────────┘    └───────────────────┘                │
│                                                                  │
│  gold_aws_fact_focus (FOCUS billing data)                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              Azure OpenAI (GPT-4/GPT-4o)                        │
│  - Analyzes metrics and costs                                   │
│  - Generates optimization recommendations                        │
│  - Returns structured JSON                                      │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### EC2 Analysis Flow

1. **User Request**: Frontend sends POST request to `/llm/aws/{project_id}` with:
   ```json
   {
     "resource_type": "ec2",
     "start_date": "2025-01-01T00:00:00",
     "end_date": "2025-01-31T23:59:59",
     "resource_id": "i-1234567890abcdef0" // Optional
   }
   ```

2. **Data Retrieval**: `fetch_ec2_utilization_data()` queries:
   - Metrics from `fact_ec2_metrics` (CPU, Network, Disk I/O)
   - Cost data from `gold_aws_fact_focus` (FOCUS billing)
   - Calculates: AVG, MAX, MAX_DATE for each metric

3. **Prompt Generation**: `generate_ec2_prompt()` creates detailed prompt with:
   - Instance details (ID, type, region)
   - Performance metrics (CPU utilization, network traffic, disk ops)
   - Cost information (billed cost, pricing category)
   - Analysis period

4. **LLM Analysis**: `llm_call()` sends prompt to Azure OpenAI

5. **Response Parsing**: `extract_json()` extracts structured recommendations

6. **Response Format**:
   ```json
   {
     "resource_id": "i-1234567890abcdef0",
     "recommendations": {
       "effective_recommendation": {
         "text": "Downsize from t3.xlarge to t3.large",
         "saving_pct": 35.0
       },
       "additional_recommendation": [
         {
           "text": "Enable auto-stop during non-business hours",
           "saving_pct": 40.0
         }
       ]
     },
     "cost_forecasting": {
       "monthly": 245.67,
       "annually": 2948.04
     },
     "anomalies": [...],
     "contract_deal": {...}
   }
   ```

### VPC Analysis Flow

Similar to EC2, but focuses on:
- VPC resources (VPCs, NAT Gateways, VPN connections, VPC Endpoints)
- Network traffic metrics
- Cost optimization for networking resources

## EC2 Metrics Tracked

The system tracks the following EC2 CloudWatch metrics:

1. **CPUUtilization** - Percentage of allocated CPU currently in use
2. **NetworkIn** - Number of bytes received by the instance
3. **NetworkOut** - Number of bytes sent by the instance
4. **DiskReadOps** - Completed read operations from all instance store volumes
5. **DiskWriteOps** - Completed write operations to all instance store volumes
6. **StatusCheckFailed** - Reports whether the instance has passed both instance and system status checks

## VPC Metrics Tracked

VPC-related metrics include:

1. **BytesIn** - Bytes received through NAT Gateway
2. **BytesOut** - Bytes sent through NAT Gateway
3. **ConnectionAttemptCount** - VPN tunnel connection attempts
4. **ConnectionEstablishedCount** - Established VPN connections
5. **PacketsIn** - Packets received
6. **PacketsOut** - Packets sent

## Recommendation Types Generated

### 1. Effective Recommendation
Primary cost-saving action (e.g., instance rightsizing)

### 2. Additional Recommendations
Secondary optimization opportunities:
- Reserved Instance / Savings Plan suggestions
- Spot Instance opportunities
- Scheduling (auto-stop/start)
- Architecture improvements

### 3. Cost Forecasting
Monthly and annual cost projections based on current usage

### 4. Anomaly Detection
Identifies unusual usage patterns:
- CPU spikes
- Network traffic surges
- Disk I/O anomalies

### 5. Contract Deal Assessment
Evaluates current pricing vs. available discounts:
- Reserved Instances
- Savings Plans
- Spot pricing opportunities

## API Usage Examples

### Analyze All EC2 Instances in a Project

```bash
curl -X POST "http://localhost:8000/llm/aws/my-project" \
  -H "Content-Type: application/json" \
  -d '{
    "resource_type": "ec2",
    "start_date": "2025-01-01T00:00:00",
    "end_date": "2025-01-31T23:59:59"
  }'
```

### Analyze Specific EC2 Instance

```bash
curl -X POST "http://localhost:8000/llm/aws/my-project" \
  -H "Content-Type: application/json" \
  -d '{
    "resource_type": "ec2",
    "resource_id": "i-1234567890abcdef0",
    "start_date": "2025-01-01T00:00:00",
    "end_date": "2025-01-31T23:59:59"
  }'
```

### Analyze VPC Resources

```bash
curl -X POST "http://localhost:8000/llm/aws/my-project" \
  -H "Content-Type: application/json" \
  -d '{
    "resource_type": "vpc",
    "start_date": "2025-01-01T00:00:00",
    "end_date": "2025-01-31T23:59:59"
  }'
```

## Database Setup

To set up the EC2 and VPC metrics tables, run the SQL files in order:

```sql
-- For EC2
\i sql/bronze_ec2_metrics.sql
\i sql/silver_ec2_metrics.sql
\i sql/gold_ec2_metrics.sql

-- For VPC
\i sql/bronze_vpc_metrics.sql
\i sql/silver_vpc_metrics.sql
\i sql/gold_vpc_metrics.sql
```

Note: Replace `__schema__` and `__budget__` placeholders with actual values.

## Environment Variables Required

Ensure these are set in your `.env` file:

```bash
# Azure OpenAI Configuration
AZURE_OPENAI_ENDPOINT=https://your-instance.openai.azure.com/
AZURE_OPENAI_KEY=your-api-key
AZURE_DEPLOYMENT_NAME=gpt-4
AZURE_OPENAI_VERSION=2024-02-15-preview

# Database Configuration
DB_USER_NAME=postgres
DB_PASSWORD=your-password
DB_HOST_NAME=localhost
DB_PORT=5432
DB_NAME=finops
```

## Error Handling

The implementation includes comprehensive error handling:

1. **Database Errors**: Caught and logged with specific error messages
2. **LLM Errors**: Empty responses are handled gracefully
3. **JSON Parsing Errors**: Malformed LLM responses are logged and skipped
4. **Missing Data**: Empty DataFrames return appropriate warnings

## Logging

All operations are logged with appropriate levels:

- `INFO`: Normal operations (analysis start, completion, data retrieval)
- `WARNING`: Non-critical issues (empty data, failed parsing)
- `ERROR`: Critical failures (database errors, LLM failures)

## Performance Considerations

1. **Batch Processing**: Multiple resources are processed in sequence
2. **Date Range**: Larger date ranges increase processing time
3. **LLM Calls**: Each resource makes one LLM API call
4. **Database Queries**: Optimized with proper indexing on `instance_id`, `timestamp`, and `metric_name`

## Future Enhancements

- [ ] Add caching for LLM responses
- [ ] Implement batch LLM calls for better performance
- [ ] Add support for more AWS services (Lambda, RDS, EBS)
- [ ] Store recommendations in database for historical tracking
- [ ] Add recommendation acceptance/rejection tracking
- [ ] Implement A/B testing for prompt optimization

## Testing

To test the implementation:

1. Ensure EC2/VPC metrics are being ingested
2. Verify database tables are populated
3. Make API calls with test data
4. Check logs for any errors
5. Validate JSON response format

## Troubleshooting

### No Recommendations Generated

- Check if metrics data exists in `fact_ec2_metrics` or `fact_vpc_metrics`
- Verify date range covers available data
- Check LLM API credentials and quotas
- Review logs for specific error messages

### JSON Parsing Errors

- LLM may return malformed JSON occasionally
- `extract_json()` utility handles most edge cases
- Check OpenAI deployment model compatibility

### High Costs from LLM Calls

- Adjust date ranges to reduce data volume
- Implement caching for frequently requested resources
- Consider using batch processing during off-peak hours

## Support

For issues or questions:
1. Check logs in the backend console
2. Review error messages in the API response
3. Verify database connectivity and schema setup
4. Ensure Azure OpenAI credentials are valid

---

**Implementation Date**: January 2025
**Version**: 1.0
**Author**: FinOps Development Team
