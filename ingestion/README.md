# TPCH Athena Pipeline

A robust dlt-based data pipeline for loading TPCH data from S3 to Athena/Iceberg tables.

## 🚀 Features

- **Comprehensive Logging**: Detailed logging with timestamps, durations, and progress tracking
- **Error Handling**: Graceful error handling with detailed error messages and stack traces
- **Configuration-Driven**: Externalized configuration for easy maintenance
- **Modular Design**: Clean, maintainable code structure with separation of concerns
- **Environment Validation**: Pre-flight checks for dependencies and configuration
- **Resilient Processing**: Continues processing other tables even if one fails

## 📋 Prerequisites

- Python 3.11+
- dlt (Data Load Tool) with Athena support
- AWS credentials configured for Athena access
- S3 access to TPCH data files

## 🏗️ Architecture

```
S3 TPCH Data → dlt Pipeline → Athena (Iceberg Tables)
     ↓              ↓              ↓
- region.tbl   → Processing  → tpch_region
- nation.tbl   → Validation → tpch_nation  
- customer.tbl → Transform  → tpch_customer
```

## 📁 Project Structure

```
ingestion/
├── s3_to_iceberg.py      # Main pipeline script
├── pipeline_config.toml   # Configuration file
├── pipeline_logs.log     # Generated log file
└── README.md            # This file
```

## ⚙️ Configuration

The pipeline uses `pipeline_config.toml` for configuration:

```toml
[pipeline]
name = "tpch_athena_pipeline"
dataset_name = "cat_rnd_odp_dev_lake_bronze"
destination = "athena"
staging = "filesystem"

[tables.tpch_region]
file_glob = "h/1/region*.tbl"
delimiter = "|"
columns = ["r_regionkey", "r_name", "r_comment"]
description = "TPCH Region dimension table"
```

## 🔧 Usage

### Basic Execution
```bash
python ingestion/s3_to_iceberg.py
```

### With Logging
The pipeline automatically logs to both console and `pipeline_logs.log`:

```
2024-10-16 10:30:15 - INFO - 🚀 TPCH ATHENA PIPELINE STARTED
2024-10-16 10:30:15 - INFO - 📅 Start Time: 2024-10-16 10:30:15
2024-10-16 10:30:15 - INFO - 🎯 Target Dataset: cat_rnd_odp_dev_lake_bronze
2024-10-16 10:30:18 - INFO - ✅ Table tpch_nation completed in 3.33 seconds
```

## 📊 Tables Processed

| Table | Source Pattern | Columns | Description |
|-------|---------------|---------|-------------|
| `tpch_region` | `h/1/region*.tbl` | 3 | Region dimension |
| `tpch_nation` | `h/100/nation*.tbl*` | 4 | Nation dimension |
| `tpch_customer` | `h/100/customer*.tbl*` | 8 | Customer dimension |

## 🔍 Monitoring

The pipeline provides comprehensive monitoring through:

### **Start/End Logging**
- Pipeline start/end timestamps
- Total execution duration
- Success/failure status
- Environment validation results

### **Table-Level Tracking**
- Individual table processing times
- Row counts and job details
- Load IDs and job file information
- Error details for failed tables

### **Performance Metrics**
- Total pipeline duration
- Per-table processing times
- Job completion statistics

## 🚨 Error Handling

The pipeline includes robust error handling:

- **Environment Validation**: Checks dependencies before execution
- **Per-Table Error Isolation**: Failed tables don't stop other processing
- **Detailed Error Logging**: Full stack traces and context
- **Graceful Degradation**: Continues processing despite individual failures

## 🔧 Customization

### Adding New Tables
1. Add table configuration to `pipeline_config.toml`
2. Update `TABLE_CONFIGS` in the script
3. Run the pipeline

### Modifying Log Format
Update the logging configuration in the script:
```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### Environment Variables
Configure these environment variables as needed:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`

## 📈 Performance Tips

1. **Parallel Processing**: Consider processing tables in parallel for large datasets
2. **Batch Size**: Adjust dlt batch sizes for optimal performance
3. **Monitoring**: Use the logs to identify bottlenecks
4. **Resource Scaling**: Scale compute resources based on data volume

## 🐛 Troubleshooting

### Common Issues

**Pipeline Fails to Start**
- Check AWS credentials
- Verify S3 access permissions
- Ensure dlt is properly installed

**Table Processing Fails**
- Check S3 file patterns in configuration
- Verify file format and delimiters
- Review column names and counts

**Slow Performance**
- Monitor S3 transfer speeds
- Check Athena query limits
- Consider file partitioning

### Debug Mode
Enable debug logging by modifying the log level:
```python
logging.basicConfig(level=logging.DEBUG)
```

## 🤝 Contributing

1. Follow the existing code structure
2. Add comprehensive logging for new features
3. Update configuration files as needed
4. Include error handling for all operations

## 📝 License

This pipeline is part of the dlthub demo project.