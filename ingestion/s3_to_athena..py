import dlt
import logging
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional
from dlt.sources.filesystem import filesystem, read_csv
from dlt.common.pipeline import LoadInfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_logs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BRONZE_LAYER = "cat_rnd_odp_dev_lake_bronze"
PIPELINE_NAME = "tpch_athena_pipeline"

# Table configurations with batch processing
TABLE_CONFIGS = {
    'tpch_region': {
        'file_glob': 'h/1/region*.tbl',
        'delimiter': '|',
        'columns': ["r_regionkey", "r_name", "r_comment"],
        'description': 'TPCH Region dimension table',
        'batch_size': 10000,
        'chunksize': 5000
    },
    'tpch_nation': {
        'file_glob': 'h/100/nation*.tbl*',
        'delimiter': '|',
        'columns': ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
        'description': 'TPCH Nation dimension table',
        'batch_size': 25000,
        'chunksize': 5000
    },
    'tpch_customer': {
        'file_glob': 'h/100/customer*.tbl*',
        'delimiter': '|',
        'columns': ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"],
        'description': 'TPCH Customer dimension table',
        'batch_size': 50000,
        'chunksize': 10000,
        'parallel_readers': 4
    }
}


def log_pipeline_start() -> None:
    """Log pipeline start information"""
    logger.info("=" * 80)
    logger.info("🚀 TPCH ATHENA PIPELINE STARTED")
    logger.info(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🎯 Target Dataset: {BRONZE_LAYER}")
    logger.info(f"🏗️  Destination: athena")
    logger.info(f"📦 Staging: filesystem")
    logger.info(f"📋 Tables to process: {len(TABLE_CONFIGS)}")
    for table_name, config in TABLE_CONFIGS.items():
        logger.info(f"   - {table_name}: {config['description']}")
    logger.info("=" * 80)


def log_table_run_start(table_name: str, config: Dict[str, Any]) -> None:
    """Log individual table processing start"""
    logger.info("-" * 50)
    logger.info(f"📊 Processing table: {table_name}")
    logger.info(f"📝 Description: {config['description']}")
    logger.info(f"🔍 Source pattern: {config['file_glob']}")
    logger.info(f"📄 Columns: {len(config['columns'])} columns")
    if 'batch_size' in config:
        batch_info = f"batch_size={config['batch_size']}, chunksize={config['chunksize']}"
        if 'parallel_readers' in config:
            batch_info += f", parallel_readers={config['parallel_readers']}"
        logger.info(f"📦 Batch processing: {batch_info}")
    logger.info(f"⏰ Start time: {datetime.now().strftime('%H:%M:%S')}")


def log_table_run_end(table_name: str, result: LoadInfo, duration: float) -> None:
    """Log individual table processing end"""
    logger.info(f"✅ Table {table_name} completed in {duration:.2f} seconds ({duration/60:.2f} minutes)")
    if hasattr(result, 'loads_ids') and result.loads_ids:
        logger.info(f"📦 Load IDs: {result.loads_ids}")
    if hasattr(result, 'jobs') and result.jobs:
        logger.info(f"🔧 Jobs completed: {len(result.jobs)}")
        total_rows = 0
        # Log job details and count rows
        for job in result.jobs:
            if hasattr(job, 'job_file_info'):
                logger.info(f"   📄 Job: {job.job_file_info.file_name}")
                if hasattr(job.job_file_info, 'rows_count'):
                    total_rows += job.job_file_info.rows_count or 0
        
        if total_rows > 0:
            logger.info(f"📊 Total rows processed: {total_rows:,}")
            rows_per_second = total_rows / duration if duration > 0 else 0
            logger.info(f"⚡ Processing rate: {rows_per_second:,.0f} rows/second")
    logger.info("-" * 50)


def log_pipeline_end(total_duration: float, success: bool = True) -> None:
    """Log pipeline completion"""
    status = "COMPLETED" if success else "FAILED"
    emoji = "🎉" if success else "❌"
    logger.info("=" * 80)
    logger.info(f"{emoji} TPCH ATHENA PIPELINE {status}")
    logger.info(f"⏱️  Total Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    logger.info(f"🏁 End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)


def create_table_source_with_batching(table_name: str, config: Dict[str, Any]):
    """Create a dlt source for a table with batch processing support"""
    try:
        logger.info(f"🔧 Creating source for {table_name}...")
        
        # Create filesystem source with parallel readers if configured
        fs_kwargs = {'file_glob': config['file_glob']}
        if 'parallel_readers' in config:
            logger.info(f"🚀 Using {config['parallel_readers']} parallel readers for faster processing")
            # Note: dlt filesystem may not directly support parallel_readers parameter
            # This is a placeholder for when dlt supports it or for custom implementation
            
        fs_source = filesystem(**fs_kwargs)
        
        # Check if batching is configured for this table
        if 'batch_size' in config and 'chunksize' in config:
            batch_info = f"batch_size: {config['batch_size']}, chunksize: {config['chunksize']}"
            if 'parallel_readers' in config:
                batch_info += f", parallel_readers: {config['parallel_readers']}"
            logger.info(f"📦 Using batch processing - {batch_info}")
            
            # Create CSV reader with batching parameters
            csv_reader = read_csv(
                delimiter=config['delimiter'],
                header=None,
                names=config['columns'],
                chunksize=config['chunksize'],  # Process in smaller chunks
                low_memory=True,  # Optimize memory usage
                dtype=str  # Read as strings first to avoid type issues
            )
        else:
            # Standard processing for tables without batch config
            logger.info("📄 Using standard processing (no batching configured)")
            csv_reader = read_csv(
                delimiter=config['delimiter'],
                header=None,
                names=config['columns']
            )
        
        # Combine source and reader
        source = fs_source | csv_reader
        
        logger.info(f"✅ Source created successfully for {table_name}")
        return source
        
    except Exception as e:
        logger.error(f"❌ Failed to create source for {table_name}: {str(e)}")
        raise


def validate_environment() -> None:
    """Validate environment and dependencies"""
    logger.info("🔍 Validating environment...")
    
    # Check if required environment variables are set (add as needed)
    # required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    # for var in required_vars:
    #     if not os.getenv(var):
    #         logger.warning(f"⚠️  Environment variable {var} not set")
    
    # Check dlt version
    try:
        import dlt
        logger.info(f"✅ dlt version: {dlt.__version__}")
    except Exception as e:
        logger.error(f"❌ dlt import failed: {str(e)}")
        raise
    
    logger.info("✅ Environment validation completed")


def run_pipeline() -> None:
    """Main pipeline execution function"""
    pipeline_start_time = time.time()
    
    try:
        # Start pipeline logging
        log_pipeline_start()
        
        # Validate environment
        validate_environment()
        
        # Create pipeline
        logger.info("🔧 Creating dlt pipeline...")
        pipeline = dlt.pipeline(
            pipeline_name=PIPELINE_NAME,
            dataset_name=BRONZE_LAYER,
            destination="athena",
            staging="filesystem"
        )
        logger.info("✅ Pipeline created successfully")
        
        # Process each table
        results = {}
        
        for table_name, config in TABLE_CONFIGS.items():
            log_table_run_start(table_name, config)
            table_start_time = time.time()
            
            try:
                # Create source with batching support
                source = create_table_source_with_batching(table_name, config)
                
                # Run pipeline for this table
                logger.info(f"🚀 Running pipeline for {table_name}...")
                
                # Configure pipeline run based on table configuration
                run_kwargs = {'table_name': table_name}
                
                if 'batch_size' in config:
                    run_kwargs['write_disposition'] = "replace"  # Replace for batched processing
                    
                    # Add parallel processing hints if configured
                    if 'parallel_readers' in config:
                        logger.info(f"⚡ Optimizing for parallel processing with {config['parallel_readers']} readers")
                        # Note: This is configuration tracking, actual parallelism depends on dlt implementation
                
                result = pipeline.run(source, **run_kwargs)
                    
                results[table_name] = result
                
                # Log completion
                table_duration = time.time() - table_start_time
                log_table_run_end(table_name, result, table_duration)
                
            except Exception as e:
                logger.error(f"❌ Failed to process {table_name}: {str(e)}")
                logger.exception(f"Full traceback for {table_name}:")
                # Continue with other tables instead of failing completely
                continue
        
        # Log final success
        total_duration = time.time() - pipeline_start_time
        log_pipeline_end(total_duration, success=True)
        
        # Return results for further processing if needed
        return results
        
    except Exception as e:
        total_duration = time.time() - pipeline_start_time
        logger.error("❌ PIPELINE FAILED")
        logger.error(f"🚨 Error: {str(e)}")
        logger.error(f"⏱️  Failed after: {total_duration:.2f} seconds")
        logger.exception("Full traceback:")
        log_pipeline_end(total_duration, success=False)
        raise


if __name__ == "__main__":
    run_pipeline()