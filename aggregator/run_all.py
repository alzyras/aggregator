import time
import logging
from aggregator.config import config
from aggregator.plugin_manager import PluginManager


# Create a logger for this module
logger = logging.getLogger(__name__)


def main() -> None:
    """Main application loop."""
    # Validate configuration
    errors = config.validate()
    if errors:
        logger.error("Configuration errors found:")
        for section, error in errors.items():
            logger.error(f"  {section}: {error}")
        return

    # Initialize plugin manager
    plugin_manager = PluginManager()
    
    enabled_modules = config.get_enabled_modules()
    if not enabled_modules:
        logger.warning("No modules enabled. Please set ENABLED_PLUGINS=asana,habitica in environment variables.")
        return
    
    logger.info(f"Starting wellness statistics with modules: {', '.join(enabled_modules)}")

    while True:
        logger.info("Running data collection for enabled modules...")
        
        # Process each enabled plugin
        for plugin in plugin_manager.get_plugins():
            try:
                logger.info(f"Processing {plugin.name} data...")
                
                # Setup database
                if plugin.setup_database():
                    logger.info(f"{plugin.name} database setup complete")
                else:
                    logger.error(f"{plugin.name} database setup failed")
                    #continue
                
                # Fetch data
                data = plugin.fetch_data()
                
                # Handle Google Fit plugin differently since it returns a dictionary
                if plugin.name == "google_fit":
                    total_records = sum(len(df) for df in data.values() if df is not None) if isinstance(data, dict) else 0
                    logger.info(f"{plugin.name} data fetched: {total_records} records across {len(data) if isinstance(data, dict) else 0} data types")
                else:
                    record_count = len(data) if data is not None else 0
                    logger.info(f"{plugin.name} data fetched: {record_count} records")
                
                if data is not None and (isinstance(data, dict) and any(df is not None and not df.empty for df in data.values())) or (not isinstance(data, dict) and not data.empty):
                    # Write to database
                    inserted_count, duplicate_count = plugin.write_to_database(data)
                    logger.info(f"{plugin.name} data written to database:")
                    logger.info(f"   - Inserted: {inserted_count} records")
                    logger.info(f"   - Duplicates: {duplicate_count} records")
                else:
                    logger.info(f"No {plugin.name} data to process")
                    
            except Exception as e:
                logger.error(f"Error processing {plugin.name}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info(f"Sleeping for {config.interval_seconds} seconds...")
        time.sleep(config.interval_seconds)


if __name__ == "__main__":
    main()