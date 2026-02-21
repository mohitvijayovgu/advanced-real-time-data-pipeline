import pandas as pd
import logging

logger = logging.getLogger(__name__)

def aggregate_by_device(df, filename):
    # Only aggregate numeric sensor columns
    numeric_cols = [c for c in ['co', 'humidity', 'lpg', 'smoke', 'temperature'] 
                    if c in df.columns]
    
    # Group by sensor_id and calculate min, max, mean, std for each numeric column
    agg_dict = {col: ['min', 'max', 'mean', 'std'] for col in numeric_cols}
    
    agg_df = df.groupby('sensor_id').agg(agg_dict)
    
    # Flatten multi-level column names e.g. ('temperature', 'min') becomes 'temperature_min'
    agg_df.columns = ['_'.join(col) for col in agg_df.columns]
    agg_df = agg_df.reset_index()
    
    # Tag with metadata — which file this aggregation came from and when
    agg_df['source_file'] = filename
    agg_df['processed_at'] = pd.Timestamp.now()
    
    logger.info("Aggregation completed for %s: %d devices", filename, len(agg_df))
    return agg_df


def aggregate(df, filename):
    logger.info("Starting aggregation on file: %s", filename)
    
    agg_df = aggregate_by_device(df, filename)
    
    logger.info("Aggregation completed. Shape: %s", str(agg_df.shape))
    return agg_df