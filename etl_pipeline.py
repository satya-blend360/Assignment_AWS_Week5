import pandas as pd
import numpy as np
import json
import boto3
from datetime import datetime
from io import StringIO

class SalesETLPipeline:
    """ETL Pipeline for Amazon Sales Data"""
    
    def __init__(self, s3_bucket_name):
        self.s3_bucket_name = s3_bucket_name
        self.s3_client = boto3.client('s3')
        self.df = None
        
    def extract(self, csv_file_path):
        """Extract data from CSV file"""
        print(f"Extracting data from {csv_file_path}...")
        self.df = pd.read_csv(csv_file_path)
        print(f"Extracted {len(self.df)} records")
        return self.df
    
    def transform(self):
        """Transform and clean the data"""
        print("Transforming data...")
        
        if self.df is None:
            raise ValueError("No data to transform. Please run extract() first.")
        
        # Convert Date column to datetime
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        
        # Convert Amount to numeric
        self.df['Amount'] = pd.to_numeric(self.df['Amount'], errors='coerce')
        
        # Convert Qty to numeric
        self.df['Qty'] = pd.to_numeric(self.df['Qty'], errors='coerce')
        
        # Remove cancelled orders for revenue calculations
        self.df_active = self.df[self.df['Status'] != 'Cancelled'].copy()
        
        # Fill NaN values
        self.df_active['Amount'] = self.df_active['Amount'].fillna(0)
        self.df_active['Qty'] = self.df_active['Qty'].fillna(0)
        
        print(f"Transformed data: {len(self.df_active)} active orders")
        return self.df_active
    
    def calculate_kpis(self):
        """Calculate key performance indicators"""
        print("Calculating KPIs...")
        
        if self.df is None:
            raise ValueError("No data to calculate KPIs. Please run extract() first.")
        
        kpis = {
            'total_revenue': float(self.df_active['Amount'].sum()),
            'total_orders': int(len(self.df_active)),
            'total_quantity': int(self.df_active['Qty'].sum()),
            'average_order_value': float(self.df_active['Amount'].mean()),
            'cancelled_orders': int(len(self.df[self.df['Status'] == 'Cancelled'])),
            'cancellation_rate': float(len(self.df[self.df['Status'] == 'Cancelled']) / len(self.df) * 100)
        }
        
        return kpis
    
    def aggregate_by_state(self):
        """Aggregate sales by state"""
        print("Aggregating by state...")
        
        state_agg = self.df_active.groupby('ship-state').agg({
            'Amount': 'sum',
            'Qty': 'sum',
            'Order ID': 'count'
        }).reset_index()
        
        state_agg.columns = ['state', 'revenue', 'quantity', 'order_count']
        state_agg = state_agg.sort_values('revenue', ascending=False)
        
        return state_agg.to_dict('records')
    
    def aggregate_by_category(self):
        """Aggregate sales by category"""
        print("Aggregating by category...")
        
        category_agg = self.df_active.groupby('Category').agg({
            'Amount': 'sum',
            'Qty': 'sum',
            'Order ID': 'count'
        }).reset_index()
        
        category_agg.columns = ['category', 'revenue', 'quantity', 'order_count']
        category_agg = category_agg.sort_values('revenue', ascending=False)
        
        return category_agg.to_dict('records')
    
    def aggregate_by_month(self):
        """Aggregate sales by month"""
        print("Aggregating by month...")
        
        month_agg = self.df_active.groupby(['Year', 'Month', 'MonthName']).agg({
            'Amount': 'sum',
            'Qty': 'sum',
            'Order ID': 'count'
        }).reset_index()
        
        month_agg.columns = ['year', 'month', 'month_name', 'revenue', 'quantity', 'order_count']
        month_agg = month_agg.sort_values(['year', 'month'])
        
        return month_agg.to_dict('records')
    
    def aggregate_by_size(self):
        """Aggregate sales by size"""
        print("Aggregating by size...")
        
        size_agg = self.df_active.groupby('Size').agg({
            'Amount': 'sum',
            'Qty': 'sum',
            'Order ID': 'count'
        }).reset_index()
        
        size_agg.columns = ['size', 'revenue', 'quantity', 'order_count']
        size_agg = size_agg.sort_values('revenue', ascending=False)
        
        return size_agg.to_dict('records')
    
    def get_top_performers(self):
        """Get top performing metrics"""
        print("Calculating top performers...")
        
        state_data = self.aggregate_by_state()
        category_data = self.aggregate_by_category()
        
        top_performers = {
            'top_state': state_data[0] if state_data else None,
            'top_5_states': state_data[:5],
            'top_category': category_data[0] if category_data else None,
            'top_5_categories': category_data[:5]
        }
        
        return top_performers
    
    def load_to_s3(self, aggregated_data, key='processed/aggregated_sales.json'):
        """Load aggregated data to S3"""
        print(f"Loading data to S3: s3://{self.s3_bucket_name}/{key}")
        
        if self.df is None:
            raise ValueError("No data available. Please run extract() first.")
        
        # Add metadata
        aggregated_data['metadata'] = {
            'last_updated': datetime.now().isoformat(),
            'total_records_processed': len(self.df),
            'active_orders_processed': len(self.df_active),
            'pipeline_version': '1.0'
        }
        
        # Convert to JSON
        json_data = json.dumps(aggregated_data, indent=2, default=str)
        
        # Upload to S3
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket_name,
                Key=key,
                Body=json_data,
                ContentType='application/json'
            )
            print(f"Successfully uploaded to S3!")
            return True
        except Exception as e:
            print(f"Error uploading to S3: {str(e)}")
            return False
    
    def run_pipeline(self, csv_file_path):
        """Run the complete ETL pipeline"""
        print("="*50)
        print("Starting ETL Pipeline")
        print("="*50)
        
        # Extract
        self.extract(csv_file_path)
        
        # Transform
        self.transform()
        
        # Calculate all aggregations
        kpis = self.calculate_kpis()
        state_data = self.aggregate_by_state()
        category_data = self.aggregate_by_category()
        month_data = self.aggregate_by_month()
        size_data = self.aggregate_by_size()
        top_performers = self.get_top_performers()
        
        # Prepare aggregated data
        aggregated_data = {
            'kpis': kpis,
            'by_state': state_data,
            'by_category': category_data,
            'by_month': month_data,
            'by_size': size_data,
            'top_performers': top_performers
        }
        
        # Load to S3
        success = self.load_to_s3(aggregated_data)
        
        # Also save locally for reference
        with open('aggregated_sales.json', 'w') as f:
            json.dump(aggregated_data, f, indent=2, default=str)
        print("Saved local copy: aggregated_sales.json")
        
        print("="*50)
        print("ETL Pipeline Completed Successfully!")
        print("="*50)
        
        # Print summary
        print("\n📊 Summary Statistics:")
        print(f"Total Revenue: ₹{kpis['total_revenue']:,.2f}")
        print(f"Total Orders: {kpis['total_orders']:,}")
        print(f"Average Order Value: ₹{kpis['average_order_value']:,.2f}")
        print(f"Top State: {top_performers['top_state']['state']} (₹{top_performers['top_state']['revenue']:,.2f})")
        print(f"Top Category: {top_performers['top_category']['category']} (₹{top_performers['top_category']['revenue']:,.2f})")
        
        return aggregated_data


if __name__ == "__main__":
    # Configuration
    S3_BUCKET_NAME = "sales-etl-data-yourname"  # Change this to your bucket name
    CSV_FILE_PATH = "Cleaned_Amazon_Sale_Report.csv"
    
    # Initialize and run pipeline
    pipeline = SalesETLPipeline(S3_BUCKET_NAME)
    
    try:
        results = pipeline.run_pipeline(CSV_FILE_PATH)
        print("\n✅ Pipeline executed successfully!")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        raise