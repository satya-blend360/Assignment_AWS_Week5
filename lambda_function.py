import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from decimal import Decimal

# Initialize S3 client
s3_client = boto3.client('s3')

# Configuration
S3_BUCKET = 'your-sales-data-bucket'
S3_KEY = 'etl-output/Cleaned_Amazon_Sale_Report.csv'

def decimal_default(obj):
    """Helper to serialize Decimal objects to JSON"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    """
    Main Lambda handler for sales analytics API
    Triggered by API Gateway or EventBridge scheduler
    """
    
    try:
        # Log execution start
        print(f"Starting sales analytics at {datetime.now()}")
        
        # Read data from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"Loaded {len(df)} records from S3")
        
        # Convert Amount to numeric
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        
        # Filter out cancelled orders for revenue calculations
        df_active = df[df['Status'] != 'Cancelled']
        
        # Calculate KPIs
        kpis = calculate_kpis(df_active)
        
        # Get regional analytics
        regional_data = get_regional_analytics(df_active)
        
        # Get category performance
        category_data = get_category_performance(df_active)
        
        # Get monthly trends
        monthly_trends = get_monthly_trends(df_active)
        
        # Prepare response
        result = {
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'data': {
                'kpis': kpis,
                'regional_analytics': regional_data,
                'category_performance': category_data,
                'monthly_trends': monthly_trends
            }
        }
        
        print("Analytics calculation completed successfully")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(result, default=decimal_default)
        }
        
    except Exception as e:
        print(f"Error processing sales data: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }

def calculate_kpis(df):
    """Calculate key performance indicators"""
    total_revenue = float(df['Amount'].sum())
    total_orders = len(df)
    avg_order_value = float(df['Amount'].mean())
    total_quantity = int(df['Qty'].sum())
    
    # B2B vs B2C split
    b2b_revenue = float(df[df['B2B'] == True]['Amount'].sum()) if 'B2B' in df.columns else 0
    b2c_revenue = total_revenue - b2b_revenue
    
    # Fulfillment split
    amazon_fulfilled = int(df[df['fulfilled-by'] == 'Amazon'].shape[0])
    merchant_fulfilled = int(df[df['fulfilled-by'] == 'Merchant'].shape[0])
    
    return {
        'total_revenue': round(total_revenue, 2),
        'total_orders': total_orders,
        'average_order_value': round(avg_order_value, 2),
        'total_quantity_sold': total_quantity,
        'b2b_revenue': round(b2b_revenue, 2),
        'b2c_revenue': round(b2c_revenue, 2),
        'amazon_fulfilled_orders': amazon_fulfilled,
        'merchant_fulfilled_orders': merchant_fulfilled
    }

def get_regional_analytics(df):
    """Get top performing regions"""
    regional = df.groupby('ship-state').agg({
        'Amount': 'sum',
        'Order ID': 'count'
    }).round(2)
    
    regional.columns = ['revenue', 'order_count']
    regional = regional.sort_values('revenue', ascending=False).head(10)
    
    return regional.reset_index().to_dict('records')

def get_category_performance(df):
    """Analyze performance by product category"""
    category = df.groupby('Category').agg({
        'Amount': 'sum',
        'Qty': 'sum',
        'Order ID': 'count'
    }).round(2)
    
    category.columns = ['revenue', 'quantity_sold', 'order_count']
    category = category.sort_values('revenue', ascending=False)
    
    return category.reset_index().to_dict('records')

def get_monthly_trends(df):
    """Calculate monthly revenue trends"""
    df['YearMonth'] = df['Year'].astype(str) + '-' + df['Month'].astype(str).str.zfill(2)
    
    monthly = df.groupby('YearMonth').agg({
        'Amount': 'sum',
        'Order ID': 'count'
    }).round(2)
    
    monthly.columns = ['revenue', 'order_count']
    monthly = monthly.sort_index()
    
    return monthly.reset_index().to_dict('records')