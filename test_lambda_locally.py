"""
Local testing script for Sales Analytics Lambda
Run this to test your function before deploying
"""

import json
import pandas as pd
from datetime import datetime

def load_local_csv(file_path):
    """Load CSV from local file system for testing"""
    return pd.read_csv(file_path)

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

def test_lambda_function():
    """Test the lambda function logic locally"""
    
    print("=" * 60)
    print("Testing Sales Analytics Lambda Function Locally")
    print("=" * 60)
    
    try:
        # Load data
        print("\n1. Loading CSV data...")
        df = load_local_csv('Cleaned_Amazon_Sale_Report.csv')
        print(f"   ✓ Loaded {len(df)} records")
        
        # Convert Amount to numeric
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        
        # Filter out cancelled orders
        df_active = df[df['Status'] != 'Cancelled']
        print(f"   ✓ Active orders: {len(df_active)}")
        
        # Calculate KPIs
        print("\n2. Calculating KPIs...")
        kpis = calculate_kpis(df_active)
        print(f"   ✓ Total Revenue: ₹{kpis['total_revenue']:,.2f}")
        print(f"   ✓ Total Orders: {kpis['total_orders']}")
        print(f"   ✓ Avg Order Value: ₹{kpis['average_order_value']:,.2f}")
        
        # Get regional analytics
        print("\n3. Analyzing regional performance...")
        regional_data = get_regional_analytics(df_active)
        print(f"   ✓ Top region: {regional_data[0]['ship-state']} (₹{regional_data[0]['revenue']:,.2f})")
        
        # Get category performance
        print("\n4. Analyzing category performance...")
        category_data = get_category_performance(df_active)
        print(f"   ✓ Top category: {category_data[0]['Category']} (₹{category_data[0]['revenue']:,.2f})")
        
        # Get monthly trends
        print("\n5. Calculating monthly trends...")
        monthly_trends = get_monthly_trends(df_active)
        print(f"   ✓ Months analyzed: {len(monthly_trends)}")
        
        # Prepare response
        result = {
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'data': {
                'kpis': kpis,
                'regional_analytics': regional_data[:5],  # Top 5 regions
                'category_performance': category_data,
                'monthly_trends': monthly_trends
            }
        }
        
        # Save result to file
        print("\n6. Saving results...")
        with open('test_output.json', 'w') as f:
            json.dump(result, f, indent=2)
        print("   ✓ Results saved to test_output.json")
        
        # Display summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(json.dumps(result['data']['kpis'], indent=2))
        
        print("\n✅ Test completed successfully!")
        print("=" * 60)
        
        return result
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    test_lambda_function()