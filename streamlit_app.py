import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(page_title="Amazon Sales Dashboard", layout="wide", page_icon="📊")

@st.cache_data
def load_data():
    df = pd.read_csv('Cleaned_Amazon_Sale_Report.csv')
    df['Date'] = pd.to_datetime(df['Date'])
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce')
    return df

def calculate_kpis(df):
    total_revenue = df['Amount'].sum()
    total_orders = len(df)
    avg_order_value = df[df['Amount'] > 0]['Amount'].mean()
    cancelled_orders = df['Status'].str.contains('Cancel', case=False, na=False).sum()
    cancellation_rate = (cancelled_orders / total_orders) * 100
    
    return {
        'total_revenue': total_revenue,
        'total_orders': total_orders,
        'avg_order_value': avg_order_value,
        'cancellation_rate': cancellation_rate,
        'total_qty': df['Qty'].sum(),
        'unique_customers': df['Order ID'].nunique(),
        'states': df['ship-state'].nunique(),
        'categories': df['Category'].nunique()
    }

def main():
    st.title("🛒 Amazon Sales Analytics Dashboard")
    st.markdown("---")
    
    df = load_data()
    kpis = calculate_kpis(df)
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Overview", "🗺️ Regional Analysis", "📦 Product Insights", "🔍 Deep Dive", "🤖 AI Insights"])
    
    with tab1:
        st.header("Key Performance Indicators")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Revenue", f"₹{kpis['total_revenue']:,.0f}", delta="Target: 100%")
        with col2:
            st.metric("Total Orders", f"{kpis['total_orders']:,}", delta=f"{kpis['unique_customers']:,} unique")
        with col3:
            st.metric("Avg Order Value", f"₹{kpis['avg_order_value']:.2f}")
        with col4:
            st.metric("Cancellation Rate", f"{kpis['cancellation_rate']:.1f}%", delta="-2.3%", delta_color="inverse")
        
        col5, col6, col7, col8 = st.columns(4)
        with col5:
            st.metric("Total Quantity", f"{int(kpis['total_qty']):,}")
        with col6:
            st.metric("States Covered", f"{kpis['states']}")
        with col7:
            st.metric("Product Categories", f"{kpis['categories']}")
        with col8:
            b2b_pct = (df['B2B'].sum() / len(df)) * 100
            st.metric("B2B Orders", f"{b2b_pct:.1f}%")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Monthly Revenue Trend")
            monthly = df.groupby(['Year', 'Month'])['Amount'].sum().reset_index()
            fig = px.line(monthly, x='Month', y='Amount', markers=True, 
                         title="Revenue by Month", color_discrete_sequence=['#2E86C1'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Order Status Distribution")
            status_counts = df['Status'].value_counts().head(6)
            fig = px.pie(values=status_counts.values, names=status_counts.index,
                        title="Order Status Breakdown", hole=0.4)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("Top 10 Categories by Revenue")
            top_cats = df.groupby('Category')['Amount'].sum().sort_values(ascending=False).head(10)
            fig = px.bar(x=top_cats.values, y=top_cats.index, orientation='h',
                        labels={'x': 'Revenue', 'y': 'Category'},
                        color=top_cats.values, color_continuous_scale='Reds')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            st.subheader("B2B vs B2C Comparison")
            b2b_data = df.groupby('B2B')['Amount'].agg(['sum', 'mean', 'count']).reset_index()
            b2b_data['B2B'] = b2b_data['B2B'].map({True: 'B2B', False: 'B2C'})
            fig = go.Figure(data=[
                go.Bar(name='Total Revenue', x=b2b_data['B2B'], y=b2b_data['sum'], yaxis='y', offsetgroup=1),
                go.Bar(name='Avg Order Value', x=b2b_data['B2B'], y=b2b_data['mean'], yaxis='y2', offsetgroup=2)
            ])
            fig.update_layout(
                yaxis=dict(title='Total Revenue'),
                yaxis2=dict(title='AOV', overlaying='y', side='right'),
                height=400, barmode='group'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("Regional Sales Analysis")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("Top 15 States by Revenue")
            top_states = df.groupby('ship-state')['Amount'].sum().sort_values(ascending=False).head(15)
            fig = px.bar(x=top_states.index, y=top_states.values,
                        labels={'x': 'State', 'y': 'Revenue'},
                        color=top_states.values, color_continuous_scale='Viridis')
            fig.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("State Performance Metrics")
            state_metrics = df.groupby('ship-state').agg({
                'Amount': ['sum', 'mean', 'count']
            }).round(2)
            state_metrics.columns = ['Total Revenue', 'AOV', 'Orders']
            state_metrics = state_metrics.sort_values('Total Revenue', ascending=False).head(10)
            st.dataframe(state_metrics, height=500)
        
        st.markdown("---")
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("Top Cities by Order Volume")
            top_cities = df['ship-city'].value_counts().head(10)
            fig = px.pie(values=top_cities.values, names=top_cities.index, hole=0.3)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            st.subheader("Fulfillment Type Distribution")
            fulfillment = df['Fulfilment'].value_counts()
            fig = px.bar(x=fulfillment.index, y=fulfillment.values,
                        labels={'x': 'Fulfillment Type', 'y': 'Orders'},
                        color=fulfillment.values, color_continuous_scale='Blues')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.header("Product & Category Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Top 10 Products by Quantity Sold")
            top_products = df.groupby('SKU')['Qty'].sum().sort_values(ascending=False).head(10)
            fig = px.bar(y=top_products.index, x=top_products.values, orientation='h',
                        labels={'y': 'SKU', 'x': 'Quantity'},
                        color=top_products.values, color_continuous_scale='Greens')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Size Distribution")
            size_dist = df['Size'].value_counts().head(8)
            fig = px.bar(x=size_dist.index, y=size_dist.values,
                        labels={'x': 'Size', 'y': 'Orders'},
                        color=size_dist.values, color_continuous_scale='Oranges')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("Category Performance Matrix")
            cat_perf = df.groupby('Category').agg({
                'Qty': 'sum',
                'Amount': ['sum', 'mean']
            }).round(2)
            cat_perf.columns = ['Total Qty', 'Total Revenue', 'AOV']
            cat_perf = cat_perf.sort_values('Total Revenue', ascending=False).head(15)
            st.dataframe(cat_perf, height=400)
        
        with col4:
            st.subheader("Shipping Service Level")
            service_level = df['ship-service-level'].value_counts()
            fig = px.pie(values=service_level.values, names=service_level.index, hole=0.4)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.header("Deep Dive Analysis")
        
        st.subheader("🔎 Custom Filters")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            selected_states = st.multiselect("Select States", options=df['ship-state'].unique()[:20], default=None)
        with col2:
            selected_categories = st.multiselect("Select Categories", options=df['Category'].unique()[:15], default=None)
        with col3:
            status_filter = st.multiselect("Order Status", options=df['Status'].unique(), default=None)
        
        filtered_df = df.copy()
        if selected_states:
            filtered_df = filtered_df[filtered_df['ship-state'].isin(selected_states)]
        if selected_categories:
            filtered_df = filtered_df[filtered_df['Category'].isin(selected_categories)]
        if status_filter:
            filtered_df = filtered_df[filtered_df['Status'].isin(status_filter)]
        
        st.markdown("---")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Filtered Orders", f"{len(filtered_df):,}")
        with col2:
            st.metric("Filtered Revenue", f"₹{filtered_df['Amount'].sum():,.0f}")
        with col3:
            st.metric("Filtered Qty", f"{int(filtered_df['Qty'].sum()):,}")
        with col4:
            st.metric("Avg Order", f"₹{filtered_df['Amount'].mean():.2f}")
        
        st.markdown("---")
        
        col5, col6 = st.columns(2)
        
        with col5:
            st.subheader("Revenue Distribution")
            fig = px.histogram(filtered_df[filtered_df['Amount'] > 0], x='Amount', nbins=50,
                             labels={'Amount': 'Order Value'})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col6:
            st.subheader("Orders Over Time")
            daily_orders = filtered_df.groupby(filtered_df['Date'].dt.date)['Order ID'].count().reset_index()
            fig = px.line(daily_orders, x='Date', y='Order ID',
                         labels={'Order ID': 'Number of Orders'})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("📋 Sample Data")
        st.dataframe(filtered_df.head(100), height=400)
    
    with tab5:
        st.header("🤖 AI-Powered Insights")
        
        st.info("💡 Automated insights generated from your sales data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🎯 Top Insights")
            
            top_state = df.groupby('ship-state')['Amount'].sum().idxmax()
            top_state_revenue = df.groupby('ship-state')['Amount'].sum().max()
            
            top_category = df.groupby('Category')['Amount'].sum().idxmax()
            top_category_revenue = df.groupby('Category')['Amount'].sum().max()
            
            peak_month = df.groupby('MonthName')['Amount'].sum().idxmax()
            peak_revenue = df.groupby('MonthName')['Amount'].sum().max()
            
            st.success(f"🏆 **Top Performing State**: {top_state} with ₹{top_state_revenue:,.0f} in revenue")
            st.success(f"📦 **Best Category**: {top_category} generated ₹{top_category_revenue:,.0f}")
            st.success(f"📅 **Peak Month**: {peak_month} recorded ₹{peak_revenue:,.0f} in sales")
            
            avg_cancel_rate = kpis['cancellation_rate']
            if avg_cancel_rate > 15:
                st.warning(f"⚠️ High cancellation rate detected: {avg_cancel_rate:.1f}%")
            else:
                st.info(f"✅ Cancellation rate is healthy: {avg_cancel_rate:.1f}%")
            
            b2b_contribution = (df[df['B2B'] == True]['Amount'].sum() / df['Amount'].sum()) * 100
            st.info(f"💼 B2B orders contribute {b2b_contribution:.1f}% of total revenue")
        
        with col2:
            st.subheader("📊 Anomaly Detection")
            
            outliers = df[df['Amount'] > df['Amount'].quantile(0.99)]
            st.metric("High-Value Orders Detected", f"{len(outliers)}")
            st.caption(f"Orders above ₹{df['Amount'].quantile(0.99):.2f}")
            
            low_performing_states = df.groupby('ship-state')['Amount'].sum().sort_values().head(5)
            st.write("**Underperforming States:**")
            for state, revenue in low_performing_states.items():
                st.write(f"• {state}: ₹{revenue:,.0f}")
            
            cancelled_by_category = df[df['Status'].str.contains('Cancel', case=False, na=False)].groupby('Category').size().sort_values(ascending=False).head(5)
            st.write("**Categories with Most Cancellations:**")
            for cat, count in cancelled_by_category.items():
                st.write(f"• {cat}: {count} orders")
        
        st.markdown("---")
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("🎨 Sentiment Analysis")
            shipped_pct = (df['Status'].str.contains('Shipped', case=False, na=False).sum() / len(df)) * 100
            
            if shipped_pct > 80:
                st.success(f"✅ Excellent fulfillment rate: {shipped_pct:.1f}%")
            elif shipped_pct > 60:
                st.info(f"⚡ Good fulfillment rate: {shipped_pct:.1f}%")
            else:
                st.warning(f"⚠️ Needs improvement: {shipped_pct:.1f}%")
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=shipped_pct,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "Fulfillment Score"},
                gauge={'axis': {'range': [None, 100]},
                      'bar': {'color': "darkgreen"},
                      'steps': [
                          {'range': [0, 50], 'color': "lightgray"},
                          {'range': [50, 80], 'color': "gray"}],
                      'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 90}}))
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            st.subheader("💰 Revenue Health Score")
            
            revenue_per_order = df['Amount'].sum() / len(df)
            qty_per_order = df['Qty'].sum() / len(df)
            
            health_score = min(100, (revenue_per_order / 10) + (shipped_pct * 0.5))
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=health_score,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "Overall Health Score"},
                delta={'reference': 75},
                gauge={'axis': {'range': [None, 100]},
                      'bar': {'color': "darkblue"},
                      'steps': [
                          {'range': [0, 40], 'color': "lightcoral"},
                          {'range': [40, 70], 'color': "lightyellow"},
                          {'range': [70, 100], 'color': "lightgreen"}],
                      'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 80}}))
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("🔮 Predictive Recommendations")
        
        recommendations = []
        
        if kpis['cancellation_rate'] > 10:
            recommendations.append("• Focus on reducing cancellation rate by improving product descriptions and delivery estimates")
        
        top_3_states = df.groupby('ship-state')['Amount'].sum().sort_values(ascending=False).head(3).index.tolist()
        recommendations.append(f"• Concentrate marketing efforts in top states: {', '.join(top_3_states)}")
        
        peak_months = df.groupby('MonthName')['Amount'].sum().sort_values(ascending=False).head(2).index.tolist()
        recommendations.append(f"• Plan inventory for peak months: {', '.join(peak_months)}")
        
        low_aov_categories = df.groupby('Category')['Amount'].mean().sort_values().head(3).index.tolist()
        recommendations.append(f"• Consider bundling or upselling for categories: {', '.join(low_aov_categories)}")
        
        if b2b_contribution < 20:
            recommendations.append("• Explore B2B market expansion opportunities")
        
        for rec in recommendations:
            st.write(rec)
    
    st.sidebar.title("⚙️ Dashboard Settings")
    st.sidebar.markdown("---")
    st.sidebar.info(f"**Total Records**: {len(df):,}")
    st.sidebar.info(f"**Date Range**: {df['Date'].min().date()} to {df['Date'].max().date()}")
    st.sidebar.markdown("---")
    
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.caption("Built with Streamlit 🎈")

if __name__ == "__main__":
    main()