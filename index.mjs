import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { parse } from 'csv-parse/sync';

// Initialize S3 client
const s3Client = new S3Client({ region: process.env.AWS_REGION || "us-east-1" });

// Configuration - Update these with your values
const S3_BUCKET = process.env.S3_BUCKET || "your-sales-data-bucket";
const S3_KEY = process.env.S3_KEY || "etl-output/Cleaned_Amazon_Sale_Report.csv";

export const handler = async (event) => {
  console.log("Starting sales analytics execution at:", new Date().toISOString());
  console.log("Event:", JSON.stringify(event, null, 2));
  
  try {
    // Step 1: Read CSV from S3
    console.log(`Reading from S3: ${S3_BUCKET}/${S3_KEY}`);
    const csvData = await readCSVFromS3(S3_BUCKET, S3_KEY);
    console.log(`Loaded ${csvData.length} records from S3`);
    
    // Step 2: Filter out cancelled orders
    const activeOrders = csvData.filter(row => row.Status !== 'Cancelled');
    console.log(`Active orders: ${activeOrders.length}`);
    
    // Step 3: Calculate KPIs
    const kpis = calculateKPIs(activeOrders);
    console.log("KPIs calculated:", kpis);
    
    // Step 4: Get regional analytics
    const regionalAnalytics = getRegionalAnalytics(activeOrders);
    
    // Step 5: Get category performance
    const categoryPerformance = getCategoryPerformance(activeOrders);
    
    // Step 6: Get monthly trends
    const monthlyTrends = getMonthlyTrends(activeOrders);
    
    // Step 7: Prepare response
    const result = {
      status: "success",
      timestamp: new Date().toISOString(),
      data: {
        kpis: kpis,
        regional_analytics: regionalAnalytics.slice(0, 10), // Top 10 regions
        category_performance: categoryPerformance,
        monthly_trends: monthlyTrends
      },
      metadata: {
        total_records: csvData.length,
        active_records: activeOrders.length,
        cancelled_records: csvData.length - activeOrders.length
      }
    };
    
    console.log("Analytics completed successfully");
    
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      },
      body: JSON.stringify(result, null, 2)
    };
    
  } catch (error) {
    console.error("Error processing sales data:", error);
    
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({
        status: "error",
        message: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};

/**
 * Read CSV file from S3
 */
async function readCSVFromS3(bucket, key) {
  try {
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key
    });
    
    const response = await s3Client.send(command);
    const csvString = await streamToString(response.Body);
    
    // Parse CSV
    const records = parse(csvString, {
      columns: true,
      skip_empty_lines: true,
      trim: true
    });
    
    return records;
  } catch (error) {
    console.error("Error reading from S3:", error);
    throw new Error(`Failed to read CSV from S3: ${error.message}`);
  }
}

/**
 * Convert stream to string
 */
async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf-8');
}

/**
 * Calculate Key Performance Indicators
 */
function calculateKPIs(orders) {
  let totalRevenue = 0;
  let totalQuantity = 0;
  let b2bRevenue = 0;
  let b2cRevenue = 0;
  let amazonFulfilled = 0;
  let merchantFulfilled = 0;
  
  orders.forEach(order => {
    const amount = parseFloat(order.Amount) || 0;
    const qty = parseInt(order.Qty) || 0;
    
    totalRevenue += amount;
    totalQuantity += qty;
    
    // B2B vs B2C
    if (order.B2B === 'True' || order.B2B === true) {
      b2bRevenue += amount;
    } else {
      b2cRevenue += amount;
    }
    
    // Fulfillment method
    if (order['fulfilled-by'] === 'Amazon') {
      amazonFulfilled++;
    } else if (order['fulfilled-by'] === 'Merchant') {
      merchantFulfilled++;
    }
  });
  
  const avgOrderValue = orders.length > 0 ? totalRevenue / orders.length : 0;
  
  return {
    total_revenue: round(totalRevenue, 2),
    total_orders: orders.length,
    average_order_value: round(avgOrderValue, 2),
    total_quantity_sold: totalQuantity,
    b2b_revenue: round(b2bRevenue, 2),
    b2c_revenue: round(b2cRevenue, 2),
    b2b_percentage: round((b2bRevenue / totalRevenue) * 100, 2),
    amazon_fulfilled_orders: amazonFulfilled,
    merchant_fulfilled_orders: merchantFulfilled
  };
}

/**
 * Get regional analytics
 */
function getRegionalAnalytics(orders) {
  const regionalMap = {};
  
  orders.forEach(order => {
    const state = order['ship-state'] || 'Unknown';
    const amount = parseFloat(order.Amount) || 0;
    
    if (!regionalMap[state]) {
      regionalMap[state] = {
        state: state,
        revenue: 0,
        order_count: 0
      };
    }
    
    regionalMap[state].revenue += amount;
    regionalMap[state].order_count++;
  });
  
  // Convert to array and sort by revenue
  const regionalArray = Object.values(regionalMap)
    .map(region => ({
      ...region,
      revenue: round(region.revenue, 2)
    }))
    .sort((a, b) => b.revenue - a.revenue);
  
  return regionalArray;
}

/**
 * Get category performance
 */
function getCategoryPerformance(orders) {
  const categoryMap = {};
  
  orders.forEach(order => {
    const category = order.Category || 'Unknown';
    const amount = parseFloat(order.Amount) || 0;
    const qty = parseInt(order.Qty) || 0;
    
    if (!categoryMap[category]) {
      categoryMap[category] = {
        category: category,
        revenue: 0,
        quantity_sold: 0,
        order_count: 0
      };
    }
    
    categoryMap[category].revenue += amount;
    categoryMap[category].quantity_sold += qty;
    categoryMap[category].order_count++;
  });
  
  // Convert to array and sort by revenue
  const categoryArray = Object.values(categoryMap)
    .map(cat => ({
      ...cat,
      revenue: round(cat.revenue, 2),
      avg_order_value: round(cat.revenue / cat.order_count, 2)
    }))
    .sort((a, b) => b.revenue - a.revenue);
  
  return categoryArray;
}

/**
 * Get monthly trends
 */
function getMonthlyTrends(orders) {
  const monthlyMap = {};
  
  orders.forEach(order => {
    const year = order.Year || '2022';
    const month = String(order.Month || '1').padStart(2, '0');
    const yearMonth = `${year}-${month}`;
    const monthName = order.MonthName || 'Unknown';
    const amount = parseFloat(order.Amount) || 0;
    
    if (!monthlyMap[yearMonth]) {
      monthlyMap[yearMonth] = {
        year_month: yearMonth,
        month_name: monthName,
        revenue: 0,
        order_count: 0
      };
    }
    
    monthlyMap[yearMonth].revenue += amount;
    monthlyMap[yearMonth].order_count++;
  });
  
  // Convert to array and sort by date
  const monthlyArray = Object.values(monthlyMap)
    .map(month => ({
      ...month,
      revenue: round(month.revenue, 2),
      avg_order_value: round(month.revenue / month.order_count, 2)
    }))
    .sort((a, b) => a.year_month.localeCompare(b.year_month));
  
  return monthlyArray;
}

/**
 * Round number to specified decimal places
 */
function round(value, decimals) {
  return Number(Math.round(value + 'e' + decimals) + 'e-' + decimals);
}