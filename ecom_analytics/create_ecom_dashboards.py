"""
Script tạo 3 Dashboard E-commerce với Plotly
Đọc data từ SQLite và tạo interactive dashboards
"""
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import webbrowser

# Database path
BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "..", "ecom_analytics.db")

if not os.path.exists(DB_PATH):
    print(f"❌ Database not found: {DB_PATH}")
    print("Please run: python load_gold_to_sqlite.py")
    exit(1)

print("=" * 80)
print("🎨 CREATING E-COMMERCE DASHBOARDS")
print("=" * 80)

# Connect to database
conn = sqlite3.connect(DB_PATH)

# Load all data
print("\n📖 Loading data from SQLite...")
df_daily = pd.read_sql_query("SELECT * FROM agg_daily_sales ORDER BY date", conn)
df_daily['date'] = pd.to_datetime(df_daily['date'])

df_product = pd.read_sql_query("SELECT * FROM agg_product_performance ORDER BY revenue DESC", conn)
df_customer = pd.read_sql_query("SELECT * FROM agg_customer_lifetime ORDER BY lifetime_revenue DESC", conn)
df_segment = pd.read_sql_query("SELECT * FROM customer_segment", conn)
df_channel = pd.read_sql_query("SELECT * FROM agg_channel_performance", conn)
df_channel['date'] = pd.to_datetime(df_channel['date'])

print(f"✅ Loaded {len(df_daily)} daily records")
print(f"✅ Loaded {len(df_product)} products")
print(f"✅ Loaded {len(df_customer)} customers")
print(f"✅ Loaded {len(df_segment)} customer segments")

# =============================================================================
# DASHBOARD 1: Business Overview
# =============================================================================
print("\n📊 Creating Dashboard 1: Business Overview...")

fig1 = make_subplots(
    rows=2, cols=2,
    subplot_titles=('Revenue Over Time', 'Orders vs Checkouts', 'New vs Returning', 'Revenue by Channel'),
    specs=[[{"secondary_y": False}, {"secondary_y": False}],
           [{"type": "bar"}, {"type": "bar"}]]
)

# Revenue line
fig1.add_trace(go.Scatter(x=df_daily['date'], y=df_daily['total_revenue'],
                          mode='lines+markers', name='Revenue'), row=1, col=1)

# Orders vs Checkouts
fig1.add_trace(go.Scatter(x=df_daily['date'], y=df_daily['total_orders'],
                          name='Orders', line=dict(color='green')), row=1, col=2)
fig1.add_trace(go.Scatter(x=df_daily['date'], y=df_daily['total_checkouts'],
                          name='Checkouts', line=dict(color='orange', dash='dash')), row=1, col=2)

# New vs Returning
new_total = df_daily['new_customers'].sum()
returning_total = df_daily['returning_customers'].sum()
fig1.add_trace(go.Bar(x=['New', 'Returning'], y=[new_total, returning_total],
                      marker_color=['orange', 'green']), row=2, col=1)

# Revenue by Channel
channel_rev = df_channel.groupby('channel')['total_revenue'].sum().reset_index()
fig1.add_trace(go.Bar(x=channel_rev['channel'], y=channel_rev['total_revenue']), row=2, col=2)

fig1.update_layout(height=800, title_text="📊 Dashboard 1: Business Overview", showlegend=True)
fig1.write_html(os.path.join(BASE_DIR, "dashboard_1_business_overview.html"))
print("✅ Saved: dashboard_1_business_overview.html")

# =============================================================================
# DASHBOARD 2: Product Performance
# =============================================================================
print("\n🎁 Creating Dashboard 2: Product Performance...")

fig2 = make_subplots(
    rows=2, cols=2,
    subplot_titles=('Top 10 Products by Revenue', 'Top 10 by Units Sold',
                    'Bottom 10 Products', 'Refund Rate Distribution')
)

# Top 10 Revenue
top10_rev = df_product.head(10)
fig2.add_trace(go.Bar(y=top10_rev['product_name'], x=top10_rev['revenue'],
                      orientation='h', marker_color='lightblue'), row=1, col=1)

# Top 10 Units
top10_units = df_product.nlargest(10, 'units_sold')
fig2.add_trace(go.Bar(y=top10_units['product_name'], x=top10_units['units_sold'],
                      orientation='h', marker_color='lightgreen'), row=1, col=2)

# Bottom 10
bottom10 = df_product.tail(10)
fig2.add_trace(go.Bar(y=bottom10['product_name'], x=bottom10['revenue'],
                      orientation='h', marker_color='lightcoral'), row=2, col=1)

# Refund Rate
fig2.add_trace(go.Histogram(x=df_product['refund_rate']*100, nbinsx=20,
                            marker_color='orange'), row=2, col=2)

fig2.update_layout(height=900, title_text="🎁 Dashboard 2: Product Performance", showlegend=False)
fig2.write_html(os.path.join(BASE_DIR, "dashboard_2_product_performance.html"))
print("✅ Saved: dashboard_2_product_performance.html")

# =============================================================================
# DASHBOARD 3: Customer & Funnel
# =============================================================================
print("\n👥 Creating Dashboard 3: Customer & Funnel...")

# Segment summary
segment_counts = df_segment['segment_name'].value_counts().reset_index()
segment_counts.columns = ['segment_name', 'count']

fig3 = make_subplots(
    rows=2, cols=2,
    subplot_titles=('Customer Segments', 'Revenue by Segment',
                    'Top 10 VIP Customers', 'Checkout → Order Funnel'),
    specs=[[{"type": "pie"}, {"type": "bar"}],
           [{"type": "bar"}, {"type": "funnel"}]]
)

# Segment pie
fig3.add_trace(go.Pie(labels=segment_counts['segment_name'],
                      values=segment_counts['count'], hole=0.4), row=1, col=1)

# Revenue by Segment
seg_rev = df_segment.groupby('segment_name')['M_revenue'].sum().reset_index()
fig3.add_trace(go.Bar(x=seg_rev['segment_name'], y=seg_rev['M_revenue']), row=1, col=2)

# Top VIP
vip = df_segment[df_segment['segment_name']=='VIP'].nlargest(10, 'M_revenue')
fig3.add_trace(go.Bar(y=vip['email'], x=vip['M_revenue'],
                      orientation='h', marker_color='gold'), row=2, col=1)

# Funnel
total_checkouts = df_daily['total_checkouts'].sum()
total_orders = df_daily['total_orders'].sum()
fig3.add_trace(go.Funnel(y=['Checkouts', 'Orders'],
                        x=[total_checkouts, total_orders]), row=2, col=2)

fig3.update_layout(height=900, title_text="👥 Dashboard 3: Customer & Funnel", showlegend=False)
fig3.write_html(os.path.join(BASE_DIR, "dashboard_3_customer_funnel.html"))
print("✅ Saved: dashboard_3_customer_funnel.html")

# =============================================================================
# Create index page
# =============================================================================
print("\n📝 Creating index page...")

total_revenue = df_daily['total_revenue'].sum()
total_orders = df_daily['total_orders'].sum()
avg_cr = df_daily['checkout_to_order_cr'].mean() * 100

index_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>E-commerce Analytics</title>
<style>
body{{font-family:Arial;margin:0;padding:40px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff}}
.container{{max-width:1200px;margin:0 auto}}
h1{{text-align:center;font-size:42px}}
.kpi{{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin:30px 0}}
.kpi-card{{background:rgba(255,255,255,0.2);padding:20px;border-radius:10px;text-align:center}}
.kpi-value{{font-size:32px;font-weight:bold;margin:10px 0}}
.cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:20px}}
.card{{background:rgba(255,255,255,0.15);padding:30px;border-radius:15px;text-align:center}}
.card:hover{{transform:translateY(-5px);transition:0.3s}}
.icon{{font-size:48px;margin-bottom:15px}}
a{{display:inline-block;margin-top:15px;padding:12px 30px;background:#fff;color:#667eea;text-decoration:none;border-radius:25px;font-weight:bold}}
a:hover{{background:#f0f0f0}}
</style></head><body><div class="container">
<h1>🛒 E-commerce Analytics Dashboard</h1>
<div class="kpi">
<div class="kpi-card"><div>Total Revenue</div><div class="kpi-value">{total_revenue:,.0f}</div></div>
<div class="kpi-card"><div>Total Orders</div><div class="kpi-value">{total_orders:,.0f}</div></div>
<div class="kpi-card"><div>Conversion Rate</div><div class="kpi-value">{avg_cr:.1f}%</div></div>
</div>
<div class="cards">
<div class="card"><div class="icon">📊</div><h2>Dashboard 1</h2><p>Tổng Quan Doanh Thu</p>
<a href="dashboard_1_business_overview.html">Xem Dashboard</a></div>
<div class="card"><div class="icon">🎁</div><h2>Dashboard 2</h2><p>Hiệu Quả Sản Phẩm</p>
<a href="dashboard_2_product_performance.html">Xem Dashboard</a></div>
<div class="card"><div class="icon">👥</div><h2>Dashboard 3</h2><p>Khách Hàng & Funnel</p>
<a href="dashboard_3_customer_funnel.html">Xem Dashboard</a></div>
</div></div></body></html>"""

with open(os.path.join(BASE_DIR, "index.html"), "w", encoding="utf-8") as f:
    f.write(index_html)

conn.close()

print("✅ Saved: index.html")
print("\n" + "=" * 80)
print("✅ ALL DASHBOARDS CREATED SUCCESSFULLY!")
print("=" * 80)
print("\n📁 Files created:")
print("   • index.html")
print("   • dashboard_1_business_overview.html")
print("   • dashboard_2_product_performance.html")
print("   • dashboard_3_customer_funnel.html")
print("\n🌐 Open index.html in browser to view dashboards")

# Auto open in browser
try:
    webbrowser.open('file://' + os.path.join(BASE_DIR, "index.html"))
    print("✅ Opened in browser automatically")
except:
    pass
