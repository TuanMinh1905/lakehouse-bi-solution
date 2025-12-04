# jobs/03_silver_to_gold.py
"""
Gold Layer: Tạo các bảng tổng hợp và phân khúc từ Silver (MinIO).
Đọc từ MinIO silver → Tạo 5 bảng Gold → Upload CSV lên MinIO gold.

Output tables:
1. agg_daily_sales      - Tổng hợp doanh thu theo ngày (Dashboard 1)
2. agg_product_performance - Hiệu quả sản phẩm (Dashboard 2)  
3. agg_customer_lifetime - Giá trị vòng đời khách hàng (Dashboard 3)
4. agg_channel_performance - Hiệu quả kênh bán (Dashboard 1)
5. customer_segment     - Phân khúc khách hàng RFM (Dashboard 3)
"""
import os
import io
import pandas as pd
import numpy as np
import boto3
from dotenv import load_dotenv
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

# Ngày hiện tại để tính RFM
TODAY = datetime.now()


def get_s3_client():
    """Tạo S3 client để kết nối MinIO"""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )


def read_csv_from_minio(s3_client, key: str) -> pd.DataFrame:
    """Đọc CSV từ MinIO"""
    response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()), low_memory=False)


def upload_csv_to_minio(df: pd.DataFrame, key: str, s3_client):
    """Upload DataFrame dưới dạng CSV lên MinIO"""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"✅ Uploaded to s3://{MINIO_BUCKET}/{key}")


def load_silver_data(s3_client):
    """Load tất cả data từ Silver layer"""
    print("📖 Loading Silver data...")
    
    orders = read_csv_from_minio(s3_client, "silver/orders_clean/data.csv")
    checkouts = read_csv_from_minio(s3_client, "silver/checkouts_clean/data.csv")
    customers = read_csv_from_minio(s3_client, "silver/customers_clean/data.csv")
    
    # Parse dates
    orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
    checkouts['created_at'] = pd.to_datetime(checkouts['created_at'], errors='coerce')
    customers['created_at'] = pd.to_datetime(customers['created_at'], errors='coerce')
    
    print(f"  → Orders: {len(orders)} rows")
    print(f"  → Checkouts: {len(checkouts)} rows")
    print(f"  → Customers: {len(customers)} rows")
    
    return orders, checkouts, customers


# ============================================================
# 2.1. agg_daily_sales - Tổng hợp theo ngày
# ============================================================
def build_agg_daily_sales(orders: pd.DataFrame, checkouts: pd.DataFrame, 
                          customers: pd.DataFrame, s3_client):
    """
    Bảng 2.1: Tổng hợp doanh thu theo ngày
    Columns: date, total_orders, total_revenue, total_checkouts, 
             checkout_to_order_cr, new_customers, returning_customers, avg_order_value
    """
    print("\n📊 Building agg_daily_sales...")
    
    # Tạo date column
    orders['date'] = orders['order_date'].dt.date
    checkouts['date'] = checkouts['created_at'].dt.date
    customers['date'] = customers['created_at'].dt.date
    
    # Tính first order date cho mỗi customer
    customer_first_order = orders.groupby('customer_id')['order_date'].min().reset_index()
    customer_first_order.columns = ['customer_id', 'first_order_date']
    customer_first_order['first_order_date'] = customer_first_order['first_order_date'].dt.date
    
    # Merge để xác định new vs returning
    orders_with_first = orders.merge(customer_first_order, on='customer_id', how='left')
    orders_with_first['is_new_customer'] = orders_with_first['date'] == orders_with_first['first_order_date']
    
    # Aggregate orders theo ngày
    orders_daily = orders.groupby('date').agg(
        total_orders=('order_id', 'nunique'),
    ).reset_index()
    
    # Tính total_revenue = count orders (không có giá trong data)
    # Giả sử mỗi order = 1 unit revenue nếu không có price
    orders_daily['total_revenue'] = orders_daily['total_orders']  # Placeholder
    
    # Aggregate checkouts theo ngày
    checkouts_daily = checkouts.groupby('date').agg(
        total_checkouts=('checkout_id', 'nunique')
    ).reset_index()
    
    # Aggregate new customers theo ngày
    new_customers_daily = orders_with_first[orders_with_first['is_new_customer']].groupby('date').agg(
        new_customers=('customer_id', 'nunique')
    ).reset_index()
    
    # Aggregate returning customers theo ngày
    returning_customers_daily = orders_with_first[~orders_with_first['is_new_customer']].groupby('date').agg(
        returning_customers=('customer_id', 'nunique')
    ).reset_index()
    
    # Merge tất cả
    agg = orders_daily.merge(checkouts_daily, on='date', how='outer')
    agg = agg.merge(new_customers_daily, on='date', how='left')
    agg = agg.merge(returning_customers_daily, on='date', how='left')
    
    # Fill NaN với 0
    agg = agg.fillna(0)
    
    # Tính conversion rate
    agg['checkout_to_order_cr'] = np.where(
        agg['total_checkouts'] > 0,
        agg['total_orders'] / agg['total_checkouts'],
        0
    )
    
    # Tính avg order value
    agg['avg_order_value'] = np.where(
        agg['total_orders'] > 0,
        agg['total_revenue'] / agg['total_orders'],
        0
    )
    
    # Sort by date
    agg = agg.sort_values('date')
    
    # Select final columns
    agg = agg[['date', 'total_orders', 'total_revenue', 'total_checkouts',
               'checkout_to_order_cr', 'new_customers', 'returning_customers', 'avg_order_value']]
    
    upload_csv_to_minio(agg, "gold/agg_daily_sales/data.csv", s3_client)
    print(f"  → Rows: {len(agg)}")
    return agg


# ============================================================
# 2.2. agg_product_performance - Hiệu quả sản phẩm
# ============================================================
def build_agg_product_performance(orders: pd.DataFrame, s3_client):
    """
    Bảng 2.2: Hiệu quả sản phẩm
    Columns: product_id (sku), product_name, variant_id, variant_title,
             units_sold, orders_count, revenue, refund_count, refund_rate
    """
    print("\n📊 Building agg_product_performance...")
    
    # Group by product
    agg = orders.groupby(['sku', 'product_name', 'variant_id', 'variant_title']).agg(
        units_sold=('order_id', 'count'),  # Mỗi row là 1 unit
        orders_count=('order_id', 'nunique'),
    ).reset_index()
    
    # Revenue = units_sold (placeholder vì không có price trong data)
    agg['revenue'] = agg['units_sold']
    
    # Tính refund (nếu order_status = Refund/Refunded)
    refund_orders = orders[orders['order_status'].str.lower().str.contains('refund', na=False)]
    refund_agg = refund_orders.groupby(['sku']).agg(
        refund_count=('order_id', 'nunique')
    ).reset_index()
    
    agg = agg.merge(refund_agg, on='sku', how='left')
    agg['refund_count'] = agg['refund_count'].fillna(0).astype(int)
    
    # Tính refund rate
    agg['refund_rate'] = np.where(
        agg['orders_count'] > 0,
        agg['refund_count'] / agg['orders_count'],
        0
    )
    
    # Rename sku to product_id
    agg = agg.rename(columns={'sku': 'product_id'})
    
    # Sort by revenue descending
    agg = agg.sort_values('revenue', ascending=False)
    
    upload_csv_to_minio(agg, "gold/agg_product_performance/data.csv", s3_client)
    print(f"  → Rows: {len(agg)}")
    return agg


# ============================================================
# 2.3. agg_customer_lifetime - Giá trị vòng đời khách hàng
# ============================================================
def build_agg_customer_lifetime(orders: pd.DataFrame, customers: pd.DataFrame, s3_client):
    """
    Bảng 2.3: Giá trị vòng đời khách hàng
    Columns: customer_id, email, first_order_date, last_order_date,
             orders_count, lifetime_revenue, avg_order_value, country, state
    """
    print("\n📊 Building agg_customer_lifetime...")
    
    # Aggregate orders theo customer
    agg = orders.groupby('customer_id').agg(
        email=('email', 'first'),
        first_order_date=('order_date', 'min'),
        last_order_date=('order_date', 'max'),
        orders_count=('order_id', 'nunique'),
        country=('country', 'first'),
        state=('state', 'first'),
    ).reset_index()
    
    # Lifetime revenue = orders_count (placeholder vì không có price)
    agg['lifetime_revenue'] = agg['orders_count']
    
    # Avg order value
    agg['avg_order_value'] = np.where(
        agg['orders_count'] > 0,
        agg['lifetime_revenue'] / agg['orders_count'],
        0
    )
    
    # Merge với customers để lấy thêm info nếu cần
    if 'country' in customers.columns:
        customer_info = customers[['customer_id', 'country', 'state']].drop_duplicates()
        # Đảm bảo cùng kiểu dữ liệu
        agg['customer_id'] = agg['customer_id'].astype(str)
        customer_info['customer_id'] = customer_info['customer_id'].astype(str)
        agg = agg.merge(customer_info, on='customer_id', how='left', suffixes=('', '_cust'))
        
        # Fill missing country/state từ customers
        if 'country_cust' in agg.columns:
            agg['country'] = agg['country'].fillna(agg['country_cust'])
            agg['state'] = agg['state'].fillna(agg['state_cust'])
            agg = agg.drop(columns=['country_cust', 'state_cust'])
    
    # Select final columns
    agg = agg[['customer_id', 'email', 'first_order_date', 'last_order_date',
               'orders_count', 'lifetime_revenue', 'avg_order_value', 'country', 'state']]
    
    # Sort by lifetime_revenue descending
    agg = agg.sort_values('lifetime_revenue', ascending=False)
    
    upload_csv_to_minio(agg, "gold/agg_customer_lifetime/data.csv", s3_client)
    print(f"  → Rows: {len(agg)}")
    return agg


# ============================================================
# 2.4. agg_channel_performance - Hiệu quả kênh bán
# ============================================================
def build_agg_channel_performance(orders: pd.DataFrame, checkouts: pd.DataFrame, s3_client):
    """
    Bảng 2.4: Hiệu quả kênh bán
    Columns: date, channel, total_orders, total_revenue, total_checkouts, checkout_to_order_cr
    """
    print("\n📊 Building agg_channel_performance...")
    
    # Tạo date columns
    orders['date'] = orders['order_date'].dt.date
    checkouts['date'] = checkouts['created_at'].dt.date
    
    # Lấy channel từ checkouts (nếu có)
    if 'channel' in checkouts.columns:
        # Aggregate checkouts theo date + channel
        checkouts_agg = checkouts.groupby(['date', 'channel']).agg(
            total_checkouts=('checkout_id', 'nunique')
        ).reset_index()
        
        # Merge orders với checkouts để lấy channel
        # Đảm bảo cùng kiểu dữ liệu
        orders['customer_id'] = orders['customer_id'].astype(str)
        checkouts['customer_id'] = checkouts['customer_id'].astype(str)
        
        # Giả sử mỗi order có thể map với checkout qua customer_id + date
        orders_with_channel = orders.merge(
            checkouts[['customer_id', 'date', 'channel']].drop_duplicates(),
            on=['customer_id', 'date'],
            how='left'
        )
        orders_with_channel['channel'] = orders_with_channel['channel'].fillna('Unknown')
        
        # Aggregate orders theo date + channel
        orders_agg = orders_with_channel.groupby(['date', 'channel']).agg(
            total_orders=('order_id', 'nunique')
        ).reset_index()
        orders_agg['total_revenue'] = orders_agg['total_orders']  # Placeholder
        
        # Merge
        agg = orders_agg.merge(checkouts_agg, on=['date', 'channel'], how='outer')
    else:
        # Nếu không có channel, tạo bảng với channel = 'Direct'
        orders_agg = orders.groupby('date').agg(
            total_orders=('order_id', 'nunique')
        ).reset_index()
        orders_agg['total_revenue'] = orders_agg['total_orders']
        orders_agg['channel'] = 'Direct'
        
        checkouts_agg = checkouts.groupby('date').agg(
            total_checkouts=('checkout_id', 'nunique')
        ).reset_index()
        checkouts_agg['channel'] = 'Direct'
        
        agg = orders_agg.merge(checkouts_agg, on=['date', 'channel'], how='outer')
    
    # Fill NaN
    agg = agg.fillna(0)
    
    # Tính conversion rate
    agg['checkout_to_order_cr'] = np.where(
        agg['total_checkouts'] > 0,
        agg['total_orders'] / agg['total_checkouts'],
        0
    )
    
    # Sort
    agg = agg.sort_values(['date', 'channel'])
    
    # Select final columns
    agg = agg[['date', 'channel', 'total_orders', 'total_revenue', 'total_checkouts', 'checkout_to_order_cr']]
    
    upload_csv_to_minio(agg, "gold/agg_channel_performance/data.csv", s3_client)
    print(f"  → Rows: {len(agg)}")
    return agg


# ============================================================
# 3.1. customer_segment - Phân khúc khách hàng (RFM)
# ============================================================
def build_customer_segment(orders: pd.DataFrame, customers: pd.DataFrame, s3_client):
    """
    Bảng 3.1: Phân khúc khách hàng theo RFM
    Columns: customer_id, email, R_days, F_orders, M_revenue,
             R_score, F_score, M_score, segment_name, country, state
    
    Segment rules (từ 3.2):
    - VIP: F_orders >= 3 AND M_revenue in top 20% AND R_days <= 30
    - Loyal: F_orders >= 3 AND (not top 20% OR R_days 30-90)
    - New: F_orders = 1 AND R_days <= 30
    - At Risk: F_orders >= 2 AND R_days 90-180
    - Churned: R_days > 180
    """
    print("\n📊 Building customer_segment...")
    
    # Aggregate RFM metrics
    rfm = orders.groupby('customer_id').agg(
        email=('email', 'first'),
        last_order_date=('order_date', 'max'),
        F_orders=('order_id', 'nunique'),
        country=('country', 'first'),
        state=('state', 'first'),
    ).reset_index()
    
    # M_revenue = F_orders (placeholder vì không có price)
    rfm['M_revenue'] = rfm['F_orders']
    
    # Tính R_days (số ngày từ đơn gần nhất đến hôm nay)
    # Normalize timezone - remove timezone info nếu có
    rfm['last_order_date'] = pd.to_datetime(rfm['last_order_date']).dt.tz_localize(None)
    today = pd.Timestamp.now().tz_localize(None)
    rfm['R_days'] = (today - rfm['last_order_date']).dt.days
    
    # Xóa rows với NaN values
    rfm = rfm.dropna(subset=['R_days', 'F_orders', 'M_revenue'])
    rfm['R_days'] = rfm['R_days'].astype(int)
    
    # Tính percentile cho M_revenue (để xác định top 20%)
    m_80_percentile = rfm['M_revenue'].quantile(0.8)
    
    # Tính R, F, M scores (1-5) - dùng try/except để handle edge cases
    try:
        rfm['R_score'] = pd.qcut(rfm['R_days'], q=5, labels=[5, 4, 3, 2, 1], duplicates='drop')
    except ValueError:
        rfm['R_score'] = 3  # Default score nếu không đủ unique values
    
    try:
        rfm['F_score'] = pd.qcut(rfm['F_orders'].rank(method='first'), q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
    except ValueError:
        rfm['F_score'] = 3
    
    try:
        rfm['M_score'] = pd.qcut(rfm['M_revenue'].rank(method='first'), q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
    except ValueError:
        rfm['M_score'] = 3
    
    # Convert to int
    rfm['R_score'] = rfm['R_score'].astype(int)
    rfm['F_score'] = rfm['F_score'].astype(int)
    rfm['M_score'] = rfm['M_score'].astype(int)
    
    # Assign segment theo rules 3.2
    def assign_segment(row):
        r_days = row['R_days']
        f_orders = row['F_orders']
        m_revenue = row['M_revenue']
        
        # Churned: R_days > 180
        if r_days > 180:
            return 'Churned'
        
        # VIP: F_orders >= 3 AND M_revenue in top 20% AND R_days <= 30
        if f_orders >= 3 and m_revenue >= m_80_percentile and r_days <= 30:
            return 'VIP'
        
        # New: F_orders = 1 AND R_days <= 30
        if f_orders == 1 and r_days <= 30:
            return 'New'
        
        # At Risk: F_orders >= 2 AND R_days 90-180
        if f_orders >= 2 and 90 <= r_days <= 180:
            return 'At Risk'
        
        # Loyal: F_orders >= 3 AND (not top 20% OR R_days 30-90)
        if f_orders >= 3 and (m_revenue < m_80_percentile or 30 < r_days <= 90):
            return 'Loyal'
        
        # Default: Other
        return 'Other'
    
    rfm['segment_name'] = rfm.apply(assign_segment, axis=1)
    
    # Select final columns
    rfm = rfm[['customer_id', 'email', 'R_days', 'F_orders', 'M_revenue',
               'R_score', 'F_score', 'M_score', 'segment_name', 'country', 'state']]
    
    # Sort by segment
    segment_order = {'VIP': 0, 'Loyal': 1, 'New': 2, 'At Risk': 3, 'Churned': 4, 'Other': 5}
    rfm['segment_order'] = rfm['segment_name'].map(segment_order)
    rfm = rfm.sort_values(['segment_order', 'M_revenue'], ascending=[True, False])
    rfm = rfm.drop(columns=['segment_order'])
    
    upload_csv_to_minio(rfm, "gold/customer_segment/data.csv", s3_client)
    print(f"  → Rows: {len(rfm)}")
    
    # Print segment summary
    print("\n  📈 Segment Summary:")
    summary = rfm['segment_name'].value_counts()
    for seg, count in summary.items():
        print(f"      {seg}: {count}")
    
    return rfm


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("🔄 SILVER → GOLD TRANSFORMATION")
    print("=" * 60)
    
    s3_client = get_s3_client()
    
    # Load Silver data
    orders, checkouts, customers = load_silver_data(s3_client)
    
    # Build Gold tables
    build_agg_daily_sales(orders.copy(), checkouts.copy(), customers.copy(), s3_client)
    build_agg_product_performance(orders.copy(), s3_client)
    build_agg_customer_lifetime(orders.copy(), customers.copy(), s3_client)
    build_agg_channel_performance(orders.copy(), checkouts.copy(), s3_client)
    build_customer_segment(orders.copy(), customers.copy(), s3_client)
    
    print("\n" + "=" * 60)
    print("🎉 Gold layer complete!")
    print("=" * 60)
    print("\n📁 Output files on MinIO:")
    print("   gold/agg_daily_sales/data.csv")
    print("   gold/agg_product_performance/data.csv")
    print("   gold/agg_customer_lifetime/data.csv")
    print("   gold/agg_channel_performance/data.csv")
    print("   gold/customer_segment/data.csv")


if __name__ == "__main__":
    main()
