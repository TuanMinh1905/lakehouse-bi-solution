# E-commerce Analytics - Lakehouse BI Solution

## Giới thiệu
Project của chị Vy Vy

## Kiến trúc
- **Bronze Layer**: Dữ liệu thô từ CSV files ( Data Raw )
- **Silver Layer**: Dữ liệu đã được làm sạch và chuẩn hóa ( Bao gồm xóa null, xóa trùng lặp, xóa cột thừa, nói chung là clean :v )
- **Gold Layer**: Dimension/Fact tables, aggregates, và segments cho BI ( Chia dim fact phục vụ cho Dashboard)

## Cài đặt

1. Clone repository
```bash
git clone https://github.com/TuanMinh1905/lakehouse-bi-solution.git
cd lakehouse-bi-solution
```

2. Cài đặt dependencies
```bash
pip install -r requirements.txt
```

3. Cấu hình MinIO
- Copy `.env.example` thành `.env`
- Điền thông tin MinIO credentials

## Sử dụng

### 1. Upload dữ liệu lên Bronze trên Minio
```bash
python jobs/01_upload_bronze.py
```

### 2. Chuyển đổi Bronze → Silver, sau đó up load lên layer Silver trên Minio :v
```bash
python jobs/02_bronze_to_silver.py
```

### 3. Tạo Gold layer, xử lí xong và up lên Minio
```bash
python jobs/03_silver_to_gold.py
```

## Cấu trúc thư mục
```
ecom_analytics/
├── config/
│   └── .env.example
├── jobs/
│   ├── spark_session.py
│   ├── 01_upload_bronze.py
│   ├── 02_bronze_to_silver.py
│   └── 03_silver_to_gold.py
├── requirements.txt
└── README.md
```

### Thứ tự chạy file 
```
- Chạy Docker để mở Minio trước
- Chạy file 01_upload_bronze để load Data thô lên lớp Bronze trên Minio
- Chạy file 02_bronze_to_silver để clean data thô và load Data vừa làm sạch lên lớp Silver trên Minio
- Chạy file 03_silver_to_gold để tạo ra dim fact từ  data đc làm sạch và load Dim Fact lên lớp Gold trên Minio
- Chạy load_gold_to_sqlite để load data bảng Gold từ Minio xuống SQLITE để thực hiện truy vấn
- Chạy create_ecom_dashboards để tạo ra "index, dashboard_3_customer_funnel, dashboard_2_product_performance, dashboard_1_business_overview" để hoàn thành Output
```