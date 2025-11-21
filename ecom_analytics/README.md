# E-commerce Analytics - Lakehouse BI Solution

## Giới thiệu
Project xây dựng data lakehouse từ dữ liệu thô để tạo các dashboard hỗ trợ quyết định kinh doanh.

## Kiến trúc
- **Bronze Layer**: Dữ liệu thô từ CSV files
- **Silver Layer**: Dữ liệu đã được làm sạch và chuẩn hóa
- **Gold Layer**: Dimension/Fact tables, aggregates, và segments cho BI

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

### 1. Upload dữ liệu lên Bronze
```bash
python jobs/01_upload_bronze.py
```

### 2. Chuyển đổi Bronze → Silver
```bash
python jobs/02_bronze_to_silver.py
```

### 3. Tạo Gold layer
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
