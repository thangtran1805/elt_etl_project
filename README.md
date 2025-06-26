# 📊 Stock ELT Project

## A. Datasource

Dự án sử dụng các API để lấy dữ liệu từ thị trường chứng khoán Mỹ.

### 1. [Sec-api.io](https://sec-api.io/docs/mapping-api/list-companies-by-exchange)
- Lấy thông tin công ty theo sàn: NYSE, NASDAQ, OTC, v.v.

### 2. [Alpha Vantage](https://www.alphavantage.co/documentation/)
- Market Status: trạng thái thị trường toàn cầu.
- News Sentiment: tin tức tài chính và phân tích cảm xúc.

### 3. [Polygon.io](https://polygon.io/docs/rest/stocks/aggregates/daily-market-summary)
- Truy xuất dữ liệu OHLC, volume, VWAP theo ngày.

## B. Khám phá dữ liệu trả về từ API
Chi tiết schema trả về từ các API như tên công ty, ticker, sector, sentiment score, giá OHLC, số lượng giao dịch.

## C. Thời gian query của từng API
- sec-api.io: 0h đầu tháng
- Alpha Vantage: mỗi phiên giao dịch trong ngày
- Polygon.io: 1h đầu ngày

## D. Phạm vi dữ liệu
- Sàn: NASDAQ, NYSE
- Giá cổ phiếu: từ 18/06/2025
- Tin tức: từ 18/06/2015

## E. Khối lượng dữ liệu trả về
- OHLC: ~15,000 dòng/ngày
- News: ~1,000 bài viết/ngày
- Công ty: ~30,000 dòng/tháng

## F. Thiết kế Database
Chuẩn hóa 3NF từ sec-api.io & market status. Gồm 5 bảng liên quan tới công ty, sàn, ngành, khu vực.

![image](https://github.com/user-attachments/assets/15d7d583-7c85-43f4-8580-fd913fd14739)

## G. Business Requirements

### 1. Phân tích xu hướng giá cổ phiếu
- Theo dõi OHLC và trend (daily, weekly, monthly)
- Dự đoán xu hướng giá tương lai

### 2. Phân tích thị trường
- Phân tích cảm xúc thị trường theo topic
- Tác động của chủ đề đến giá cổ phiếu

### 3. Phân tích khối lượng giao dịch
- Theo dõi volume, phát hiện outliers
- Tương quan giữa volume và giá

### 4. Phân tích các nhóm ngành
- Hiệu suất ngành, xác định nhóm ngành tiềm năng

## H. Thiết kế hệ thống ETL / ELT

### 1. Galaxy Schema
- Thiết kế bảng dim và fact phục vụ Business Requirements
![image](https://github.com/user-attachments/assets/19c34ac3-3cab-4a2e-97dc-bfbe24e9aa5f)

### 2. ELT Diagram / Taskflow
- Luồng xử lý dữ liệu: từ crawl -> transform -> load -> analytics
![image](https://github.com/user-attachments/assets/9029adf7-7360-4ade-8064-ffa5adea5c60)

![image](https://github.com/user-attachments/assets/16bf5e38-a36f-4a87-9a13-e748ee17dafe)

### 3. ELT Tools
- Sử dụng: Python, Pandas, DuckDB, Amazon S3, Airflow
![image](https://github.com/user-attachments/assets/277292f4-f35e-4f33-8259-5a5a23cfb14a)


### 4. Cấu trúc thư mục
## Cấu trúc thư mục lưu trữ dữ liệu và Scripts.
## ETL cho Backend Database

![image](https://github.com/user-attachments/assets/50eb0c41-7e66-4430-81ca-9731988bacb2)

## ELT cho Data Warehouse.

![image](https://github.com/user-attachments/assets/39ecf622-1d6a-4e53-a85d-a05ffa8d54b7)

## Cấu hình thiết kế cho Database và Data Warehouse.

![image](https://github.com/user-attachments/assets/8449a5c2-f630-4e50-a994-00f2834fbf07)

## Cấu hình thiết kế DAG của Airflow.

![image](https://github.com/user-attachments/assets/eba65539-434f-413b-90a8-614b7d57bc4c)

### 5. Thiết kế DAGs:
## Thiết kế các DAGs để quản lý tự động hóa luồng dữ liệu.
![image](https://github.com/user-attachments/assets/571e5c8b-455b-4a18-8185-6dbe26d5027f)

### 6. Data Flow Diagram
## Sơ đồ data flow trong hệ thống.
![image](https://github.com/user-attachments/assets/15b04d67-ff4c-4cc1-88f4-85ddbdd9e55e)


![image](https://github.com/user-attachments/assets/715bad56-7216-4fe4-8a4a-074bba8b20c1)


