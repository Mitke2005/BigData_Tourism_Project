@echo off
chcp 65001 > nul
title Big Data Tourism Pipeline - Demo
echo =========================================================================
echo   ĐỒ ÁN BIG DATA: PHÂN TÍCH DỮ LIỆU DU LỊCH & GỢI Ý LỊCH TRÌNH THÔNG MINH
echo   Khởi chạy toàn bộ hệ thống: Kafka, Spark MLlib, HDFS Parquet & Web Live
echo =========================================================================
cd /d "D:\BigData\DOAN"
python run_all_demo.py
pause
