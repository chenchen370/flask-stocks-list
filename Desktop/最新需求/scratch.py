import tushare as ts

token = "0d0814421a33be8374842770e00eee83a225d4ed62b9d33b44f1d77f"
ts.set_token(token)

realtime_quote = ts.get_realtime_quotes('000001')
print(realtime_quote)

pro = ts.pro_api()
df = pro.daily(ts_code='600521.SH', start_date='20100101', end_date='20240718')
print(df)

# Save DataFrame to CSV
df.to_csv('daily_stock_data.csv', index=False)
