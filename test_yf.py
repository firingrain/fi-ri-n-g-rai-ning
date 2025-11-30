import yfinance as yf

print("=== Testing yfinance ===")

ticker = yf.Ticker("7203.T")   # Toyota

info = ticker.fast_info

print("fast_info:", info)

hist = ticker.history(period="5d")
print("history:")
print(hist)
