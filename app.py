import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import io
import plotly.express as px
import urllib3
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 關閉 SSL 憑證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── 頁面設定 ─────────────────────────────────────────────────────────────────
st.set_page_config(page_title="專業基金與 ETF 觀測站", layout="wide", page_icon="📈")

if "cart" not in st.session_state:
    st.session_state.cart = pd.DataFrame()

st.title("📈 Yahoo Finance, MoneyDJ, DataGOV — ETF Dashboard")
st.caption("資料來源：Yahoo Finance, MoneyDJ, 台灣政府資料開放平台")


# 移除舊式被阻擋的 MoneyDJ 爬蟲，全面擁抱 Yahoo Finance

# ── 主畫面 (TABS) ─────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs([
    "🌐 ETF 與海外基金 (Yahoo)", 
    "🔍 全球基金搜尋雷達 (Yahoo)", 
])

# -------------------------------------------------------------------------
# Tab 1: ETF 與海外基金分析 (Yahoo Finance)
# -------------------------------------------------------------------------
with tab1:
    st.subheader("🌐 全球 ETF 與海外基金分析")
    st.write("透過 Yahoo Finance 擷取最新報價、走勢與歷年配息。支援台股 ETF (如 `0050.TW`) 與美股 (如 `SPY`, `ALZIX`)。")
    
    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        ticker_input = st.text_input("輸入 Ticker 代碼（多筆可用逗號分隔）", "0050.TW, SPY", key="yf_ticker")
    with c2:
        period_label = st.selectbox("圖表期間", ["1天", "5天", "1個月", "半年", "1年", "3年", "5年", "所有"], index=5, key="yf_period")
        period_map = {"1天":"1d", "5天":"5d", "1個月":"1mo", "半年":"6mo", "1年":"1y", "3年":"3y", "5年":"5y", "所有":"max"}
        period = period_map[period_label]
    with c3:
        st.write("") # push down
        st.write("")
        do_yf_search = st.button("🔍 搜尋", type="primary", use_container_width=True, key="btn_yf")

    if do_yf_search and ticker_input.strip():
        tickers = [t.strip() for t in ticker_input.split(",") if t.strip()]
        
        summary_data = []      # To hold data for the master table
        detailed_ui_data = {}  # To hold data to render later in expanders
        
        my_bar = st.progress(0, text="正在擷取資料...")
        
        for i, t in enumerate(tickers):
            my_bar.progress((i) / len(tickers), text=f"正在擷取 {t} ...")
            ticker = yf.Ticker(t)
            
            # 取得基本資訊 (名稱與是否為美國本土發行)，優先使用 longName 以防 shortName 被強制截斷(30字元)
            info = ticker.info if hasattr(ticker, 'info') else {}
            name = info.get('longName') or info.get('shortName') or t
            
            is_us = False
            if info.get('country') == 'United States' or info.get('currency') == 'USD':
                is_us = True
            elif not t.endswith('.TW') and not t.endswith('.TWO') and ('.' not in t or t.endswith('.US')):
                is_us = True
            tax_flag = "🇺🇸預扣30%稅" if is_us else ""
            
            # 使用 auto_adjust=True 計算含息總報酬率與真實波動率
            hist_10y_adj = ticker.history(period="10y", auto_adjust=True)
            # 使用 auto_adjust=False 計算精準的單期配息率
            hist_3y_raw = ticker.history(period="3y", auto_adjust=False)
            hist_chart  = ticker.history(period=period)
            
            if hist_10y_adj.empty and hist_3y_raw.empty:
                st.error(f"查無 {t} 的資料。")
                continue
                
            valid_closes_adj = hist_10y_adj['Close'].dropna()
            valid_closes_raw = hist_3y_raw['Close'].dropna() if not hist_3y_raw.empty else valid_closes_adj
            
            latest_price_raw = valid_closes_raw.iloc[-1] if not valid_closes_raw.empty else 0.0
            latest_date = valid_closes_raw.index[-1].strftime("%Y-%m-%d") if not valid_closes_raw.empty else "N/A"
            
            # 1. 波動率 (標準差)
            ann_std = None
            if len(valid_closes_adj) > 20:
                # 為了避免 10 年標準差失真，波動率通常使用近 1 至 3 年計算，這裡取近 3 年 (約 750 個交易日) 計算
                recent_adj = valid_closes_adj.tail(750) 
                ann_std = recent_adj.pct_change().dropna().std() * (252 ** 0.5)
                
            # 2. 淨值成長 (含息報酬)
            def get_ret(months):
                if valid_closes_adj.empty: return None
                target = valid_closes_adj.index[-1] - pd.DateOffset(months=months)
                # 為了容許假日誤差，給予 15 天的寬限期往後找
                past = valid_closes_adj[(valid_closes_adj.index >= target - pd.DateOffset(days=15)) & 
                                        (valid_closes_adj.index <= target + pd.DateOffset(days=15))]
                if past.empty: return None
                # 取最靠近 target 的那天
                idx = abs(past.index - target).argmin()
                return (valid_closes_adj.iloc[-1] / past.iloc[idx]) - 1

            ret_3m = get_ret(3)
            ret_6m = get_ret(6)
            ret_1y = get_ret(12)
            ret_2y = get_ret(24)
            ret_3y = get_ret(36)
            ret_5y = get_ret(60)
            
            # 3. 配息率與年化計算
            divs = ticker.dividends
            ann_yield = 0.0
            freq = 0
            df_merged = pd.DataFrame()
            
            if not divs.empty:
                if divs.index.tz is not None:
                    divs.index = divs.index.tz_localize(None)
                divs_3y = divs[divs.index >= pd.Timestamp.now() - pd.DateOffset(years=3)]
                
                if not divs_3y.empty:
                    df_divs = divs_3y.reset_index()
                    df_divs.columns = ['除息日', '每單位配息金額']
                    
                    df_hist = valid_closes_raw.reset_index()
                    df_hist.columns = ['Date', 'Close']
                    if df_hist['Date'].dt.tz is not None:
                        df_hist['Date'] = df_hist['Date'].dt.tz_localize(None)
                    
                    df_divs = df_divs.sort_values('除息日')
                    df_hist = df_hist.sort_values('Date')
                    
                    df_merged = pd.merge_asof(df_divs, df_hist, left_on='除息日', right_on='Date', direction='backward')
                    df_merged['單期配息率'] = df_merged['每單位配息金額'] / df_merged['Close']
                    
                    # 修正配息次數計算：以總時間跨度計算平均每年配息次數，更穩定
                    total_days = (df_merged['除息日'].max() - df_merged['除息日'].min()).days
                    if total_days > 180:
                        freq = round(len(df_merged) / (total_days / 365.25))
                    else:
                        freq = len(df_merged)
                    freq = max(freq, 1)
                    
                    latest_single_yield = df_merged.iloc[-1]['單期配息率'] if not df_merged.empty else 0.0
                    latest_div_amount = df_merged.iloc[-1]['每單位配息金額'] if not df_merged.empty else 0.0
                    ann_yield = latest_single_yield * freq
            
            # 4. 特性分類標籤邏輯
            tags = []
            
            # (1) 穩定度 (Volatility) 比較基準(約 15%)
            if ann_std is not None:
                if ann_std < 0.03:
                    tags.append("🧊超低波動")
                elif ann_std < 0.12:
                    tags.append("🟢低波動")
                elif ann_std <= 0.18:
                    tags.append("🟡大盤波動")
                else:
                    tags.append("🔴高波動")
            else:
                tags.append("⚪波動未知")
                
            # (2) 成長度 (以 3 年複合成長 CAGR 為基準)
            if ret_3y is not None:
                cagr_3y = (1 + ret_3y) ** (1/3) - 1
                if cagr_3y > 0.12:
                    tags.append("🚀高成長")
                elif cagr_3y >= 0.05:
                    tags.append("📈大盤成長")
                elif cagr_3y > 0:
                    tags.append("🐢緩步成長")
                else:
                    tags.append("📉衰退")
            else:
                tags.append("⚪成長未知")
                
            # (3) 配息穩定度與趨勢
            if not df_merged.empty and len(df_merged) >= 3:
                # df_merged 目前是最新的在第一列，轉為舊到新計算趨勢
                yields_seq = df_merged['單期配息率'].values[::-1]
                mean_y = yields_seq.mean()
                if mean_y > 0:
                    cv = yields_seq.std() / mean_y # 變異係數
                    # 利用序列索引計算皮爾森相關係數看出線性趨勢方向
                    trend = pd.Series(yields_seq).corr(pd.Series(range(len(yields_seq))))
                    
                    if cv < 0.15:
                        tags.append("🛡️高穩定配息")
                    elif pd.notna(trend) and trend > 0.6:
                        tags.append("🔥配息增長中")
                    elif pd.notna(trend) and trend < -0.6:
                        tags.append("⚠️配息下降中")
                    else:
                        tags.append("🔄配息波動大")
                else:
                    tags.append("⚪無配息")
            elif len(df_merged) > 0:
                tags.append("⚪配息次數偏少")
            else:
                tags.append("⚪無配息")
                
            fund_style = " | ".join(tags)
            
            def fmt_pct(v): return f"{v*100:.2f}%" if v is not None and not pd.isna(v) else "—"
            
            summary_data.append({
                "代碼": t,
                "名稱": name,
                "稅務屬性": tax_flag,
                "📌特性標籤": fund_style,
                "最新收盤價": f"{latest_price_raw:.2f}",
                "預估年化配息": fmt_pct(ann_yield),
                "年化波動風險": fmt_pct(ann_std),
                "3個月成長": fmt_pct(ret_3m),
                "半年成長": fmt_pct(ret_6m),
                "1年成長": fmt_pct(ret_1y),
                "2年成長": fmt_pct(ret_2y),
                "3年成長": fmt_pct(ret_3y),
                "5年成長": fmt_pct(ret_5y),
                "最新單次配息金額": f"{latest_div_amount:.4f}" if divs is not None and not divs.empty else "0.0000"
            })
            
            detailed_ui_data[t] = {
                "latest_date": latest_date,
                "latest_price": latest_price_raw,
                "ann_yield_str": fmt_pct(ann_yield),
                "freq": freq,
                "df_merged": df_merged,
                "hist_chart": hist_chart
            }

        my_bar.empty()
        
        # 將抓取的結果獨立存入 session_state，避免點擊購物車造成畫面重啟而消失
        st.session_state.yf_summary_data = summary_data
        st.session_state.yf_detailed_ui_data = detailed_ui_data

    # -- 繪製 UI 區塊 (只要有暫存資料就繪製，不再被「搜尋按鈕剛被按下」的條件綁定) --
    if "yf_summary_data" in st.session_state and st.session_state.yf_summary_data:
        summary_data = st.session_state.yf_summary_data
        detailed_ui_data = st.session_state.yf_detailed_ui_data
        
        st.markdown("### 📊 查詢標的總覽 (可勾選加入購物車)")
        df_summary = pd.DataFrame(summary_data)
        df_summary.insert(0, "加入購物車", False)
        
        edited_df = st.data_editor(
            df_summary,
            use_container_width=True,
            hide_index=True,
            column_config={
                "加入購物車": st.column_config.CheckboxColumn("加入購物車", required=True),
                "名稱": st.column_config.TextColumn("名稱", width="large")
            }
        )
        
        # --- PriceHistory Consolidation ---
        price_df = pd.DataFrame()
        for t, d in detailed_ui_data.items():
            close_series = d["hist_chart"]['Close'].rename(t)
            if price_df.empty:
                price_df = close_series.to_frame()
            else:
                price_df = price_df.join(close_series, how='outer')
        
        p1, p2 = st.columns(2)
        with p1:
            if st.button("🛒 將勾選項目加入購物車", use_container_width=True):
                selected = edited_df[edited_df["加入購物車"]]
                if not selected.empty:
                    new_cart = selected.drop(columns=["加入購物車"])
                    if st.session_state.cart.empty:
                        st.session_state.cart = new_cart
                    else:
                        st.session_state.cart = pd.concat([st.session_state.cart, new_cart]).drop_duplicates(subset=["代碼"], keep='last')
                    st.rerun()
                else:
                    st.warning("請先在上方表格勾選欲加入購物車的標的。")
        with p2:
            if not price_df.empty:
                price_csv = price_df.sort_index(ascending=False).to_csv()
                st.download_button("⬇️ 下載所選標的歷史報價 (PriceHistory CSV)",
                    data=price_csv.encode("utf-8-sig"),
                    file_name=f"PriceHistory_{datetime.date.today()}.csv",
                    mime="text/csv", use_container_width=True)
                    
        st.divider()
        
        # --- 購物車顯示與輸出 ---
        if not st.session_state.cart.empty:
            st.markdown("### 🛒 您的購物車清單")
            st.dataframe(
                st.session_state.cart, 
                use_container_width=True, 
                hide_index=True,
                column_config={"名稱": st.column_config.TextColumn("名稱", width="large")}
            )
            
            # --- 動態日期報價附掛功能 ---
            st.markdown("#### 📅 附加指定日期範圍淨值至 Excel")
            date_input = st.text_input("輸入欲附掛的對應日期 (單日如 `2026/4/2`，多日如 `2026/2/3, 2026/3/13`，或區間如 `2026/1/1~2026/4/2`)", "")
            
            export_df = st.session_state.cart.copy()
            
            if date_input.strip() and not price_df.empty:
                try:
                    p_df = price_df.copy()
                    # 消除時區影響以精確匹配
                    if p_df.index.tz is not None:
                        p_df.index = p_df.index.tz_localize(None)
                    p_df.index = pd.to_datetime(p_df.index)
                    
                    if "~" in date_input:
                        dt1_str, dt2_str = date_input.split("~")
                        dt1, dt2 = pd.to_datetime(dt1_str.strip()), pd.to_datetime(dt2_str.strip())
                        # 把終點日期拉到當天晚上 23:59:59 涵蓋整天
                        mask = (p_df.index >= dt1) & (p_df.index <= dt2 + pd.Timedelta(days=1))
                        filtered_prices = p_df.loc[mask]
                    elif "," in date_input:
                        dates_list = [pd.to_datetime(d.strip()).normalize() for d in date_input.split(",") if d.strip()]
                        mask = p_df.index.normalize().isin(dates_list)
                        filtered_prices = p_df.loc[mask]
                    else:
                        dt1 = pd.to_datetime(date_input.strip())
                        mask = p_df.index.normalize() == dt1.normalize()
                        filtered_prices = p_df.loc[mask]
                        
                    if not filtered_prices.empty:
                        # 將日期矩陣轉置 (Ticker 為 Index，Date 為 Columns)
                        transposed = filtered_prices.T
                        transposed.columns = [d.strftime("%Y-%m-%d") for d in transposed.columns]
                        transposed.index.name = "代碼"
                        transposed = transposed.reset_index()
                        
                        # 把這些新欄位水平對齊 JOIN 給原本的購物車
                        export_df = pd.merge(export_df, transposed, left_on="代碼", right_on="代碼", how="left")
                        st.success(f"✅ 已成功附掛 {len(transposed.columns)-1} 天的歷史淨值！可以點擊下方匯出了。")
                    else:
                        st.warning("⚠️ 查無此日期區間的淨值資料（可能是假日無交易）。")
                except Exception as e:
                    st.error(f"日期解析錯誤: {e}。請確保格式為 YYYY/MM/DD。")

            try:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer) as writer:
                    export_df.to_excel(writer, index=False, sheet_name="ETF_Cart")
                excel_data = excel_buffer.getvalue()
                
                c_btn1, c_btn2 = st.columns(2)
                with c_btn1:
                    st.download_button("📥 輸出附掛淨值後之 Excel",
                                       data=excel_data,
                                       file_name=f"ETF_Cart_with_prices_{datetime.date.today()}.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                       use_container_width=True)
                with c_btn2:
                    if st.button("🗑️ 清空購物車", use_container_width=True):
                        st.session_state.cart = pd.DataFrame()
                        st.rerun()
            except Exception as e:
                st.error(f"匯出 Excel 時發生錯誤 (可能是環境缺少 openpyxl): {e}")
                if st.button("🗑️ 清空購物車", use_container_width=True, key="clear_err"):
                    st.session_state.cart = pd.DataFrame()
                    st.rerun()
            st.divider()
        
        st.markdown("### 🔍 個別詳細資訊 (請點擊下列區塊展開查看走勢與配息)")
        
        for t, d in detailed_ui_data.items():
            with st.expander(f"**{t}** — 走勢圖與配息歷史"):
                m1, m2, m3 = st.columns(3)
                m1.metric("📅 最新交易日", d["latest_date"])
                m2.metric("💰 最新收盤價", f"{d['latest_price']:.2f}")
                freq_txt = f"年配約 {d['freq']} 次" if d['freq'] > 0 else "無配息"
                m3.metric("✨ 預估年化配息率", d["ann_yield_str"], freq_txt)
                
                if not d["hist_chart"].empty:
                    st.line_chart(d["hist_chart"][['Close']].dropna(), height=200)
                
                st.markdown("**配息歷程 (Dividend History) - 最近 3 年**")
                df_m = d["df_merged"]
                if df_m.empty:
                    st.info("該標的查無最近 3 年配息紀錄。")
                else:
                    df_display = df_m.copy()
                    df_display['單期配息率'] = (df_display['單期配息率'] * 100).map("{:.2f}%".format)
                    df_display.rename(columns={'Close': '除息前淨值'}, inplace=True)
                    df_display = df_display[['除息日', '每單位配息金額', '除息前淨值', '單期配息率']]
                    df_display = df_display.sort_values('除息日', ascending=False)
                    
                    ca, cb = st.columns([3, 1])
                    with ca:
                        st.dataframe(df_display, use_container_width=True, hide_index=True)
                    with cb:
                        st.download_button("⬇️ 下載 CSV",
                            data=df_display.to_csv(index=False).encode("utf-8-sig"),
                            file_name=f"{t}_dividends_3y.csv", mime="text/csv",
                            use_container_width=True, key=f"dl_{t}")

# -------------------------------------------------------------------------
# Tab 2: 台灣可申購境外基金搜尋 (鉅亨網 SSR)
# -------------------------------------------------------------------------
import re as _re
import json as _json
import time as _time

CNYES_SSR_URL = "https://fund.cnyes.com/search/"
CNYES_HTML_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
}
CNYES_API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://fund.cnyes.com/search/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
    "Origin": "https://fund.cnyes.com",
}

CNYES_FUND_GROUP = {
    "全部": "", "股票型": "G1", "債券型": "G2", "平衡型": "G3",
    "保本型": "G4", "貨幣型": "G5", "其他": "G6",
}
CNYES_AREA = {
    "全部": "", "全球市場": "A3", "美國": "A13", "亞洲地區": "A1",
    "台灣": "A6", "中國/大中華": "A0", "歐洲(含歐元區)": "A10",
    "全球新興市場": "A4", "日本": "A9", "北美洲": "A5",
    "拉丁美洲": "A7", "新興歐洲": "A8", "歐非中東": "A11",
}
CNYES_CURRENCY = {
    "全部": "", "美元": "USD", "台幣": "TWD", "歐元": "EUR",
    "日圓": "JPY", "人民幣": "CNY", "澳幣": "AUD", "英鎊": "GBP",
}


def cnyes_page1(keyword, fund_group, area, currency, for_sale_only):
    """第1頁：GET HTML → 解析 window.__data（不會被 404 擋）"""
    params = {}
    if keyword:       params["keyword"]        = keyword
    if fund_group:    params["fundGroup"]      = fund_group
    if area:          params["investmentArea"] = area
    if currency:      params["classCurrency"]  = currency
    if for_sale_only: params["forSale"]        = 1
    r = requests.get(CNYES_SSR_URL, params=params,
                     headers=CNYES_HTML_HEADERS, timeout=15)
    r.raise_for_status()
    match = _re.search(r'window\.__data\s*=\s*({.+?});\s*</script>',
                       r.text, _re.DOTALL)
    if not match:
        return None
    data = _json.loads(match.group(1))
    fbq = data.get("fundByQuery", {})
    if not fbq:
        return None
    return fbq[list(fbq.keys())[0]].get("data", {})


def cnyes_next_page(next_page_url):
    """第2頁起：用上一頁 meta.next_page_url 直接打 API"""
    url = f"https://fund.cnyes.com{next_page_url}"
    r = requests.get(url, headers=CNYES_API_HEADERS, timeout=12)
    r.raise_for_status()
    return r.json().get("data", {})


def fmt_ret(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v:+.2f}%"


def ts_to_date(unix):
    try:
        return datetime.datetime.utcfromtimestamp(int(unix)).strftime("%Y-%m-%d")
    except Exception:
        return "—"


with tab2:
    st.subheader("🔍 台灣可申購境外基金搜尋")
    st.caption("資料來源：鉅亨網，涵蓋全部台灣核備可申購境外基金，無 5 筆限制。")

    # ── 篩選列 ──
    ck1, ck2, ck3, ck4, ck5 = st.columns([3, 1.5, 1.5, 1.5, 1])
    with ck1:
        adv_kw = st.text_input("基金名稱關鍵字（中英文皆可）", "", key="cnyes_kw",
                               placeholder="如：安聯、貝萊德、Income、Blackrock")
    with ck2:
        fg_lbl = st.selectbox("基金類型", list(CNYES_FUND_GROUP.keys()), key="cnyes_fg")
    with ck3:
        area_lbl = st.selectbox("投資地區", list(CNYES_AREA.keys()), key="cnyes_area")
    with ck4:
        curr_lbl = st.selectbox("計價幣別", list(CNYES_CURRENCY.keys()), key="cnyes_curr")
    with ck5:
        st.write("")
        st.write("")
        for_sale = st.checkbox("僅顯示可申購", value=True, key="cnyes_forsale")

    cb1, cb2 = st.columns([1, 5])
    with cb1:
        btn_cnyes = st.button("🚀 搜尋", type="primary", use_container_width=True, key="btn_cnyes")

    # ── 執行搜尋 ──
    MAX_PAGES = 10
    if btn_cnyes:
        with st.spinner("連線鉅亨網搜尋中..."):
            try:
                data = cnyes_page1(
                    keyword=adv_kw.strip(),
                    fund_group=CNYES_FUND_GROUP[fg_lbl],
                    area=CNYES_AREA[area_lbl],
                    currency=CNYES_CURRENCY[curr_lbl],
                    for_sale_only=for_sale,
                )
            except Exception as e:
                st.error(f"鉅亨網連線失敗：{e}")
                data = None
        if data:
            meta      = data.get("meta", {})
            items     = data.get("items", [])
            total     = meta.get("total", len(items))
            last_page = meta.get("last_page", 1)
            next_url  = meta.get("next_page_url")

            # 第2頁起用 next_page_url 直接打 API
            if last_page > 1 and next_url:
                bar = st.progress(1 / min(last_page, MAX_PAGES), text="抓取更多結果...")
                for pg in range(2, min(last_page, MAX_PAGES) + 1):
                    try:
                        more = cnyes_next_page(next_url)
                        items.extend(more.get("items", []))
                        next_url = more.get("meta", {}).get("next_page_url")
                    except Exception:
                        break
                    bar.progress(pg / min(last_page, MAX_PAGES))
                    _time.sleep(0.15)
                    if not next_url:
                        break
                bar.empty()

            if items:
                rows = []
                for it in items:
                    rows.append({
                        "鉅亨ID":       it.get("cnyesId", ""),
                        "基金名稱":      it.get("displayNameLocal", ""),
                        "計價幣別":      it.get("classCurrencyLocal", ""),
                        "投資地區":      it.get("investmentArea", ""),
                        "基金組別":      it.get("categoryAbbr", ""),
                        "淨值":          it.get("nav"),
                        "今日漲跌%":     fmt_ret(it.get("changePercent")),
                        "近1月報酬%":    fmt_ret(it.get("return1Month")),
                        "近3月報酬%":    fmt_ret(it.get("return3Month")),
                        "近6月報酬%":    fmt_ret(it.get("return6Month")),
                        "近1年報酬%":    fmt_ret(it.get("return1Year")),
                        "淨值日期":      ts_to_date(it.get("priceDate")),
                        "可申購":        "✓" if it.get("forSale") == 1 else "✗",
                    })
                df_cnyes = pd.DataFrame(rows)
                st.session_state["cnyes_results"] = df_cnyes
                st.session_state["cnyes_total"] = total
                st.session_state["cnyes_last_page"] = last_page
            else:
                st.warning("查無結果，請調整篩選條件。")
                st.session_state.pop("cnyes_results", None)

    # ── 顯示結果（與搜尋按鈕解耦，rerun 不消失）──
    if "cnyes_results" in st.session_state:
        df_cnyes   = st.session_state["cnyes_results"]
        total      = st.session_state.get("cnyes_total", len(df_cnyes))
        last_page  = st.session_state.get("cnyes_last_page", 1)

        showing = len(df_cnyes)
        if last_page > 10:
            st.success(f"共找到 **{total:,}** 檔，顯示前 **{showing}** 筆（最多抓 200 筆）")
        else:
            st.success(f"共找到 **{total:,}** 檔，全部顯示")

        st.dataframe(
            df_cnyes,
            use_container_width=True,
            hide_index=True,
            column_config={
                "基金名稱": st.column_config.TextColumn("基金名稱", width="large"),
                "淨值": st.column_config.NumberColumn("淨值", format="%.4f"),
            },
            height=500,
        )

        st.info(
            "💡 **操作提示**：找到目標後，可至 **鉅亨網** 查詢完整資訊；"
            "或將基金代碼貼回第一頁 **【🌐 ETF 與海外基金】** 查詢 Yahoo 走勢圖（部分境外基金在 Yahoo 有對應 ticker）。"
        )

        dl1, dl2 = st.columns(2)
        with dl1:
            csv_bytes = df_cnyes.to_csv(index=False, encoding="utf-8-sig").encode()
            st.download_button(
                "📥 下載 CSV",
                data=csv_bytes,
                file_name=f"cnyes_fund_{datetime.date.today()}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with dl2:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df_cnyes.to_excel(writer, index=False, sheet_name="基金搜尋結果")
            st.download_button(
                "📥 下載 Excel",
                data=buf.getvalue(),
                file_name=f"cnyes_fund_{datetime.date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
