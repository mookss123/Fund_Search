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

st.title("📈 Yahoo Finance & Cnyes — ETF Dashboard")
st.caption("資料來源：Yahoo Finance, 鉅亨網 Cnyes")


# 移除舊式被阻擋的 MoneyDJ 爬蟲，全面擁抱 Yahoo Finance

# ── 主畫面 (TABS) ─────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs([
    "🌐 ETF 與海外基金 (Yahoo)", 
    "🔍 全球基金搜尋雷達 (Cnyes)", 
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
# Tab 2: 台灣可申購境外基金搜尋 (鉅亨網)
# -------------------------------------------------------------------------
import time as _time
import numpy as _np

CNYES_BASE    = "https://fund.api.cnyes.com"
CNYES_SEARCH  = f"{CNYES_BASE}/fund/api/v2/search/fund"
CNYES_NAV_URL = f"{CNYES_BASE}/fund/api/v1/funds/{{cid}}/nav"
CNYES_DIV_URL = f"{CNYES_BASE}/fund/api/v1/funds/{{cid}}/dividend"

CNYES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://fund.cnyes.com/",
    "Origin": "https://fund.cnyes.com",
    "X-Platform": "WEB",
    "X-System-Kind": "FUND-DESKTOP",
}
CNYES_FIELDS = (
    "categoryAbbr,change,changePercent,classCurrencyLocal,cnyesId,"
    "displayNameLocal,forSale,investmentArea,nav,priceDate,"
    "return1Month,return3Month,return6Month,return1Year,saleStatus"
)

CNYES_FUND_GROUP = {
    "全部":"","股票型":"G1","債券型":"G2","平衡型":"G3",
    "保本型":"G4","貨幣型":"G5","其他":"G6",
}
CNYES_AREA = {
    "全部":"","全球市場":"A3","美國":"A13","亞洲地區":"A1",
    "台灣":"A6","中國/大中華":"A0","歐洲(含歐元區)":"A10",
    "全球新興市場":"A4","日本":"A9","北美洲":"A5",
    "拉丁美洲":"A7","新興歐洲":"A8","歐非中東":"A11",
}
CNYES_CURRENCY = {
    "全部":"","美元":"USD","台幣":"TWD","歐元":"EUR",
    "日圓":"JPY","人民幣":"CNY","澳幣":"AUD","英鎊":"GBP",
}
CNYES_BRAND = {
    "全部":"","安聯":"I26","宏利":"I27","富蘭克林":"I28",
    "富達":"I29","富邦":"I30","摩根":"I37","施羅德":"I41",
    "景順":"I45","聯博":"I66","貝萊德":"I73","高盛":"I79",
    "野村":"I80","瀚亞":"I81","國泰":"I82",
}


# ── 工具函式 ─────────────────────────────────────────────────────────────────

def _ts(unix):
    try:
        return datetime.datetime.utcfromtimestamp(int(unix)).strftime("%Y-%m-%d")
    except Exception:
        return "—"

def _fmt_pct(v):
    if v is None or (isinstance(v, float) and _np.isnan(v)):
        return "—"
    return f"{v:+.2f}%"

def _fmt_pct_plain(v):
    if v is None or (isinstance(v, float) and _np.isnan(v)):
        return "—"
    return f"{v:.2f}%"


# ── API 函式 ─────────────────────────────────────────────────────────────────

def cnyes_search(page, fund_group, area, currency, brand, for_sale_only):
    params = {
        "order":"priceDate","sort":"desc","page":page,
        "institutional":0,"isShowTag":1,"fields":CNYES_FIELDS,
        "isVendor":0,"userFrom":"anue",
    }
    if fund_group:    params["fundGroup"]                   = fund_group
    if area:          params["investmentArea"]              = area
    if currency:      params["classCurrency"]               = currency
    if brand:         params["investmentProviderShortName"] = brand
    if for_sale_only: params["forSale"]                     = 1
    try:
        r = requests.get(CNYES_SEARCH, params=params, headers=CNYES_HEADERS, timeout=12)
        r.raise_for_status()
        raw = r.json().get("items", {})
        return {
            "items": raw.get("data", []),
            "meta": {
                "last_page": raw.get("last_page", 1),
                "total":     raw.get("total", 0),
                "next_page_url": raw.get("next_page_url"),
            }
        }
    except Exception as e:
        st.error(f"鉅亨搜尋 API 錯誤：{e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def cnyes_get_nav(cnyes_id: str) -> pd.DataFrame:
    """抓取歷史淨值（最多 5 頁 = ~500 筆），回傳 DataFrame"""
    all_data = []
    cid_enc = cnyes_id.replace(",", "%2C")
    for pg in range(1, 6):
        try:
            url = CNYES_NAV_URL.format(cid=cid_enc)
            r = requests.get(url, params={"format":"table","page":pg},
                             headers=CNYES_HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json().get("items", {}).get("data", [])
            if not data:
                break
            all_data.extend(data)
            if len(data) < 100:
                break
        except Exception:
            break
    if not all_data:
        return pd.DataFrame()
    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["tradeDate"], unit="s")
    df = df.sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def cnyes_get_div(cnyes_id: str) -> pd.DataFrame:
    """抓取配息紀錄"""
    cid_enc = cnyes_id.replace(",", "%2C")
    try:
        url = CNYES_DIV_URL.format(cid=cid_enc)
        r = requests.get(url, params={"page":1}, headers=CNYES_HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json().get("items", {}).get("data", [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["除息日"] = pd.to_datetime(df["excludingDate"], unit="s")
        df = df.sort_values("除息日", ascending=False).reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def calc_fund_stats(nav_df: pd.DataFrame):
    """從歷史淨值計算各期報酬與波動率"""
    if nav_df.empty or len(nav_df) < 5:
        return {}
    closes = nav_df.set_index("date")["nav"]
    latest = closes.iloc[-1]

    def ret(months):
        target = closes.index[-1] - pd.DateOffset(months=months)
        past = closes[(closes.index >= target - pd.DateOffset(days=15)) &
                      (closes.index <= target + pd.DateOffset(days=15))]
        if past.empty:
            return None
        idx = abs(past.index - target).argmin()
        return (latest / past.iloc[idx] - 1) * 100

    ann_std = None
    if len(closes) > 20:
        recent = closes.tail(min(750, len(closes)))
        ann_std = recent.pct_change().dropna().std() * (252 ** 0.5) * 100

    return {
        "ret_3m":  ret(3),
        "ret_6m":  ret(6),
        "ret_1y":  ret(12),
        "ret_3y":  ret(36),
        "ann_std": ann_std,
    }


def calc_div_stats(div_df: pd.DataFrame, latest_nav: float):
    """從配息紀錄計算年化配息率"""
    if div_df.empty or latest_nav == 0:
        return {"ann_yield": None, "freq": 0, "latest_div": None}
    recent = div_df[div_df["除息日"] >= pd.Timestamp.now() - pd.DateOffset(years=3)]
    if recent.empty:
        return {"ann_yield": None, "freq": 0, "latest_div": None}
    total_days = (recent["除息日"].max() - recent["除息日"].min()).days
    freq = round(len(recent) / (total_days / 365.25)) if total_days > 180 else len(recent)
    freq = max(freq, 1)
    latest_div = float(recent.iloc[0]["totalDistribution"])
    ann_yield  = (latest_div / latest_nav) * freq * 100
    return {"ann_yield": ann_yield, "freq": freq, "latest_div": latest_div}


def make_tags(stats: dict, div_stats: dict, div_df: pd.DataFrame) -> str:
    tags = []
    # 波動率
    std = stats.get("ann_std")
    if std is not None:
        if std < 3:    tags.append("🧊超低波動")
        elif std < 12: tags.append("🟢低波動")
        elif std < 18: tags.append("🟡大盤波動")
        else:          tags.append("🔴高波動")
    else:
        tags.append("⚪波動未知")
    # 成長
    r3y = stats.get("ret_3y")
    if r3y is not None:
        cagr = (1 + r3y/100) ** (1/3) - 1
        if cagr > 0.12:   tags.append("🚀高成長")
        elif cagr >= 0.05: tags.append("📈大盤成長")
        elif cagr > 0:     tags.append("🐢緩步成長")
        else:              tags.append("📉衰退")
    else:
        tags.append("⚪成長未知")
    # 配息
    if not div_df.empty and len(div_df) >= 3:
        yields = div_df["distributionYield"].values[::-1][::-1]
        mean_y = yields.mean()
        if mean_y > 0:
            cv = yields.std() / mean_y
            trend = pd.Series(yields).corr(pd.Series(range(len(yields))))
            if cv < 0.15:                          tags.append("🛡️高穩定配息")
            elif pd.notna(trend) and trend > 0.6:  tags.append("🔥配息增長中")
            elif pd.notna(trend) and trend < -0.6: tags.append("⚠️配息下降中")
            else:                                  tags.append("🔄配息波動大")
        else:
            tags.append("⚪無配息")
    elif div_df.empty:
        tags.append("⚪無配息")
    else:
        tags.append("⚪配息次數偏少")
    return " | ".join(tags)


# ── Tab 2 UI ─────────────────────────────────────────────────────────────────

if "cnyes_cart" not in st.session_state:
    st.session_state.cnyes_cart = pd.DataFrame()

with tab2:
    st.subheader("🔍 台灣可申購境外基金搜尋 (鉅亨網)")

    # 篩選列
    ck1, ck2, ck3, ck4, ck5 = st.columns([2, 1.5, 1.5, 1.5, 1.5])
    with ck1:
        brand_lbl = st.selectbox("品牌", list(CNYES_BRAND.keys()), key="cnyes_brand")
    with ck2:
        fg_lbl = st.selectbox("基金類型", list(CNYES_FUND_GROUP.keys()), key="cnyes_fg")
    with ck3:
        area_lbl = st.selectbox("投資地區", list(CNYES_AREA.keys()), key="cnyes_area")
    with ck4:
        curr_lbl = st.selectbox("計價幣別", list(CNYES_CURRENCY.keys()), key="cnyes_curr")
    with ck5:
        st.write(""); st.write("")
        for_sale = st.checkbox("僅可申購", value=True, key="cnyes_forsale")

    btn_cnyes = st.button("🚀 搜尋", type="primary", key="btn_cnyes")
    MAX_PAGES = 10

    if btn_cnyes:
        bv = CNYES_BRAND[brand_lbl]
        fv = CNYES_FUND_GROUP[fg_lbl]
        av = CNYES_AREA[area_lbl]
        cv = CNYES_CURRENCY[curr_lbl]

        with st.spinner("連線鉅亨網中..."):
            data = cnyes_search(1, fv, av, cv, bv, for_sale)

        if data:
            meta      = data["meta"]
            items     = data["items"]
            total     = meta["total"]
            last_page = meta["last_page"]

            if last_page > 1:
                bar = st.progress(1/min(last_page, MAX_PAGES), text="抓取更多結果...")
                for pg in range(2, min(last_page, MAX_PAGES)+1):
                    more = cnyes_search(pg, fv, av, cv, bv, for_sale)
                    if more:
                        items.extend(more["items"])
                    bar.progress(pg/min(last_page, MAX_PAGES))
                    _time.sleep(0.12)
                bar.empty()

            rows = []
            for it in items:
                rows.append({
                    "加入購物車": False,
                    "鉅亨ID":      it.get("cnyesId",""),
                    "基金名稱":    it.get("displayNameLocal",""),
                    "計價幣別":    it.get("classCurrencyLocal",""),
                    "投資地區":    it.get("investmentArea",""),
                    "基金組別":    it.get("categoryAbbr",""),
                    "淨值":        it.get("nav"),
                    "今日漲跌%":   _fmt_pct(it.get("changePercent")),
                    "近1月%":      _fmt_pct(it.get("return1Month")),
                    "近3月%":      _fmt_pct(it.get("return3Month")),
                    "近6月%":      _fmt_pct(it.get("return6Month")),
                    "近1年%":      _fmt_pct(it.get("return1Year")),
                    "淨值日期":    _ts(it.get("priceDate")),
                    "可申購":      "✓" if it.get("forSale")==1 else "✗",
                })

            df_cnyes = pd.DataFrame(rows)
            st.session_state["cnyes_results"] = df_cnyes
            st.session_state["cnyes_total"]   = total
            st.session_state["cnyes_last_pg"] = last_page
            st.session_state["cnyes_items"]   = {it["cnyesId"]: it for it in items}
        else:
            st.warning("查無結果，請調整篩選條件。")
            st.session_state.pop("cnyes_results", None)

    # ── 顯示結果 ─────────────────────────────────────────────────────────────
    if "cnyes_results" in st.session_state:
        df_cnyes  = st.session_state["cnyes_results"]
        total     = st.session_state.get("cnyes_total", len(df_cnyes))
        last_page = st.session_state.get("cnyes_last_pg", 1)
        showing   = len(df_cnyes)

        if last_page > MAX_PAGES:
            st.success(f"共 **{total:,}** 檔，顯示前 **{showing}** 筆")
        else:
            st.success(f"共找到 **{total:,}** 檔")

        st.markdown("### 📊 搜尋結果（可勾選加入購物車）")
        edited_df = st.data_editor(
            df_cnyes,
            use_container_width=True,
            hide_index=True,
            column_config={
                "加入購物車": st.column_config.CheckboxColumn("加入購物車", required=True),
                "基金名稱":   st.column_config.TextColumn("基金名稱", width="large"),
                "淨值":       st.column_config.NumberColumn("淨值", format="%.4f"),
            },
            height=400,
        )

        if st.button("🛒 將勾選項目加入購物車", key="cnyes_add_cart"):
            selected = edited_df[edited_df["加入購物車"]].drop(columns=["加入購物車"])
            if not selected.empty:
                if st.session_state.cnyes_cart.empty:
                    st.session_state.cnyes_cart = selected
                else:
                    st.session_state.cnyes_cart = pd.concat(
                        [st.session_state.cnyes_cart, selected]
                    ).drop_duplicates(subset=["鉅亨ID"], keep="last")
                st.rerun()
            else:
                st.warning("請先在表格勾選標的。")

        st.divider()

        # ── 個別詳細資訊 ─────────────────────────────────────────────────────
        st.markdown("### 🔍 個別詳細資訊（點擊展開）")
        items_map = st.session_state.get("cnyes_items", {})

        for _, row in df_cnyes.iterrows():
            cid  = row["鉅亨ID"]
            name = row["基金名稱"]
            with st.expander(f"**{name}**　{row['計價幣別']}　{row['投資地區']}"):
                with st.spinner(f"載入 {name} 詳細資料..."):
                    nav_df = cnyes_get_nav(cid)
                    div_df = cnyes_get_div(cid)

                latest_nav = row["淨值"] or 0
                stats      = calc_fund_stats(nav_df)
                div_stats  = calc_div_stats(div_df, latest_nav)
                tags       = make_tags(stats, div_stats, div_df)

                # Metrics
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("💰 最新淨值", f"{latest_nav:.4f}")
                ann_y = div_stats.get("ann_yield")
                freq  = div_stats.get("freq", 0)
                m2.metric("✨ 預估年化配息率",
                          f"{ann_y:.2f}%" if ann_y else "—",
                          f"年配約 {freq} 次" if freq else "無配息")
                std = stats.get("ann_std")
                m3.metric("📉 年化波動率", f"{std:.2f}%" if std else "—")
                m4.metric("📌 特性標籤", tags)

                # 報酬表
                ret_data = {
                    "期間": ["近3月","近6月","近1年","近3年"],
                    "報酬率": [
                        _fmt_pct_plain(stats.get("ret_3m")),
                        _fmt_pct_plain(stats.get("ret_6m")),
                        _fmt_pct_plain(stats.get("ret_1y")),
                        _fmt_pct_plain(stats.get("ret_3y")),
                    ]
                }
                # 如果 API 直接有值就優先用
                it = items_map.get(cid, {})
                if it.get("return3Month") is not None:
                    ret_data["報酬率"][0] = _fmt_pct_plain(it["return3Month"])
                if it.get("return6Month") is not None:
                    ret_data["報酬率"][1] = _fmt_pct_plain(it["return6Month"])
                if it.get("return1Year") is not None:
                    ret_data["報酬率"][2] = _fmt_pct_plain(it["return1Year"])

                st.dataframe(pd.DataFrame(ret_data), hide_index=True, use_container_width=False)

                # 走勢圖
                if not nav_df.empty:
                    st.markdown("**淨值走勢（近3年）**")
                    recent_nav = nav_df[nav_df["date"] >= pd.Timestamp.now() - pd.DateOffset(years=3)]
                    st.line_chart(recent_nav.set_index("date")["nav"], height=200)

                # 配息歷史
                st.markdown("**配息歷程**")
                if div_df.empty:
                    st.info("查無配息紀錄。")
                else:
                    div_display = div_df[["除息日","totalDistribution","nav","distributionYield"]].copy()
                    div_display.columns = ["除息日","每單位配息","除息前淨值","單期配息率%"]
                    div_display["除息日"] = div_display["除息日"].dt.strftime("%Y-%m-%d")
                    div_display["單期配息率%"] = div_display["單期配息率%"].map("{:.2f}%".format)
                    st.dataframe(div_display, hide_index=True, use_container_width=True)

        st.divider()

        # ── 購物車 ───────────────────────────────────────────────────────────
        if not st.session_state.cnyes_cart.empty:
            st.markdown("### 🛒 鉅亨基金購物車")
            st.dataframe(st.session_state.cnyes_cart, use_container_width=True,
                         hide_index=True,
                         column_config={"基金名稱": st.column_config.TextColumn("基金名稱", width="large")})

            dl1, dl2, dl3 = st.columns(3)
            with dl1:
                st.download_button(
                    "📥 下載 CSV",
                    data=st.session_state.cnyes_cart.to_csv(index=False, encoding="utf-8-sig").encode(),
                    file_name=f"cnyes_cart_{datetime.date.today()}.csv",
                    mime="text/csv", use_container_width=True,
                )
            with dl2:
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                    st.session_state.cnyes_cart.to_excel(writer, index=False, sheet_name="鉅亨購物車")
                st.download_button(
                    "📥 下載 Excel",
                    data=buf.getvalue(),
                    file_name=f"cnyes_cart_{datetime.date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            with dl3:
                if st.button("🗑️ 清空購物車", use_container_width=True, key="cnyes_clear_cart"):
                    st.session_state.cnyes_cart = pd.DataFrame()
                    st.rerun()
