# -------------------------------------------------------------------------
# Tab 2: 台灣可申購境外基金搜尋 (鉅亨網 SSR)
# -------------------------------------------------------------------------
# 用法：把這段貼到 app.py 的 with tab2: 區塊，取代原本的 Yahoo 搜尋

import re as _re
import json as _json
import time as _time

CNYES_SSR_URL = "https://fund.cnyes.com/search/"
CNYES_HEADERS = {
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


def _cnyes_page1(keyword, fund_group, area, currency, for_sale_only):
    """第1頁：SSR HTML → window.__data"""
    params = {}
    if keyword:       params["keyword"]        = keyword
    if fund_group:    params["fundGroup"]      = fund_group
    if area:          params["investmentArea"] = area
    if currency:      params["classCurrency"]  = currency
    if for_sale_only: params["forSale"]        = 1
    r = requests.get(CNYES_SSR_URL, params=params,
                     headers=CNYES_HEADERS, timeout=15)
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


def _cnyes_next(next_page_url):
    """第2頁起：直接用 meta.next_page_url 打 API（同網域，cookie 已帶）"""
    url = f"https://fund.cnyes.com{next_page_url}"
    r = requests.get(url, headers=CNYES_API_HEADERS, timeout=12)
    r.raise_for_status()
    return r.json().get("data", {})


def _ts(unix):
    try:
        return datetime.datetime.utcfromtimestamp(int(unix)).strftime("%Y-%m-%d")
    except Exception:
        return "—"


def _fmt(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v:+.2f}%"


def _items_to_rows(items):
    rows = []
    for it in items:
        rows.append({
            "鉅亨ID":    it.get("cnyesId", ""),
            "基金名稱":   it.get("displayNameLocal", ""),
            "計價幣別":   it.get("classCurrencyLocal", ""),
            "投資地區":   it.get("investmentArea", ""),
            "基金組別":   it.get("categoryAbbr", ""),
            "淨值":       it.get("nav"),
            "今日漲跌%":  _fmt(it.get("changePercent")),
            "近1月報酬%": _fmt(it.get("return1Month")),
            "淨值日期":   _ts(it.get("priceDate")),
            "可申購":     "✓" if it.get("forSale") == 1 else "✗",
        })
    return rows


with tab2:
    st.subheader("🔍 台灣可申購境外基金搜尋")
    st.caption("資料來源：鉅亨網，涵蓋全部台灣核備可申購境外基金，無 5 筆限制。")

    ck1, ck2, ck3, ck4, ck5 = st.columns([3, 1.5, 1.5, 1.5, 1])
    with ck1:
        adv_kw = st.text_input("基金名稱關鍵字（中英文皆可）", "",
                               key="cnyes_kw",
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
        for_sale = st.checkbox("僅可申購", value=True, key="cnyes_forsale")

    MAX_PAGES = 10  # 最多抓 10 頁 = 200 筆

    if st.button("🚀 搜尋", type="primary", use_container_width=False, key="btn_cnyes"):
        with st.spinner("連線鉅亨網中..."):
            try:
                # 第 1 頁（SSR）
                data = _cnyes_page1(
                    keyword=adv_kw.strip(),
                    fund_group=CNYES_FUND_GROUP[fg_lbl],
                    area=CNYES_AREA[area_lbl],
                    currency=CNYES_CURRENCY[curr_lbl],
                    for_sale_only=for_sale,
                )
                if not data:
                    st.warning("查無結果或頁面解析失敗，請調整篩選條件。")
                    st.session_state.pop("cnyes_results", None)
                else:
                    meta  = data.get("meta", {})
                    items = data.get("items", [])
                    total      = meta.get("total", len(items))
                    last_page  = meta.get("last_page", 1)
                    next_url   = meta.get("next_page_url")

                    # 第 2 頁起（API，next_page_url）
                    if last_page > 1 and next_url:
                        bar = st.progress(1 / min(last_page, MAX_PAGES),
                                          text="抓取更多結果...")
                        for pg in range(2, min(last_page, MAX_PAGES) + 1):
                            try:
                                more = _cnyes_next(next_url)
                                items.extend(more.get("items", []))
                                next_url = more.get("meta", {}).get("next_page_url")
                            except Exception:
                                break
                            bar.progress(pg / min(last_page, MAX_PAGES))
                            _time.sleep(0.15)
                            if not next_url:
                                break
                        bar.empty()

                    df_cnyes = pd.DataFrame(_items_to_rows(items))
                    st.session_state["cnyes_results"]   = df_cnyes
                    st.session_state["cnyes_total"]     = total
                    st.session_state["cnyes_last_page"] = last_page

            except Exception as e:
                st.error(f"搜尋失敗：{e}")

    # ── 顯示結果（rerun 不消失）──
    if "cnyes_results" in st.session_state:
        df_cnyes  = st.session_state["cnyes_results"]
        total     = st.session_state.get("cnyes_total", len(df_cnyes))
        last_page = st.session_state.get("cnyes_last_page", 1)

        showing = len(df_cnyes)
        if last_page > MAX_PAGES:
            st.success(f"共 **{total:,}** 檔，顯示前 **{showing}** 筆（最多 {MAX_PAGES*20} 筆）")
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
            height=520,
        )

        st.info("💡 將「鉅亨ID」對應的 Yahoo ticker 貼回第一頁可查走勢圖（部分境外基金在 Yahoo 有對應代碼）。")

        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                "📥 下載 CSV",
                data=df_cnyes.to_csv(index=False, encoding="utf-8-sig").encode(),
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
