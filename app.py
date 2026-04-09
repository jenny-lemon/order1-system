import streamlit as st
from 儲值金系統設定 import run_process_web


# =========================
# 頁面設定
# =========================
st.set_page_config(
    page_title="儲值金訂單系統",
    page_icon="💰",
    layout="wide",
)

st.markdown("""
<style>
    .main {
        background: #f6f8fb;
    }

    .block-container {
        max-width: 1080px;
        padding-top: 2rem;
        padding-bottom: 3rem;
    }

    .page-title {
        font-size: 2.5rem;
        font-weight: 800;
        color: #1f2937;
        margin-bottom: 0.2rem;
    }

    .page-subtitle {
        color: #6b7280;
        margin-bottom: 1.5rem;
        font-size: 1rem;
    }

    .card {
        background: white;
        border-radius: 18px;
        padding: 22px 22px 8px 22px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06);
        border: 1px solid #eef2f7;
        margin-bottom: 18px;
    }

    .section-title {
        font-size: 1.05rem;
        font-weight: 700;
        color: #111827;
        margin-bottom: 0.8rem;
    }

    .hint-box {
        background: #f9fafb;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 12px 14px;
        color: #4b5563;
        font-size: 0.95rem;
        margin-bottom: 10px;
    }

    .stButton > button {
        background: linear-gradient(135deg, #4f46e5, #6366f1);
        color: white;
        border: none;
        border-radius: 10px;
        font-weight: 700;
        height: 44px;
        padding: 0 20px;
    }

    .stButton > button:hover {
        opacity: 0.95;
    }

    div[data-testid="stMetric"] {
        background: white;
        border: 1px solid #eef2f7;
        border-radius: 14px;
        padding: 8px 14px;
        box-shadow: 0 4px 12px rgba(15, 23, 42, 0.04);
    }

    .action-note {
        font-size: 0.92rem;
        color: #6b7280;
        margin-top: -6px;
        margin-bottom: 10px;
    }

    .fail-box {
        background: #fff7f7;
        border: 1px solid #fecaca;
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 10px;
    }

    .ok-box {
        background: #f0fdf4;
        border: 1px solid #bbf7d0;
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)


# =========================
# 工具函式
# =========================
def parse_row_input(row_text: str):
    """
    支援：
    - 3
    - 3,5,7
    - 3,5,7-10
    - 2-4,8,10-12
    """
    if not row_text or not row_text.strip():
        raise ValueError("請輸入列號，例如：2,3,5-7")

    rows = set()
    parts = [p.strip() for p in row_text.split(",") if p.strip()]

    for part in parts:
        if "-" in part:
            start_str, end_str = part.split("-", 1)
            start = int(start_str.strip())
            end = int(end_str.strip())

            if start <= 0 or end <= 0:
                raise ValueError("列號必須大於 0")
            if start > end:
                raise ValueError(f"區間錯誤：{part}")

            rows.update(range(start, end + 1))
        else:
            row_no = int(part)
            if row_no <= 0:
                raise ValueError("列號必須大於 0")
            rows.add(row_no)

    return sorted(rows)


# =========================
# 標題
# =========================
st.markdown('<div class="page-title">💰 儲值金訂單系統</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="page-subtitle">支援建單、寄確認信、改 Google 日曆，並可指定列號批次處理。</div>',
    unsafe_allow_html=True
)


# =========================
# 表單區
# =========================
with st.container():
    st.markdown('<div class="card">', unsafe_allow_html=True)

    st.markdown('<div class="section-title">⚙️ 執行設定</div>', unsafe_allow_html=True)

    top_col1, top_col2 = st.columns(2)

    with top_col1:
        env = st.selectbox("執行環境", ["dev", "prod"])
        region = st.selectbox("執行區域", ["台北", "台中", "桃園", "新竹", "高雄"])

    with top_col2:
        sheet_name = st.text_input("工作表名稱", value="202604")
        row_input = st.text_input(
            "執行列號",
            value="2,3,5-7",
            help="可輸入：2,3,5-7",
        )

    st.markdown(
        '<div class="hint-box">列號支援單列、逗號分隔、多段區間，例如：<b>2</b>、<b>2,3,5</b>、<b>2,3,5-7</b></div>',
        unsafe_allow_html=True
    )

    st.markdown('<div class="section-title">🧩 執行項目</div>', unsafe_allow_html=True)

    selected_actions = st.multiselect(
        "請勾選要執行的項目",
        options=["建單", "寄確認信", "改 Google 日曆"],
        default=["建單", "寄確認信", "改 Google 日曆"],
    )
    st.markdown(
        '<div class="action-note">可自由組合，例如只寄確認信、只改日曆，或全流程一起跑。</div>',
        unsafe_allow_html=True
    )

    st.markdown('<div class="section-title">🔐 後台登入</div>', unsafe_allow_html=True)

    auth_col1, auth_col2 = st.columns(2)
    with auth_col1:
        backend_email = st.text_input("後台帳號")
    with auth_col2:
        backend_password = st.text_input("後台密碼", type="password")

    run_clicked = st.button("🚀 開始執行")

    st.markdown('</div>', unsafe_allow_html=True)


# =========================
# 執行區
# =========================
if run_clicked:
    if not sheet_name.strip():
        st.error("請輸入工作表名稱")
        st.stop()

    if not backend_email.strip():
        st.error("請輸入後台帳號")
        st.stop()

    if not backend_password.strip():
        st.error("請輸入後台密碼")
        st.stop()

    if not selected_actions:
        st.error("請至少選擇一個執行項目")
        st.stop()

    try:
        target_rows = parse_row_input(row_input)
    except Exception as e:
        st.error(f"列號格式錯誤：{e}")
        st.stop()

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📋 執行摘要</div>', unsafe_allow_html=True)

    st.write(f"**環境**：{env}")
    st.write(f"**區域**：{region}")
    st.write(f"**工作表**：{sheet_name}")
    st.write(f"**列號**：{target_rows}")
    st.write(f"**執行項目**：{'、'.join(selected_actions)}")

    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🪵 執行紀錄</div>', unsafe_allow_html=True)

    log_box = st.empty()
    logs = []

    def ui_log(msg):
        logs.append(str(msg))
        log_box.code("\n".join(logs[-120:]))

    total_success = 0
    total_fail = 0
    total_processed = 0
    failed_records = []

    for row_no in target_rows:
        ui_log(f"🚀 開始執行第 {row_no} 列...")

        try:
            result = run_process_web(
                env_name=env,
                region=region,
                backend_email=backend_email.strip(),
                backend_password=backend_password.strip(),
                sheet_name=sheet_name.strip(),
                start_row=row_no,
                end_row=row_no,
                selected_actions=selected_actions,
                logger=ui_log,
            )

            if isinstance(result, dict):
                total_success += result.get("success_count", 0)
                total_fail += result.get("fail_count", 0)
                total_processed += result.get("total_processed", 0)

                # 後端若有回傳 failed_records，整合進來
                result_failed_records = result.get("failed_records", [])
                if isinstance(result_failed_records, list):
                    failed_records.extend(result_failed_records)

        except TypeError as e:
            if "selected_actions" in str(e):
                st.error("目前 `儲值金系統設定.py` 的 `run_process_web()` 尚未加入 `selected_actions` 參數。")
                st.info("先把這份介面存好，然後再把後端補上 `selected_actions` 支援。")
                st.stop()

            total_fail += 1
            total_processed += 1
            failed_records.append({
                "row": row_no,
                "name": "未知",
                "error": str(e),
            })
            ui_log(f"❌ 第 {row_no} 列失敗：{e}")

        except Exception as e:
            total_fail += 1
            
            failed_records.append({
                "row": row_no,
                "name": "未知",
                "error": str(e),
            })

            ui_log(f"❌ 第 {row_no} 列失敗：{e}")
            total_processed += 1

    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📊 執行結果</div>', unsafe_allow_html=True)

    m1, m2, m3 = st.columns(3)
    m1.metric("成功", total_success)
    m2.metric("失敗", total_fail)
    m3.metric("總處理", total_processed)

    if total_fail == 0:
        st.success("🎉 執行完成")
    else:
        st.warning("⚠️ 已執行完成，但有部分列失敗，請查看下方失敗清單。")

    st.markdown('</div>', unsafe_allow_html=True)

    # =========================
    # 失敗清單
    # =========================
    if failed_records:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">❌ 失敗清單</div>', unsafe_allow_html=True)

        for item in failed_records:
            row_text = item.get("row", "")
            name_text = item.get("name", "未知")
            error_text = item.get("error", "")

            st.markdown('<div class="fail-box">', unsafe_allow_html=True)
            st.write(f"**第 {row_text} 列｜{name_text}**")
            if error_text:
                st.caption(error_text)
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">✅ 執行狀態</div>', unsafe_allow_html=True)
        st.markdown('<div class="ok-box">🎉 本次沒有失敗資料</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
