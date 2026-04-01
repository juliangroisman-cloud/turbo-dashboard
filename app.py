import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from datetime import datetime
import textwrap

st.set_page_config(
    page_title="Turbo Sales Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
[data-testid="stMetricValue"] { font-size: 2rem; }
[data-testid="stMetricLabel"] { font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: .05em; }
.block-container { padding-top: 1.5rem; }
div[data-testid="stDataFrame"] { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ── Conexión Snowflake ────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_conn():
    cfg = st.secrets["snowflake"]
    params = dict(
        account   = cfg["account"],
        user      = cfg["user"],
        warehouse = cfg.get("warehouse", "OPERATIONS_ANALYSTS"),
        role      = cfg.get("role", "OPERATIONS_ANALYSTS_ROLE"),
        database  = "RP_SILVER_DB_PROD",
        schema    = "TURBO_CORE",
    )
    # Keypair auth (preferred for service accounts)
    if "private_key_plain" in cfg:
        from cryptography.hazmat.primitives import serialization
        pk = serialization.load_pem_private_key(
            cfg["private_key_plain"].encode(),
            password=None,
        )
        params["private_key"] = pk.private_bytes(
            serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    elif "password" in cfg:
        params["password"] = cfg["password"]
    else:
        raise ValueError("Secrets: necesitás 'password' o 'private_key_plain'")
    return snowflake.connector.connect(**params)

# ── Queries ───────────────────────────────────────────────────────────────────
SQLS = {
    "Diario": """
        SELECT commercial_name, name,
               DATE_TRUNC('DAY',  created_at)::DATE::VARCHAR AS periodo,
               SUM(units)                                    AS units,
               0::FLOAT                                      AS usd
        FROM   RP_SILVER_DB_PROD.TURBO_CORE.AR_ORDER_DISCOUNTS
        WHERE  country = 'AR'
          AND  DATE_TRUNC('DAY', created_at)
                   BETWEEN DATEADD(day,-7,CURRENT_DATE) AND CURRENT_DATE
        GROUP  BY 1,2,3
    """,
    "Semanal": """
        SELECT commercial_name, name,
               DATE_TRUNC('WEEK', created_at)::DATE::VARCHAR AS periodo,
               SUM(units)                                    AS units,
               0::FLOAT                                      AS usd
        FROM   RP_SILVER_DB_PROD.TURBO_CORE.AR_ORDER_DISCOUNTS
        WHERE  country = 'AR'
          AND  DATE_TRUNC('WEEK', created_at)
                   BETWEEN DATEADD(day,-60,CURRENT_DATE) AND CURRENT_DATE
        GROUP  BY 1,2,3
    """,
    "Mensual": """
        SELECT commercial_name, name,
               DATE_TRUNC('MONTH', created_at)::DATE::VARCHAR AS periodo,
               SUM(units)                                     AS units,
               ROUND(SUM(total_price_wo_iva_w_discounts_wo_iva_usd),2) AS usd
        FROM   RP_SILVER_DB_PROD.TURBO_CORE.AR_ORDER_DISCOUNTS
        WHERE  country = 'AR'
          AND  DATE_TRUNC('MONTH', created_at) > '2023-10-01'
        GROUP  BY 1,2,3
        ORDER  BY 3 DESC
    """,
}

@st.cache_data(ttl=300, show_spinner=False)
def load(_gran: str) -> pd.DataFrame:
    cur = get_conn().cursor()
    cur.execute(SQLS[_gran])
    cols = [d[0].lower() for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    df["units"] = pd.to_numeric(df["units"], errors="coerce").fillna(0)
    df["usd"]   = pd.to_numeric(df["usd"],   errors="coerce").fillna(0)
    return df

def human(v: float, prefix: str = "") -> str:
    if v >= 1_000_000: return f"{prefix}{v/1_000_000:.1f}M"
    if v >= 1_000:     return f"{prefix}{v/1_000:.0f}k"
    return f"{prefix}{v:,.0f}"

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🚀 Turbo Sales Dashboard")

c1, c2, c3, c4, c5 = st.columns([2,2,2,2,1])
gran   = c1.selectbox("Granularidad", ["Diario","Semanal","Mensual"], index=2)
view   = c2.selectbox("Vista", ["Por proveedor","Por producto"])
metric = c3.selectbox("Métrica", ["Unidades","$ USD"])
top_n  = c4.selectbox("Top N (gráfico)", [10,5,20], index=0)
with c5:
    st.write("")
    if st.button("↺", help="Refrescar datos", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Carga ─────────────────────────────────────────────────────────────────────
with st.spinner("Cargando datos..."):
    try:
        df = load(gran)
        st.caption(f"✅  {len(df):,} filas · actualizado {datetime.now().strftime('%H:%M')}")
    except Exception as e:
        st.error(f"❌ Error Snowflake: {e}")
        st.stop()

# ── Filtros ───────────────────────────────────────────────────────────────────
group_col  = "commercial_name" if view == "Por proveedor" else "name"
metric_col = "units" if metric == "Unidades" else "usd"
metric_lbl = metric

fa, fb = st.columns(2)
provs  = ["Todos"] + sorted(df["commercial_name"].dropna().unique().tolist())
psel   = fa.selectbox("Proveedor", provs)
periodos = ["Todos"] + sorted(df["periodo"].unique().tolist(), reverse=True)
pper   = fb.selectbox("Período", periodos)

dff = df.copy()
if psel  != "Todos": dff = dff[dff["commercial_name"] == psel]
if pper  != "Todos": dff = dff[dff["periodo"] == pper]

# ── KPIs ──────────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric(f"Total {metric_lbl}", human(dff[metric_col].sum(), "$" if metric_col=="usd" else ""))
k2.metric("Proveedores",  dff["commercial_name"].nunique())
k3.metric("Productos",    dff["name"].nunique())
k4.metric("Períodos",     dff["periodo"].nunique())

st.divider()

# ── Tabla pivoteada ───────────────────────────────────────────────────────────
st.subheader("Tabla de datos")

pivot = (
    dff.groupby([group_col, "periodo"])[metric_col]
       .sum().reset_index()
       .pivot(index=group_col, columns="periodo", values=metric_col)
       .fillna(0)
)
all_p = sorted(pivot.columns.tolist())
show_p = all_p[-8:]
tbl = pivot[show_p].copy()
tbl["Total"] = pivot.sum(axis=1)

# Comparativas
if len(all_p) >= 1:
    lv = pivot[all_p[-1]]
    tbl["vs Ant."]    = ((lv - pivot[all_p[-2]]) / pivot[all_p[-2]].replace(0,float("nan")) * 100) if len(all_p)>=2 else float("nan")
    avg4 = pivot[all_p[-4:]].mean(axis=1)
    tbl["vs Prom 4p"] = (lv - avg4) / avg4.replace(0,float("nan")) * 100
    mx   = pivot.max(axis=1)
    tbl["vs Máx"]     = (lv - mx)   / mx.replace(0,float("nan"))   * 100

tbl = tbl.sort_values("Total", ascending=False)

PCT_COLS = [c for c in ["vs Ant.","vs Prom 4p","vs Máx"] if c in tbl.columns]
NUM_COLS = show_p + ["Total"]

def _fmt_n(v): return "—" if (pd.isna(v) or v==0) else human(v, "$" if metric_col=="usd" else "")
def _fmt_p(v): return "—" if pd.isna(v) else f"{'+'if v>0 else ''}{v:.1f}%"

fmt_map = {c: _fmt_n for c in NUM_COLS}
fmt_map.update({c: _fmt_p for c in PCT_COLS})

def _color_pct(v):
    if pd.isna(v): return "color:#999"
    return "color:#1e6b3c;font-weight:600" if v > 0 else "color:#b91c1c;font-weight:600"

styled = (
    tbl.style
       .format(fmt_map)
       .applymap(_color_pct, subset=PCT_COLS)
)

st.dataframe(styled, use_container_width=True, height=min(600, 40 + len(tbl)*36))
st.caption(f"{len(tbl)} {'proveedores' if view=='Por proveedor' else 'productos'}")

st.divider()

# ── Gráficos ──────────────────────────────────────────────────────────────────
st.subheader("Gráficos")

top_ents = (
    dff.groupby(group_col)[metric_col].sum()
       .nlargest(top_n).index.tolist()
)
dff_top = dff[dff[group_col].isin(top_ents)]

tab_bar, tab_line = st.tabs(["Barras — top entidades", "Líneas — evolución"])

with tab_bar:
    bar_df = (
        dff_top.groupby(group_col)[metric_col].sum()
               .reset_index()
               .sort_values(metric_col, ascending=True)
    )
    fig = px.bar(
        bar_df, x=metric_col, y=group_col, orientation="h",
        labels={metric_col: metric_lbl, group_col: ""},
        color=metric_col, color_continuous_scale="Blues",
        height=max(380, top_n * 36 + 80),
    )
    fig.update_layout(coloraxis_showscale=False, margin=dict(l=0,r=20,t=10,b=20))
    fig.update_traces(texttemplate="%{x:,.0f}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

with tab_line:
    line_df = (
        dff_top.groupby([group_col, "periodo"])[metric_col].sum()
               .reset_index()
    )
    fig2 = px.line(
        line_df, x="periodo", y=metric_col, color=group_col,
        markers=line_df["periodo"].nunique() <= 20,
        labels={metric_col: metric_lbl, "periodo": "", group_col: ""},
        height=440,
    )
    fig2.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=0,r=0,t=50,b=20),
    )
    st.plotly_chart(fig2, use_container_width=True)
