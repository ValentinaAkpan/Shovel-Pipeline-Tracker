import streamlit as st
import mysql.connector
import pandas as pd
from datetime import date, datetime
from pathlib import Path

# ---- Setup ----
st.set_page_config(page_title="Shovel Pipeline Tracker", layout="centered")

# ---- Header ----
col1, col2 = st.columns([1, 5])
with col1:
    logo_path = Path("logo.svg")
    if logo_path.exists():
        st.image(str(logo_path), width=100)
with col2:
    st.title("Shovel Pipeline Tracker")

# ---- Internal Use Warning ----
st.warning("🚧 Internal Use Only - For Motion Metrics Remote Support Team.")

st.markdown("Use this tool to retrieve engine pipeline logs for a specific shovel over a selected date range.")

# ---- Input Form ----
with st.form("pipeline_query_form"):
    st.subheader("Search Inputs")

    col1, col2 = st.columns(2)
    with col1:
        source_name = st.text_input(
            "Shovel Name",
            help="This must match the name exactly as it appears in MMPro (e.g. EX8388)"
        ).strip()
    with col2:
        start_date = st.date_input("Start Date", value=date(2025, 5, 1))

    end_date = st.date_input("End Date", value=date(2025, 6, 2))
    submitted = st.form_submit_button("Get Pipeline Logs")

# ---- Query Logic ----
if submitted:
    if not source_name:
        st.error("❌ Please enter a shovel name that matches exactly what’s shown in MMPro.")
    else:
        try:
            with st.spinner("Connecting to database..."):
                # Using Streamlit secrets
                db_config = st.secrets["mysql"]
                conn = mysql.connector.connect(
                    host=db_config["host"],
                    user=db_config["user"],
                    password=db_config["password"],
                    database=db_config["database"]
                )
                cursor = conn.cursor()

            # ---- Get Shovel UUID ----
            cursor.execute("SELECT uuid FROM air_field_units WHERE name = %s LIMIT 1;", (source_name,))
            result = cursor.fetchone()

            if not result:
                st.error(f"❌ No shovel found with name '{source_name}'. Make sure it matches MMPro exactly.")
            else:
                uuid = result[0]
                table_name = f"air_cloud_logs_{uuid}"
                st.success(f"Found shovel UUID: `{uuid}`. Querying `{table_name}`...")

                def label_pipeline(engine_id):
                    if engine_id == "stereo_v01":
                        return "3D Stereo"
                    elif engine_id == "fallback_v01":
                        return "2D Mono"
                    elif pd.isna(engine_id):
                        return "Unknown"
                    return engine_id

                # ---- Query Log Data ----
                query = f"""
                    SELECT
                        log_timestamp,
                        JSON_UNQUOTE(JSON_EXTRACT(log_key_value_fields, '$.get_engine_id')) AS engine_id
                    FROM `{table_name}`
                    WHERE log_type = 'EngineIDLog'
                      AND log_timestamp BETWEEN %s AND %s
                    ORDER BY log_timestamp ASC
                    LIMIT 500;
                """
                cursor.execute(query, (start_date, end_date))
                logs = cursor.fetchall()
                df = pd.DataFrame(logs, columns=["log_timestamp", "engine_id"])

                if not df.empty:
                    df["pipeline_label"] = df["engine_id"].apply(label_pipeline)
                    st.success(f"Found {len(df)} pipeline log(s).")
                    st.dataframe(df, use_container_width=True)

                    # Export CSV
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"{source_name}_pipeline_logs_{start_date}_to_{end_date}_{timestamp}.csv"
                    csv = df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label="Download Logs CSV",
                        data=csv,
                        file_name=filename,
                        mime="text/csv"
                    )
                else:
                    st.warning("⚠️ No EngineIDLog entries found in this date range.")

            cursor.close()
            conn.close()

        except mysql.connector.Error as err:
            st.error(f"❌ Database error: {err}")
