import io
import json
import re
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st


st.set_page_config(page_title="WSR Task Builder", layout="wide")


DEFAULT_EXCLUDED_SHEETS = {
    "summary",
    "summary per loc",
    "montreal summary",
    "london summary",
}

DEFAULT_METADATA_COLUMNS = {
    "Report Name",
    "Vendor",
    "Date",
    "Shot",
    "Asset",
    "Asset Count",
    "Turnover Status",
    "Turnover Code",
    "Turnover Date",
    "Vendor Status",
    "Vendor Note",
    "Production Note",
    "Latest Version Name",
    "Shared",
}

PREFERRED_TASK_COLUMN_ORDER = [
    "Turnover Date",
    "Version Zero Delivery Date",
    "Anim Block",
    "Anim Primary",
    "Anim Final",
    "WIP Anim",
    "WIP Asset",
    "WIP CFX",
    "WIP Comp",
    "Comp Creative Final",
    "Final Asset",
    "Tech Final",
    "Next Submission Date",
]

EXCLUDED_TASK_COLUMNS = set()

DEFAULT_PIPELINE_COLUMNS_BY_ENTITY = {
    "Shot": [
        "Version Zero Delivery Date",
        "Anim Primary",
        "Anim Final",
        "WIP Comp",
        "Comp Creative Final",
        "Tech Final",
        "Next Submission Date",
    ],
    "Asset": [
        "WIP Asset",
        "Final Asset",
        "Next Submission Date",
    ],
}

DEFAULT_PIPELINE_STEP_BY_ENTITY = {
    "Shot": "Weekly Status Report",
    "Asset": "Weekly Status Report",
}



def build_profile_state(
    usable_sheet_names: List[str],
    workbook_sheets: Dict[str, pd.DataFrame],
) -> Dict[str, object]:
    profile_state = {
        "include_blank_dates": bool(st.session_state.get("include_blank_dates", True)),
        "included_sheets": [
            sheet for sheet in st.session_state.get("included_sheets", []) if sheet in usable_sheet_names
        ],
        "pipeline_steps": {},
        "task_columns": {},
        "output_optional_columns": list(st.session_state.get("selected_optional_columns", [])),
    }

    for sheet_name in usable_sheet_names:
        entity_type, _ = detect_entity_info(workbook_sheets[sheet_name], sheet_name)
        profile_state["pipeline_steps"][sheet_name] = clean_text(
            st.session_state.get(
                f"pipeline_step_{sheet_name}",
                DEFAULT_PIPELINE_STEP_BY_ENTITY.get(entity_type, ""),
            )
        )
        profile_state["task_columns"][sheet_name] = list(
            st.session_state.get(f"task_columns_{sheet_name}", [])
        )

    return profile_state


def apply_profile_state(
    profile_state: Dict[str, object],
    usable_sheet_names: List[str],
    workbook_sheets: Dict[str, pd.DataFrame],
) -> None:
    st.session_state["include_blank_dates"] = bool(profile_state.get("include_blank_dates", True))
    st.session_state["included_sheets"] = [
        sheet for sheet in profile_state.get("included_sheets", []) if sheet in usable_sheet_names
    ]
    st.session_state["selected_optional_columns"] = list(
        profile_state.get("output_optional_columns", [])
    )

    pipeline_steps = profile_state.get("pipeline_steps", {})
    task_columns = profile_state.get("task_columns", {})

    for sheet_name in usable_sheet_names:
        entity_type, _ = detect_entity_info(workbook_sheets[sheet_name], sheet_name)
        st.session_state[f"pipeline_step_{sheet_name}"] = clean_text(
            pipeline_steps.get(
                sheet_name,
                DEFAULT_PIPELINE_STEP_BY_ENTITY.get(entity_type, ""),
            )
        )
        st.session_state[f"task_columns_{sheet_name}"] = list(task_columns.get(sheet_name, []))


def parse_uploaded_profile(uploaded_profile_file) -> Dict[str, object] | None:
    if uploaded_profile_file is None:
        return None
    try:
        raw = json.loads(uploaded_profile_file.getvalue().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def normalize_sheet_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().lower())


def clean_text(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def to_timestamp(value):
    if pd.isna(value) or value == "":
        return pd.NaT
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return pd.NaT
    return parsed


def format_date(value) -> str:
    parsed = to_timestamp(value)
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def detect_entity_info(df: pd.DataFrame, fallback_sheet_name: str) -> Tuple[str, str]:
    columns = {str(col).strip(): col for col in df.columns}
    if "Shot" in columns:
        return "Shot", columns["Shot"]
    if "Asset" in columns:
        return "Asset", columns["Asset"]

    normalized = normalize_sheet_name(fallback_sheet_name)
    if "asset" in normalized:
        return "Asset", "Asset"
    return "Shot", "Shot"


def is_mostly_dates(series: pd.Series) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return False
    parsed = pd.to_datetime(non_null, errors="coerce")
    return parsed.notna().any()


# Helper function for display formatting
def dataframe_for_display(df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()
    for col in display_df.columns:
        series = display_df[col]
        col_name = str(col).strip().lower()

        # Never treat ID columns as dates
        if col_name == "id":
            display_df[col] = series.map(lambda value: "" if pd.isna(value) else str(value))
            continue

        if pd.api.types.is_datetime64_any_dtype(series):
            display_df[col] = series.dt.strftime("%Y-%m-%d").fillna("")
            continue

        parsed = pd.to_datetime(series, errors="coerce")
        if parsed.notna().any():
            display_df[col] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), series.map(lambda value: "" if pd.isna(value) else str(value)))
        else:
            display_df[col] = series.map(lambda value: "" if pd.isna(value) else str(value))

    return display_df


@st.cache_data(show_spinner=False)
def load_workbook(file_bytes: bytes) -> Dict[str, pd.DataFrame]:
    workbook = pd.ExcelFile(io.BytesIO(file_bytes))
    sheets = {}
    for sheet_name in workbook.sheet_names:
        sheets[sheet_name] = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)
    return sheets


@st.cache_data(show_spinner=False)
def build_task_rows(
    file_bytes: bytes,
    included_sheets: List[str],
    selected_task_columns_by_sheet: Dict[str, List[str]],
    pipeline_step_by_sheet: Dict[str, str],
    include_blank_dates: bool,
) -> pd.DataFrame:
    workbook_sheets = load_workbook(file_bytes)
    rows = []

    for sheet_name in included_sheets:
        df = workbook_sheets[sheet_name].copy()
        entity_type, entity_column = detect_entity_info(df, sheet_name)
        selected_task_columns = selected_task_columns_by_sheet.get(sheet_name, [])
        selected_task_column_names = {str(col).strip() for col in selected_task_columns}
        pipeline_step_value = clean_text(pipeline_step_by_sheet.get(sheet_name, ""))

        normalized_metadata_columns = {str(col).strip() for col in DEFAULT_METADATA_COLUMNS}
        detected_date_columns = []
        for col in df.columns:
            col_name = str(col).strip()
            if (
                col_name in EXCLUDED_TASK_COLUMNS
                or col_name in normalized_metadata_columns
                or col_name == entity_column
                or col_name.lower() == "id"
            ):
                continue
            if is_mostly_dates(df[col]):
                detected_date_columns.append(col)

        if entity_column not in df.columns:
            continue

        for row_index, row in df.iterrows():
            entity_code = clean_text(row.get(entity_column, ""))
            if not entity_code:
                continue

            report_name = clean_text(row.get("Report Name", ""))
            vendor = clean_text(row.get("Vendor", ""))
            report_date = format_date(row.get("Date", ""))
            vendor_status = clean_text(row.get("Vendor Status", ""))
            turnover_status = clean_text(row.get("Turnover Status", ""))
            turnover_code = clean_text(row.get("Turnover Code", ""))
            vendor_note = clean_text(row.get("Vendor Note", ""))
            production_note = clean_text(row.get("Production Note", ""))
            latest_version_name = clean_text(row.get("Latest Version Name", ""))
            shared = clean_text(row.get("Shared", ""))
            asset_count = clean_text(row.get("Asset Count", ""))

            for task_column in selected_task_columns:
                pipeline_date = format_date(row.get(task_column, ""))
                if not include_blank_dates and not pipeline_date:
                    continue

                task_row = {
                    "Task Name": task_column,
                    "Pipeline Step": pipeline_step_value,
                    "Milestone": "x",
                    "Entity Type": entity_type,
                    "Link": entity_code,
                    "Delivery Date": pipeline_date,
                    "Vendor": vendor,
                    "WSR Report Name": report_name,
                    "WSR Report Date": report_date,
                    "Source Sheet": sheet_name,
                    "Source Row": row_index + 2,
                    "Vendor Status": vendor_status,
                    "Turnover Status": turnover_status,
                    "Turnover Code": turnover_code,
                    "Vendor Note": vendor_note,
                    "Production Note": production_note,
                    "Latest Version Name": latest_version_name,
                    "Shared": shared,
                    "Asset Count": asset_count,
                    "Description": "\n".join([line for line in [
                        f"WSR Report Name: {report_name}",
                        f"WSR Report Date: {report_date}",
                        f"Vendor: {vendor}",
                        f"Entity Type: {entity_type}",
                        f"Link: {entity_code}",
                        f"Pipeline: {task_column}",
                        *[
                            f"{str(date_col).strip()}: {format_date(row.get(date_col, ''))}"
                            for date_col in detected_date_columns
                            if str(date_col).strip() not in selected_task_column_names and format_date(row.get(date_col, ""))
                        ],
                        f"Asset Count: {asset_count}" if asset_count else "",
                        f"Shared: {shared}" if shared else "",
                        f"Vendor Note: {vendor_note}" if vendor_note else "",
                        f"Production Note: {production_note}" if production_note else "",
                    ] if line.split(": ", 1)[-1] != ""]),
                }

                for source_col in df.columns:
                    source_col_name = str(source_col).strip()
                    if not source_col_name or source_col_name in task_row:
                        continue

                    source_value = row.get(source_col, "")
                    if source_col_name.lower() == "id":
                        task_row[source_col_name] = clean_text(source_value)
                    elif pd.api.types.is_datetime64_any_dtype(df[source_col]):
                        task_row[source_col_name] = format_date(source_value)
                    else:
                        parsed_source_value = pd.to_datetime(source_value, errors="coerce")
                        if pd.notna(parsed_source_value):
                            task_row[source_col_name] = parsed_source_value.strftime("%Y-%m-%d")
                        else:
                            task_row[source_col_name] = clean_text(source_value)

                rows.append(task_row)

    result = pd.DataFrame(rows)
    if result.empty:
        return result

    result = result.sort_values(by=["Entity Type", "Link", "Task Name", "WSR Report Date"]).reset_index(drop=True)
    return result


st.title("WSR Task Builder")
st.write("Upload a vendor WSR and convert each pipeline date on each WSR item into an individual Task row for ShotGrid import.")

uploaded_file = st.file_uploader("Upload WSR workbook", type=["xlsx", "xlsm", "xls"])

if uploaded_file is None:
    st.info("Upload a WSR workbook to begin.")
    st.stop()

file_bytes = uploaded_file.getvalue()
workbook_sheets = load_workbook(file_bytes)
all_sheet_names = list(workbook_sheets.keys())
usable_sheet_names = [
    sheet_name
    for sheet_name in all_sheet_names
    if normalize_sheet_name(sheet_name) not in DEFAULT_EXCLUDED_SHEETS
    and "summary" not in normalize_sheet_name(sheet_name)
]

if not usable_sheet_names:
    st.error("No usable data sheets were found in this workbook.")
    st.stop()

default_included_sheets = [
    sheet for sheet in usable_sheet_names if normalize_sheet_name(sheet) in {"shots", "assets"}
]

if "include_blank_dates" not in st.session_state:
    st.session_state["include_blank_dates"] = True
if "included_sheets" not in st.session_state:
    st.session_state["included_sheets"] = default_included_sheets
else:
    st.session_state["included_sheets"] = [
        sheet for sheet in st.session_state["included_sheets"] if sheet in usable_sheet_names
    ]
if "selected_optional_columns" not in st.session_state:
    st.session_state["selected_optional_columns"] = []

with st.sidebar:
    st.header("Profiles")
    profile_name = st.text_input("Profile name", value="WSR Profile")

    uploaded_profile_file = st.file_uploader(
        "Import profile",
        type=["json"],
        accept_multiple_files=False,
        key="uploaded_profile_file",
        help="Upload a previously exported profile JSON to restore selected sheets, task columns, pipeline steps, and optional output columns.",
    )

    imported_profile = parse_uploaded_profile(uploaded_profile_file)
    if uploaded_profile_file is not None and imported_profile is None:
        st.error("Could not read that profile JSON.")
    elif uploaded_profile_file is not None and st.button("Apply imported profile", use_container_width=True):
        apply_profile_state(imported_profile, usable_sheet_names, workbook_sheets)
        st.rerun()

    st.header("Output Options")
    include_blank_dates = st.checkbox(
        "Include tasks even when the pipeline date is blank",
        value=st.session_state.get("include_blank_dates", True),
        key="include_blank_dates",
    )

    st.header("Sheets")
    included_sheets = st.multiselect(
        "Choose sheets to convert",
        options=usable_sheet_names,
        default=st.session_state.get("included_sheets", default_included_sheets),
        key="included_sheets",
    )

if not included_sheets:
    st.warning("Select at least one sheet to convert.")
    st.stop()

selected_task_columns_by_sheet: Dict[str, List[str]] = {}
pipeline_step_by_sheet: Dict[str, str] = {}

st.subheader("Pipeline date columns")
for sheet_name in included_sheets:
    df = workbook_sheets[sheet_name].copy()
    entity_type, entity_column = detect_entity_info(df, sheet_name)

    with st.expander(f"{sheet_name} ({entity_type})", expanded=True):
        st.write(f"Entity column: `{entity_column}`")
        default_pipeline_step = DEFAULT_PIPELINE_STEP_BY_ENTITY.get(entity_type, "")
        pipeline_step_key = f"pipeline_step_{sheet_name}"
        if pipeline_step_key not in st.session_state:
            st.session_state[pipeline_step_key] = DEFAULT_PIPELINE_STEP_BY_ENTITY.get(entity_type, "")
        pipeline_step_value = st.text_input(
            f"Pipeline Step for {sheet_name}",
            value=st.session_state.get(pipeline_step_key, default_pipeline_step),
            key=pipeline_step_key,
            help="Enter the ShotGrid Pipeline Step value to assign to every task generated from this sheet.",
        )
        pipeline_step_by_sheet[sheet_name] = pipeline_step_value.strip()

        default_pipeline_columns = DEFAULT_PIPELINE_COLUMNS_BY_ENTITY.get(entity_type, [])

        available_task_columns = []
        normalized_metadata_columns = {str(col).strip() for col in DEFAULT_METADATA_COLUMNS}
        preferred_columns_in_sheet = []
        detected_date_columns = []

        for preferred_col in PREFERRED_TASK_COLUMN_ORDER:
            matching_col = next(
                (col for col in df.columns if str(col).strip() == preferred_col and str(col).strip() not in EXCLUDED_TASK_COLUMNS),
                None,
            )
            if matching_col is not None and matching_col not in preferred_columns_in_sheet:
                preferred_columns_in_sheet.append(matching_col)

        for col in df.columns:
            col_name = str(col).strip()
            if (
                col_name in EXCLUDED_TASK_COLUMNS
                or col_name in normalized_metadata_columns
                or col_name == entity_column
                or col_name.lower() == "id"
                or col in preferred_columns_in_sheet
            ):
                continue
            if is_mostly_dates(df[col]):
                detected_date_columns.append(col)

        available_task_columns = preferred_columns_in_sheet + [
            col for col in detected_date_columns if col not in preferred_columns_in_sheet
        ]
        default_columns = [col for col in available_task_columns if str(col).strip() in default_pipeline_columns]
        task_columns_key = f"task_columns_{sheet_name}"
        saved_task_columns = st.session_state.get(task_columns_key, default_columns)
        sanitized_task_columns = [col for col in saved_task_columns if col in available_task_columns]
        if task_columns_key not in st.session_state or saved_task_columns != sanitized_task_columns:
            st.session_state[task_columns_key] = sanitized_task_columns
        selected_columns = st.multiselect(
            f"Task columns for {sheet_name}",
            options=available_task_columns,
            default=st.session_state.get(task_columns_key, default_columns),
            key=task_columns_key,
            help="Choose which detected date columns should generate tasks for this sheet. Default pipeline columns are preselected, and any other column containing at least one parseable date will also appear as an option.",
        )
        selected_task_columns_by_sheet[sheet_name] = selected_columns

        preview_df = dataframe_for_display(df)
        selected_column_names = {str(col).strip() for col in selected_columns}

        def highlight_selected_columns(series):
            if str(series.name).strip() in selected_column_names:
                return ["color: #155724; font-weight: bold;" for _ in range(len(series))]
            return ["" for _ in range(len(series))]

        styled_preview_df = preview_df.style.apply(highlight_selected_columns, axis=0)
        st.dataframe(styled_preview_df, width="stretch")

if not any(selected_task_columns_by_sheet.values()):
    st.warning("Select at least one pipeline date column.")
    st.stop()

tasks_df = build_task_rows(
    file_bytes=file_bytes,
    included_sheets=included_sheets,
    selected_task_columns_by_sheet=selected_task_columns_by_sheet,
    pipeline_step_by_sheet=pipeline_step_by_sheet,
    include_blank_dates=include_blank_dates,
)

st.subheader("Task output preview")
if tasks_df.empty:
    st.warning("No task rows were generated with the current settings.")
    st.stop()

default_output_columns = [
    "Task Name",
    "Pipeline Step",
    "Milestone",
    "Entity Type",
    "Link",
    "Delivery Date",
    "Description",
    "WSR Report Name",
]

available_output_columns = list(tasks_df.columns)

required_columns = [
    "Task Name",
    "Milestone",
    "Pipeline Step",
    "Link",
    "Delivery Date",
]

base_optional_columns = [
    "Description",
    "Entity Type",
]

workbook_optional_columns = []
excluded_workbook_optional_columns = {"Shot", "Asset"}
for included_sheet_name in included_sheets:
    source_df = workbook_sheets[included_sheet_name]
    for source_col in source_df.columns:
        source_col_name = str(source_col).strip()
        if (
            source_col_name
            and source_col_name in available_output_columns
            and source_col_name not in required_columns
            and source_col_name not in base_optional_columns
            and source_col_name not in excluded_workbook_optional_columns
            and "thumbnail" not in source_col_name.lower()
            and source_col_name not in workbook_optional_columns
        ):
            workbook_optional_columns.append(source_col_name)

optional_columns = [
    col for col in [*base_optional_columns, *workbook_optional_columns]
    if col in available_output_columns and col not in required_columns
]

default_optional = [col for col in default_output_columns if col in optional_columns]

saved_optional_columns = st.session_state.get("selected_optional_columns", default_optional)
sanitized_optional_columns = [col for col in saved_optional_columns if col in optional_columns]
if "selected_optional_columns" not in st.session_state or saved_optional_columns != sanitized_optional_columns:
    st.session_state["selected_optional_columns"] = sanitized_optional_columns


selected_optional_columns = st.multiselect(
    "Optional columns to include in output CSV",
    options=optional_columns,
    default=st.session_state.get("selected_optional_columns", default_optional),
    key="selected_optional_columns",
    help="Required columns are always included. Choose any additional columns to include in the output CSV.",
)

current_profile_state = build_profile_state(usable_sheet_names, workbook_sheets)
profile_json = json.dumps(current_profile_state, indent=2, sort_keys=True)

st.sidebar.download_button(
    label="Export profile",
    data=profile_json.encode("utf-8"),
    file_name=f"{profile_name.strip() or 'wsr_profile'}.json",
    mime="application/json",
    use_container_width=True,
)

selected_output_columns = required_columns + selected_optional_columns

if not selected_output_columns:
    st.warning("Select at least one output column.")
    st.stop()

output_df = tasks_df[selected_output_columns]

col1, col2, col3 = st.columns(3)
col1.metric("Task rows", len(tasks_df))
col2.metric("Unique entities", tasks_df["Link"].nunique())
col3.metric("Unique task names", tasks_df["Task Name"].nunique())

st.dataframe(dataframe_for_display(output_df), width="stretch", height=520)

csv_bytes = output_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download task CSV",
    data=csv_bytes,
    file_name="wsr_tasks_for_shotgrid.csv",
    mime="text/csv",
)

