# Databricks notebook source
# MAGIC %md
# MAGIC # Service Plan Document Intelligence - Sample Data Generator
# MAGIC This notebook creates the catalog, schemas, volume, users table, and generates
# MAGIC **extensive multi-page** Service Plan PDFs for the HBSS (Home Based Support Services)
# MAGIC demo. The layout mirrors the *Geneva Healthcare HBSS Complex Service Plan V6* reference
# MAGIC form: cover grid, referral narrative, clinical background, referral goals, other-provider
# MAGIC linkage, 15 care-domain narrative sections, support-task grids, a Home Safety Risk
# MAGIC Assessment, and the rights/responsibilities agreement.

# COMMAND ----------

# MAGIC %pip install reportlab
# MAGIC %restart_python

# COMMAND ----------

import random
import os
import io
from datetime import datetime, timedelta

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    PageBreak, ListFlowable, ListItem,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.pdfgen import canvas as _canvas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Read Configuration from Widgets

# COMMAND ----------

# Define dbutils.widgets for job/notebook parameterization with sensible defaults
try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "DocProcessing"

try:
    bronze_schema = dbutils.widgets.get("bronze_schema")
except:
    bronze_schema = "DocProcess_Bronze"

try:
    silver_schema = dbutils.widgets.get("silver_schema")
except:
    silver_schema = "DocProcess_Silver"

try:
    gold_schema = dbutils.widgets.get("gold_schema")
except:
    gold_schema = "DocProcess_Gold"

try:
    volume_name = dbutils.widgets.get("volume_name")
except:
    volume_name = "InputPDFs"

try:
    num_users = int(dbutils.widgets.get("num_users"))
except:
    num_users = 10

try:
    num_documents = int(dbutils.widgets.get("num_documents"))
except:
    num_documents = 20

# The Databricks App runs as its own service principal (NOT the deploying user), so it
# needs read access on the catalog for its queries (My Submissions, Pipeline Ops) to
# succeed. We resolve that SP dynamically from the app name — no hardcoded id. An
# explicit `app_principal` widget still overrides, if ever needed.
try:
    app_name = dbutils.widgets.get("app_name")
except:
    app_name = ""

try:
    app_principal = dbutils.widgets.get("app_principal")
except:
    app_principal = ""

# Prefer resolving the SP from the app name (survives redeploys / workspace moves).
if not app_principal and app_name:
    try:
        from databricks.sdk import WorkspaceClient
        _app = WorkspaceClient().apps.get(name=app_name)
        app_principal = (
            getattr(_app, "service_principal_client_id", None)
            or getattr(_app, "service_principal_id", None)
            or ""
        )
        if app_principal:
            print(f"Resolved app '{app_name}' service principal: {app_principal}")
        else:
            print(f"App '{app_name}' found but no service principal id yet "
                  f"(deploy the app first, then re-run setup to grant access).")
    except Exception as e:
        print(f"Could not resolve app '{app_name}' service principal: {e}")

print(f"Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Bronze Schema: {bronze_schema}")
print(f"  Silver Schema: {silver_schema}")
print(f"  Gold Schema: {gold_schema}")
print(f"  Volume Name: {volume_name}")
print(f"  Num Users: {num_users}")
print(f"  Num Documents: {num_documents}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog, Schemas, and Volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.{volume_name}")

print("Catalog, schemas, and volume created successfully!")

# Grant the Databricks App's service principal read access so its queries work.
# (The app runs as its own SP; without SELECT it silently returns empty tables.)
if app_principal:
    try:
        spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{app_principal}`")
        spark.sql(f"GRANT USE SCHEMA ON CATALOG {catalog} TO `{app_principal}`")
        spark.sql(f"GRANT SELECT ON CATALOG {catalog} TO `{app_principal}`")
        spark.sql(f"GRANT READ VOLUME ON CATALOG {catalog} TO `{app_principal}`")
        spark.sql(f"GRANT WRITE VOLUME ON CATALOG {catalog} TO `{app_principal}`")
        print(f"Granted read/volume access on {catalog} to app principal: {app_principal}")
    except Exception as e:
        print(f"WARNING: could not grant to app principal '{app_principal}': {e}")
else:
    print("No app_principal widget set — skipping app SP grants. "
          "Set it (the app's service principal id) so the app can read the catalog.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Users Table with NZ Care Coordinators

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# NZ-style care coordinator names and emails
users_data = [
    (1, "Aroha Ngata", "aroha.ngata@geneva.co.nz", "Care Coordinator", "Auckland", "Auckland Central"),
    (2, "Hemi Takutai", "hemi.takutai@geneva.co.nz", "Care Coordinator", "Wellington", "Wellington Central"),
    (3, "Rangi Waata", "rangi.waata@geneva.co.nz", "Senior Coordinator", "Christchurch", "Christchurch Central"),
    (4, "Eru Mahuta", "eru.mahuta@geneva.co.nz", "Care Coordinator", "Waikato", "Hamilton"),
    (5, "Hinetai Parata", "hinetai.parata@geneva.co.nz", "Care Coordinator", "Otago", "Dunedin"),
    (6, "Wiremu Te Ake", "wiremu.te.ake@geneva.co.nz", "Care Coordinator", "Auckland", "North Shore"),
    (7, "Tui Mahoe", "tui.mahoe@geneva.co.nz", "Senior Coordinator", "Wellington", "Hutt Valley"),
    (8, "Kahu Williams", "kahu.williams@geneva.co.nz", "Care Coordinator", "Christchurch", "West Christchurch"),
    (9, "Hina Koro", "hina.koro@geneva.co.nz", "Care Coordinator", "Waikato", "Cambridge"),
    (10, "Awhina Parata", "awhina.parata@geneva.co.nz", "Senior Coordinator", "Otago", "Cromwell"),
]

schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("full_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("role", StringType(), False),
    StructField("region", StringType(), False),
    StructField("office", StringType(), False),
])

users_df = spark.createDataFrame(users_data, schema=schema)
users_df.write.mode("overwrite").saveAsTable(f"{catalog}.{bronze_schema}.users")

print(f"Users table created with {len(users_data)} care coordinators!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Extensive Service Plan PDF Builder
# MAGIC Palette, synthetic data pools, layout helpers, and the multi-page builder that mirrors
# MAGIC the Geneva HBSS Complex Service Plan V6 reference form.

# COMMAND ----------

# ── Palette / page geometry matching the reference form ──────────────────────
GENEVA_GREEN = colors.HexColor("#D6E3BC")   # pale green section headers
GRID_GREY = colors.HexColor("#7F7F7F")
LIGHT_GREY = colors.HexColor("#F2F2F2")

PAGE_W, PAGE_H = A4
LEFT = RIGHT = 0.5 * inch
CONTENT_W = PAGE_W - LEFT - RIGHT   # usable width for full-span tables

# ── Synthetic data pools ─────────────────────────────────────────────────────
FUNDER_TYPES = ["DHB", "MOH", "ACC"]
CONTRACT_TYPES = ["Community Care", "Complex Care", "Supported Living", "Respite"]
VULNERABILITY_TIERS = ["Level 1", "Level 2", "Level 3", "N/A"]
CONDITIONS = [
    "Type 2 Diabetes", "Stroke", "Progressive Neurological condition", "Dementia",
    "COPD", "Heart Failure", "Chronic Pain", "Arthritis", "Hypertension",
]
SERVICES = [
    "Personal Support", "Household Support", "Nursing Care", "Mobility Assistance",
    "Meal Preparation", "Medication Management",
]
RISKS = ["Falls", "Fragile Skin", "Bed Bound", "Seizure Risk", "Cognitive Impairment", "Medication Interactions"]
EQUIPMENT = ["commode", "wheelchair", "standing hoist", "hospital bed", "shower chair", "walking frame", "slide sheet"]
ALLERGY_POOL = ["No known allergies", "Penicillin", "Sulfa drugs", "Latex", "Shellfish", "Aspirin"]
ETHNICITIES = ["Cook Island Maori", "NZ European", "Maori", "Samoan", "Tongan", "Chinese", "Indian", "Fijian"]
GP_NAMES = ["Dr. Who", "Dr. Patel", "Dr. Ngata", "Dr. Chen", "Dr. Williams", "Dr. Kumar"]
NASC_NAMES = ["DUTY NASC", "Regional NASC", "Community NASC", "Older Persons NASC"]
EC_RELATIONSHIPS = ["Daughter", "Son", "Spouse", "Sister", "Brother", "Niece", "Neighbour", "Friend"]
REVIEW_FREQS = ["Monthly", "Quarterly", "6-Monthly", "Annually"]

CLIENT_FIRST_NAMES = ["James", "Margaret", "Robert", "Patricia", "David", "Jennifer", "Michael", "Linda",
                      "William", "Barbara", "John", "Susan", "George", "Jessica", "Edward", "Sarah",
                      "Anahera", "Whanau", "Aroha", "Te Koro"]
CLIENT_LAST_NAMES = ["Smith", "Jones", "Brown", "Williams", "Taylor", "White", "Harris", "Martin",
                     "Thompson", "Garcia", "Maori", "Ngata", "Te Ake", "Mahoe", "Parata", "Takutai"]
NZ_STREETS = ["Ponsonby Road", "Queen Street", "Mountain Road", "The Strand", "Lambton Quay", "High Street",
              "Riccarton Road", "Main Street", "State Highway 1", "Mill Street"]
NZ_SUBURBS = ["Auckland", "Ponsonby", "Grey Lynn", "Wellington", "Newtown", "Te Aro",
              "Christchurch", "Riccarton", "Harewood", "Dunedin", "Mosgiel", "Hamilton", "Te Awamutu"]


def generate_nhi():
    """Synthetic NHI number: 3 letters + 4 digits (e.g. BOY6505)."""
    letters = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=3))
    numbers = "".join(random.choices("0123456789", k=4))
    return f"{letters}{numbers}"


def generate_phone():
    area = random.choice(["09", "07", "06", "04", "03"])
    rest = "".join(random.choices("0123456789", k=7))
    return f"({area}) {rest[:3]} {rest[3:]}"


def generate_email():
    return f"client{random.randint(1000, 9999)}@email.co.nz"

# COMMAND ----------

# ── Numbered canvas: boxed per-page footer with "Page X of Y" ─────────────────
# Total page count is unknown until the doc is built, so we buffer each page's
# state and stamp the footer during save() (the classic two-pass pattern).
class NumberedCanvas(_canvas.Canvas):
    def __init__(self, *args, footer_cn="Footer_cn", footer_nhi="Footer_N_H_I", **kwargs):
        super().__init__(*args, **kwargs)
        self._saved = []
        self._footer_cn = footer_cn
        self._footer_nhi = footer_nhi

    def showPage(self):
        self._saved.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total = len(self._saved)
        for state in self._saved:
            self.__dict__.update(state)
            self._draw_footer(total)
            super().showPage()
        super().save()

    def _draw_footer(self, total):
        x0, x1 = LEFT, PAGE_W - RIGHT
        mid = (x0 + x1) / 2
        y_box = 0.62 * inch
        self.setStrokeColor(colors.black)
        self.setLineWidth(0.6)
        self.rect(x0 + 0.6 * inch, y_box, (x1 - x0) - 1.2 * inch, 14, stroke=1, fill=0)
        self.line(mid, y_box, mid, y_box + 14)
        self.setFont("Helvetica", 8)
        self.drawString(x0 + 0.75 * inch, y_box + 3.5, f"Client Name : {self._footer_cn}")
        self.drawString(mid + 6, y_box + 3.5, f"NHI: {self._footer_nhi}")
        self.setFont("Helvetica", 7)
        self.drawString(x0, 0.42 * inch, "Geneva Healthcare Ltd")
        self.drawCentredString(mid, 0.42 * inch,
                               "Confidential record if found please call 0508 GENEVA   HBSS Complex Service Plan V6")
        self.drawRightString(x1, 0.5 * inch, "Aug 2015")
        self.drawRightString(x1, 0.40 * inch, f"Page {self._pageNumber} of {total}")

# COMMAND ----------

# ── Style + flowable helpers ─────────────────────────────────────────────────
_styles = getSampleStyleSheet()
BODY = ParagraphStyle("body", parent=_styles["Normal"], fontSize=8.5, leading=11)
BODY_SM = ParagraphStyle("body_sm", parent=_styles["Normal"], fontSize=8, leading=10)
BOLD = ParagraphStyle("bold", parent=BODY, fontName="Helvetica-Bold")
TITLE = ParagraphStyle("title", parent=_styles["Heading1"], fontSize=13, spaceAfter=4,
                       fontName="Helvetica-Bold", textColor=colors.black)
H_SUB = ParagraphStyle("hsub", parent=_styles["Heading2"], fontSize=11, fontName="Helvetica-Bold")


def P(text, style=BODY):
    return Paragraph(str(text) if text is not None else "", style)


def section_header(text, hint=""):
    """Full-width pale-green section header row (matches the reference)."""
    label = f"<b>{text}</b>"
    if hint:
        label += f"  <font size=7>({hint})</font>"
    t = Table([[P(label, BODY)]], colWidths=[CONTENT_W])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), GENEVA_GREEN),
        ("BOX", (0, 0), (-1, -1), 0.6, GRID_GREY),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


def chk(checked):
    """A tiny bordered box cell, 'X' when checked. Font-safe (no unicode glyphs)."""
    b = Table([["X" if checked else ""]], colWidths=[10], rowHeights=[10])
    b.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.6, colors.black),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return b


def care_domain(story, title, hint, goal_text, comments_text):
    """A care-domain section: green header + [Goal | Comments/interventions] table."""
    story.append(section_header(title, hint))
    data = [
        [P("<b>Goal</b> including timeframe &amp; review date", BODY_SM),
         P("<b>Comments, support and interventions required</b>", BODY_SM)],
        [P(goal_text, BODY_SM), P(comments_text, BODY_SM)],
    ]
    t = Table(data, colWidths=[CONTENT_W * 0.38, CONTENT_W * 0.62], rowHeights=[None, 52])
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("BACKGROUND", (0, 0), (-1, 0), GENEVA_GREEN),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(t)
    story.append(Spacer(1, 6))


def task_grid(story, title, actions):
    """Support-task grid: Action | Dependent | Assist | Independent | N/A | Details."""
    story.append(section_header(title))
    header = [P("<b>Action</b>", BODY_SM), P("<b>Dep.</b>", BODY_SM), P("<b>Assist</b>", BODY_SM),
              P("<b>Indep.</b>", BODY_SM), P("<b>N/A</b>", BODY_SM), P("<b>Details / Routine / Frequency</b>", BODY_SM)]
    rows = [header]
    for name, level, detail in actions:
        rows.append([
            P(name, BODY_SM),
            chk(level == "Dependent"), chk(level == "Assist"),
            chk(level == "Independent"), chk(level == "N/A"),
            P(detail, BODY_SM),
        ])
    col = [CONTENT_W * 0.24, CONTENT_W * 0.08, CONTENT_W * 0.08, CONTENT_W * 0.10,
           CONTENT_W * 0.08, CONTENT_W * 0.42]
    t = Table(rows, colWidths=col)
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_GREY),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (1, 0), (4, -1), "CENTER"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    story.append(t)
    story.append(Spacer(1, 8))


# Home Safety Risk Assessment rows (fixed hazards from the reference)
SAFETY_HAZARDS = [
    "Broken/cracked windows or mirrors",
    "Stairs with no hand rails or unsafe tread",
    "Unsafe/slippery floor surfaces or loose mats",
    "Unsafe electrical appliances or cords",
    "Inadequate or no smoke detectors",
    "Difficult to access cleaning equipment",
    "Heavy or difficult to move around furniture or beds",
    "Cleaning fluids or chemicals not clearly marked",
    "Laundry basket & clothes line difficult to access",
    "Exposure to body fluids/contaminated waste",
    "Client Moving & Handling",
    "Challenging or aggressive behaviours",
    "Exposure to cigarette smoke or other fumes",
    "Animals with aggressive behaviour or tripping risk",
]

# COMMAND ----------

def create_service_plan_pdf(d):
    """Build an extensive (multi-page) HBSS Complex Service Plan PDF from a dict `d`.
    Returns PDF bytes. `d` carries every field the pipeline later extracts."""
    buffer = io.BytesIO()
    footer_cn = f"{d['client_last_name']}, {d['client_first_name']}"
    doc = SimpleDocTemplate(
        buffer, pagesize=A4, topMargin=0.5 * inch, bottomMargin=0.8 * inch,
        leftMargin=LEFT, rightMargin=RIGHT, title="HBSS Complex Service Plan",
    )
    story = []

    # ── PAGE 1: header + cover grid ─────────────────────────────────────────
    story.append(P("<b>GENEVA</b> Northlink Healthcare", H_SUB))
    story.append(P("<b>Home Based Support Services (HBSS) MOH and DHB</b>", TITLE))
    story.append(Spacer(1, 2))
    story.append(P("<b>SERVICE PLAN | COMPLEX CARE</b>", H_SUB))
    story.append(Spacer(1, 6))

    # Completed-by / date + vulnerability tier checkboxes
    tiers = d["vulnerability_tier"]
    vt = Table([
        [P("Completed by:", BOLD), P(d["care_coordinator"]),
         P("<i>Vulnerability Tier:</i>", BOLD),
         P("Level 1", BODY_SM), chk(tiers == "Level 1"), P("Level 2", BODY_SM), chk(tiers == "Level 2")],
        [P("Date:", BOLD), P(d["completed_date"]),
         P("", BODY_SM),
         P("Level 3", BODY_SM), chk(tiers == "Level 3"), P("N/A", BODY_SM), chk(tiers == "N/A")],
    ], colWidths=[CONTENT_W * 0.16, CONTENT_W * 0.30, CONTENT_W * 0.18,
                  CONTENT_W * 0.10, 12, CONTENT_W * 0.08, 12])
    vt.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("BACKGROUND", (2, 0), (-1, -1), LIGHT_GREY),
        ("SPAN", (2, 0), (2, 1)),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TEXTCOLOR", (2, 0), (2, 1), colors.red),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
    ]))
    story.append(vt)
    story.append(Spacer(1, 4))

    # Client / contacts grid
    addr = f"{random.randint(1, 999)} {random.choice(NZ_STREETS)}, {random.choice(NZ_SUBURBS)}, New Zealand"
    client = [
        [P("Client Last Name:", BOLD), P(d["client_last_name"]),
         P("First Name:", BOLD), P(d["client_first_name"]),
         P("Prefers to be called:", BOLD), P(d["prefers_to_be_called"])],
        [P("NHI:", BOLD), P(d["nhi_number"]),
         P("Gender:", BOLD), P(d["gender"]),
         P("Date of Birth:", BOLD), P(d["date_of_birth"])],
        [P("Address:", BOLD), P(addr),
         P("Phone Number:", BOLD), P(f"Mobile: {d['phone']}"),
         P("EPOA:", BOLD), P(d["epoa_status"])],
        [P("Email address:", BOLD), P(d["email"]),
         P("InterRAI Score:", BOLD), P(str(d["interrai_score"])),
         P("Package of care:", BOLD), P(d["package_of_care"])],
        [P("Funder:", BOLD), P(d["funder"]),
         P("Contract Type:", BOLD), P(d["contract_type"]),
         P("Region:", BOLD), P(d["region"])],
        [P("NASC Contact:", BOLD), P(f"{d['nasc_contact_name']}<br/>Ph: {d['nasc_contact_phone']}"),
         P("Geneva Care Coordinator:", BOLD), P(d["care_coordinator"]),
         P("Review Frequency:", BOLD), P(d["review_frequency"])],
        [P("GP Name:", BOLD), P(d["gp_name"]),
         P("GP Contact Details:", BOLD), P(d["gp_contact"]),
         P("", BODY), P("", BODY)],
        [P("Emergency Contact 1:", BOLD),
         P(f"{d['emergency_contact_name']} ({d['emergency_contact_relationship']})<br/>Ph: {d['emergency_contact_phone']}"),
         P("Emergency Contact 2:", BOLD), P(d["emergency_contact_2"]),
         P("", BODY), P("", BODY)],
        [P("Referral Date:", BOLD), P(d["referral_date"]),
         P("Service start date:", BOLD), P(d["service_start_date"]),
         P("Review Date:", BOLD), P(d["review_date"])],
    ]
    ct = Table(client, colWidths=[CONTENT_W * 0.15, CONTENT_W * 0.22, CONTENT_W * 0.16,
                                  CONTENT_W * 0.19, CONTENT_W * 0.14, CONTENT_W * 0.14])
    ct.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("BACKGROUND", (0, 0), (0, -1), LIGHT_GREY),
        ("BACKGROUND", (2, 0), (2, -1), LIGHT_GREY),
        ("BACKGROUND", (4, 0), (4, -1), LIGHT_GREY),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(ct)
    story.append(Spacer(1, 6))

    # Referral details / hours narrative
    story.append(section_header("Referral Details"))
    story.append(P(f"<b>Service details and hours per referral</b> (Weekly Care Hours: {d['weekly_care_hours']}):", BODY))
    story.append(P(d["referral_narrative"], BODY))
    story.append(PageBreak())

    # ── PAGE 2: clinical background + referral goals ────────────────────────
    clinical = [
        [P("<b>Pre existing medical conditions</b>", BODY_SM), P(", ".join(d["primary_conditions"]), BODY_SM)],
        [P("<b>Medications</b> (as at time of planning care) and Issues/requirements", BODY_SM), P(d["medications"], BODY_SM)],
        [P("<b>Allergies</b>", BODY_SM), P(f"Type: {d['allergies']}", BODY_SM)],
        [P("<b>Home Situation including natural supports</b>", BODY_SM), P(d["home_situation"], BODY_SM)],
        [P("<b>Other formal supports</b> (name of provider and what is being provided)", BODY_SM), P(d["other_formal_supports"], BODY_SM)],
        [P("<b>Hazards/risks/vulnerabilities/barriers to care</b>", BODY_SM),
         P("See attached home safety risk assessment.<br/>Risk Flags: " + ", ".join(d["risk_flags"]), BODY_SM)],
        [P("<b>Allied Health requirements and/or equipment</b>", BODY_SM), P(", ".join(d["allied_health_equipment"]), BODY_SM)],
        [P("<b>Ethnicity/Cultural/spiritual/religious individual considerations</b>", BODY_SM), P(d["cultural_considerations"], BODY_SM)],
    ]
    clt = Table(clinical, colWidths=[CONTENT_W * 0.34, CONTENT_W * 0.66])
    clt.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("BACKGROUND", (0, 0), (0, -1), LIGHT_GREY),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(clt)
    story.append(Spacer(1, 8))

    story.append(section_header("Referral Goals"))
    story.append(P(f"<b>My long term goal(s):</b> {d['long_term_goal']}", BODY))
    story.append(Spacer(1, 2))
    story.append(P("<b>My short term goal(s):</b>", BODY))
    story.append(ListFlowable(
        [ListItem(P(g, BODY_SM)) for g in d["short_term_goals"]],
        bulletType="1", leftIndent=16,
    ))
    story.append(Spacer(1, 6))
    goalplan = [
        [P("<b>Goal Plan</b>", BODY_SM), P("<b>Steps To Achieve</b>", BODY_SM)],
        [P("Week 1 and on going", BODY_SM),
         P("<br/>".join([
             "I will receive support with personal cares",
             "I will attend to my GP appointment as recommended",
             "I will take my medications as prescribed",
             "I will do things as I can slowly and safely",
             "I will contact my GP, NASC or Geneva coordinator if my needs change",
         ]), BODY_SM)],
    ]
    gt = Table(goalplan, colWidths=[CONTENT_W * 0.30, CONTENT_W * 0.70])
    gt.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("BACKGROUND", (0, 0), (-1, 0), GENEVA_GREEN),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(gt)
    story.append(PageBreak())

    # ── PAGE 3: other providers, home & community, training, participants ───
    story.append(section_header("Other Providers/Suppliers/Services Details and linkage planning"))
    providers = [("Physiotherapist", "No"), ("Occupational Therapist", "No"), ("Nursing", "Yes"),
                 ("Podiatrist", "No"), ("Pharmacy", "Yes"), ("Psychological", "No"),
                 ("Paediatrician", "NA"), ("Social Worker", "No"), ("School Principal", "NA")]
    prov_rows = [[P(name, BODY_SM), P(val, BODY_SM)] for name, val in providers]
    pt = Table(prov_rows, colWidths=[CONTENT_W * 0.35, CONTENT_W * 0.65])
    pt.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    story.append(pt)
    story.append(Spacer(1, 6))

    story.append(section_header("Home and Community Information"))
    hci = [
        [P("Description of home incl. modifications, other residents & pets", BODY_SM), P(d["home_description"], BODY_SM)],
        [P("Community & social activities/interests and support available", BODY_SM), P(d["community_activities"], BODY_SM)],
        [P("Access to transport and assistance required", BODY_SM), P(d["transport_access"], BODY_SM)],
    ]
    hcit = Table(hci, colWidths=[CONTENT_W * 0.35, CONTENT_W * 0.65])
    hcit.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(hcit)
    story.append(Spacer(1, 6))

    story.append(section_header("Essential Service and Public Holiday Arrangements Required"))
    ess = Table([[chk("Household Support" in d["services_required"]), P("Household Support", BODY_SM),
                  chk("Personal Support" in d["services_required"]), P("Personal Support", BODY_SM),
                  chk(False), P("Childcare", BODY_SM)]],
                colWidths=[12, CONTENT_W * 0.30, 12, CONTENT_W * 0.30, 12, CONTENT_W * 0.28])
    ess.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 0.5, GRID_GREY),
                             ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("FONTSIZE", (0, 0), (-1, -1), 8)]))
    story.append(ess)
    story.append(Spacer(1, 6))

    story.append(section_header("Participants in Service Planning"))
    part = [[P("<b>Name</b>", BODY_SM), P("<b>Relationship to Client</b>", BODY_SM)],
            [P(d["emergency_contact_name"], BODY_SM), P(d["emergency_contact_relationship"], BODY_SM)],
            [P(d["care_coordinator"], BODY_SM), P("Service coordinator", BODY_SM)],
            [P("Clinical Team", BODY_SM), P("Clinical Coordinator", BODY_SM)]]
    partt = Table(part, colWidths=[CONTENT_W * 0.5, CONTENT_W * 0.5])
    partt.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
                               ("BACKGROUND", (0, 0), (-1, 0), GENEVA_GREEN), ("FONTSIZE", (0, 0), (-1, -1), 8)]))
    story.append(partt)
    story.append(PageBreak())

    # ── PAGES 4-5: care-domain narrative sections ───────────────────────────
    care_domain(story, "Personal Support", "hygiene, grooming & dressing requirements & routine",
                "I will receive support with personal cares",
                "Client requires 2 people assist during personal cares. Shower 2x a week; be extra gentle - fragile skin.")
    care_domain(story, "Bowel/Bladder Support", "routine, continence, catheter cares, bowel regime",
                "I will maintain normal bowel and bladder function", d.get("bowel_note", "Double incontinent - uses pads 24/7."))
    care_domain(story, "Mobility", "dependence level, transferring & mobilizing, equipment, falls strategies",
                f"Manual Handling Plan Completed? {'Yes' if d['manual_handling_plan_completed'] else 'No'}",
                "Bed bound - 2 carer assist required at all times. Standing hoist to be used.")
    care_domain(story, "Skin Care Support", "condition/integrity, disorders, wounds, dressings, risks, creams",
                f"Pressure Area Plan Completed? {'Yes' if d['pressure_area_plan_completed'] else 'No'}",
                "Bruises easily - fragile skin. Left toe wound dressing; DN visiting. Notify NOK of concerns.")
    care_domain(story, "Communication", "speech impaired, non-verbal, aids/technology, interpreter",
                "I will be understood and be able to communicate my needs and wants", d["communication_note"])
    care_domain(story, "Sensory Function", "vision or hearing deficits, aids required, temperature control",
                "", d["sensory_note"])
    care_domain(story, "Breathing", "difficulties/disorders, equipment, management, oxygen use, smoking",
                "I will maintain normal breathing function", d["breathing_note"])
    care_domain(story, "Nutrition/Hydration", "status, dietary & feeding requirements & aids, allergies, likes/dislikes",
                "I will maintain good nutrition and hydration", d["nutrition_note"])
    care_domain(story, "Sleeping", "overnight care, usual routine & sleep patterns, turning/positioning",
                "", d.get("sleeping_note", "Nil issues noted"))
    care_domain(story, "Pain", "site(s), occurs, frequency/severity, analgesia, management",
                "I will be able to manage pain", d["pain_note"])
    care_domain(story, "Medication Management", "dependence, prompting/supervision, blister packs, recording",
                "I will take my medications as prescribed", "Daughter assists with medication on blister pack.")
    care_domain(story, "Psychological Support", "memory, orientation, insight, motivation, anxiety, mood",
                "I will be supported in the home to remain safe", d["psychological_note"])
    care_domain(story, "Values and beliefs Support", "ethnic, spiritual, cultural or individual requirements",
                "I will have rapport with my carer", "Client would like to have a good rapport with the carer.")
    care_domain(story, "Sexuality", "beliefs, supports or individual requirements", "", "")
    care_domain(story, "Activities of daily living", "", "", "")

    # Emergency management
    story.append(section_header("Emergency Management"))
    em = [[P("<b>Potential Emergency</b>", BODY_SM), P("<b>Management</b>", BODY_SM)],
          [P("Fire/Evacuation", BODY_SM), P("Evacuation & exit assistance required. Smoke alarm in place.", BODY_SM)],
          [P("Civil Defence (Flood, earthquake etc)", BODY_SM), P("First aid/emergency kit, torches/batteries. Supplies in the home.", BODY_SM)],
          [P("Client Specific", BODY_SM), P(d.get("emergency_specific", "Recognise deterioration; contact NOK and Geneva coordinator."), BODY_SM)]]
    emt = Table(em, colWidths=[CONTENT_W * 0.32, CONTENT_W * 0.68])
    emt.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
                             ("BACKGROUND", (0, 0), (-1, 0), GENEVA_GREEN),
                             ("VALIGN", (0, 0), (-1, -1), "TOP"), ("FONTSIZE", (0, 0), (-1, -1), 8)]))
    story.append(emt)
    story.append(PageBreak())

    # ── PAGES 6-7: support task grids ───────────────────────────────────────
    household = [
        ("Wash clothes, hang out & bring in washing", random.choice(["Dependent", "Assist", "N/A"]), ""),
        ("Iron & put away clothes", "N/A", ""),
        ("Dust / polish / remove cobwebs", random.choice(["Assist", "N/A"]), ""),
        ("Vacuum / sweep / mop floors", random.choice(["Dependent", "Assist"]), ""),
        ("Make beds / change linen", "Assist", ""),
        ("Put out rubbish", "Independent", ""),
        ("Clean kitchen incl sink/bench/stove/microwave", "Assist", ""),
        ("Prepare meal", "Dependent", "Daughter cooks meals"),
        ("Wash, dry and put away dishes", "Assist", ""),
        ("Shopping", "Dependent", ""),
        ("Clean toilet", "Assist", ""),
        ("Clean bath / shower / basin", "Assist", ""),
    ]
    task_grid(story, "Household Support", household)

    personal = [
        ("Bathing / Showering", "Assist", "2 people assist, 2x/week"),
        ("Dressing / Undressing", "Assist", ""),
        ("Grooming incl nails, hair & shaving", "Assist", ""),
        ("Bladder / Bowel care / Toileting", "Dependent", "Pads 24/7"),
        ("Nutrition / Hydration", "Dependent", "Requires spoon feeding"),
        ("Medication", "Assist", "Blister pack, daughter assisting"),
    ]
    task_grid(story, "Personal Support", personal)

    story.append(section_header("Essential Classification Codes and Public Holiday Arrangements"))
    ecc = [[P("Essential household management required", BODY_SM), P("NA", BODY_SM)],
           [P("Essential personal care required", BODY_SM), P("Yes", BODY_SM)],
           [P("Essential child care required", BODY_SM), P("NA", BODY_SM)]]
    ecct = Table(ecc, colWidths=[CONTENT_W * 0.6, CONTENT_W * 0.4])
    ecct.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY), ("FONTSIZE", (0, 0), (-1, -1), 8),
                              ("TOPPADDING", (0, 0), (-1, -1), 3)]))
    story.append(ecct)
    story.append(PageBreak())

    # ── PAGE 8: Home Safety Risk Assessment ─────────────────────────────────
    story.append(section_header("Home Safety Risk Assessment"))
    hdr = [P("<b>Risk or Hazard</b>", BODY_SM), P("<b>Yes</b>", BODY_SM), P("<b>No</b>", BODY_SM),
           P("<b>H</b>", BODY_SM), P("<b>M</b>", BODY_SM), P("<b>L</b>", BODY_SM),
           P("<b>Strategies to eliminate, isolate or minimise</b>", BODY_SM)]
    safety_rows = [hdr]
    flagged = set(random.sample(range(len(SAFETY_HAZARDS)), k=random.randint(1, 3)))
    for i, hazard in enumerate(SAFETY_HAZARDS):
        is_yes = i in flagged
        rating = random.choice(["H", "M", "L"]) if is_yes else None
        safety_rows.append([
            P(hazard, BODY_SM), chk(is_yes), chk(not is_yes),
            chk(rating == "H"), chk(rating == "M"), chk(rating == "L"),
            P("Monitor and mitigate" if is_yes else "", BODY_SM),
        ])
    used = 0.26 * CONTENT_W + 22 + 22 + 16 + 16 + 16
    col = [CONTENT_W * 0.26, 22, 22, 16, 16, 16, CONTENT_W - used]
    st = Table(safety_rows, colWidths=col)
    st.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY),
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_GREY),
        ("ALIGN", (1, 0), (5, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    story.append(st)
    story.append(Spacer(1, 4))
    legend = [
        [P("<b>Likelihood of occurrence:</b> 1 very rare  2 unlikely  3 moderate  4 likely  5 almost certain", BODY_SM)],
        [P("<b>Consequence:</b> 1 minor first aid  2 medical treatment  3 lost time injury  4 serious harm  5 fatality", BODY_SM)],
        [P("<b>Risk Rating (L+C):</b>  2,3,4 = L (low)   5,6,7 = M (medium)   8,9,10 = H (high)", BODY_SM)],
    ]
    lt = Table(legend, colWidths=[CONTENT_W])
    lt.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.5, GRID_GREY), ("FONTSIZE", (0, 0), (-1, -1), 8)]))
    story.append(lt)
    story.append(PageBreak())

    # ── PAGE 9: agreement / rights & responsibilities ───────────────────────
    story.append(P("This is a formal agreement between Geneva Healthcare Limited (incorporating Geneva "
                   "Northlink) and you for the provision of Home Based Support Services detailed in the "
                   "preceding pages of this Service Plan.", BODY))
    story.append(Spacer(1, 4))
    story.append(P("This service is funded by your specific Needs Assessment agency (e.g. DHB, Ministry of "
                   "Health or ACC) and will be effective for the duration specified in the referral "
                   "documentation, unless otherwise cancelled by either party.", BODY))
    story.append(Spacer(1, 6))

    def bullets(title, items):
        story.append(P(f"<b>{title}</b>", BODY))
        story.append(ListFlowable([ListItem(P(x, BODY_SM)) for x in items], bulletType="bullet", leftIndent=14))
        story.append(Spacer(1, 4))

    bullets("1. Rights of the Client:", [
        "You must be informed about your rights and responsibilities.",
        "You have the right to be treated with dignity and respect.",
        "Your knowledge and experience of disability must be respected.",
        "Your cultural and personal background, age, beliefs and values must be taken into account.",
        "You have the right to be consulted about decisions that affect you.",
        "Your privacy and confidentiality must be respected at all times.",
        "You can refuse to have or withdraw from the service.",
        "You have the right to change your Support Worker or transfer to an alternative provider.",
    ])
    bullets("2. Responsibilities of the Client:", [
        "To make sure your Service Plan is available for the Support Worker to access.",
        "To provide a safe working environment for Support Workers.",
        "To treat the Support Worker and other Geneva staff with courtesy and respect.",
        "To inform Geneva if you will not be home at the arranged time.",
    ])
    bullets("3. Responsibilities of Geneva Healthcare Limited:", [
        "To provide the service specified in the service requisition received from the NASC agency.",
        "To develop a service plan to meet the identified needs of the client.",
        "To ensure the safety of the service by carrying out health and safety assessments.",
        "To ensure privacy and confidentiality is maintained for all client information (Privacy Act 1993).",
    ])
    story.append(P("<b>4. Termination of the Service:</b>", BODY))
    story.append(P("Either party may cancel this agreement at any time. If you wish to cancel this agreement "
                   "or request your support be transferred to another provider, please contact your Care "
                   "Coordinator. Cancellation of service is considered a last resort when all alternative "
                   "measures have been taken.", BODY_SM))
    story.append(Spacer(1, 6))
    story.append(P("<b>I agree that:</b>", BODY))
    story.append(P("I have been issued with Geneva's Client Information booklet, Complaints Procedures and the "
                   "Code of Rights. I confirm that I have been informed about and have participated in completing "
                   "my Service Plan and Goals and agree to receive the support detailed.  Initials: __________", BODY_SM))

    doc.build(
        story,
        canvasmaker=lambda *a, **k: NumberedCanvas(*a, footer_cn=footer_cn, footer_nhi=d["nhi_number"], **k),
    )
    buffer.seek(0)
    return buffer.getvalue()

# COMMAND ----------

def build_plan_record(client_first_name, client_last_name, care_coordinator, region):
    """Assemble one synthetic Service Plan record (all fields the PDF + pipeline use)."""
    hours = random.randint(4, 40)
    conditions = random.sample(CONDITIONS, k=random.randint(1, 3))
    services = random.sample(SERVICES, k=random.randint(2, 4))
    risks = random.sample(RISKS, k=random.randint(2, 4))
    equipment = random.sample(EQUIPMENT, k=random.randint(2, 4))
    now = datetime.now()
    referral = now - timedelta(days=random.randint(60, 400))
    start = referral + timedelta(days=random.randint(3, 40))
    dob = now - timedelta(days=random.randint(365 * 55, 365 * 92))
    return {
        "client_first_name": client_first_name,
        "client_last_name": client_last_name,
        "prefers_to_be_called": client_first_name,
        "nhi_number": generate_nhi(),
        "gender": random.choice(["Male", "Female", "Other"]),
        "date_of_birth": dob.strftime("%d/%m/%Y"),
        "epoa_status": random.choice(["Y", "N"]),
        "interrai_score": random.randint(10, 28),
        "phone": generate_phone(),
        "email": generate_email(),
        "funder": random.choice(FUNDER_TYPES),
        "contract_type": random.choice(CONTRACT_TYPES),
        "package_of_care": f"PC:{hours}hrs",
        "region": region,
        "vulnerability_tier": random.choice(VULNERABILITY_TIERS),
        "weekly_care_hours": hours,
        "care_coordinator": care_coordinator,
        "nasc_contact_name": random.choice(NASC_NAMES),
        "nasc_contact_phone": generate_phone(),
        "gp_name": random.choice(GP_NAMES),
        "gp_contact": f"{random.choice(NZ_SUBURBS)} Medical Centre",
        "emergency_contact_name": f"{random.choice(CLIENT_FIRST_NAMES)} {client_last_name}",
        "emergency_contact_relationship": random.choice(EC_RELATIONSHIPS),
        "emergency_contact_phone": generate_phone(),
        "emergency_contact_2": "Name:  Phone:",
        "completed_date": now.strftime("%d.%m.%Y"),
        "referral_date": referral.strftime("%d.%m.%Y"),
        "service_start_date": start.strftime("%d.%m.%Y"),
        "review_date": (start + timedelta(days=365)).strftime("%d.%m.%Y"),
        "review_frequency": random.choice(REVIEW_FREQS),
        "primary_conditions": conditions,
        "services_required": services,
        "risk_flags": risks,
        "allied_health_equipment": equipment,
        "allergies": random.choice(ALLERGY_POOL),
        "medications": "Takes medications on blister pack; daughter is assisting.",
        "cultural_considerations": f"{random.choice(ETHNICITIES)}. Carer to remove shoes prior entering the home.",
        "home_situation": "Lives with family who provide natural supports; other relatives visit when required.",
        "other_formal_supports": "DN visits - checks and changes wound dressing. GP visits every 3 months and when needed.",
        "home_description": "Single level home with wheelchair access; client's room is at the back.",
        "community_activities": "Family takes client out on weekends; applying for wheelchair access van.",
        "transport_access": "Family assists with transport.",
        "long_term_goal": "To remain in the home with support.",
        "short_term_goals": ["To have support with personal cares", "To remain safe"],
        "manual_handling_plan_completed": random.choice([True, False]),
        "pressure_area_plan_completed": random.choice([True, False]),
        "communication_note": "Speaks English and first language; slight hearing issues - speak slowly and clearly.",
        "sensory_note": "No issues with vision.",
        "breathing_note": "Gets short of breath occasionally - allow rest periods.",
        "nutrition_note": "Eats soft meals; requires spoon feeding; weakness on right side due to stroke.",
        "pain_note": "May not always report pain; family reads body language and gestures.",
        "psychological_note": "Memory issues due to stroke; all communication through to family.",
        "bowel_note": "Double incontinent - uses pads 24/7.",
        "sleeping_note": "Nil issues noted.",
        "emergency_specific": "Recognise deterioration early; contact NOK and Geneva coordinator.",
        "referral_narrative": (
            "Morning cares - Two care workers to assist each other for 1 hour each, total 2 hours in the "
            "morning. Evening cares - Two support workers to assist each other for 1 hour each, total 2 hours "
            "in the evening. Carer Support of 28 days also in place so family can arrange respite. "
            "Morning - assist with feeding, bathing, dressing, personal hygiene, tidy room and make the bed. "
            "Evening - assist with feeding if required, change into evening wear, settle for the evening. "
            "Comments: Carer to remove shoes prior entering the home. Requires 2 people assist during personal "
            "cares. Shower 2x a week; hoist to be used with 2 people assist - NO LIFT policy. Be extra gentle - "
            "fragile skin."
        ),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate and Upload PDFs

# COMMAND ----------

# Ensure we have users data as a list for reference
users_list = users_data

# Generate PDFs and upload to volume
base_path = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}"

# Create the volume path if needed (this is implicit with CREATE VOLUME, but ensure directory exists)
os.makedirs(base_path, exist_ok=True)

pdf_count = 0
generated_files = []

# Generate base timestamp once for batch, then add offsets to prevent collisions
base_timestamp = datetime.now()

for doc_idx in range(num_documents):
    # Distribute documents across users
    user_idx = doc_idx % len(users_list)
    user_id, user_name, user_email, role, region, office = users_list[user_idx]

    # Generate unique submission_id by adding offset to base timestamp
    submission_timestamp = base_timestamp + timedelta(seconds=doc_idx)
    submission_id = submission_timestamp.strftime("%Y%m%d%H%M%S")

    # Synthetic client identity for this document
    client_first_name = random.choice(CLIENT_FIRST_NAMES)
    client_last_name = random.choice(CLIENT_LAST_NAMES)

    # Realistic ORIGINAL filename (as a real coordinator would name it). Submitter email
    # + submission id live in the folder path, not the filename:
    #   InputPDFs/{email_slug}/{submission_id}/{original_name}
    email_slug = user_email.replace("@", "_at_").replace(".", "_dot_")
    filename = f"Service_Plan_{client_last_name.replace(' ', '')}_{submission_id}.pdf"

    # Assemble the full synthetic record, then render the extensive multi-page PDF.
    record = build_plan_record(
        client_first_name=client_first_name,
        client_last_name=client_last_name,
        care_coordinator=user_name,
        region=region,
    )
    pdf_bytes = create_service_plan_pdf(record)

    # Write to volume under the {email_slug}/{submission_id}/ folder so the original
    # filename is preserved (mirrors how the app uploads).
    file_dir = f"{base_path}/{email_slug}/{submission_id}"
    os.makedirs(file_dir, exist_ok=True)
    file_path = f"{file_dir}/{filename}"
    with open(file_path, 'wb') as f:
        f.write(pdf_bytes)

    generated_files.append({
        'filename': filename,
        'user': user_name,
        'email': user_email,
        'path': file_path,
        'size_kb': len(pdf_bytes) / 1024
    })

    pdf_count += 1

print(f"Successfully generated {pdf_count} extensive Service Plan PDFs!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Summary

# COMMAND ----------

print(f"""
╔════════════════════════════════════════════════════════════════╗
║          SAMPLE DATA GENERATION COMPLETE                       ║
╚════════════════════════════════════════════════════════════════╝

CATALOG & SCHEMAS:
  - Catalog: {catalog}
  - Bronze Schema: {bronze_schema}
  - Silver Schema: {silver_schema}
  - Gold Schema: {gold_schema}
  - Volume: {volume_name}

USERS TABLE:
  - Location: {catalog}.{bronze_schema}.users
  - Records: {len(users_list)} NZ-based care coordinators
  - Regions: Auckland, Wellington, Christchurch, Waikato, Otago
  - Email domain: @geneva.co.nz

GENERATED SERVICE PLAN PDFs:
  - Total PDFs: {pdf_count}
  - Storage location: /Volumes/{catalog}/{bronze_schema}/{volume_name}/
  - Folder layout: {{email_slug}}/{{submission_id}}/Service_Plan_{{LastName}}_{{submission_id}}.pdf
  - Example: {generated_files[0]['filename'] if generated_files else 'N/A'}

SAMPLE FILE DETAILS (first PDF):
  - Filename: {generated_files[0]['filename'] if generated_files else 'N/A'}
  - Uploaded by: {generated_files[0]['user'] if generated_files else 'N/A'}
  - Email (for extraction): {generated_files[0]['email'] if generated_files else 'N/A'}
  - Size: {generated_files[0]['size_kb']:.1f} KB

NEXT STEPS:
  1. Set up Auto Loader in bronze schema to ingest raw_documents from volume
  2. Configure document_submissions table to parse folder path into submitter email + id
  3. Deploy ai_parse_document pipeline to create parsed_documents in silver schema
  4. Run ai_extract on parsed text to populate service_plan_extracted silver table
  5. Create gold schema materialized views for analytics and Genie access

EXTENSIVE PDF CONTENT (mirrors Geneva HBSS Complex Service Plan V6):
  ✓ Cover grid: completed-by, vulnerability tier checkboxes, client identity,
    NHI, gender, DOB, EPOA, InterRAI score, package of care
  ✓ Contacts: NASC, Geneva Care Coordinator, GP, Emergency contacts
  ✓ Referral details + weekly hours narrative (morning/evening cares)
  ✓ Clinical background: conditions, medications, allergies, home situation,
    other formal supports, allied-health equipment, cultural considerations
  ✓ Referral goals (long/short term) + Goal Plan / Steps To Achieve
  ✓ Other providers linkage grid + Home & Community info + Participants
  ✓ 15 care-domain narrative sections (Personal Support, Mobility, Skin Care,
    Communication, Nutrition, Pain, Psychological Support, etc.)
  ✓ Support-task grids (Household / Personal) with Dependent/Assist/Independent
  ✓ Home Safety Risk Assessment (14 hazards, Yes/No + H/M/L rating)
  ✓ Rights / Responsibilities / Termination agreement
  ✓ Per-page numbered footer (Client Name | NHI | Page X of Y)

All data is synthetic. No real PII present.
""")

# COMMAND ----------

# Display generated file manifest
print("\nGENERATED FILES MANIFEST:")
print("─" * 100)
print(f"{'Filename':<70} {'User':<20} {'Size (KB)':<10}")
print("─" * 100)
for file_info in generated_files[:10]:  # Show first 10
    print(f"{file_info['filename']:<70} {file_info['user']:<20} {file_info['size_kb']:>8.1f}")
if len(generated_files) > 10:
    print(f"... and {len(generated_files) - 10} more files")
print("─" * 100)
