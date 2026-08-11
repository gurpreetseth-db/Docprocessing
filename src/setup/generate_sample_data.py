# Databricks notebook source
# MAGIC %md
# MAGIC # Service Plan Document Intelligence - Sample Data Generator
# MAGIC This notebook creates the catalog, schemas, volume, users table, and generates
# MAGIC professional multi-section Service Plan PDFs for the HBSS (Home Based Support Services) demo.

# COMMAND ----------

# MAGIC %pip install reportlab
# MAGIC %restart_python

# COMMAND ----------

import random
import os
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    PageBreak, HRFlowable
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
import io

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

# Optional: service principal / group that the Databricks App runs as. When set, we
# grant it read access on the catalog so the app's queries (My Submissions, Pipeline
# Ops) succeed — the app runs as its own SP, not as the deploying user.
try:
    app_principal = dbutils.widgets.get("app_principal")
except:
    app_principal = ""

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
# MAGIC ## Step 3: Generate Professional Service Plan PDFs

# COMMAND ----------

# Synthetic data pools for realistic variation
FUNDER_TYPES = ["DHB", "MOH", "ACC"]
VULNERABILITY_TIERS = ["Level 1", "Level 2", "Level 3", "N/A"]
CONDITIONS = [
    "Type 2 Diabetes",
    "Stroke",
    "Progressive Neurological condition",
    "Dementia",
    "COPD",
    "Heart Failure",
    "Chronic Pain",
    "Arthritis",
    "Hypertension"
]
SERVICES = [
    "Personal Support",
    "Household Support",
    "Nursing Care",
    "Mobility Assistance",
    "Meal Preparation",
    "Medication Management"
]
RISKS = ["Falls", "Fragile Skin", "Bed Bound", "Seizure Risk", "Cognitive Impairment", "Medication Interactions"]

# NZ surnames and first names for synthetic clients
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
    """Generate a synthetic NHI number in format NHI-XXNNNN"""
    import random
    letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
    numbers = ''.join(random.choices('0123456789', k=4))
    return f"NHI-{letters}{numbers}"

def generate_phone():
    """Generate a synthetic NZ phone number"""
    area_codes = ["09", "07", "06", "04", "03"]
    area = random.choice(area_codes)
    rest = ''.join(random.choices('0123456789', k=7))
    return f"({area}) {rest[:3]} {rest[3:]}"

def generate_email():
    """Generate a synthetic email"""
    return f"client{random.randint(1000, 9999)}@email.co.nz"

def create_service_plan_pdf(client_first_name, client_last_name, care_coordinator, funder, vulnerability_tier,
                            conditions, services, risks, region, hours, submission_id, dob=None, nhi=None, gender=None):
    """
    Generate a professional Service Plan PDF with multi-section layout.
    Returns PDF bytes in memory.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch,
                           leftMargin=0.5*inch, rightMargin=0.5*inch)
    styles = getSampleStyleSheet()
    story = []

    # Generate synthetic data if not provided
    if dob is None:
        dob = (datetime.now() - timedelta(days=random.randint(365*40, 365*90))).strftime("%d/%m/%Y")
    if nhi is None:
        nhi = generate_nhi()
    if gender is None:
        gender = random.choice(["Male", "Female", "Other"])

    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=14,
        textColor=colors.HexColor('#003366'),
        spaceAfter=6,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )

    header_style = ParagraphStyle(
        'Header',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#003366'),
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )

    section_style = ParagraphStyle(
        'SectionHeader',
        parent=styles['Heading2'],
        fontSize=11,
        textColor=colors.white,
        backColor=colors.HexColor('#003366'),
        spaceAfter=8,
        spaceBefore=8,
        fontName='Helvetica-Bold'
    )

    normal_small = ParagraphStyle(
        'NormalSmall',
        parent=styles['Normal'],
        fontSize=9,
        spaceAfter=4
    )

    # Header Section
    story.append(Paragraph("GENEVA HEALTHCARE", header_style))
    story.append(Paragraph("Home Based Support Services (HBSS)", header_style))
    story.append(Spacer(1, 6))
    story.append(Paragraph("SERVICE PLAN | COMPLEX CARE", title_style))
    story.append(Spacer(1, 12))

    # Metadata and Vulnerability Tier
    date_str = datetime.now().strftime("%d/%m/%Y")
    meta_data = [
        ["Completed by:", care_coordinator, "Date:", date_str],
        ["Vulnerability Tier:", vulnerability_tier, "", ""]
    ]
    meta_table = Table(meta_data, colWidths=[1.2*inch, 2*inch, 1.2*inch, 1.2*inch])
    meta_table.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
    ]))
    story.append(meta_table)
    story.append(Spacer(1, 10))

    # Client Information Table
    story.append(Paragraph("CLIENT INFORMATION", section_style))
    client_info = [
        ["Client Last Name:", client_last_name, "First Name:", client_first_name, "Prefers to be called:", client_first_name],
        ["NHI:", nhi, "Gender:", gender, "Date of Birth:", dob],
        ["Address:", f"{random.randint(1, 999)} {random.choice(NZ_STREETS)}", "", "", "", ""],
        ["", f"{random.choice(NZ_SUBURBS)}, New Zealand", "", "", "", ""],
        ["Phone:", generate_phone(), "Email:", generate_email(), "", ""],
    ]
    client_table = Table(client_info, colWidths=[1.1*inch, 1.3*inch, 0.95*inch, 1.3*inch, 1.2*inch, 1*inch])
    client_table.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    story.append(client_table)
    story.append(Spacer(1, 10))

    # Funder and Care Details
    story.append(Paragraph("FUNDER & CARE DETAILS", section_style))
    service_start = (datetime.now() - timedelta(days=random.randint(30, 365))).strftime("%d/%m/%Y")
    referral_date = (datetime.now() - timedelta(days=random.randint(60, 400))).strftime("%d/%m/%Y")
    review_freq = random.choice(["Monthly", "Quarterly", "6-Monthly"])

    funder_info = [
        ["Funder:", funder, "Contract Type:", "Community Care", "Region:", region],
        ["Referral Date:", referral_date, "Service Start Date:", service_start, "Review Frequency:", review_freq],
        ["Care Coordinator:", care_coordinator, "Weekly Care Hours:", str(hours), "", ""],
    ]
    funder_table = Table(funder_info, colWidths=[1.1*inch, 1.3*inch, 1.2*inch, 1.3*inch, 1.2*inch, 1*inch])
    funder_table.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
    ]))
    story.append(funder_table)
    story.append(Spacer(1, 10))

    # Pre-existing Medical Conditions
    story.append(Paragraph("PRE-EXISTING MEDICAL CONDITIONS", section_style))
    selected_conditions = conditions if conditions else random.sample(CONDITIONS, k=min(random.randint(1, 3), len(CONDITIONS)))
    conditions_text = ", ".join(selected_conditions)
    story.append(Paragraph(f"<b>Primary Conditions:</b> {conditions_text}", normal_small))
    story.append(Spacer(1, 10))

    # Services Required
    story.append(Paragraph("SERVICES REQUIRED", section_style))
    selected_services = services if services else random.sample(SERVICES, k=random.randint(2, 4))
    story.append(Paragraph(f"<b>Services Required:</b> {', '.join(selected_services)}", normal_small))
    services_data = [[service, "✓"] for service in selected_services]
    services_table = Table(services_data, colWidths=[3.5*inch, 0.5*inch])
    services_table.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
    ]))
    story.append(services_table)
    story.append(Spacer(1, 10))

    # Referral Goals
    story.append(Paragraph("REFERRAL GOALS", section_style))
    long_goal = "To maintain independence and quality of life through coordinated home-based support services."
    story.append(Paragraph(f"<b>Long-term Goal:</b> {long_goal}", normal_small))
    story.append(Spacer(1, 6))
    short_goals = [
        "To establish regular care routines and build rapport with support team.",
        "To monitor and manage health conditions with appropriate clinical oversight.",
    ]
    for i, goal in enumerate(short_goals, 1):
        story.append(Paragraph(f"<b>Short-term Goal {i}:</b> {goal}", normal_small))
    story.append(Spacer(1, 10))

    # Home Safety Risk Assessment
    story.append(Paragraph("HOME SAFETY RISK ASSESSMENT", section_style))
    selected_risks = risks if risks else random.sample(RISKS, k=random.randint(2, 4))
    risk_data = [["Risk / Hazard", "Yes/No", "Rating", "Notes"]]
    for risk in selected_risks:
        rating = random.choice(["High", "Medium", "Low"])
        response = random.choice(["Yes", "No"])
        notes = "Monitor closely" if response == "Yes" else "No immediate action"
        risk_data.append([risk, response, rating, notes])

    risk_table = Table(risk_data, colWidths=[2*inch, 1*inch, 1*inch, 1.5*inch])
    risk_table.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    story.append(risk_table)
    story.append(Spacer(1, 10))

    # Risk Flags (explicit text)
    risks_text = ", ".join(selected_risks)
    story.append(Paragraph(f"<b>Risk Flags:</b> {risks_text}", normal_small))
    story.append(Spacer(1, 10))

    # Manual Handling Plan
    manual_plan = random.choice(["Yes", "No"])
    story.append(Paragraph(f"<b>Manual Handling Plan Completed:</b> {manual_plan}", normal_small))
    story.append(Spacer(1, 15))

    # Footer
    story.append(Paragraph("CONFIDENTIAL RECORD", ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.grey,
        alignment=TA_CENTER,
    )))

    # Build PDF
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()

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

    # Build filename: email_slug__submission_id__service_plan.pdf
    email_slug = user_email.replace("@", "_at_").replace(".", "_dot_")
    filename = f"{email_slug}__{submission_id}__service_plan.pdf"

    # Generate synthetic data for this document (keep first/last names separate to handle multi-word names)
    client_first_name = random.choice(CLIENT_FIRST_NAMES)
    client_last_name = random.choice(CLIENT_LAST_NAMES)
    funder = random.choice(FUNDER_TYPES)
    vulnerability_tier = random.choice(VULNERABILITY_TIERS)
    selected_conditions = random.sample(CONDITIONS, k=random.randint(1, 3))
    selected_services = random.sample(SERVICES, k=random.randint(2, 4))
    selected_risks = random.sample(RISKS, k=random.randint(2, 4))
    hours = random.randint(4, 40)

    # Generate PDF
    pdf_bytes = create_service_plan_pdf(
        client_first_name=client_first_name,
        client_last_name=client_last_name,
        care_coordinator=user_name,
        funder=funder,
        vulnerability_tier=vulnerability_tier,
        conditions=selected_conditions,
        services=selected_services,
        risks=selected_risks,
        region=region,
        hours=hours,
        submission_id=submission_id
    )

    # Write directly to volume using native file I/O
    file_path = f"{base_path}/{filename}"
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

print(f"Successfully generated {pdf_count} Service Plan PDFs!")

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
  - Filename convention: {{email_slug}}__{{submission_id}}__service_plan.pdf
  - Example: {generated_files[0]['filename'] if generated_files else 'N/A'}

SAMPLE FILE DETAILS (first PDF):
  - Filename: {generated_files[0]['filename'] if generated_files else 'N/A'}
  - Uploaded by: {generated_files[0]['user'] if generated_files else 'N/A'}
  - Email (for extraction): {generated_files[0]['email'] if generated_files else 'N/A'}
  - Size: {generated_files[0]['size_kb']:.1f} KB

NEXT STEPS:
  1. Set up Auto Loader in bronze schema to ingest raw_documents from volume
  2. Configure document_submissions table to parse filenames and extract metadata
  3. Deploy ai_parse_document pipeline to create parsed_documents in silver schema
  4. Run ai_extract on parsed text to populate service_plan_extracted silver table
  5. Create gold schema materialized views for analytics and Genie access

PDF CONTENT INCLUDES:
  ✓ Multi-section professional Service Plan layout
  ✓ Geneva Healthcare branding and HBSS title
  ✓ Client information table with synthetic NHI, DOB, contact details
  ✓ Vulnerability Tier assignment (Level 1/2/3/N/A)
  ✓ Funder & care package details (DHB/MOH/ACC, weekly hours)
  ✓ Pre-existing medical conditions (Type 2 Diabetes, Stroke, Dementia, etc.)
  ✓ Services Required (Personal Support, Household Support, Nursing, etc.)
  ✓ Referral Goals (long-term + short-term)
  ✓ Home Safety Risk Assessment with Yes/No + rating
  ✓ Manual Handling Plan status
  ✓ Footer with confidentiality notice

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
