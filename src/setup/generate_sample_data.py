# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Document Intelligence - Sample Data Generator
# MAGIC This notebook creates the catalog, schemas, volume, users table, and generates 20 realistic sample PDF documents.

# COMMAND ----------

# MAGIC %pip install reportlab
# MAGIC %restart_python

# COMMAND ----------

import random
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
import io

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog, Schemas, and Volume

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS DocProcessing")
spark.sql("CREATE SCHEMA IF NOT EXISTS DocProcessing.DocProcess_Bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS DocProcessing.DocProcess_Silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS DocProcessing.DocProcess_Gold")
spark.sql("CREATE VOLUME IF NOT EXISTS DocProcessing.DocProcess_Bronze.InputPDFs")
print("Catalog, schemas, and volume created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Users Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

users_data = [
    (1, "Sarah Chen", "sarah.chen@pacificshield.com", "Sales", "Senior Agent", "West"),
    (2, "Marcus Johnson", "marcus.johnson@pacificshield.com", "Claims", "Manager", "Southeast"),
    (3, "Priya Patel", "priya.patel@pacificshield.com", "Underwriting", "Agent", "Northeast"),
    (4, "James O'Brien", "james.obrien@pacificshield.com", "Sales", "Director", "Midwest"),
    (5, "Maria Rodriguez", "maria.rodriguez@pacificshield.com", "Claims", "Senior Agent", "Southwest"),
    (6, "David Kim", "david.kim@pacificshield.com", "Operations", "Agent", "West"),
    (7, "Emily Watson", "emily.watson@pacificshield.com", "Sales", "Agent", "Northeast"),
    (8, "Robert Singh", "robert.singh@pacificshield.com", "Claims", "Senior Agent", "Southeast"),
    (9, "Jessica Martinez", "jessica.martinez@pacificshield.com", "Underwriting", "Manager", "Southwest"),
    (10, "Thomas Wright", "thomas.wright@pacificshield.com", "Sales", "Agent", "Midwest")
]

schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("full_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("department", StringType(), False),
    StructField("role", StringType(), False),
    StructField("region", StringType(), False)
])

df_users = spark.createDataFrame(users_data, schema)
df_users.write.mode("overwrite").saveAsTable("DocProcessing.DocProcess_Bronze.users")
print(f"Created users table with {df_users.count()} records")
display(df_users)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Sample PDF Documents

# COMMAND ----------

def create_sales_report_pdf(agent_name, agent_email, period, region):
    """Generate a realistic monthly sales report PDF."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Title'], fontSize=18, textColor=colors.HexColor('#1a237e'), spaceAfter=6)
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#455a64'), alignment=TA_CENTER)
    header_style = ParagraphStyle('Header', parent=styles['Heading2'], fontSize=13, textColor=colors.HexColor('#0d47a1'), spaceBefore=20)

    elements = []
    elements.append(Paragraph("PACIFIC SHIELD INSURANCE GROUP", title_style))
    elements.append(Paragraph("Monthly Sales Performance Report", subtitle_style))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1565c0')))
    elements.append(Spacer(1, 15))

    info_data = [["Agent:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period], ["Department:", "Sales", "Report Type:", "Monthly Sales"]]
    info_table = Table(info_data, colWidths=[60, 180, 70, 150])
    info_table.setStyle(TableStyle([("FONTSIZE", (0, 0), (-1, -1), 9), ("TEXTCOLOR", (0, 0), (0, -1), colors.grey), ("TEXTCOLOR", (2, 0), (2, -1), colors.grey)]))
    elements.append(info_table)
    elements.append(Spacer(1, 20))

    elements.append(Paragraph("Sales by Product Line", header_style))
    products = ["Auto", "Home", "Life", "Health", "Commercial"]
    sales_data = [["Product Line", "Policies Sold", "Premium Amount ($)", "Target ($)", "Achievement %"]]
    total_policies = 0
    total_premium = 0
    for prod in products:
        policies = random.randint(8, 45)
        premium = round(random.uniform(25000, 180000), 2)
        target = round(premium * random.uniform(0.85, 1.15), 2)
        achievement = round((premium / target) * 100, 1)
        sales_data.append([prod, str(policies), f"{premium:,.2f}", f"{target:,.2f}", f"{achievement}%"])
        total_policies += policies
        total_premium += premium
    sales_data.append(["TOTAL", str(total_policies), f"{total_premium:,.2f}", "", ""])

    table = Table(sales_data, colWidths=[100, 85, 110, 100, 95])
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor('#1565c0')), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTSIZE", (0, 0), (-1, -1), 9), ("GRID", (0, 0), (-1, -1), 0.5, colors.grey), ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor('#e3f2fd')), ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold")]))
    elements.append(table)
    elements.append(Spacer(1, 20))

    elements.append(Paragraph("Summary", header_style))
    elements.append(Paragraph(f"Total policies sold this period: {total_policies}", styles['Normal']))
    elements.append(Paragraph(f"Total premium generated: ${total_premium:,.2f}", styles['Normal']))
    elements.append(Spacer(1, 30))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    elements.append(Paragraph(f"Report generated: {datetime.now().strftime('{0}'.format('%Y-%m-%d %H:%M'))}", ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, textColor=colors.grey)))

    doc.build(elements)
    return buffer.getvalue()


def create_claims_processed_pdf(agent_name, agent_email, period, region):
    """Generate a claims processing summary PDF."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Title'], fontSize=18, textColor=colors.HexColor('#1a237e'), spaceAfter=6)
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#455a64'), alignment=TA_CENTER)
    header_style = ParagraphStyle('Header', parent=styles['Heading2'], fontSize=13, textColor=colors.HexColor('#0d47a1'), spaceBefore=20)

    elements = []
    elements.append(Paragraph("PACIFIC SHIELD INSURANCE GROUP", title_style))
    elements.append(Paragraph("Claims Processing Summary", subtitle_style))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1565c0')))
    elements.append(Spacer(1, 15))

    info_data = [["Processor:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period], ["Department:", "Claims", "Report Type:", "Claims Processed"]]
    info_table = Table(info_data, colWidths=[70, 180, 70, 150])
    info_table.setStyle(TableStyle([("FONTSIZE", (0, 0), (-1, -1), 9), ("TEXTCOLOR", (0, 0), (0, -1), colors.grey), ("TEXTCOLOR", (2, 0), (2, -1), colors.grey)]))
    elements.append(info_table)
    elements.append(Spacer(1, 20))

    elements.append(Paragraph("Processed Claims Detail", header_style))
    claims_data = [["Claim ID", "Product", "Amount ($)", "Processing Date", "Status"]]
    num_claims = random.randint(12, 30)
    total_amount = 0
    statuses = ["Approved", "Approved", "Approved", "Approved", "Partial", "Denied"]
    for i in range(num_claims):
        claim_id = f"CLM-{random.randint(100000, 999999)}"
        product = random.choice(["Auto", "Home", "Life", "Health", "Commercial"])
        amount = round(random.uniform(500, 75000), 2)
        proc_date = (datetime.now() - timedelta(days=random.randint(1, 28))).strftime('%Y-%m-%d')
        status = random.choice(statuses)
        claims_data.append([claim_id, product, f"{amount:,.2f}", proc_date, status])
        total_amount += amount

    table = Table(claims_data[:16], colWidths=[95, 80, 95, 100, 70])  # Limit rows for PDF readability
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor('#2e7d32')), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTSIZE", (0, 0), (-1, -1), 8), ("GRID", (0, 0), (-1, -1), 0.5, colors.grey)]))
    elements.append(table)
    elements.append(Spacer(1, 15))
    elements.append(Paragraph(f"Total claims processed: {num_claims}", styles['Normal']))
    elements.append(Paragraph(f"Total amount processed: ${total_amount:,.2f}", styles['Normal']))
    elements.append(Spacer(1, 30))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))

    doc.build(elements)
    return buffer.getvalue()


def create_outstanding_claims_pdf(agent_name, agent_email, period, region):
    """Generate an outstanding claims report PDF."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Title'], fontSize=18, textColor=colors.HexColor('#1a237e'), spaceAfter=6)
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#455a64'), alignment=TA_CENTER)
    header_style = ParagraphStyle('Header', parent=styles['Heading2'], fontSize=13, textColor=colors.HexColor('#b71c1c'), spaceBefore=20)

    elements = []
    elements.append(Paragraph("PACIFIC SHIELD INSURANCE GROUP", title_style))
    elements.append(Paragraph("Outstanding Claims Report", subtitle_style))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#c62828')))
    elements.append(Spacer(1, 15))

    info_data = [["Reviewer:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period], ["Department:", "Claims", "Report Type:", "Outstanding Claims"]]
    info_table = Table(info_data, colWidths=[70, 180, 70, 150])
    info_table.setStyle(TableStyle([("FONTSIZE", (0, 0), (-1, -1), 9), ("TEXTCOLOR", (0, 0), (0, -1), colors.grey), ("TEXTCOLOR", (2, 0), (2, -1), colors.grey)]))
    elements.append(info_table)
    elements.append(Spacer(1, 20))

    reasons_list = ["Missing Documentation", "Under Investigation", "Awaiting Medical Records", "Pending Appraisal", "Legal Review Required", "Fraud Investigation"]
    elements.append(Paragraph("Outstanding Claims by Reason", header_style))
    reason_data = [["Reason", "Count", "Total Amount ($)", "Avg Days Pending"]]
    total_count = 0
    total_amount = 0
    for reason in random.sample(reasons_list, random.randint(3, 6)):
        count = random.randint(2, 15)
        amount = round(random.uniform(10000, 250000), 2)
        avg_days = random.randint(7, 90)
        reason_data.append([reason, str(count), f"{amount:,.2f}", str(avg_days)])
        total_count += count
        total_amount += amount
    reason_data.append(["TOTAL", str(total_count), f"{total_amount:,.2f}", ""])

    table = Table(reason_data, colWidths=[150, 60, 120, 100])
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor('#c62828')), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTSIZE", (0, 0), (-1, -1), 9), ("GRID", (0, 0), (-1, -1), 0.5, colors.grey), ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor('#ffebee')), ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold")]))
    elements.append(table)
    elements.append(Spacer(1, 20))
    elements.append(Paragraph(f"Total outstanding claims: {total_count}", styles['Normal']))
    elements.append(Paragraph(f"Total outstanding amount: ${total_amount:,.2f}", styles['Normal']))
    elements.append(Spacer(1, 30))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))

    doc.build(elements)
    return buffer.getvalue()

# COMMAND ----------

# Generate and upload 20 PDFs
import os

users = [
    ("Sarah Chen", "sarah.chen@pacificshield.com", "West"),
    ("Marcus Johnson", "marcus.johnson@pacificshield.com", "Southeast"),
    ("Priya Patel", "priya.patel@pacificshield.com", "Northeast"),
    ("James O'Brien", "james.obrien@pacificshield.com", "Midwest"),
    ("Maria Rodriguez", "maria.rodriguez@pacificshield.com", "Southwest"),
    ("David Kim", "david.kim@pacificshield.com", "West"),
    ("Emily Watson", "emily.watson@pacificshield.com", "Northeast"),
    ("Robert Singh", "robert.singh@pacificshield.com", "Southeast"),
    ("Jessica Martinez", "jessica.martinez@pacificshield.com", "Southwest"),
    ("Thomas Wright", "thomas.wright@pacificshield.com", "Midwest")
]

periods = ["June 2026", "July 2026", "May 2026", "April 2026"]
doc_types = ["sales_report", "sales_report", "claim_processed", "claim_processed", "claim_outstanding", "claim_outstanding", "sales_report", "claim_processed", "claim_outstanding", "sales_report"]

volume_path = "/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs"
generated_files = []

for i in range(20):
    user_idx = i % len(users)
    name, email, region = users[user_idx]
    period = periods[i % len(periods)]
    doc_type = doc_types[i % len(doc_types)]
    date_str = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y%m%d')

    if doc_type == "sales_report":
        pdf_bytes = create_sales_report_pdf(name, email, period, region)
    elif doc_type == "claim_processed":
        pdf_bytes = create_claims_processed_pdf(name, email, period, region)
    else:
        pdf_bytes = create_outstanding_claims_pdf(name, email, period, region)

    filename = f"{email}_{date_str}_{doc_type}.pdf"
    file_path = f"{volume_path}/{filename}"

    # Write using dbutils
    dbutils.fs.put(file_path.replace("/Volumes/", "dbfs:/Volumes/"), "", overwrite=True)
    # Use python to write binary
    with open(f"/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs/{filename}", "wb") as f:
        f.write(pdf_bytes)

    generated_files.append(filename)
    print(f"  Generated: {filename}")

print(f"\nTotal PDFs generated: {len(generated_files)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("  SETUP COMPLETE - Insurance Document Intelligence Platform")
print("=" * 60)
print(f"\n  Catalog: DocProcessing")
print(f"  Schemas: DocProcess_Bronze, DocProcess_Silver, DocProcess_Gold")
print(f"  Volume: DocProcessing.DocProcess_Bronze.InputPDFs")
print(f"  Users: 10 sample insurance agents")
print(f"  PDFs Generated: {len(generated_files)} documents")
print(f"    - Sales Reports: {sum(1 for f in generated_files if 'sales' in f)}")
print(f"    - Claims Processed: {sum(1 for f in generated_files if 'claim_processed' in f)}")
print(f"    - Outstanding Claims: {sum(1 for f in generated_files if 'outstanding' in f)}")
print(f"\n  Next Steps:")
print(f"  1. Deploy the bundle: databricks bundle deploy --target dev")
print(f"  2. Run the pipeline to process documents")
print(f"  3. Configure the Genie space with Gold tables")
print(f"  4. Deploy the app and set environment variables")
print("=" * 60)