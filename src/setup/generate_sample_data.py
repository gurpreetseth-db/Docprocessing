# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Document Intelligence - Sample Data Generator
# MAGIC Creates catalog, schemas, volume, users table, and 20 sample PDF documents.

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
from reportlab.lib.enums import TA_CENTER
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
    (4, "James OBrien", "james.obrien@pacificshield.com", "Sales", "Director", "Midwest"),
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

def create_pdf(doc_type, agent_name, agent_email, period, region):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Title'], fontSize=18, textColor=colors.HexColor('#1a237e'))
    subtitle_style = ParagraphStyle('CustomSub', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#455a64'), alignment=TA_CENTER)
    header_style = ParagraphStyle('CustomH2', parent=styles['Heading2'], fontSize=13, textColor=colors.HexColor('#0d47a1'))
    elements = []
    elements.append(Paragraph("PACIFIC SHIELD INSURANCE GROUP", title_style))

    if doc_type == "sales_report":
        elements.append(Paragraph("Monthly Sales Performance Report", subtitle_style))
        elements.append(Spacer(1, 6))
        elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1565c0')))
        elements.append(Spacer(1, 15))
        info = [["Agent:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period]]
        elements.append(Table(info, colWidths=[60, 180, 60, 150]))
        elements.append(Spacer(1, 20))
        elements.append(Paragraph("Sales by Product Line", header_style))
        data = [["Product", "Policies", "Premium ($)", "Target ($)", "Achievement"]]
        total_policies, total_premium = 0, 0
        for prod in ["Auto", "Home", "Life", "Health", "Commercial"]:
            policies = random.randint(8, 45)
            premium = round(random.uniform(25000, 180000), 2)
            target = round(premium * random.uniform(0.85, 1.15), 2)
            data.append([prod, str(policies), f"{premium:,.2f}", f"{target:,.2f}", f"{(premium/target*100):.1f}%"])
            total_policies += policies
            total_premium += premium
        data.append(["TOTAL", str(total_policies), f"{total_premium:,.2f}", "", ""])
        t = Table(data, colWidths=[90, 70, 110, 100, 80])
        t.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor('#1565c0')), ("TEXTCOLOR", (0,0), (-1,0), colors.white), ("GRID", (0,0), (-1,-1), 0.5, colors.grey), ("FONTSIZE", (0,0), (-1,-1), 9)]))
        elements.append(t)

    elif doc_type == "claim_processed":
        elements.append(Paragraph("Claims Processing Summary", subtitle_style))
        elements.append(Spacer(1, 6))
        elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#2e7d32')))
        elements.append(Spacer(1, 15))
        info = [["Processor:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period]]
        elements.append(Table(info, colWidths=[70, 180, 60, 150]))
        elements.append(Spacer(1, 20))
        elements.append(Paragraph("Processed Claims", header_style))
        data = [["Claim ID", "Product", "Amount ($)", "Date", "Status"]]
        num_claims = random.randint(12, 30)
        total_amt = 0
        for _ in range(min(num_claims, 15)):
            amt = round(random.uniform(500, 75000), 2)
            data.append([f"CLM-{random.randint(100000,999999)}", random.choice(["Auto","Home","Life","Health"]), f"{amt:,.2f}", (datetime.now()-timedelta(days=random.randint(1,28))).strftime('%Y-%m-%d'), random.choice(["Approved","Approved","Partial","Denied"])])
            total_amt += amt
        t = Table(data, colWidths=[85, 65, 90, 85, 65])
        t.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor('#2e7d32')), ("TEXTCOLOR", (0,0), (-1,0), colors.white), ("GRID", (0,0), (-1,-1), 0.5, colors.grey), ("FONTSIZE", (0,0), (-1,-1), 8)]))
        elements.append(t)
        elements.append(Spacer(1, 10))
        elements.append(Paragraph(f"Total processed: {num_claims} claims, ${total_amt:,.2f}", styles['Normal']))

    else:  # claim_outstanding
        elements.append(Paragraph("Outstanding Claims Report", subtitle_style))
        elements.append(Spacer(1, 6))
        elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#c62828')))
        elements.append(Spacer(1, 15))
        info = [["Reviewer:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period]]
        elements.append(Table(info, colWidths=[70, 180, 60, 150]))
        elements.append(Spacer(1, 20))
        elements.append(Paragraph("Outstanding Claims by Reason", header_style))
        reasons = ["Missing Documentation", "Under Investigation", "Awaiting Medical Records", "Pending Appraisal", "Legal Review Required", "Fraud Investigation"]
        data = [["Reason", "Count", "Amount ($)", "Avg Days"]]
        total_count, total_amt = 0, 0
        for reason in random.sample(reasons, random.randint(3, 6)):
            count = random.randint(2, 15)
            amt = round(random.uniform(10000, 250000), 2)
            data.append([reason, str(count), f"{amt:,.2f}", str(random.randint(7, 90))])
            total_count += count
            total_amt += amt
        data.append(["TOTAL", str(total_count), f"{total_amt:,.2f}", ""])
        t = Table(data, colWidths=[150, 50, 110, 70])
        t.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor('#c62828')), ("TEXTCOLOR", (0,0), (-1,0), colors.white), ("GRID", (0,0), (-1,-1), 0.5, colors.grey), ("FONTSIZE", (0,0), (-1,-1), 9)]))
        elements.append(t)

    doc.build(elements)
    return buffer.getvalue()

# COMMAND ----------

users = [
    ("Sarah Chen", "sarah.chen@pacificshield.com", "West"),
    ("Marcus Johnson", "marcus.johnson@pacificshield.com", "Southeast"),
    ("Priya Patel", "priya.patel@pacificshield.com", "Northeast"),
    ("James OBrien", "james.obrien@pacificshield.com", "Midwest"),
    ("Maria Rodriguez", "maria.rodriguez@pacificshield.com", "Southwest"),
    ("David Kim", "david.kim@pacificshield.com", "West"),
    ("Emily Watson", "emily.watson@pacificshield.com", "Northeast"),
    ("Robert Singh", "robert.singh@pacificshield.com", "Southeast"),
    ("Jessica Martinez", "jessica.martinez@pacificshield.com", "Southwest"),
    ("Thomas Wright", "thomas.wright@pacificshield.com", "Midwest")
]

periods = ["June 2026", "July 2026", "May 2026", "April 2026"]
doc_types = ["sales_report", "sales_report", "claim_processed", "claim_processed", "claim_outstanding", "claim_outstanding", "sales_report", "claim_processed", "claim_outstanding", "sales_report"]

generated = []
for i in range(20):
    name, email, region = users[i % len(users)]
    period = periods[i % len(periods)]
    doc_type = doc_types[i % len(doc_types)]
    date_str = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y%m%d')
    pdf_bytes = create_pdf(doc_type, name, email, period, region)
    filename = f"{email}_{date_str}_{doc_type}.pdf"
    with open(f"/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs/{filename}", "wb") as f:
        f.write(pdf_bytes)
    generated.append(filename)
    print(f"  Generated: {filename}")

print(f"\nTotal PDFs: {len(generated)}")

# COMMAND ----------

print("=" * 60)
print("  SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog: DocProcessing")
print(f"  Schemas: DocProcess_Bronze, DocProcess_Silver, DocProcess_Gold")
print(f"  Volume: DocProcessing.DocProcess_Bronze.InputPDFs")
print(f"  Users: 10 agents | PDFs: {len(generated)} documents")
print(f"  Next: bundle deploy --target dev, then run pipeline")
print("=" * 60)
