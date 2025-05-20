pip install pygit2


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws, coalesce
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DatasetComparison") \
        .getOrCreate()

    # Input Paths
    path1 = "default.icebergtest_ac0503"  # Hive table
    path2 = "s3a://cdpmodakbucket/mt24082/dataset2.parquet/"  # Parquet path

    # Read Datasets
    df1 = spark.table(path1)
    df2 = spark.read.parquet(path2)

    # Collect schema and row counts
    schema1 = df1.schema
    schema2 = df2.schema
    count1 = df1.count()
    count2 = df2.count()

    # Normalize schema for comparison
    normalizedSchema1 = set((f.name.lower(), f.dataType) for f in schema1)
    normalizedSchema2 = set((f.name.lower(), f.dataType) for f in schema2)
    schemaChanged = normalizedSchema1 != normalizedSchema2

    # Align schemas
    allColumns = list(set(df1.columns + df2.columns))
    for colName in allColumns:
        if colName not in df1.columns:
            df1 = df1.withColumn(colName, lit(None))
        if colName not in df2.columns:
            df2 = df2.withColumn(colName, lit(None))

    df1 = df1.select(*allColumns)
    df2 = df2.select(*allColumns)

    def add_hash_column(df):
        return df.withColumn(
            "row_hash",
            sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in allColumns]), 256)
        )

    df1Hashed = add_hash_column(df1)
    df2Hashed = add_hash_column(df2)

    # Find mismatched rows
    df1OnlyRows = df1Hashed.select("row_hash").subtract(df2Hashed.select("row_hash"))
    df2OnlyRows = df2Hashed.select("row_hash").subtract(df1Hashed.select("row_hash"))

    df1OnlyCount = df1OnlyRows.count()
    df2OnlyCount = df2OnlyRows.count()
    mismatchedCount = df1OnlyCount + df2OnlyCount

    # PDF Report Generation
    pdfPath = "/tmp/data_comparison_summary.pdf"
    doc = SimpleDocTemplate(pdfPath, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Data Validation Report", styles["Heading1"]))
    elements.append(Spacer(1, 12))

    # Pipeline Metadata
    elements.append(Paragraph("1. Pipeline Metadata", styles["Heading2"]))
    pipelineName = "Manual_Run"
    scheduledBy = "CLI_User"
    pipelineStartTs = "Manual_Execution"
    pipelineId = "N/A"
    elements.extend([
        Paragraph(f"Pipeline Name: {pipelineName}", styles["Normal"]),
        Paragraph(f"Scheduled By: {scheduledBy}", styles["Normal"]),
        Paragraph(f"Pipeline Start Timestamp: {pipelineStartTs}", styles["Normal"]),
        Paragraph(f"Pipeline Id: {pipelineId}", styles["Normal"]),
        Paragraph(f"Source Dataset Path: {path1}", styles["Normal"]),
        Paragraph(f"Target Dataset Path: {path2}", styles["Normal"]),
        Spacer(1, 12)
    ])

    # Data Stats
    elements.append(Paragraph("2. Data Statistics", styles["Heading2"]))
    stats_data = [
        ["No.", "Category", "Source Dataset", "Target Dataset"],
        ["1", "Row Count", str(count1), str(count2)],
        ["2", "Column Count", str(len(schema1)), str(len(schema2))],
        ["3", "Differing Rows", str(df1OnlyCount), str(df2OnlyCount)],
    ]
    stats_table = Table(stats_data)
    stats_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(stats_table)
    elements.append(Spacer(1, 12))

    # Validation Checks
    elements.append(Paragraph("3. Validation Checks", styles["Heading2"]))
    validation_data = [
        ["Check", "Status"],
        ["Schema Validation", "Failed" if schemaChanged else "Passed"],
        ["Data Validation", "Failed" if mismatchedCount > 0 else "Passed"],
        ["Row Validation", "Failed" if df1OnlyCount != df2OnlyCount else "Passed"],
        ["Column Validation", "Failed" if len(schema1) != len(schema2) else "Passed"],
    ]
    validation_table = Table(validation_data)
    validation_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(validation_table)
    elements.append(Spacer(1, 12))

    # Dataset Fields
    elements.append(Paragraph("4. Dataset Fields", styles["Heading2"]))
    fields_data = [
        ["Dataset", "Fields"],
        ["Source Dataset", ", ".join(f.name for f in schema1)],
        ["Target Dataset", ", ".join(f.name for f in schema2)],
    ]
    fields_table = Table(fields_data)
    fields_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(fields_table)

    doc.build(elements)
    print(f"\n✅ PDF Report generated at: {pdfPath}\n")

    spark.stop()

if __name__ == "__main__":
    main()
