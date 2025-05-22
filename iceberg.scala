  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession
  import java.io.FileOutputStream
  import com.itextpdf.kernel.pdf._
  import com.itextpdf.layout._
  import com.itextpdf.layout.element._
  import spark.implicits._
  
  val path1 = "default.icebergtest_ac0503"
  val path2 ="s3a://cdpmodakbucket/mt24082/dataset2.parquet/"

  val df1 = spark.table(path1)
  val df2 = spark.read.parquet(path2)

  val schema1 = df1.schema
  val schema2 = df2.schema

  val count1 = df1.count()
  val count2 = df2.count()

val normalizedSchema1 = schema1.map(f => (f.name.toLowerCase, f.dataType)).toSet
val normalizedSchema2 = schema2.map(f => (f.name.toLowerCase, f.dataType)).toSet

val schemaChanged = normalizedSchema1 != normalizedSchema2

  val allColumns = (df1.columns ++ df2.columns).distinct

  val df1Aligned = allColumns.foldLeft(df1)((df, colName) =>
    if (df.columns.contains(colName)) df else df.withColumn(colName, lit(null))
  ).select(allColumns.map(col): _*)

  val df2Aligned = allColumns.foldLeft(df2)((df, colName) =>
    if (df.columns.contains(colName)) df else df.withColumn(colName, lit(null))
  ).select(allColumns.map(col): _*)

  val df1Compare = df1Aligned.withColumn("row_hash", sha2(concat_ws("||", allColumns.map(c => coalesce(col(c).cast("string"), lit("NULL"))): _*), 256))
  val df2Compare = df2Aligned.withColumn("row_hash", sha2(concat_ws("||", allColumns.map(c => coalesce(col(c).cast("string"), lit("NULL"))): _*), 256))

  val df1OnlyRows = df1Compare.select("row_hash").except(df2Compare.select("row_hash"))
  val df2OnlyRows = df2Compare.select("row_hash").except(df1Compare.select("row_hash"))

  val df1OnlyCount = df1OnlyRows.count()
  val df2OnlyCount = df2OnlyRows.count()
  val mismatchedCount = df1OnlyCount + df2OnlyCount

  val pdfPath = "/home/spark-srv-account/data_comparison_summary.pdf_82_iceberg"
  val pdfWriter = new PdfWriter(new FileOutputStream(pdfPath))
  val pdfDoc = new PdfDocument(pdfWriter)
  val document = new Document(pdfDoc)

  document.add(new Paragraph("Data Validation Report").setBold().setFontSize(18))
  document.add(new Paragraph("\n"))

  document.add(new Paragraph("1. Pipeline Metadata").setBold().setFontSize(14))

  val pipelineName = Option(variablesMap.get("pipeline_name")).map(_.toString).getOrElse("unknown_pipeline")
  val scheduledBy = Option(variablesMap.get("job_schedule_user_id")).map(_.toString).getOrElse("unknown_user")
  val pipelineStartTs = Option(variablesMap.get("pipeline_start_ts")).map(_.toString).getOrElse("N/A")
  val pipelineId = Option(variablesMap.get("pipeline_id")).map(_.toString).getOrElse("N/A")
  val sourceDatasetPath = path1
  val targetDatasetPath = path2

  document.add(new Paragraph(s"Pipeline Name: $pipelineName"))
  document.add(new Paragraph(s"Scheduled By: $scheduledBy"))
  document.add(new Paragraph(s"Pipeline Start Timestamp: $pipelineStartTs"))
  document.add(new Paragraph(s"Pipeline Id: $pipelineId"))
  document.add(new Paragraph(s"Source Dataset Path: $sourceDatasetPath"))
  document.add(new Paragraph(s"Target Dataset Path: $targetDatasetPath"))
  document.add(new Paragraph("\n"))

  document.add(new Paragraph("2. Data Statistics").setBold().setFontSize(14))

  val statsTable = new Table(4)
  statsTable.setAutoLayout() // iText 8 style for responsive width

  statsTable.addHeaderCell(new Cell().add(new Paragraph("No.")))
  statsTable.addHeaderCell(new Cell().add(new Paragraph("Category")))
  statsTable.addHeaderCell(new Cell().add(new Paragraph("Source Dataset")))
  statsTable.addHeaderCell(new Cell().add(new Paragraph("Target Dataset")))

  statsTable.addCell(new Cell().add(new Paragraph("1")))
  statsTable.addCell(new Cell().add(new Paragraph("Row Count")))
  statsTable.addCell(new Cell().add(new Paragraph(count1.toString)))
  statsTable.addCell(new Cell().add(new Paragraph(count2.toString)))

  statsTable.addCell(new Cell().add(new Paragraph("2")))
  statsTable.addCell(new Cell().add(new Paragraph("Column Count")))
  statsTable.addCell(new Cell().add(new Paragraph(schema1.fields.length.toString)))
  statsTable.addCell(new Cell().add(new Paragraph(schema2.fields.length.toString)))

  statsTable.addCell(new Cell().add(new Paragraph("3")))
  statsTable.addCell(new Cell().add(new Paragraph("Differing Rows")))
  statsTable.addCell(new Cell().add(new Paragraph(df1OnlyCount.toString)))
  statsTable.addCell(new Cell().add(new Paragraph(df2OnlyCount.toString)))

  document.add(statsTable)
  document.add(new Paragraph("\n"))

  document.add(new Paragraph("3. Validation Checks").setBold().setFontSize(14))

  val validationTable = new Table(2)
  validationTable.setAutoLayout()

  validationTable.addHeaderCell(new Cell().add(new Paragraph("Check")))
  validationTable.addHeaderCell(new Cell().add(new Paragraph("Status")))

  validationTable.addCell(new Cell().add(new Paragraph("Schema Validation")))
  validationTable.addCell(new Cell().add(new Paragraph(if (schemaChanged) "Failed" else "Passed")))

  validationTable.addCell(new Cell().add(new Paragraph("Data Validation")))
  validationTable.addCell(new Cell().add(new Paragraph(if (mismatchedCount > 0) "Failed" else "Passed")))

  validationTable.addCell(new Cell().add(new Paragraph("Row Validation")))
  validationTable.addCell(new Cell().add(new Paragraph(if (df1OnlyCount != df2OnlyCount) "Failed" else "Passed")))

  validationTable.addCell(new Cell().add(new Paragraph("Column Validation")))
  validationTable.addCell(new Cell().add(new Paragraph(if (schema1.length != schema2.length) "Failed" else "Passed")))

  document.add(validationTable)
  document.add(new Paragraph("\n"))

  document.add(new Paragraph("4. Dataset Fields").setBold().setFontSize(14))

  val fieldsTable = new Table(2)
  fieldsTable.setAutoLayout()

  fieldsTable.addHeaderCell(new Cell().add(new Paragraph("Dataset")))
  fieldsTable.addHeaderCell(new Cell().add(new Paragraph("Fields")))

  fieldsTable.addCell(new Cell().add(new Paragraph("Source Dataset")))
  fieldsTable.addCell(new Cell().add(new Paragraph(schema1.map(_.name).mkString(", "))))

  fieldsTable.addCell(new Cell().add(new Paragraph("Target Dataset")))
  fieldsTable.addCell(new Cell().add(new Paragraph(schema2.map(_.name).mkString(", "))))

  document.add(fieldsTable)
  document.close()

  println(s"PDF Report generated: $pdfPath")

