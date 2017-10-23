package tableau

import com.tableausoftware.common._
import com.tableausoftware.extract._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}


object TableauDataFrame {
  implicit def applyTableau(df: DataFrame) = new TableauDataFrameImplicity(df)
}

class TableauDataFrameImplicity(df: DataFrame) extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(classOf[TableauDataFrameImplicity])

  def saveToTableau(filename: String) = {
    logger.debug("Initialize Tableau Extract API")
    ExtractAPI.initialize()

    val colTypes = columnTypes()
    val columnIndexes: Seq[(Int, Type, Int)] = getColumnsIndexes(colTypes, df)
    df.repartition(1).foreachPartition { it =>
      logger.info("Creating tableau table")
      val table = createTableauTable(colTypes, filename)
      logger.info("Tableau table created")
      val tableDef = table.getTableDefinition()
      logger.info("Inserting rows in Tableau table")
      it.map(createTableauRowFromRow(tableDef, columnIndexes, _))
        .foreach(table.insert)
    }
    logger.info("Tableau extractor created '{}'", filename)

    logger.debug("Clean up Tableau Extract API")
    ExtractAPI.cleanup()
  }

  private def columnTypes(): Seq[(String, Type)] = {
    df.schema.fields.map {
      f => (f.name, dataFrameTypeToTableauType(f.dataType))
    }
  }

  private def dataFrameTypeToTableauType(dataType: DataType): Type = {
    dataType match {
      case StringType => Type.CHAR_STRING
      case IntegerType => Type.INTEGER
      case LongType => Type.INTEGER
      case ShortType => Type.INTEGER
      case FloatType => Type.DOUBLE
      case DoubleType => Type.DOUBLE
      case BooleanType => Type.BOOLEAN
      case DateType => Type.DATETIME
    }
  }

  private def getColumnsIndexes(colTypes: Seq[(String, Type)], df: org.apache.spark.sql.DataFrame) = {
    colTypes.zipWithIndex.map {
      case ((columnName, columnType), i) => (i, columnType, df.schema.fieldIndex(columnName))
    }
  }

  private def createTableauTable(colTypes: Seq[(String, Type)], filename: String): Table = {
    val extract: Extract = new Extract(filename)
    val table: Table = if (!extract.hasTable("Extract")) {
      val tblDef: TableDefinition = makeTableDefinition(colTypes)
      extract.addTable("Extract", tblDef)
    } else {
      extract.openTable("Extract")
    }
    table
  }

  private def makeTableDefinition(columnsTypes: Seq[(String, Type)]): TableDefinition = {
    val tableDef: TableDefinition = new TableDefinition()
    tableDef.setDefaultCollation(Collation.PT_BR)
    columnsTypes.foreach((tableDef.addColumn _).tupled)
    tableDef
  }

  private def createTableauRowFromRow(tableDef: TableDefinition, columnIndexes: Seq[(Int, Type, Int)], dfRow: org.apache.spark.sql.Row): Row = {
    val row: Row = new Row(tableDef)
    columnIndexes.foreach {
      case (i, columnType, columnIndex) =>
        if (dfRow.get(columnIndex) == null) {
          row.setNull(i)
        } else {
          columnType match {
            case (Type.CHAR_STRING) => row.setCharString(i, dfRow.getString(columnIndex))
            case (Type.INTEGER) =>
              dfRow.get(columnIndex) match {
                case in: scala.Int => row.setInteger(i, in.toInt)
                case lo: scala.Long => row.setLongInteger(i, lo.toLong)
                case sh: scala.Short => row.setInteger(i, sh.toShort.toInt)
              }
            case (Type.DOUBLE) =>
              dfRow.get(columnIndex) match {
                case dou: scala.Double => row.setDouble(i, dou.toDouble)
                case flo: scala.Float => row.setDouble(i, flo.toFloat.toDouble)
              }
            case (Type.BOOLEAN) => row.setBoolean(i, dfRow.getBoolean(columnIndex))
            case (Type.DATETIME) => {
              val dt = java.util.Calendar.getInstance
              dt.setTime(new java.util.Date(dfRow.getLong(columnIndex)))
              row.setDateTime(i, dt.get(java.util.Calendar.YEAR),
                dt.get(java.util.Calendar.MONTH) + 1,
                dt.get(java.util.Calendar.DAY_OF_MONTH),
                dt.get(java.util.Calendar.HOUR_OF_DAY),
                dt.get(java.util.Calendar.MINUTE),
                dt.get(java.util.Calendar.SECOND),
                dt.get(java.util.Calendar.MILLISECOND))
            }
            case _ =>
          }
        }
    }
    row
  }
}
