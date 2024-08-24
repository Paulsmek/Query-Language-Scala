type Row = Map[String, String]
type Tabular = List[Row]

case class Table(tableName: String, tableData: Tabular) {

  override def toString: String = {
    val headers = header.mkString(",")
    val rows = tableData.map(_.values.mkString(",")).mkString("\n")
    s"$headers\n$rows"
  }

  def insert(row: Row): Table = {
    if (tableData.contains(row)) {
      this
    } else {
      Table(tableName, tableData :+ row)
    }
  }


  def delete(row: Row): Table = {
    Table(tableName, tableData.filterNot(_ == row))
  }

  def sort(column: String): Table = {
    val sortedData = tableData.sortBy(_.getOrElse(column, ""))
    Table(tableName, sortedData)
  }

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val updatedData = tableData.map { row =>
      if (f.eval(row).contains(true)) row ++ updates else row
    }
    Table(tableName, updatedData)
  }

  def filter(f: FilterCond): Table = {
    val filteredData = tableData.filter(row => f.eval(row).contains(true))
    Table(tableName, filteredData)
  }


  def select(columns: List[String]): Table = {
    val selectedData = tableData.map(row => {
      val filteredRow = row.filterKeys(columns.contains)
      filteredRow.toMap
    })
    Table(tableName, selectedData)
  }

  def header: List[String] = tableData.headOption.map(_.keys.toList).getOrElse(List.empty)

  def data: Tabular = tableData

  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table = {
    val rows = s.split("\n").map(_.split(",").map(_.trim))
    val headers = rows.head
    val data = rows.tail.map(row => headers.zip(row).toMap)
    Table(name, data.toList)
  }
  
}

extension (table: Table) {
  def todo(i: Int): Table = {
    val indexedRow = table.tableData.lift(i).getOrElse(Map.empty)
    Table(table.tableName, List(indexedRow))
  }
}
