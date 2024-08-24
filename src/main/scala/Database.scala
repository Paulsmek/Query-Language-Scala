case class Database(tables: List[Table]) {

  override def toString: String = tables.mkString("\n")

  def create(tableName: String): Database = {
    val newTable = Table(tableName, List.empty)
    Database(tables :+ newTable)
  }

  def drop(tableName: String): Database = {
    val updatedTables = tables.filterNot(_.tableName == tableName)
    Database(updatedTables)
  }

  def selectTables(tableNames: List[String]): Option[Database] = {
    val selectedTables = tables.filter(table => tableNames.contains(table.tableName))
    if (selectedTables.length == tableNames.length) Some(Database(selectedTables))
    else None
  }

  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
    val t1 = tables.find(_.name == table1)
    val t2 = tables.find(_.name == table2)

    (t1, t2) match {
      case (Some(table1), Some(table2)) =>
        val matchedRowsFromTable1 = table1.data.filter(row1 =>
          table2.data.exists(row2 => row1.getOrElse(c1, "") == row2.getOrElse(c2, ""))
        )

        val matchedRowsFromTable2 = table2.data.filter(row2 =>
          table1.data.exists(row1 => row1.getOrElse(c1, "") == row2.getOrElse(c2, ""))
        )

        val combinedRows = matchedRowsFromTable1.flatMap { row1 =>
          val matchingRowsFromTable2 = table2.data.filter(row2 => row1.getOrElse(c1, "") == row2.getOrElse(c2, ""))
          matchingRowsFromTable2.map { matchedRow =>
            val combinedRow = row1 ++ matchedRow.filterKeys(_ != c2).map {
              case (k, v) if k == c1 => (k, s"${row1(c1)};${v}")
              case (k, v) if row1.contains(k) && matchedRow.contains(k) && row1(k) != v =>
                (k, s"${row1(k)};${v}")
              case (k, v) => (k, v)
            }
            combinedRow
          }
        }

        val unmatchedRowsFromTable1 = table1.data.filterNot(row1 =>
          table2.data.exists(row2 => row1.getOrElse(c1, "") == row2.getOrElse(c2, ""))
        ).map { row1 =>
          val newRowKeys = row1.keys.toSet
          val row2Keys = table2.header.toSet - c2
          val missingKeys = row2Keys -- newRowKeys
          val finalRow = row1 ++ missingKeys.map(key => (key, ""))
          finalRow
        }

        val unmatchedRowsFromTable2 = table2.data.filterNot(row2 =>
          table1.data.exists(row1 => row1.getOrElse(c1, "") == row2.getOrElse(c2, ""))
        ).map { row =>
          val newRowKeys = row.keys.toSet
          val row1Keys = table1.header.toSet
          val missingKeys = row1Keys -- newRowKeys

          val newRowWithoutC2 = row - c2

          val finalRow = newRowWithoutC2 ++ missingKeys.map(key => (key, "")) + (c1 -> row(c2))
          finalRow
        }

        val combinedTable = Table("joined_table", combinedRows ++ unmatchedRowsFromTable1 ++ unmatchedRowsFromTable2)
        Some(combinedTable)

      case _ => None
    }
  }


  def apply(index: Int): Table = {
    tables(index)
  }

}
