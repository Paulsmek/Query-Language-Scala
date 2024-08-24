object Queries {
  def queryT(p: Option[PP_SQL_Table]): Option[Table] = p.flatMap(_.eval)

  def queryDB(p: Option[PP_SQL_DB]): Option[Database] = p.flatMap(_.eval)

  def killJackSparrow(t: Table): Option[Table] = queryT(Some(FilterRows(t, Not(Field("name", _ == "Jack")))))

  def insertLinesThenSort(db: Database): Option[Table] = {
    val insertions = List(
      Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
      Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
      Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
      Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")
    )

    val newTable = Table("Inserted Fellas", insertions).sort("age")
    val updatedTables = db.tables :+ newTable
    val updatedDb = Database(updatedTables)
    Some(newTable)
  }

  def youngAdultHobbiesJ(db: Database): Option[Table] = {
    val joinedTableOption: Option[Table] = db.join("People", "name", "Hobbies", "name")

    joinedTableOption.map { joinedTable =>
      val filteredRows = joinedTable.data.filter { row =>
        val age = row.get("age").flatMap(_.toIntOption)
        val name = row.get("name")
        val hobby = row.get("hobby")
        age.exists(_ < 25) && name.exists(_.startsWith("J")) && hobby.isDefined
      }

      val extractedRows = filteredRows.map { row =>
        Map("name" -> row("name"), "hobby" -> row("hobby"))
      }

      Table(joinedTable.tableName, extractedRows)
    }
  }

}
