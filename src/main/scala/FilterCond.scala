import scala.language.implicitConversions

trait FilterCond { def eval(r: Row): Option[Boolean] }

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = r.get(colName).map(predicate)
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = conditions.flatMap(_.eval(r))
    if (results.isEmpty) None
    else Some(results.reduce(op))
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = f.eval(r).map(!_)
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_ && _, List(f1, f2))
def Or(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_ || _, List(f1, f2))
def Equal(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_ == _, List(f1, f2))

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = fs.flatMap(_.eval(r))
    if (results.isEmpty) None
    else Some(results.exists(identity))
  }
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = fs.flatMap(_.eval(r))
    if (results.isEmpty) None
    else Some(results.forall(identity))
  }
}

implicit def tuple2Field(t: (String, String => Boolean)): Field = Field(t._1, t._2)

extension (f: FilterCond) {
  def ===(other: FilterCond): FilterCond = Equal(f, other)
  def &&(other: FilterCond): FilterCond = And(f, other)
  def ||(other: FilterCond): FilterCond = Or(f, other)
  def !! : FilterCond = Not(f)
}
