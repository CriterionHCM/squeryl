/*******************************************************************************
 * Copyright 2010 Maxime Lévesque
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************** */
package org.squeryl.dsl

import org.squeryl.dsl.ast._
import org.squeryl.internals._
import org.squeryl.{Query, Schema, Session}

import java.sql.ResultSet

sealed trait PrimitiveType
sealed trait TNumeric extends PrimitiveType
sealed trait TOptionBigDecimal extends TNumeric
sealed trait TBigDecimal extends TOptionBigDecimal with TNonOption

sealed trait TOptionDouble extends TOptionBigDecimal
sealed trait TDouble extends TOptionDouble with TBigDecimal with TNonOption

sealed trait TOptionLong extends TOptionDouble
sealed trait TLong extends TOptionLong with TDouble with TNonOption

sealed trait TOptionFloat extends TOptionDouble
sealed trait TFloat extends TOptionFloat with TDouble with TNonOption

sealed trait TOptionInt extends TOptionLong with TOptionFloat
sealed trait TInt extends TOptionInt with TLong with TNonOption with TFloat

sealed trait TOptionByte extends TOptionInt
sealed trait TByte extends TOptionByte with TInt with TNonOption

sealed trait TOption
    extends TOptionByte
    with TOptionInt
    with TOptionFloat
    with TOptionLong
    with TOptionDouble
    with TOptionBigDecimal
    with TOptionDate
    with TOptionString
    with TOptionTimestamp

sealed trait TNumericLowerTypeBound extends TByte with TInt with TFloat with TLong with TDouble with TBigDecimal

sealed trait TNonOption

sealed trait TOptionLowerBound
    extends TOptionByte
    with TOptionInt
    with TOptionFloat
    with TOptionLong
    with TOptionDouble
    with TOptionBigDecimal
    with TOptionDate
    with TOptionString
    with TOptionTimestamp

sealed trait TString extends TOptionString with TNonOption
sealed trait TDate extends TOptionDate with TNonOption
sealed trait TTimestamp extends TOptionTimestamp with TNonOption
sealed trait TByteArray extends TOptionByteArray with TNonOption
sealed trait TIntArray extends TOptionIntArray with TNonOption
sealed trait TLongArray extends TOptionLongArray with TNonOption
sealed trait TDoubleArray extends TOptionDoubleArray with TNonOption
sealed trait TStringArray extends TOptionStringArray with TNonOption
sealed trait TOptionString extends PrimitiveType
sealed trait TOptionDate extends PrimitiveType
sealed trait TOptionTimestamp extends PrimitiveType
sealed trait TOptionByteArray extends PrimitiveType
sealed trait TOptionIntArray extends PrimitiveType
sealed trait TOptionLongArray extends PrimitiveType
sealed trait TOptionDoubleArray extends PrimitiveType
sealed trait TOptionStringArray extends PrimitiveType
sealed trait TBoolean extends TOptionBoolean with TNonOption
sealed trait TOptionBoolean extends PrimitiveType
sealed trait TUUID extends TOptionUUID with TNonOption
sealed trait TOptionUUID extends PrimitiveType

@scala.annotation.implicitNotFound(
  "The left side of the comparison (===, <>, between, ...) is not compatible with the right side."
)
sealed class CanCompare[-A1, -A2]

sealed class TypeComparer[A, B]

sealed trait =!=[-A, +B]

object =!= {
  implicit def notEq[A, B]: =!=[A, B] = new =!=[A, B] {}

  implicit def ambig1[A, B](implicit ev: A =:= B): =!=[A, B] = sys.error("unreachable")
  implicit def ambig2[A, B](implicit ev: A =:= B): =!=[A, B] = sys.error("unreachable")
}

trait TypedExpression[A1, T1 <: PrimitiveType] extends ExpressionNode {
  outer =>

  def plus[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = f.convert(new BinaryOperatorNode(this, e, "+"))

  def times[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = f.convert(new BinaryOperatorNode(this, e, "*"))

  def minus[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = f.convert(new BinaryOperatorNode(this, e, "-"))

  def div[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3, A4, T4 <: PrimitiveType](
    e: TypedExpression[A2, T2]
  )(implicit f: TypedExpressionFactory[A3, T3], tf: Floatifier[T3, A4, T4]): TypedExpression[A4, T4] =
    tf.floatify(new BinaryOperatorNode(this, e, "/"))

  def +[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = plus(e)

  def *[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = times(e)

  def -[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = minus(e)

  def /[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3, A4, T4 <: PrimitiveType](
    e: TypedExpression[A2, T2]
  )(implicit f: TypedExpressionFactory[A3, T3], tf: Floatifier[T3, A4, T4]): TypedExpression[A4, T4] =
    tf.floatify(new BinaryOperatorNode(this, e, "/"))

  def ===[A2, T2 <: PrimitiveType](
    b: TypedExpression[A2, T2]
  )(implicit evT: TypeComparer[T1, T2], ev: TypeComparer[A1, A2]) =
    new EqualityExpression(this, b)

  def ===[A2, T2 <: PrimitiveType](b: A2)(implicit
    tef: TypedExpressionFactory[A2, T2],
    ev: TypeComparer[A1, A2],
    evT: TypeComparer[T1, T2]
  ) = new EqualityExpression(this, tef.create(b))

  def ===[A2, T2 <: PrimitiveType](
    q: Query[Measures[A2]]
  )(implicit tef: TypedExpressionFactory[A2, T2], ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(asRoot = false, Nil).ast, "=")

  def ===[A2, T2 <: PrimitiveType](
    q: Query[A2]
  )(implicit tef: TypedExpressionFactory[A2, T2], ev: TypeComparer[T1, T2], notEv: A2 =!= Measures[_]) =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(asRoot = false, Nil).ast, "=")

  def <>[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "<>")

  def isDistinctFrom[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "IS DISTINCT FROM")
  def isNotDistinctFrom[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "IS NOT DISTINCT FROM")

  def <>[A2, T2 <: PrimitiveType](
    q: Query[Measures[A2]]
  )(implicit tef: TypedExpressionFactory[A2, T2], ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, "<>")

  def gt[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, ">")
  def lt[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "<")
  def gte[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, ">=")
  def lte[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "<=")

  def gt[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, ">")
  def gte[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, ">=")
  def lt[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, "<")
  def lte[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, "<=")

  def >[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean = gt(q)
  def >=[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean = gte(q)
  def <[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean = lt(q)
  def <=[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean = lte(q)

  def >[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) = gt(b)
  def <[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) = lt(b)
  def >=[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) = gte(b)
  def <=[A2, T2 <: PrimitiveType](b: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) = lte(b)

  // TODO: add T1 <:< TOption to isNull and isNotNull
  def isNull = new PostfixOperatorNode("is null", this) with LogicalBoolean
  def isNotNull = new PostfixOperatorNode("is not null", this) with LogicalBoolean

  def between[A2, T2 <: PrimitiveType, A3, T3 <: PrimitiveType](
    b1: TypedExpression[A2, T2],
    b2: TypedExpression[A3, T3]
  )(implicit
    ev1: TypeComparer[T1, T2],
    ev2: TypeComparer[T2, T3]
  ) = new BetweenExpression(this, b1, b2)

  def like[A2, T2 <: TOptionString](s: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, s, "like")

  def ilike[A2, T2 <: TOptionString](s: TypedExpression[A2, T2])(implicit ev: TypeComparer[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, s, "ilike")

  def ||[A2, T2 <: PrimitiveType](e: TypedExpression[A2, T2]) = new ConcatOp[A1, A2, T1, T2](this, e)

  def regex(pattern: String) = new FunctionNode(pattern, Seq(this)) with LogicalBoolean {

    override def doWrite(sw: StatementWriter) =
      Session.currentSession.databaseAdapter.writeRegexExpression(outer, pattern, sw)
  }

  def is(columnAttributes: AttributeValidOnNumericalColumn*)(implicit restrictUsageWithinSchema: Schema) =
    new ColumnAttributeAssignment(_fieldMetaData, columnAttributes)

  def in[A2, T2 <: PrimitiveType](
    t: Iterable[A2]
  )(implicit tef: TypedExpressionFactory[A2, T2], ev: TypeComparer[A1, A2], evT: TypeComparer[T1, T2]): LogicalBoolean =
    new InclusionOperator(
      this,
      new RightHandSideOfIn(new ConstantExpressionNodeList(t.map(tef.createConstant))).toIn
    )

  def in[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean =
    new InclusionOperator(this, new RightHandSideOfIn(q.copy(asRoot = false, Nil).ast))

  def notIn[A2, T2 <: PrimitiveType](
    t: Iterable[A2]
  )(implicit ev: TypedExpressionFactory[A2, T2], cc: TypeComparer[T1, T2]): LogicalBoolean =
    new ExclusionOperator(this, new RightHandSideOfIn(new ConstantExpressionNodeList(t.map(ev.createConstant))).toNotIn)

  def notIn[A2, T2 <: PrimitiveType](q: Query[A2])(implicit cc: TypeComparer[T1, T2]): LogicalBoolean =
    new ExclusionOperator(this, new RightHandSideOfIn(q.copy(asRoot = false, Nil).ast))

  def ~ = this

  def sample: A1 = mapper.sample

  def mapper: OutMapper[A1]

  def :=[B](b: B)(implicit ev: B => TypedExpression[A1, T1]): UpdateAssignment =
    new UpdateAssignment(_fieldMetaData, ev(b): TypedExpression[A1, T1])

  def :=(q: Query[Measures[A1]]) =
    new UpdateAssignment(_fieldMetaData, q.ast)

  def defaultsTo[B](b: B)(implicit ev: B => TypedExpression[A1, T1]): DefaultValueAssignment =
    new DefaultValueAssignment(_fieldMetaData, ev(b): TypedExpression[A1, T1])

  /**
   * TODO: make safer with compiler plugin
   * Not type safe ! a TypedExpressionNode[T] might not be a SelectElementReference[_] that refers to a FieldSelectElement...   
   */
  private[squeryl] def _fieldMetaData: FieldMetaData = {
    val ser =
      try {
        this.asInstanceOf[SelectElementReference[_, _]]
      } catch { // TODO: validate this at compile time with a scalac plugin
        case e: ClassCastException => {
          throw new RuntimeException(
            "left side of assignment '" + Utils.failSafeString(
              this.toString
            ) + "' is invalid, make sure statement uses *only* closure argument.",
            e
          )
        }
      }

    val fmd =
      try {
        ser.selectElement.asInstanceOf[FieldSelectElement].fieldMetaData
      } catch { // TODO: validate this at compile time with a scalac plugin
        case e: ClassCastException => {
          throw new RuntimeException(
            "left side of assignment '" + Utils.failSafeString(
              this.toString
            ) + "' is invalid, make sure statement uses *only* closure argument.",
            e
          )
        }
      }
    fmd
  }
}

class TypedExpressionConversion[A1, T1 <: PrimitiveType](val e: ExpressionNode, bf: TypedExpressionFactory[A1, T1])
    extends TypedExpression[A1, T1] {

  def mapper: OutMapper[A1] = bf.createOutMapper

  override def inhibited = e.inhibited

  override def doWrite(sw: StatementWriter) = e.doWrite((sw))

  override def children = e.children
}

trait Floatifier[T1 <: PrimitiveType, A2, T2 <: PrimitiveType] {
  def floatify(v: ExpressionNode): TypedExpressionConversion[A2, T2]
}

trait IdentityFloatifier[A1, T1 <: PrimitiveType] extends Floatifier[T1, A1, T1]

trait FloatTypedExpressionFactory[A1, T1 <: PrimitiveType]
    extends TypedExpressionFactory[A1, T1]
    with IdentityFloatifier[A1, T1] {
  self: JdbcMapper[_, A1] =>
  def floatify(v: ExpressionNode): TypedExpressionConversion[A1, T1] = convert(v)
}

trait JdbcMapper[P, A] {
  self: TypedExpressionFactory[A, _] =>
  def thisTypedExpressionFactory: TypedExpressionFactory[A, _] = this
  def extractNativeJdbcValue(rs: ResultSet, i: Int): P
  def convertFromJdbc(v: P): A
  def convertToJdbc(v: A): P
  def defaultColumnLength: Int
  def map(rs: ResultSet, i: Int): A = convertFromJdbc(extractNativeJdbcValue(rs, i))
}

trait ArrayJdbcMapper[P, A] extends JdbcMapper[P, A] {
  self: TypedExpressionFactory[A, _] =>
  def nativeJdbcType = sample.asInstanceOf[AnyRef].getClass
}

trait PrimitiveJdbcMapper[A] extends JdbcMapper[A, A] {
  self: TypedExpressionFactory[A, _] =>
  def extractNativeJdbcValue(rs: ResultSet, i: Int): A
  def convertFromJdbc(v: A) = v
  def convertToJdbc(v: A) = v
  def nativeJdbcType = sample.asInstanceOf[AnyRef].getClass
}

abstract class NonPrimitiveJdbcMapper[P, A, T <: PrimitiveType](
  val primitiveMapper: PrimitiveJdbcMapper[P],
  val fieldMapper: FieldMapper
) extends JdbcMapper[P, A]
    with TypedExpressionFactory[A, T] {
  self: TypedExpressionFactory[A, T] =>

  def extractNativeJdbcValue(rs: ResultSet, i: Int): P = primitiveMapper.extractNativeJdbcValue(rs, i)
  def defaultColumnLength: Int = primitiveMapper.defaultColumnLength
  def sample: A =
    convertFromJdbc(primitiveMapper.thisTypedExpressionFactory.sample)

  def createFromNativeJdbcValue(v: P) = create(convertFromJdbc(v))

  fieldMapper.register(this)
}

object NonPrimitiveJdbcMapper {
  def create[P, A, T <: PrimitiveType](from: P => A, to: A => P)(implicit
    tef: PrimitiveJdbcMapper[P] with TypedExpressionFactory[P, T],
    fieldMapper: FieldMapper
  ): NonPrimitiveJdbcMapper[P, A, T] = new NonPrimitiveJdbcMapper[P, A, T](tef, fieldMapper) {
    override def convertFromJdbc(v: P): A = from(v)
    override def convertToJdbc(v: A): P = to(v)
  }
}

trait TypedExpressionFactory[A, T <: PrimitiveType] {
  self: JdbcMapper[_, A] =>

  def thisAnyRefMapper = this.asInstanceOf[JdbcMapper[AnyRef, A]]

  def create(a: A): TypedExpression[A, T] =
    FieldReferenceLinker.takeLastAccessedFieldReference match {
      case None =>
        createConstant(a)
      case Some(n: SelectElement) =>
        new SelectElementReference[A, T](n, createOutMapper)
    }

  def createConstant(a: A) =
    new ConstantTypedExpression[A, T](a, thisAnyRefMapper.convertToJdbc(a), Some(this))

  def jdbcSample =
    thisAnyRefMapper.convertToJdbc(sample)

  /**
   * Converts the argument into a TypedExpression[A,T], the resulting expression
   * is meant to be equivalent in terms of SQL generation, the conversion is only
   * at the type level
   */
  def convert(v: ExpressionNode) = new TypedExpressionConversion[A, T](v, this)

  def sample: A

  def defaultColumnLength: Int

  def thisMapper: JdbcMapper[_, A] = this

  private def zis = this

  def createOutMapper: OutMapper[A] = new OutMapper[A] {

    def doMap(rs: ResultSet): A =
      zis.map(rs, index)

    def sample: A = zis.sample
  }
}

object TypedExpressionFactory {

  def createOptionMapper[A, A1, T <: PrimitiveType, TOpt <: PrimitiveType](implicit
    tef: JdbcMapper[A1, A] with TypedExpressionFactory[A, T]
  ): JdbcMapper[A1, Option[A]]
    with TypedExpressionFactory[Option[A], TOpt]
    with DeOptionizer[A1, A, T, Option[A], TOpt] =
    new JdbcMapper[A1, Option[A]]
      with TypedExpressionFactory[Option[A], TOpt]
      with DeOptionizer[A1, A, T, Option[A], TOpt] {
      override def deOptionizer: TypedExpressionFactory[A, T] with JdbcMapper[A1, A] = tef
    }
}

trait IntegralTypedExpressionFactory[A1, T1 <: PrimitiveType, A2, T2 <: PrimitiveType]
    extends TypedExpressionFactory[A1, T1]
    with Floatifier[T1, A2, T2] {
  self: JdbcMapper[_, A1] =>

  def floatify(v: ExpressionNode): TypedExpressionConversion[A2, T2] = floatifyer.convert(v)
  def floatifyer: TypedExpressionFactory[A2, T2]
}

trait DeOptionizer[P1, A1, T1 <: PrimitiveType, A2 >: Option[A1] <: Option[A1], T2 <: PrimitiveType]
    extends JdbcMapper[P1, A2] {
  self: TypedExpressionFactory[A2, T2] =>

  def deOptionizer: TypedExpressionFactory[A1, T1] with JdbcMapper[P1, A1]

  def sample = Option(deOptionizer.sample)

  def defaultColumnLength: Int = deOptionizer.defaultColumnLength

  def convertFromJdbc(v: P1): A2 = Option(deOptionizer.convertFromJdbc(v))

  def convertToJdbc(v: A2): P1 =
    v map (p => deOptionizer.convertToJdbc(p)) getOrElse (null.asInstanceOf[P1])

  def extractNativeJdbcValue(rs: ResultSet, i: Int) = deOptionizer.extractNativeJdbcValue(rs, i)

  override def createOutMapper: OutMapper[A2] = new OutMapper[A2] {
    def doMap(rs: ResultSet): A2 = {

      val v = deOptionizer.thisMapper.map(rs, index)
      val r =
        if (rs.wasNull)
          None
        else
          Option(v)

      r
    }

    def sample: A2 = Option(deOptionizer.sample)
  }
}

class ConcatOp[A1, A2, T1 <: PrimitiveType, T2 <: PrimitiveType](
  val a1: TypedExpression[A1, T1],
  val a2: TypedExpression[A2, T2]
) extends BinaryOperatorNode(a1, a2, "||") {
  override def doWrite(sw: StatementWriter) =
    sw.databaseAdapter.writeConcatOperator(a1, a2, sw)
}

class NvlNode[A, T <: PrimitiveType](e1: TypedExpression[_, _], e2: TypedExpression[A, T])
    extends BinaryOperatorNode(e1, e2, "nvl", false)
    with TypedExpression[A, T] {

  def mapper = e2.mapper

  override def doWrite(sw: StatementWriter) =
    sw.databaseAdapter.writeNvlCall(left, right, sw)
}
