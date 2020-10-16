/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.expressions

import scala.collection.{mutable, GenSet, GenTraversableOnce}
import scala.collection.mutable.ArrayBuffer

/**
 * This class stores the constraints available at each node.
 * The constraint expressions are stored in canonicalized form.
 * The major way in which it differs from the [[ExpressionSet]] is that
 * in case of Project Node, it stores information about the aliases
 * and groups them on the basis of equivalence. In stock spark all the
 * constraints are pre-created pessimistically for all possible combinations
 * of equivalent aliases. While in this class only one constraint per filter
 * is present & the rest are created in expand function at the time of
 * new filter inference. Also even then, not all combinations are created,
 * instead one filter constraint for each alias in the group is created.
 *
 * The core logic of new algorithm is as follows
 * The base constraints present in this object will always be composed of
 * as far as possible, of those attributes which were present in the incoming
 * set & are also part of the output set for the node. If any attribute or expression
 * is part of any outgoing alias, they will be added to either attribute equivalence list
 * or expression equivalence list, depending upon whether the Alias's child is attribute
 * or generic expression. The 0th element of each of the buffer in the attribute equivalence
 * list & expression equivalence list are special in the sense, that any constraint,
 * if it is referring to any attribute or expression in the two lists, is guaranteed to
 * use the 0th element and not any other members. An attempt is made to ensure that the
 * constraint survives when bubbling up. If an attribute or expression, which is part
 * of incoming constraint, but is not present in output set, makes the survival of the
 * constraint susceptible. In such case, the attribute equivalence list & expression
 * equivalence list are consulted & if found that the buffer containing 0th element as
 * the attribute which is getting removed, has another element, then that element ( the
 * 1th member) is chosen to replace the attribute being removed  in the constraint.
 * The constraint is updated to use the 1th element. This 1th element is then put in
 * 0th place of the buffer.
 * It is to be noted that attribute equivalence list will have buffers where each
 * element will be of type attribute only. While expression equivalence list will
 * have 0th element as of type generic expression, rest being attributes.
 * It is also to be noted, that the 0th element of expression equivalence list being
 * generic expression, itself is composed of attributes. And the expression equivalence
 * list needs to be updated, if any of the attribute it refers to is being eliminated
 * from the output set.
 *
 * For eg. consider an existing constraint a + b + c + d > 7
 * let the input set comprise of attributes a, b, c, d
 * Let the output set be  a, a as a1, a as a2, b + c as z, d as d1
 * In the above d , b  & c are getting eliminated
 * while a survives and also has a1 & a2.
 * d is referred as d1.
 * the initial attribute equivalence list will be
 * a, a1, a2
 * d, d1
 * expression equivalence list will be
 * b + c , z
 * Now for the constraint a + b + c + d > 7 to survive
 * b + c => can be replaced by z
 * d can be replaced by d1
 * so the constraint will be updated as
 * a + z + d1 > 7
 * the updated attribute equivalence list will be
 * a, a1, a2
 * since d1 will be left alone, it will no longer be part of the list
 * same is the case with expression equivalence list.
 * as b + c, will be removed, only z1 remains, so it will be removed
 * from expression equivalence list & it will be empty.
 *
 * @param baseSet    [[mutable.Set[Expression]] which contains Canonicalized Constraint Expression
 * @param originals    [[mutable.Buffer[Expression]] buffer containing the original constraint
 *                   expression
 * @param attribRefBasedEquivalenceList    A List of List which contains grouping of equivalent
 *                   Aliases referring to same Attribute
 * @param expressionBasedEquivalenceList     A List of List which contains grouping of equivalent
 *  Aliases referring to same Expression( which is not an Attribute)
 */

class ConstraintSet private(
  baseSet: mutable.Set[Expression],
  originals: mutable.Buffer[Expression] = new ArrayBuffer,
  val attribRefBasedEquivalenceList: Seq[mutable.Buffer[Expression]],
  val expressionBasedEquivalenceList: Seq[mutable.Buffer[Expression]]
) extends ExpressionSet(baseSet, originals) {

  def this(actuals: mutable.Buffer[Expression]) =
    this(actuals.map(_.canonicalized).to[mutable.Set], actuals,
      Seq.empty[mutable.Buffer[Expression]], Seq.empty[mutable.Buffer[Expression]])

  def this(
    actuals: mutable.Buffer[Expression],
    attribRefBasedEquivalenceList: Seq[mutable.Buffer[Expression]],
    expressionBasedEquivalenceList: Seq[mutable.Buffer[Expression]]) =
    this(actuals.map(_.canonicalized).to[mutable.Set], actuals, attribRefBasedEquivalenceList,
      expressionBasedEquivalenceList)

  def this(baseSet: mutable.Set[Expression], actuals: mutable.Buffer[Expression]) =
    this(baseSet, actuals, Seq.empty[mutable.Buffer[Expression]],
      Seq.empty[mutable.Buffer[Expression]])

  def this() = this(mutable.Buffer.empty[Expression])

  override def +(elem: Expression): ConstraintSet = {
    val newSet = new ConstraintSet(baseSet.clone(), this.originals.clone(),
      this.attribRefBasedEquivalenceList.map(_.clone),
      this.expressionBasedEquivalenceList.map(_.clone))
    // If the constraint is already present in canonicalized form do not add
    val conditionedElement = convertToCanonicalizedIfRequired(elem)
    if (!this.contains(conditionedElement)) {
      newSet.add(conditionedElement)
    }
    newSet
  }

  override def union(that: GenSet[Expression]): ConstraintSet = {
    that match {
      case thatX: ConstraintSet =>
        val newSet = super.union(that).asInstanceOf[ConstraintSet]
        new ConstraintSet(
          newSet.baseSet,
          newSet.originals,
          newSet.attribRefBasedEquivalenceList ++ thatX.attribRefBasedEquivalenceList,
          newSet.expressionBasedEquivalenceList ++ thatX.expressionBasedEquivalenceList
        )
      case _ => super.union(that).asInstanceOf[ConstraintSet]
    }
  }

  override def ++(elems: GenTraversableOnce[Expression]): ConstraintSet = {
    val newSet = new ConstraintSet(baseSet.clone(), originals.clone(),
      this.attribRefBasedEquivalenceList.map(_.clone),
      this.expressionBasedEquivalenceList.map(_.clone))
    elems.foreach(ele => {
      val conditionedElement = convertToCanonicalizedIfRequired(ele)
      if (!this.contains(conditionedElement)) {
        newSet.add(conditionedElement)
      }
    })
    newSet
  }

  override def convertToCanonicalizedIfRequired(ele: Expression): Expression = {
    val suspectAttribs = ele.references.toSet --
      this.attribRefBasedEquivalenceList.map(buff => buff.head.asInstanceOf[Attribute])
    if (suspectAttribs.isEmpty) {
      ele
    } else {
      val mappings = suspectAttribs.map(attrib =>
        (this.attribRefBasedEquivalenceList ++ this.expressionBasedEquivalenceList).find(
          buff => buff.exists(_.canonicalized == attrib.canonicalized)).
          map(buff => attrib -> Option(buff.head)).getOrElse(attrib -> None)).toMap
      ele.transformUp {
        case attr: Attribute if mappings.contains(attr) => mappings(attr).getOrElse(attr)
      }
    }
  }

  override def -(elem: Expression): ConstraintSet = {
    if (elem.deterministic) {
      val newBaseSet = baseSet.clone().filterNot(_ == elem.canonicalized)
      val newOriginals =
        originals.clone().filterNot(_.canonicalized == elem.canonicalized)
      new ConstraintSet(newBaseSet, newOriginals, this.attribRefBasedEquivalenceList.map(_.clone),
        this.expressionBasedEquivalenceList.map(_.clone))
    } else {
      new ConstraintSet(baseSet.clone(), originals.clone(),
        this.attribRefBasedEquivalenceList.map(_.clone),
        this.expressionBasedEquivalenceList.map(_.clone))
    }
  }

  /**
   * This function updates the existing non redundant, non trivial constraints stored
   * as per the basis of incoming attributes and outgoing attributes of the node.
   * If the attributes forming the constraints are not going to be part of the output set,
   * then attempt is made ,as much as possible, to see if the constraint can survive
   * by modifying it with 1st available alias for the attribute getting removed.
   * It also tracks the aliases of the attribute which are then used to generate
   * redundant constraints in the expand function & for pruning in the contains function
   *
   * @param outputAttribs The attributes which are part of the output set
   * @param inputAttribs The attributes which make up the incoming attributes
   * @param projectList The list of projections containing the NamedExpression
   * @param oldAliasedConstraintsCreator A partial function used for generating all
   *                                     combination of constraints as per old code.
   *                                     Used only when the un optimized constraint propagation
   *                                     is used. Used in ExpressionSet
   * @return The new valid ConstraintSet
   */
  override def updateConstraints(outputAttribs: Seq[Attribute],
    inputAttribs: Seq[Attribute], projectList: Seq[NamedExpression],
    oldAliasedConstraintsCreator: Option[Seq[NamedExpression] => Set[Expression]]):
  ConstraintSet = {
    val (aliasBased, _) = projectList.partition {
      case _: Alias => true
      case _ => false
    }
    val groupHeadToGroupMap: ExpressionMap[mutable.Buffer[Expression]] =
      new ExpressionMap[mutable.Buffer[Expression]]()

    this.attribRefBasedEquivalenceList.foreach(x =>
      groupHeadToGroupMap += (x.head -> x.clone())
    )
    this.expressionBasedEquivalenceList.foreach(x =>
      groupHeadToGroupMap += (x.head -> x.clone())
    )
    // clone the keys so that the set obtained is static & detached from
    // the groupHeadToGroupMap
    val existingAttribGroupHead = groupHeadToGroupMap.keySet.toSet

    // add the incoming attribs list
    aliasBased.foreach( ne => ne match {
      case al: Alias =>
        // find the group to which this alias's child belongs to
        // if the child is an attribute
        val alChild = al.child
        val key = this.attribRefBasedEquivalenceList
          .find(_.exists(_.canonicalized == alChild.canonicalized)).map(_.head)
          .getOrElse(this.expressionBasedEquivalenceList.find(buff =>
            buff.exists(_.canonicalized == alChild.canonicalized)).map(_.head).getOrElse(alChild))

        groupHeadToGroupMap.get(key) match {
          case Some(seq) => seq += al.toAttribute
          case None =>
            val temp: mutable.Buffer[Expression] = mutable.ArrayBuffer(al.child, al.toAttribute)
            groupHeadToGroupMap += al.child -> temp
        }
      case _ =>  // not expected
    })


    // Find those incoming attributes which are not projecting out
    val attribsRemoved = inputAttribs.filterNot(attr =>
        outputAttribs.exists(_.canonicalized == attr.canonicalized)).toSet
    // for each of the attribute getting removed , find replacement if any
    val replaceableAttributeMap: ExpressionMap[Attribute] = new ExpressionMap[Attribute]()
    fillReplacementOrClearGroupHeadForRemovedAttributes(attribsRemoved, replaceableAttributeMap,
      groupHeadToGroupMap)
    val (attribBasedEquivalenceList, initialExprBasedEquivalenceList) = {
      val tup = groupHeadToGroupMap.values.partition(buff =>
        buff.head match {
          case _: Attribute => true
          case _ => false
        }
      )
      (tup._1.to[mutable.Buffer], tup._2.to[mutable.Buffer])
    }

    // now work on expression (other than attribute based)
    val replaceableExpressionMap: ExpressionMap[Attribute] = new ExpressionMap[Attribute]()
    val exprBasedEquivalenceList = getUpdatedExpressionEquivalenceListWithSideEffects(
        attribsRemoved, replaceableAttributeMap, replaceableExpressionMap,
        attribBasedEquivalenceList, initialExprBasedEquivalenceList)

    // Now update or remove the filters depending upon which
    // can survive based on replacement available
    val updatedFilterExprs = getUpdatedConstraints(attribsRemoved, replaceableAttributeMap,
      replaceableExpressionMap)

    exprBasedEquivalenceList.foreach(buffer => {
      val expr = buffer.head
      if (!existingAttribGroupHead.exists(_.canonicalized == expr.canonicalized)) {
        val newConstraints = expr match {
          case _: Literal => buffer.drop(1).map(x => EqualNullSafe(x, expr))
          case _ => Seq(EqualNullSafe(expr, buffer(1)))
        }
        newConstraints.foreach(newConstraint =>
        if (!updatedFilterExprs.exists(_.canonicalized == newConstraint.canonicalized)) {
          updatedFilterExprs += newConstraint
        })
      }
    })

    // remove all mapping sof constants as the expression
     val newExprBasedEquivalenceList = exprBasedEquivalenceList.filter(_.head.references.nonEmpty)

    // Now filter the attribBasedEquivalenceList which only has 1 element
    // This is because if there is only 1 element in the buffer, it cannot
    // be of any help in making a constraint survive, in case that attribute
    // is not part of output set, so no point in keeping it in the attribute
    // equivalence list.
    val newAttribBasedEquivalenceList = attribBasedEquivalenceList.filter(_.size > 1)

    // generate 1 new nullsafe equality constraint based on equivalence list
    newAttribBasedEquivalenceList.foreach(buffer => {
      assert(buffer.size > 1)
      if (!existingAttribGroupHead.exists(_.canonicalized == buffer.head)) {
        val newConstraint = EqualNullSafe(buffer.head, buffer(1))
        if (!updatedFilterExprs.exists(_.canonicalized == newConstraint.canonicalized)) {
          updatedFilterExprs += newConstraint
        }
      }
    })
    val canonicalized = updatedFilterExprs.map(_.canonicalized).to[mutable.Set]

    assert(canonicalized.size == updatedFilterExprs.size)
    new ConstraintSet(canonicalized, updatedFilterExprs, newAttribBasedEquivalenceList,
      newExprBasedEquivalenceList)
  }

  private def getUpdatedExpressionEquivalenceListWithSideEffects(attribsRemoved: Set[Attribute],
    replaceableAttributeMap: ExpressionMap[Attribute],
    replaceableExpressionMap: ExpressionMap[Attribute],
    attribBasedEquivalenceList: mutable.Buffer[mutable.Buffer[Expression]],
    initialExprBasedEquivalenceList: mutable.Buffer[mutable.Buffer[Expression]]
  ): mutable.Buffer[mutable.Buffer[Expression]] = {
    initialExprBasedEquivalenceList.map(buff => {
        val zerothElem = buff.head
        val refs = zerothElem.references
        if (refs.nonEmpty) {
          if (refs.exists(ref => attribsRemoved.exists(_.canonicalized == ref.canonicalized))) {
            val newZeroth = zerothElem.transformUp {
              case attr: Attribute =>
                if (attribsRemoved.exists( _.canonicalized == attr.canonicalized)) {
                  replaceableAttributeMap.get(attr) match {
                    case Some(x) => x
                    case None => attr
                  }
                } else attr
            }
            if (newZeroth.references.exists(ref =>
              attribsRemoved.exists(_.canonicalized == ref.canonicalized))) {
              val removedExpression = buff.remove(0)
              if (buff.nonEmpty) {
                replaceableExpressionMap += (removedExpression -> buff.head
                  .asInstanceOf[Attribute])
              }
              // If the buffer size after removal is > 1
              // transfer the remaining attributes in the buffer to attrib equivalent list
              // If the buffer size == 1, then it will be removed in the final filtration
              // as the buffer size == 1 implies that the 0th position expression cannot
              // survive up the chain, if any of the attribute it is referencing is lost,
              // as there is no alias to support it
              if (buff.size > 1) {
                assert(buff.forall {
                  case _: Attribute => true
                  case _ => false
                })
                attribBasedEquivalenceList += buff
                mutable.Buffer.empty[Expression]
              } else {
                buff
              }
            } else {
              buff(0) = newZeroth
              buff
            }
          } else {
            buff
          }
        } else {
          attribsRemoved.foreach(ConstraintSet.removeCanonicalizedAttribute(buff, _))
          buff
        }
      }).filter(_.size > 1)
    // The above filtering ensures that if the buffer size after removal is 1,
    // then purge the buffer
  }

  private def getUpdatedConstraints(attribsRemoved: Set[Attribute],
    replaceableAttributeMap: ExpressionMap[Attribute],
    replaceableExpressionMap: ExpressionMap[Attribute]): mutable.Buffer[Expression] = {
    this.originals.flatMap(filterExpr => {
      val attribRefs = filterExpr.references
      if (attribRefs.isEmpty) {
        Set.empty[Expression]
      } else {
        if (attribRefs.exists(ref => attribsRemoved.exists(_.canonicalized == ref.canonicalized))) {
          val newFilterExp = filterExpr.transformUp {
            case attr: Attribute =>
              if (attribsRemoved.exists(_.canonicalized == attr.canonicalized)) {
                replaceableAttributeMap.get(attr) match {
                  case Some(x) => x
                  case None => attr
                }
              } else attr
          }
          // if filter still contains attribs which will be removed,
          // below code checks if filter can survive by replacement with a complex expression
          if (newFilterExp.references.exists(ref =>
            attribsRemoved.exists(_.canonicalized == ref.canonicalized))) {
            val newNewFilterExp = newFilterExp.transformUp {
              case expr: Expression =>
                replaceableExpressionMap.get(expr) match {
                  case Some(x) => x
                  case None => expr
                }
            }
            if (newNewFilterExp.references.exists(ref =>
                attribsRemoved.exists(_.canonicalized == ref.canonicalized))) {
              Set.empty[Expression]
            } else {
              Set(newNewFilterExp).filterNot(x =>
                this.originals.exists(_.canonicalized == x.canonicalized))
            }
          } else {
            Set(newFilterExp).filterNot(x =>
              this.originals.exists(_.canonicalized == x.canonicalized))
          }
        } else {
          Set(filterExpr)
        }
      }
    })
  }

  private def fillReplacementOrClearGroupHeadForRemovedAttributes(attribsRemoved: Set[Attribute],
    replaceableAttributeMap: ExpressionMap[Attribute],
    groupHeadToGroupMap: ExpressionMap[mutable.Buffer[Expression]]): Unit = {
    attribsRemoved.foreach(attrib => {
      groupHeadToGroupMap.get(attrib) match {
        case Some(buff) =>
          val exprRemoved = buff.remove(0)
          assert(attrib.exprId == exprRemoved.asInstanceOf[Attribute].exprId)
          // remove any attributes which may be in position other than 0 in the buffer
          attribsRemoved.foreach(x => ConstraintSet.removeCanonicalizedAttribute(buff, x))
          if (buff.nonEmpty) {
            replaceableAttributeMap += attrib -> buff.head.asInstanceOf[Attribute]
          } else {
            // purge the buffer if there is no replacement and the attribute being removed
            // was the only one present
            groupHeadToGroupMap.remove(exprRemoved)
          }
        case None => // there may be attributes which are removed but lying in
          // position other than 0th
          // which may be such that zeroth attrib is not present in the list of attrib being
          // removed & hence escaped in above op. so we need to again filter the map
          // at this point it is guaranteed that once the filtering has happened , there will
          // be no buffer which can be empty
          groupHeadToGroupMap.values.foreach(buff => {
            ConstraintSet.removeCanonicalizedAttribute(buff, attrib)
            assert(buff.nonEmpty)
          })
      }
    })
  }

  override def withNewConstraints(filters: Set[Expression]): ConstraintSet = {
    new ConstraintSet(filters.map(_.canonicalized).to[mutable.Set], filters.to[mutable.Buffer],
      this.attribRefBasedEquivalenceList.map(_.clone()),
      this.expressionBasedEquivalenceList.map(_.clone))
  }

  override def attributesRewrite(mapping: AttributeMap[Attribute]): ConstraintSet = {
    val transformer: PartialFunction[Expression, Expression] = {
      case a: Attribute => mapping(a)
    }
    val newOriginals = this.originals.map(x => x.transformUp(transformer))
    val newAttribBasedEquiList = this.attribRefBasedEquivalenceList.map(buff =>
      buff.map(x => x.transformUp(transformer)))
    val newExpBasedEquiList = this.expressionBasedEquivalenceList.map(buff =>
      buff.map(x => x.transformUp(transformer)))
    new ConstraintSet(newOriginals.map(_.canonicalized).to[mutable.Set], newOriginals,
      newAttribBasedEquiList, newExpBasedEquiList)
  }

  /**
   * The constraintset contains the bare minimum non redundant conditions.
   * The expand method creates other redundant combinations of constraint
   * conditions so that filters for push down through join etc can be created
   * This function guarantees that there will be at least one condition created from a
   * given non redundant constraint in the constraintset, such that each of the alias
   * of the base attribute found in the non redundant constraint, included.
   * for eg, lets say the non redundant constraint contained is
   * a + b + c > 10.
   * and the aliases are:
   * for a -> a1, a2
   * for b -> b1
   * for c -> c1, c2, c3
   * Then the expand function will return following constraints
   * a + b + c > 10, a1 + b + c > 10, a2 + b + c > 10, a + b1 + c > 10,
   * a + b + c1 > 10, a + b + c2 > 10, a + b + c3 > 10
   * P.S: due to a trivial bug( or behaviour in the logic of code below)
   * it is possible that in case of a simple  equalNullsafe condition of the form
   * a = a1  which is present in the constraintset as a non redundant condition
   * and if aliases are a -> a1, a2
   * the expanded form returned will be  a <=> a1 && a2 <=> a1. ( instead of ideal condition
   * a <=> a2). But this does not effect the overall logic of code as far as the behaviour
   * of expand function is concerned. as it is returning a condition inovlving each of the
   * attribute. Will try to make it behave ideally later.
   *
   * @return A set containing all the non redundant conditions in the constraintset +
   *         all other rdundant form of constraints which gurantee that each alias for
   *         the base attribute in the non redundant constraint, is represented at least once.
   */
  override def expand: Set[Expression] = {
    def createConstraintsFromAttribEquivalence(): Seq[Expression] = {
      // find all attribs ref in all base expressions

      val baseAttribs = this.originals.flatMap(_.references)
      // collect all the list of substitutables for these base attribs
      val substitutables = baseAttribs
        .map(x => x -> this.attribRefBasedEquivalenceList.filter(_.head == x)).filter(_._2.nonEmpty)
        .toMap
      this.originals.flatMap(expr => {
        val attribRefs = expr.references
        val commonRefs = substitutables.keySet.intersect(attribRefs.toSet)
        if (commonRefs.nonEmpty) {
          commonRefs.flatMap(attr => {
            substitutables(attr).flatMap(subst => subst.drop(1).map(substWith => {
                  val newConstraint = expr.transformUp {
                    case att: Attribute if att.canonicalized == attr.canonicalized &&
                        !attribRefs.contains(substWith.asInstanceOf[Attribute]) =>
                      substWith.asInstanceOf[Attribute]
                  }
                  newConstraint match {
                    case x: EqualNullSafe => if (this.originals.exists(x => x.canonicalized ==
                      newConstraint.canonicalized)) {
                        null
                      } else {
                        x
                      }
                    case _ => newConstraint
                  }
                }).filterNot(_ == null)
            )
          })
        } else {
          Set.empty[Expression]
        }
      })
    }

    def createConstraintFromExprEquivalence(): Seq[Expression] = {
      // find all attribs ref in all base expressions
      val baseAttribsSet = this.originals.map(_.references)
      // filter out the equivalence expressions of interest
      val exprsOfInterest = this.expressionBasedEquivalenceList.filter(buff =>
        baseAttribsSet.exists(y => buff.head.references.subsetOf(y))
      )
      this.originals.zip(baseAttribsSet).flatMap {
        case (cond, attribSet) =>
          exprsOfInterest.flatMap(buffer => {
            if (buffer.head.references.subsetOf(attribSet) &&
                cond.find(_.canonicalized == buffer.head.canonicalized).isDefined) {
              buffer.drop(1).filterNot(e => attribSet.contains(e.asInstanceOf[NamedExpression])).
                map(x => cond.transformUp {
                    case y: Expression
                      if y.canonicalized == buffer.head.canonicalized => x
                  }).filterNot(e => this.originals.exists(_.canonicalized == e.canonicalized))
            } else {
              Seq.empty[Expression]
            }
          })
      }
    }
    val newOriginals = createConstraintsFromAttribEquivalence() ++
      createConstraintFromExprEquivalence() ++ this.originals
    ExpressionSet(newOriginals)
  }

  /**
   * This function is used during pruning and also when any new condition is being
   * added to the constraintset. The idea is that existing conditions in the constraintset
   * are the bare minimum essential (non redundant) constraints. So any filter to be checked
   * if it can be pruned or not can be checked using this function, if that filter is
   * derivable using the constraints available. If it is derivable it means the filter is
   * redundant and can be pruned. Also if any new constraint is being added to the
   * constraintset that also can be checked if it is redundant or not. If redundant,
   * it will not get added. This method converts the incoming expression into its
   * constituents attributes before being checked.
   * For. eg if the incoming expression is say z + c > 10, where z is an alias of base
   * attributes a + b. And say constraintset already contains a condition
   * a + b + c > 10. Then z + c > 10, is converted into a + b + c > 10 making use
   * of tacking data of aliases, and it will be found in the constraintset &
   * contains will return as true
   * @param elem Expression to be checked if it is redundant or not.
   * @return boolean true if it already exists in constraintset( is redundant)
   */
  override def contains(elem: Expression): Boolean = {
    if (super.contains(elem)) {
      true
    } else {
      // check canonicalized
      // find all attribs ref in all base expressions
      val baseAttribs = elem.references
      // collect all the list of canonicalized attributes for these base attribs
      val substitutables = baseAttribs
        .map(x => {
          val seqContainingAttrib = this.attribRefBasedEquivalenceList
            .filter(buff => buff.exists(_.canonicalized == x.canonicalized))
          assert(seqContainingAttrib.isEmpty || seqContainingAttrib.size == 1)
          if (seqContainingAttrib.nonEmpty) {
            x -> seqContainingAttrib.head.head
          } else {
            x -> null
          }
        })
        .filter(_._2 ne null)
        .toMap
      val canonicalizedExp = elem.transformUp {
        case att: Attribute => substitutables.getOrElse(att, att)
      }

      if (super.contains(canonicalizedExp)) {
        true
      } else {
        val newCanonicalized = canonicalizedExp.transformUp {
          case expr =>
            this.expressionBasedEquivalenceList
              .find(buff => buff.exists(_.canonicalized == expr.canonicalized))
              .map(_.head)
              .getOrElse(expr)
        }
        super.contains(newCanonicalized)
      }
    }
  }

  /**
   * This gives all the constraints whose references are subset of canonicalized
   * attributes of interests and the number of references are > 1.
   * Eventually this function should be augmented to return all the
   * conditions for join push down & not just of compound types. This will
   * eliminate the need of expand function which is still extravagant
   * @param attribsOfInterest A sequence of attributes for which constraints are desired &
   *                          constraints should be such that its references are subset of
   *                          the canonicalized version of attributes in the passed sequence
   * @return Sequence of constraint expressions of compound types.
   */
  override def getConstraintsSubsetOfAttributes(attribsOfInterest: Seq[Attribute]): Seq[Expression]
  = {
    val canonicalAttribsMapping = attribsOfInterest.map(att => this.attribRefBasedEquivalenceList.
      find(buff => buff.exists(_.canonicalized == att.canonicalized)).map(buff =>
      buff.head.asInstanceOf[Attribute] -> att).getOrElse(att -> att)).toMap
    this.originals.collect {
        case expr if expr.references.toSet.subsetOf(canonicalAttribsMapping.keySet) &
            expr.references.size > 1 && expr.deterministic => expr
      }.map(expr => expr.transformUp {
          case attr: Attribute => canonicalAttribsMapping(attr)
        })
  }

  /**
   * Consider a new filter generated out of constraints of the form
   * IsNotNull(case....a....b...c) where the case expressions are
   * complex. Since new filters generated out of constraints are always canonicalized
   * it is possible that they are not compact as they are written in terms of
   * basic attributes. If an alias to this complex expression is present, then it
   * makes sense to rewrite the newly generated filter as IsNotNull(alias.attribute)
   * to avoid expensive calculation, especially that we have large case optimization.
   * This function simply tries to compact the expression where possible by replacing
   * the expression with an alias's attribute.
   *
   * @param expr Expression to compact ( decanonicalize)
   * @return Expression which is compacted, if possible
   */
  override def rewriteUsingAlias(expr: Expression): Expression =
    expr.transformDown {
      case x: Attribute => x
      case x => getDecanonicalizedAttributeForExpression(x)
    }

  /**
   * Decanonicalizes the NullIntolerant Expression.
   * The need for this arises, because when spark is attempting to generate
   * new NotNull constraints from the existing constraint, it may not return
   * any NotNull constraints, if the underlying subexpression is not of type
   * NullIntolerant.
   * Consider following two cases:
   * Lets say the base  canonicalized constraint is of the form a + b > 5.
   * A GreaterThan expression is NullIntolerant, so spark delves deep
   * and finds, it is composed of a & b attributes, & thus returns two
   * new IsNotNull constraints, namely IsNotNull(a) and IsNotNull(b).
   * But if the base canonicalized constraint is of the form
   * case(a....., b...) > 5, in this situation because case expression
   * does not implement NullIntolerant, spark does not go deep & hence
   * returns 0 not null constraints.
   * This function handles this situation, by replacing an underlying
   * canonicalized complex expression with an alias's attribute so
   * that NotNull constraint can be generated.
   * Thus case (a.....b) > 5 will be temporarily converted into z > 5.
   * Once an IsNotNull(z) is returned as a new constraint, we store
   * IsNotNull(z) in the Constraint set, again as canonicalized constraint,
   * that is IsNotNull(case...a...b), which will ensure that pruning logic
   * works fine.
   *
   * @return Set of constraint expressions where underlying NullIntolerant
   *         Expressions have been decanonicalized.
   */
  override def getConstraintsWithDecanonicalizedNullIntolerant: Set[Expression] = {
    def decanonicalizeNotNullIntolerant(expr: Expression): Expression = {
      var foundNotNullIntolerant = false
      expr.transformUp {
        case x: LeafExpression => x
        case x: NullIntolerant => if (foundNotNullIntolerant) {
          val y = getDecanonicalizedAttributeForExpression(x)
          if (y ne x) {
            foundNotNullIntolerant = false
          }
          y
        } else {
          x
        }
        case x => val y = getDecanonicalizedAttributeForExpression(x)
          if (y ne x) {
            foundNotNullIntolerant = false
          } else {
            foundNotNullIntolerant = true
          }
          y
      }
    }

    this.originals.map(expr => expr match {
      case _: NullIntolerant => decanonicalizeNotNullIntolerant(expr)
      case _ => expr
    }).toSet
  }

  private def getDecanonicalizedAttributeForExpression(expr: Expression): Expression = {
    val canonicalizedExpr = convertToCanonicalizedIfRequired(expr)
    val bufferIndex = this.expressionBasedEquivalenceList.indexWhere(_.head == canonicalizedExpr)
    if (bufferIndex != -1) {
      // buffer size will always be > 1
      this.expressionBasedEquivalenceList(bufferIndex).last
    } else {
      expr
    }
  }
}

object ConstraintSet {
  def removeCanonicalizedAttribute(
    buff: mutable.Buffer[Expression],
    attr: Attribute
  ): Unit = {
    var keepGoing = true
    while (keepGoing) {
      val indx = buff.indexWhere(_.canonicalized == attr.canonicalized)
      if (indx == -1) {
        keepGoing = false
      } else {
        buff.remove(indx)
      }
    }
  }
}
