// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include "Private/Malterlib_SQL_Database_Private.hpp"

namespace NMib::NSQL
{
	inline CSqlRowDataPointer CSqlRowMapping::f_CreateRow() const
	{
		return m_fCreateRow();
	}

	constexpr ICSqlPreparedSelectStatement::ICSqlPreparedSelectStatement(CSqlQueryID _QueryID)
		: m_QueryID(_QueryID)
	{
	}

	constexpr ICSqlPreparedInsertStatement::ICSqlPreparedInsertStatement(CSqlQueryID _QueryID)
		: m_QueryID(_QueryID)
	{
	}

	constexpr ICSqlPreparedUpdateStatement::ICSqlPreparedUpdateStatement(CSqlQueryID _QueryID)
		: m_QueryID(_QueryID)
	{
	}

	constexpr ICSqlPreparedDeleteStatement::ICSqlPreparedDeleteStatement(CSqlQueryID _QueryID)
		: m_QueryID(_QueryID)
	{
	}

	constexpr ICSqlPreparedUpsertStatement::ICSqlPreparedUpsertStatement(CSqlQueryID _QueryID)
		: m_QueryID(_QueryID)
	{
	}

	constexpr ESqlValueType CSqlParameterTypesDescription::f_GetType(umint _iParameter) const
	{
		return m_pTypes[_iParameter];
	}

	template <ESqlValueType ...tp_Types>
	constexpr CSqlQueryID TCSqlParameterTypes<tp_Types...>::fs_QueryID()
	{
		CSqlQueryID QueryID{14695981039346656037ull};
		((QueryID = NPrivate::fg_SqlMixQueryID(QueryID, tp_Types)), ...);

		return QueryID;
	}

	template <ESqlValueType ...tp_Types>
	constexpr CSqlParameterTypesDescription TCSqlParameterTypes<tp_Types...>::fs_Describe()
	{
		return
			{
				.m_QueryID = fs_QueryID()
				, .m_pTypes = mc_nTypes ? mc_Types : nullptr
				, .m_nTypes = mc_nTypes
			}
		;
	}

	template <auto tf_pMember>
	consteval auto fg_SqlParamEq()
	{
		return TCSqlParameterPredicate<tf_pMember, ESqlPredicateType::mc_EqualParameter>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlParamNe()
	{
		return TCSqlParameterPredicate<tf_pMember, ESqlPredicateType::mc_NotEqualParameter>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlParamLt()
	{
		return TCSqlParameterPredicate<tf_pMember, ESqlPredicateType::mc_LessParameter>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlParamLe()
	{
		return TCSqlParameterPredicate<tf_pMember, ESqlPredicateType::mc_LessEqualParameter>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlParamGt()
	{
		return TCSqlParameterPredicate<tf_pMember, ESqlPredicateType::mc_GreaterParameter>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlParamGe()
	{
		return TCSqlParameterPredicate<tf_pMember, ESqlPredicateType::mc_GreaterEqualParameter>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlParamLike()
	{
		return TCSqlParameterPredicate<tf_pMember, ESqlPredicateType::mc_LikeParameter>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlIsNull()
	{
		return TCSqlNullPredicate<tf_pMember, ESqlPredicateType::mc_IsNull>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlIsNotNull()
	{
		return TCSqlNullPredicate<tf_pMember, ESqlPredicateType::mc_IsNotNull>{};
	}

	template <auto tf_pMember, umint tf_nParameters>
	consteval auto fg_SqlParamIn()
		requires (tf_nParameters > 0)
	{
		return TCSqlInPredicate<tf_pMember, tf_nParameters>{};
	}

	template <auto tf_pMember, auto &tf_Subquery>
	consteval auto fg_SqlInSubquery()
	{
		return TCSqlInSubqueryPredicate<tf_pMember, tf_Subquery>{};
	}

	template <auto &tf_Subquery>
	consteval auto fg_SqlExists()
	{
		return TCSqlExistsPredicate<tf_Subquery, ESqlPredicateType::mc_Exists>{};
	}

	template <auto &tf_Subquery>
	consteval auto fg_SqlNotExists()
	{
		return TCSqlExistsPredicate<tf_Subquery, ESqlPredicateType::mc_NotExists>{};
	}

	template <typename tf_CLeft, typename tf_CRight>
	consteval auto fg_SqlAnd(tf_CLeft _Left, tf_CRight _Right)
	{
		return TCSqlBinaryPredicate<tf_CLeft, tf_CRight, ESqlPredicateType::mc_And>{.m_Left = _Left, .m_Right = _Right};
	}

	template <typename tf_CLeft, typename tf_CRight>
	consteval auto fg_SqlOr(tf_CLeft _Left, tf_CRight _Right)
	{
		return TCSqlBinaryPredicate<tf_CLeft, tf_CRight, ESqlPredicateType::mc_Or>{.m_Left = _Left, .m_Right = _Right};
	}

	template <typename tf_CPredicate>
	consteval auto fg_SqlNot(tf_CPredicate _Predicate)
	{
		return TCSqlNotPredicate<tf_CPredicate>{.m_Predicate = _Predicate};
	}

	consteval auto fg_SqlCount()
	{
		return TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Count>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlColumn()
	{
		return TCSqlColumnExpression<tf_pMember>{};
	}

	template <auto tf_pResultMember, typename tf_CExpression>
	consteval auto fg_SqlAlias(tf_CExpression _Expression)
	{
		return TCSqlAliasedExpression<tf_pResultMember, tf_CExpression>{.m_Expression = _Expression};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlAdd()
	{
		return TCSqlBinaryColumnExpression<ESqlSelectExpressionType::mc_Add, tf_pLeftMember, tf_pRightMember>{};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlSubtract()
	{
		return TCSqlBinaryColumnExpression<ESqlSelectExpressionType::mc_Subtract, tf_pLeftMember, tf_pRightMember>{};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlMultiply()
	{
		return TCSqlBinaryColumnExpression<ESqlSelectExpressionType::mc_Multiply, tf_pLeftMember, tf_pRightMember>{};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlDivide()
	{
		return TCSqlBinaryColumnExpression<ESqlSelectExpressionType::mc_Divide, tf_pLeftMember, tf_pRightMember>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlLower()
	{
		return TCSqlUnaryColumnExpression<ESqlSelectExpressionType::mc_Lower, tf_pMember>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlUpper()
	{
		return TCSqlUnaryColumnExpression<ESqlSelectExpressionType::mc_Upper, tf_pMember>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlLength()
	{
		return TCSqlUnaryColumnExpression<ESqlSelectExpressionType::mc_Length, tf_pMember>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlCastFloat()
	{
		return TCSqlUnaryColumnExpression<ESqlSelectExpressionType::mc_CastFloat, tf_pMember>{};
	}

	template <auto &tf_FunctionName, auto tf_pMember>
	consteval auto fg_SqlBackendFunction()
	{
		return TCSqlBackendFunctionExpression<tf_FunctionName, tf_pMember>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlSum()
	{
		return TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Sum, tf_pMember>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlAvg()
	{
		return TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Avg, tf_pMember>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlMin()
	{
		return TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Min, tf_pMember>{};
	}

	template <auto tf_pMember>
	consteval auto fg_SqlMax()
	{
		return TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Max, tf_pMember>{};
	}

	template <typename tf_CExpression>
	consteval auto fg_SqlHavingGt(tf_CExpression _Expression)
	{
		return TCSqlHavingAggregatePredicate<tf_CExpression, ESqlPredicateType::mc_GreaterParameter>{.m_Expression = _Expression};
	}

	template <typename tf_CExpression>
	consteval auto fg_SqlHavingGe(tf_CExpression _Expression)
	{
		return TCSqlHavingAggregatePredicate<tf_CExpression, ESqlPredicateType::mc_GreaterEqualParameter>{.m_Expression = _Expression};
	}

	template <typename tf_CExpression>
	consteval auto fg_SqlHavingLt(tf_CExpression _Expression)
	{
		return TCSqlHavingAggregatePredicate<tf_CExpression, ESqlPredicateType::mc_LessParameter>{.m_Expression = _Expression};
	}

	template <typename tf_CExpression>
	consteval auto fg_SqlHavingLe(tf_CExpression _Expression)
	{
		return TCSqlHavingAggregatePredicate<tf_CExpression, ESqlPredicateType::mc_LessEqualParameter>{.m_Expression = _Expression};
	}

	template <typename tf_CExpression>
	consteval auto fg_SqlHavingEq(tf_CExpression _Expression)
	{
		return TCSqlHavingAggregatePredicate<tf_CExpression, ESqlPredicateType::mc_EqualParameter>{.m_Expression = _Expression};
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	constexpr TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::TCSqlPreparedSelect
		(
			CTable const &_Table
			, CPredicate _Predicate
			, COrderBy _OrderBy
			, CLimitOffset _LimitOffset
			, CDistinct _Distinct
			, CSelection _Selection
			, CGroupBy _GroupBy
			, CHaving _Having
		)
		: ICSqlPreparedSelectStatement(NPrivate::fg_SqlPreparedSelectQueryID(_Table, _Predicate, _OrderBy, _LimitOffset, _Distinct, _Selection, _GroupBy, _Having))
		, m_Table(_Table)
		, m_Predicate(_Predicate)
		, m_OrderBy(_OrderBy)
		, m_LimitOffset(_LimitOffset)
		, m_Distinct(_Distinct)
		, m_Selection(_Selection)
		, m_GroupBy(_GroupBy)
		, m_Having(_Having)
	{
	}

	template <typename t_CTable>
	template <typename tf_CPredicate>
	consteval auto TCSqlPreparedSelectBuilder<t_CTable>::f_Where(tf_CPredicate _Predicate) const
	{
		return TCSqlPreparedSelect<CTable, tf_CPredicate>(m_Table, _Predicate);
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	template <auto tf_pMember>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_OrderByAscending() const
	{
		return TCSqlPreparedSelect
			<
				CTable
				, CPredicate
				, typename NPrivate::TCSqlAppendOrderBy<COrderBy, TCSqlOrderByTerm<tf_pMember, false>>::CType
				, CLimitOffset
				, CDistinct
				, CSelection
				, CGroupBy
				, CHaving
			>
			(m_Table, m_Predicate, {}, m_LimitOffset, m_Distinct, m_Selection, m_GroupBy, m_Having)
		;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	template <auto tf_pMember>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_OrderByDescending() const
	{
		return TCSqlPreparedSelect
			<
				CTable
				, CPredicate
				, typename NPrivate::TCSqlAppendOrderBy<COrderBy, TCSqlOrderByTerm<tf_pMember, true>>::CType
				, CLimitOffset
				, CDistinct
				, CSelection
				, CGroupBy
				, CHaving
			>
			(m_Table, m_Predicate, {}, m_LimitOffset, m_Distinct, m_Selection, m_GroupBy, m_Having)
		;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_WithLimit() const
	{
		return TCSqlPreparedSelect<CTable, CPredicate, COrderBy, TCSqlLimitOffset<true, CLimitOffset::mc_bHasOffset>, CDistinct, CSelection, CGroupBy, CHaving>
			(m_Table, m_Predicate, m_OrderBy, {}, m_Distinct, m_Selection, m_GroupBy, m_Having)
		;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_WithOffset() const
	{
		return TCSqlPreparedSelect<CTable, CPredicate, COrderBy, TCSqlLimitOffset<CLimitOffset::mc_bHasLimit, true>, CDistinct, CSelection, CGroupBy, CHaving>
			(m_Table, m_Predicate, m_OrderBy, {}, m_Distinct, m_Selection, m_GroupBy, m_Having)
		;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_Distinct() const
	{
		return TCSqlPreparedSelect<CTable, CPredicate, COrderBy, CLimitOffset, CSqlDistinct, CSelection, CGroupBy, CHaving>
			(m_Table, m_Predicate, m_OrderBy, m_LimitOffset, {}, m_Selection, m_GroupBy, m_Having)
		;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	template <auto ...tfp_pMembers>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_Select() const
	{
		return TCSqlPreparedSelect<CTable, CPredicate, COrderBy, CLimitOffset, CDistinct, TCSqlSelectedColumns<tfp_pMembers...>, CGroupBy, CHaving>
			(m_Table, m_Predicate, m_OrderBy, m_LimitOffset, m_Distinct, {}, m_GroupBy, m_Having)
		;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	template <typename ...tfp_CExpressions>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_Select(tfp_CExpressions ...) const
		requires (gc_SqlExpressionsConsistentAliasing<tfp_CExpressions...>)
	{
		return TCSqlPreparedSelect<CTable, CPredicate, COrderBy, CLimitOffset, CDistinct, TCSqlSelectedExpressions<tfp_CExpressions...>, CGroupBy, CHaving>
			(m_Table, m_Predicate, m_OrderBy, m_LimitOffset, m_Distinct, {}, m_GroupBy, m_Having)
		;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	template <auto ...tfp_pMembers>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_GroupBy() const
	{
		return TCSqlPreparedSelect<CTable, CPredicate, COrderBy, CLimitOffset, CDistinct, CSelection, TCSqlGroupBy<tfp_pMembers...>, CHaving>
			(m_Table, m_Predicate, m_OrderBy, m_LimitOffset, m_Distinct, m_Selection, {}, m_Having)
		;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	template <typename tf_CHaving>
	consteval auto TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_Having(tf_CHaving _Having) const
	{
		return TCSqlPreparedSelect<CTable, CPredicate, COrderBy, CLimitOffset, CDistinct, CSelection, CGroupBy, tf_CHaving>
			(m_Table, m_Predicate, m_OrderBy, m_LimitOffset, m_Distinct, m_Selection, m_GroupBy, _Having)
		;
	}

	template <typename tf_CTable>
	consteval auto fg_SqlPreparedSelect(tf_CTable const &_Table)
	{
		return TCSqlPreparedSelectBuilder<NTraits::TCRemoveReferenceAndQualifiers<tf_CTable>>{.m_Table = _Table};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnEq()
	{
		return TCSqlJoinOnEqual<tf_pLeftMember, tf_pRightMember>{};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnNe()
	{
		return TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_NotEqualParameter>{};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnLt()
	{
		return TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_LessParameter>{};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnLe()
	{
		return TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_LessEqualParameter>{};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnGt()
	{
		return TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_GreaterParameter>{};
	}

	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnGe()
	{
		return TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_GreaterEqualParameter>{};
	}

	template <typename ...tfp_CPredicates>
	consteval auto fg_SqlJoinOnAll(tfp_CPredicates ...p_Predicates)
	{
		((void)p_Predicates, ...);

		return TCSqlJoinOnAll<tfp_CPredicates...>{};
	}

	template <typename t_CLeftTable, typename t_CRightTable, typename t_CJoinOn, ESqlJoinType t_JoinType>
	constexpr TCSqlPreparedJoinSelect<t_CLeftTable, t_CRightTable, t_CJoinOn, t_JoinType>::TCSqlPreparedJoinSelect
		(
			CLeftTable const &_LeftTable
			, CRightTable const &_RightTable
			, CJoinOn _JoinOn
		)
		: ICSqlPreparedSelectStatement(NPrivate::fg_SqlPreparedJoinSelectQueryID<t_JoinType>(_LeftTable, _RightTable, _JoinOn))
		, m_LeftTable(_LeftTable)
		, m_RightTable(_RightTable)
		, m_JoinOn(_JoinOn)
	{
	}

	template <typename t_CLeftTable, typename t_CRightTable, typename t_CJoinOn, ESqlJoinType t_JoinType>
	template <typename tf_CNextTable, typename tf_CJoinOn>
	consteval auto TCSqlPreparedJoinSelect<t_CLeftTable, t_CRightTable, t_CJoinOn, t_JoinType>::f_InnerJoin(tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn) const
	{
		return TCSqlPreparedJoinNSelect
			<
				TCSqlJoinedTables<CLeftTable, CRightTable, NTraits::TCRemoveReferenceAndQualifiers<tf_CNextTable>>
				, TCSqlJoinTerms<TCSqlJoinTerm<CRightTable, CJoinOn, t_JoinType>, TCSqlJoinTerm<NTraits::TCRemoveReferenceAndQualifiers<tf_CNextTable>, tf_CJoinOn, ESqlJoinType::mc_Inner>>
			>
			(m_LeftTable, m_RightTable, _NextTable, m_JoinOn, _JoinOn)
		;
	}

	template <typename t_CLeftTable, typename t_CRightTable, typename t_CJoinOn, ESqlJoinType t_JoinType>
	template <typename tf_CNextTable, typename tf_CJoinOn>
	consteval auto TCSqlPreparedJoinSelect<t_CLeftTable, t_CRightTable, t_CJoinOn, t_JoinType>::f_LeftJoin(tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn) const
	{
		static_assert
			(
				NPrivate::fg_SqlTableMembersAllNullable<NTraits::TCRemoveReferenceAndQualifiers<tf_CNextTable>>()
				, "The right side of a LEFT JOIN must have only nullable (TCOptional) columns: an unmatched left row yields NULL for every right-side column"
			)
		;
		return TCSqlPreparedJoinNSelect
			<
				TCSqlJoinedTables<CLeftTable, CRightTable, NTraits::TCRemoveReferenceAndQualifiers<tf_CNextTable>>
				, TCSqlJoinTerms<TCSqlJoinTerm<CRightTable, CJoinOn, t_JoinType>, TCSqlJoinTerm<NTraits::TCRemoveReferenceAndQualifiers<tf_CNextTable>, tf_CJoinOn, ESqlJoinType::mc_Left>>
			>
			(m_LeftTable, m_RightTable, _NextTable, m_JoinOn, _JoinOn)
		;
	}

	template <typename ...tp_CTables, typename ...tp_CTerms>
	constexpr TCSqlPreparedJoinNSelect<TCSqlJoinedTables<tp_CTables...>, TCSqlJoinTerms<tp_CTerms...>>::TCSqlPreparedJoinNSelect
		(
			tp_CTables const &...p_Tables
			, typename tp_CTerms::CJoinOn ...p_JoinOns
		)
		: ICSqlPreparedSelectStatement(NPrivate::fg_SqlPreparedJoinNSelectQueryID(TCSqlJoinTerms<tp_CTerms...>{}, p_Tables...))
		, m_Tables(p_Tables...)
		, m_JoinOns(p_JoinOns...)
	{
	}

	template <typename ...tp_CTables, typename ...tp_CTerms>
	template <typename tf_CNextTable, typename tf_CJoinOn>
	consteval auto TCSqlPreparedJoinNSelect<TCSqlJoinedTables<tp_CTables...>, TCSqlJoinTerms<tp_CTerms...>>::f_InnerJoin(tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn) const
	{
		return NPrivate::fg_SqlAppendPreparedJoin<ESqlJoinType::mc_Inner>(*this, _NextTable, _JoinOn);
	}

	template <typename ...tp_CTables, typename ...tp_CTerms>
	template <typename tf_CNextTable, typename tf_CJoinOn>
	consteval auto TCSqlPreparedJoinNSelect<TCSqlJoinedTables<tp_CTables...>, TCSqlJoinTerms<tp_CTerms...>>::f_LeftJoin(tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn) const
	{
		static_assert
			(
				NPrivate::fg_SqlTableMembersAllNullable<NTraits::TCRemoveReferenceAndQualifiers<tf_CNextTable>>()
				, "The right side of a LEFT JOIN must have only nullable (TCOptional) columns: an unmatched left row yields NULL for every right-side column"
			)
		;
		return NPrivate::fg_SqlAppendPreparedJoin<ESqlJoinType::mc_Left>(*this, _NextTable, _JoinOn);
	}

	template
	<
		typename t_CLeftTable
		, typename t_CMiddleTable
		, typename t_CRightTable
		, typename t_CFirstJoinOn
		, typename t_CSecondJoinOn
		, ESqlJoinType t_FirstJoinType
		, ESqlJoinType t_SecondJoinType
	>
	constexpr TCSqlPreparedJoin3Select<t_CLeftTable, t_CMiddleTable, t_CRightTable, t_CFirstJoinOn, t_CSecondJoinOn, t_FirstJoinType, t_SecondJoinType>::TCSqlPreparedJoin3Select
		(
			CLeftTable const &_LeftTable
			, CMiddleTable const &_MiddleTable
			, CRightTable const &_RightTable
			, CFirstJoinOn _FirstJoinOn
			, CSecondJoinOn _SecondJoinOn
		)
		: ICSqlPreparedSelectStatement(NPrivate::fg_SqlPreparedJoin3SelectQueryID<t_FirstJoinType, t_SecondJoinType>(_LeftTable, _MiddleTable, _RightTable, _FirstJoinOn, _SecondJoinOn))
		, m_LeftTable(_LeftTable)
		, m_MiddleTable(_MiddleTable)
		, m_RightTable(_RightTable)
		, m_FirstJoinOn(_FirstJoinOn)
		, m_SecondJoinOn(_SecondJoinOn)
	{
	}

	template <typename tf_CLeftTable, typename tf_CRightTable, typename tf_CJoinOn>
	consteval auto fg_SqlPreparedInnerJoin(tf_CLeftTable const &_LeftTable, tf_CRightTable const &_RightTable, tf_CJoinOn _JoinOn)
	{
		return TCSqlPreparedJoinSelect<NTraits::TCRemoveReferenceAndQualifiers<tf_CLeftTable>, NTraits::TCRemoveReferenceAndQualifiers<tf_CRightTable>, tf_CJoinOn, ESqlJoinType::mc_Inner>
			(_LeftTable, _RightTable, _JoinOn)
		;
	}

	template <typename tf_CLeftTable, typename tf_CRightTable, typename tf_CJoinOn>
	consteval auto fg_SqlPreparedLeftJoin(tf_CLeftTable const &_LeftTable, tf_CRightTable const &_RightTable, tf_CJoinOn _JoinOn)
	{
		static_assert
			(
				NPrivate::fg_SqlTableMembersAllNullable<NTraits::TCRemoveReferenceAndQualifiers<tf_CRightTable>>()
				, "The right side of a LEFT JOIN must have only nullable (TCOptional) columns: an unmatched left row yields NULL for every right-side column"
			)
		;
		return TCSqlPreparedJoinSelect<NTraits::TCRemoveReferenceAndQualifiers<tf_CLeftTable>, NTraits::TCRemoveReferenceAndQualifiers<tf_CRightTable>, tf_CJoinOn, ESqlJoinType::mc_Left>
			(_LeftTable, _RightTable, _JoinOn)
		;
	}

	template <auto &tf_LeftSelect, auto &tf_RightSelect, ESqlSetOperationType tf_Type>
	constexpr TCSqlPreparedSetSelect<tf_LeftSelect, tf_RightSelect, tf_Type>::TCSqlPreparedSetSelect()
		: ICSqlPreparedSelectStatement(NPrivate::fg_SqlPreparedSetSelectQueryID<tf_LeftSelect, tf_RightSelect, tf_Type>())
	{
	}

	template <auto &tf_LeftSelect, auto &tf_RightSelect, ESqlSetOperationType tf_Type>
	CSqlPreparedSelectStatementDescription TCSqlPreparedSetSelect<tf_LeftSelect, tf_RightSelect, tf_Type>::f_Describe() const
	{
		CSqlPreparedSelectStatementDescription Description = tf_LeftSelect.f_Describe();
		Description.m_QueryID = m_QueryID;
		Description.m_SetOperations.f_InsertLast({.m_Type = tf_Type, .m_pStatement = &tf_RightSelect});

		return Description;
	}

	template <auto &tf_LeftSelect, auto &tf_RightSelect>
	consteval auto fg_SqlUnion()
	{
		return TCSqlPreparedSetSelect<tf_LeftSelect, tf_RightSelect, ESqlSetOperationType::mc_Union>{};
	}

	template <auto &tf_LeftSelect, auto &tf_RightSelect>
	consteval auto fg_SqlUnionAll()
	{
		return TCSqlPreparedSetSelect<tf_LeftSelect, tf_RightSelect, ESqlSetOperationType::mc_UnionAll>{};
	}

	template <auto &tf_LeftSelect, auto &tf_RightSelect>
	consteval auto fg_SqlIntersect()
	{
		return TCSqlPreparedSetSelect<tf_LeftSelect, tf_RightSelect, ESqlSetOperationType::mc_Intersect>{};
	}

	template <auto &tf_LeftSelect, auto &tf_RightSelect>
	consteval auto fg_SqlExcept()
	{
		return TCSqlPreparedSetSelect<tf_LeftSelect, tf_RightSelect, ESqlSetOperationType::mc_Except>{};
	}

	template <typename t_CTable, auto ...tp_pMembers>
	constexpr TCSqlPreparedInsert<t_CTable, tp_pMembers...>::TCSqlPreparedInsert(CTable const &_Table)
		: ICSqlPreparedInsertStatement(NPrivate::fg_SqlPreparedInsertQueryID<tp_pMembers...>(_Table))
		, m_Table(_Table)
	{
	}

	template <typename t_CTable, auto ...tp_pMembers>
	template <auto ...tfp_pMembers>
	consteval auto TCSqlPreparedInsert<t_CTable, tp_pMembers...>::f_Columns() const
	{
		return TCSqlPreparedInsert<CTable, tfp_pMembers...>(m_Table);
	}

	template <typename tf_CTable>
	consteval auto fg_SqlPreparedInsert(tf_CTable const &_Table)
	{
		return TCSqlPreparedInsert<NTraits::TCRemoveReferenceAndQualifiers<tf_CTable>>(_Table);
	}

	template <typename t_CTable, typename t_CPredicate, typename t_CSet>
	constexpr TCSqlPreparedUpdate<t_CTable, t_CPredicate, t_CSet>::TCSqlPreparedUpdate(CTable const &_Table, CPredicate _Predicate, CSet _Set)
		: ICSqlPreparedUpdateStatement(NPrivate::fg_SqlPreparedUpdateQueryID(_Table, _Predicate, _Set))
		, m_Table(_Table)
		, m_Predicate(_Predicate)
		, m_Set(_Set)
	{
	}

	template <typename t_CTable, typename t_CPredicate>
	template <auto ...tfp_pMembers>
	consteval auto TCSqlPreparedUpdateSetBuilder<t_CTable, t_CPredicate>::f_Set() const
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlPreparedUpdate<CTable, CPredicate, TCSqlSelectedColumns<tfp_pMembers...>>(m_Table, m_Predicate, {});
	}

	template <typename t_CTable>
	template <typename tf_CPredicate>
	consteval auto TCSqlPreparedUpdateBuilder<t_CTable>::f_Where(tf_CPredicate _Predicate) const
	{
		return TCSqlPreparedUpdateSetBuilder<CTable, tf_CPredicate>{.m_Table = m_Table, .m_Predicate = _Predicate};
	}

	template <typename t_CTable>
	consteval auto TCSqlPreparedUpdateBuilder<t_CTable>::f_AllRows() const
	{
		return TCSqlPreparedUpdateSetBuilder<CTable, CSqlAllRowsPredicate>{.m_Table = m_Table, .m_Predicate = {}};
	}

	template <typename tf_CTable>
	consteval auto fg_SqlPreparedUpdate(tf_CTable const &_Table)
	{
		return TCSqlPreparedUpdateBuilder<NTraits::TCRemoveReferenceAndQualifiers<tf_CTable>>{.m_Table = _Table};
	}

	template <typename t_CTable, typename t_CPredicate>
	constexpr TCSqlPreparedDelete<t_CTable, t_CPredicate>::TCSqlPreparedDelete(CTable const &_Table, CPredicate _Predicate)
		: ICSqlPreparedDeleteStatement(NPrivate::fg_SqlPreparedDeleteQueryID(_Table, _Predicate))
		, m_Table(_Table)
		, m_Predicate(_Predicate)
	{
	}

	template <typename t_CTable>
	template <typename tf_CPredicate>
	consteval auto TCSqlPreparedDeleteBuilder<t_CTable>::f_Where(tf_CPredicate _Predicate) const
	{
		return TCSqlPreparedDelete<CTable, tf_CPredicate>(m_Table, _Predicate);
	}

	template <typename t_CTable>
	consteval auto TCSqlPreparedDeleteBuilder<t_CTable>::f_AllRows() const
	{
		return TCSqlPreparedDelete<CTable, CSqlAllRowsPredicate>(m_Table, {});
	}

	template <typename tf_CTable>
	consteval auto fg_SqlPreparedDelete(tf_CTable const &_Table)
	{
		return TCSqlPreparedDeleteBuilder<NTraits::TCRemoveReferenceAndQualifiers<tf_CTable>>{.m_Table = _Table};
	}

	template <typename t_CTable, typename t_CConflict, typename t_CUpdate>
	constexpr TCSqlPreparedUpsert<t_CTable, t_CConflict, t_CUpdate>::TCSqlPreparedUpsert(CTable const &_Table, CConflict _Conflict, CUpdate _Update)
		: ICSqlPreparedUpsertStatement(NPrivate::fg_SqlPreparedUpsertQueryID(_Table, _Conflict, _Update))
		, m_Table(_Table)
		, m_Conflict(_Conflict)
		, m_Update(_Update)
	{
	}

	template <typename t_CTable, typename t_CConflict>
	template <auto ...tfp_pMembers>
	consteval auto TCSqlPreparedUpsertUpdateBuilder<t_CTable, t_CConflict>::f_Update() const
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlPreparedUpsert<CTable, CConflict, TCSqlSelectedColumns<tfp_pMembers...>>(m_Table, m_Conflict, {});
	}

	template <typename t_CTable>
	template <auto ...tfp_pMembers>
	consteval auto TCSqlPreparedUpsertBuilder<t_CTable>::f_OnConflict() const
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlPreparedUpsertUpdateBuilder<CTable, TCSqlSelectedColumns<tfp_pMembers...>>{.m_Table = m_Table, .m_Conflict = {}};
	}

	template <typename tf_CTable>
	consteval auto fg_SqlPreparedUpsert(tf_CTable const &_Table)
	{
		return TCSqlPreparedUpsertBuilder<NTraits::TCRemoveReferenceAndQualifiers<tf_CTable>>{.m_Table = _Table};
	}

	template <typename t_CTable, typename t_CPredicate, typename t_COrderBy, typename t_CLimitOffset, typename t_CDistinct, typename t_CSelection, typename t_CGroupBy, typename t_CHaving>
	CSqlPreparedSelectStatementDescription TCSqlPreparedSelect<t_CTable, t_CPredicate, t_COrderBy, t_CLimitOffset, t_CDistinct, t_CSelection, t_CGroupBy, t_CHaving>::f_Describe() const
	{
		CSqlPreparedSelectStatementDescription Description;
		Description.m_QueryID = m_QueryID;
		Description.m_TableName = m_Table.f_Name();
		Description.m_bDistinct = CDistinct::mc_bDistinct;
		Description.m_Predicate = NPrivate::fg_SqlPredicateDescription(m_Table, m_Predicate);
		NPrivate::fg_SqlAppendGroupBy(Description, m_Table, m_GroupBy);
		// Offset HAVING placeholders by the described WHERE parameter count, not the static type constant, so a
		// parameterized EXISTS/IN subquery in WHERE (which the type constant reports as zero parameters) does not
		// make the HAVING placeholders reuse the subquery's PostgreSQL $n indexes.
		NPrivate::fg_SqlApplyHaving(Description, m_Table, m_Having, Description.m_Predicate.m_nParameters);
		NPrivate::fg_SqlAppendOrderBy(Description, m_Table, m_OrderBy);
		NPrivate::fg_SqlApplyLimitOffset<CLimitOffset>(Description);
		NPrivate::fg_SqlApplySelectColumns(Description, m_Table, m_Selection);

		return Description;
	}

	template <typename t_CLeftTable, typename t_CRightTable, typename t_CJoinOn, ESqlJoinType t_JoinType>
	CSqlPreparedSelectStatementDescription TCSqlPreparedJoinSelect<t_CLeftTable, t_CRightTable, t_CJoinOn, t_JoinType>::f_Describe() const
	{
		CSqlPreparedSelectStatementDescription Description;
		Description.m_QueryID = m_QueryID;
		Description.m_TableName = m_LeftTable.f_Name();
		Description.m_Predicate = NPrivate::fg_SqlPredicateDescription(m_LeftTable, CSqlAllRowsPredicate{});
		NPrivate::fg_SqlApplyJoinedRowColumns<CRow>(Description, m_LeftTable, m_RightTable);
		NPrivate::fg_SqlApplyJoinOn(Description, m_LeftTable, m_RightTable, m_JoinOn, t_JoinType);

		return Description;
	}

	template
	<
		typename t_CLeftTable
		, typename t_CMiddleTable
		, typename t_CRightTable
		, typename t_CFirstJoinOn
		, typename t_CSecondJoinOn
		, ESqlJoinType t_FirstJoinType
		, ESqlJoinType t_SecondJoinType
	>
	auto TCSqlPreparedJoin3Select<t_CLeftTable, t_CMiddleTable, t_CRightTable, t_CFirstJoinOn, t_CSecondJoinOn, t_FirstJoinType, t_SecondJoinType>::f_Describe() const
		-> CSqlPreparedSelectStatementDescription
	{
		CSqlPreparedSelectStatementDescription Description;
		Description.m_QueryID = m_QueryID;
		Description.m_TableName = m_LeftTable.f_Name();
		Description.m_Predicate = NPrivate::fg_SqlPredicateDescription(m_LeftTable, CSqlAllRowsPredicate{});
		NPrivate::fg_SqlApplyJoinedRowColumns<CRow>(Description, m_LeftTable, m_MiddleTable, m_RightTable);
		NPrivate::fg_SqlApplyJoinOn(Description, m_LeftTable, m_MiddleTable, m_FirstJoinOn, t_FirstJoinType);
		NPrivate::fg_SqlApplyJoinOn(Description, m_LeftTable, m_MiddleTable, m_RightTable, m_SecondJoinOn, t_SecondJoinType);

		return Description;
	}

	template <typename ...tp_CTables, typename ...tp_CTerms>
	CSqlPreparedSelectStatementDescription TCSqlPreparedJoinNSelect<TCSqlJoinedTables<tp_CTables...>, TCSqlJoinTerms<tp_CTerms...>>::f_Describe() const
	{
		CSqlPreparedSelectStatementDescription Description;
		Description.m_QueryID = m_QueryID;
		Description.m_TableName = fg_Get<0>(m_Tables).f_Name();
		Description.m_Predicate = NPrivate::fg_SqlPredicateDescription(fg_Get<0>(m_Tables), CSqlAllRowsPredicate{});
		NPrivate::fg_SqlApplyJoinedRowColumns<CRow>(Description, m_Tables);
		NPrivate::fg_SqlApplyJoinTerms<CTerms>(Description, m_Tables, m_JoinOns);

		return Description;
	}

	template <typename t_CTable, auto ...tp_pMembers>
	CSqlPreparedInsertStatementDescription TCSqlPreparedInsert<t_CTable, tp_pMembers...>::f_Describe() const
	{
		CSqlPreparedInsertStatementDescription Description;
		Description.m_QueryID = m_QueryID;
		Description.m_TableName = m_Table.f_Name();

		if constexpr (sizeof...(tp_pMembers) != 0)
		{
			// Explicit f_Columns<...>(): record the INSERT columns and parameter types in the requested member order,
			// not the table declaration order. The value appender (TCSqlPreparedInsertValueAppender) binds the
			// arguments in this same pack order and parameters bind positionally, so a reordered or subset column list
			// must describe the columns in the order the values are supplied - otherwise each value lands in the wrong
			// column (and PostgreSQL would prepare with mismatched parameter types).
			(
				[&]
				{
					using CMember = typename NPrivate::TCSqlMemberPointerTraits<tp_pMembers>::CMember;
					Description.m_InsertColumns.f_InsertLast(NPrivate::fg_SqlColumnNameForMember(m_Table, tp_pMembers));
					Description.m_InsertColumnTypes.f_InsertLast(NPrivate::TCSqlValueType<CMember>::mc_Type);
				}
				()
				, ...
			);
		}
		else
		{
			// No explicit column list: insert every implicitly selected column (excludes generated columns and the
			// autoincrement primary key) in table declaration order, matching the implicit value appender.
			m_Table.f_ForEachColumn
				(
					[&](auto const &...p_Columns)
					{
						(
							[&]
							{
								if (!NPrivate::fg_SqlPreparedInsertColumnIsSelected<decltype(p_Columns)>(p_Columns))
									return;

								Description.m_InsertColumns.f_InsertLast(p_Columns.f_Name());
								Description.m_InsertColumnTypes.f_InsertLast(NPrivate::TCSqlValueType<typename NTraits::TCDecay<decltype(p_Columns)>::CMember>::mc_Type);
							}
							()
							, ...
						);
					}
				)
			;
		}

		return Description;
	}

	template <typename t_CTable, typename t_CPredicate, typename t_CSet>
	CSqlPreparedUpdateStatementDescription TCSqlPreparedUpdate<t_CTable, t_CPredicate, t_CSet>::f_Describe() const
	{
		CSqlPreparedUpdateStatementDescription Description;
		Description.m_QueryID = m_QueryID;
		Description.m_TableName = m_Table.f_Name();
		Description.m_Predicate = NPrivate::fg_SqlPredicateDescription(m_Table, m_Predicate);
		NPrivate::fg_SqlApplyUpdateColumns(Description, m_Table, m_Set);

		return Description;
	}

	template <typename t_CTable, typename t_CPredicate>
	CSqlPreparedDeleteStatementDescription TCSqlPreparedDelete<t_CTable, t_CPredicate>::f_Describe() const
	{
		CSqlPreparedDeleteStatementDescription Description;
		Description.m_QueryID = m_QueryID;
		Description.m_TableName = m_Table.f_Name();
		Description.m_Predicate = NPrivate::fg_SqlPredicateDescription(m_Table, m_Predicate);

		return Description;
	}

	template <typename t_CTable, typename t_CConflict, typename t_CUpdate>
	CSqlPreparedUpsertStatementDescription TCSqlPreparedUpsert<t_CTable, t_CConflict, t_CUpdate>::f_Describe() const
	{
		CSqlPreparedUpsertStatementDescription Description;
		Description.m_QueryID = m_QueryID;
		Description.m_TableName = m_Table.f_Name();
		NPrivate::fg_SqlApplyImplicitInsertColumns(Description, m_Table);
		NPrivate::fg_SqlApplyUpsertConflictColumns(Description, m_Table, m_Conflict);
		NPrivate::fg_SqlApplyUpsertUpdateColumns(Description, m_Table, m_Update);

		return Description;
	}

	template <auto &tf_Repository>
	auto CSqlDatabaseClient::f_Repository()
		-> TCSqlRepositoryConnection<CSqlDatabaseClient, tf_Repository>
	{
		return TCSqlRepositoryConnection<CSqlDatabaseClient, tf_Repository>(this);
	}

	template <typename tf_CTable>
	NConcurrency::TCFuture<void> CSqlDatabaseClient::f_Insert(tf_CTable const &_Table, typename tf_CTable::CRow &&_Row)
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Insert, NPrivate::fg_SqlInsertOperation(_Table, fg_Move(_Row)));
	}

	template <typename tf_CTable, typename ...tfp_CValues>
	NConcurrency::TCFuture<void> CSqlDatabaseClient::f_Insert(tf_CTable const &_Table, tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlTableInsertValuesMatch<tf_CTable, tfp_CValues...>())
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Insert, NPrivate::fg_SqlInsertOperation(_Table, fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	NConcurrency::TCFuture<void> CSqlDatabaseClient::f_Insert(tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>())
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Insert, NPrivate::fg_SqlInsertOperation<tf_PreparedInsert>(fg_Forward<tfp_CValues>(p_Values)...));
	}

	namespace NPrivate
	{
		template <auto &tf_PreparedInsert>
		auto fg_SqlBulkInsertGenerator
			(
				NConcurrency::TCAsyncGenerator<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedInsert)>::CRow>> _RowBatches
			)
			-> NConcurrency::TCAsyncGenerator<CSqlBulkInsertRowBatch>
		{
			auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

			for (auto iBatch = co_await fg_Move(_RowBatches).f_GetPipelinedIterator(); iBatch; co_await ++iBatch)
			{
				CSqlBulkInsertRowBatch Batch;
				Batch.f_Reserve((*iBatch).f_GetLen());
				for (auto &Row : *iBatch)
				{
					CSqlInsertOperation Op;
					fg_SqlAppendPreparedInsertRow<tf_PreparedInsert>(Op, fg_Move(Row));
					Batch.f_InsertLast(fg_Move(Op.m_Values));
				}

				co_yield fg_Move(Batch);
			}

			co_return {};
		}
	}

	template <auto &tf_PreparedInsert>
	auto CSqlDatabaseClient::f_InsertMany
		(
			NConcurrency::TCAsyncGenerator<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedInsert)>::CRow>> _RowBatches
		)
		-> NConcurrency::TCFuture<umint>
	{
		return f_WithTransaction
			(
				[RowBatches = fg_Move(_RowBatches)](CSqlTransaction _Transaction) mutable -> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
					auto Transaction = fg_Move(_Transaction);

					co_return co_await Transaction.template f_InsertMany<tf_PreparedInsert>(fg_Move(RowBatches));
				}
			)
		;
	}

	template <auto tf_pMember, auto &tf_PreparedInsert, typename ...tfp_CValues>
	auto CSqlDatabaseClient::f_InsertReturning(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
		requires
		(
			NPrivate::fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>()
			&& NPrivate::fg_SqlPreparedInsertGeneratedPrimaryKeyColumnCount<tf_PreparedInsert>() == 1
			&& NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<tf_PreparedInsert.m_Table, tf_pMember>()
		)
	{
		return NPrivate::fg_SqlInsertReturning<tf_pMember, tf_PreparedInsert>
			(
				mp_Backend
				, NPrivate::fg_SqlInsertReturningOperation<tf_pMember, tf_PreparedInsert>(fg_Forward<tfp_CValues>(p_Values)...)
			)
		;
	}

	template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
	NConcurrency::TCFuture<umint> CSqlDatabaseClient::f_Upsert(tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Upsert, NPrivate::fg_SqlUpsertOperation<tf_PreparedUpsert>(fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto tf_pMember, auto &tf_PreparedUpsert, typename ...tfp_CValues>
	auto CSqlDatabaseClient::f_UpsertReturning(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
		requires (NPrivate::fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
	{
		return NPrivate::fg_SqlMutationReturning<tf_pMember, tf_PreparedUpsert>
			(
				mp_Backend
				, &ICSqlDatabaseBackendActor::f_UpsertReturning
				, NPrivate::fg_SqlUpsertReturningOperation<tf_pMember, tf_PreparedUpsert>(fg_Forward<tfp_CValues>(p_Values)...)
			)
		;
	}

	template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
	NConcurrency::TCFuture<umint> CSqlDatabaseClient::f_Update(tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Update, NPrivate::fg_SqlUpdateOperation<tf_PreparedUpdate>(fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto tf_pMember, auto &tf_PreparedUpdate, typename ...tfp_CValues>
	auto CSqlDatabaseClient::f_UpdateReturning(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
		requires (NPrivate::fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
	{
		return NPrivate::fg_SqlMutationReturning<tf_pMember, tf_PreparedUpdate>
			(
				mp_Backend
				, &ICSqlDatabaseBackendActor::f_UpdateReturning
				, NPrivate::fg_SqlUpdateReturningOperation<tf_pMember, tf_PreparedUpdate>(fg_Forward<tfp_CValues>(p_Values)...)
			)
		;
	}

	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	NConcurrency::TCFuture<umint> CSqlDatabaseClient::f_Delete(tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Delete, NPrivate::fg_SqlDeleteOperation<tf_PreparedDelete>(fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto tf_pMember, auto &tf_PreparedDelete, typename ...tfp_CValues>
	auto CSqlDatabaseClient::f_DeleteReturning(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
		requires (NPrivate::fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
	{
		return NPrivate::fg_SqlMutationReturning<tf_pMember, tf_PreparedDelete>
			(
				mp_Backend
				, &ICSqlDatabaseBackendActor::f_DeleteReturning
				, NPrivate::fg_SqlDeleteReturningOperation<tf_pMember, tf_PreparedDelete>(fg_Forward<tfp_CValues>(p_Values)...)
			)
		;
	}

	template <auto &tf_Table, auto tf_pMember, typename tf_CValue>
	auto CSqlDatabaseClient::f_GetByID(tf_CValue _Value)
		-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
		requires
		(
			NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
			&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
			&& NPrivate::fg_SqlPreparedSelectParameterMatches<NPrivate::gc_SqlSelectByID<tf_Table, tf_pMember>, tf_CValue>()
		)
	{
		return f_QueryOptional<NPrivate::gc_SqlSelectByID<tf_Table, tf_pMember>>(fg_Move(_Value));
	}

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	auto CSqlDatabaseClient::f_GetByCompositeID(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
		requires
		(
			NPrivate::fg_SqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>()
			&& NPrivate::fg_SqlCompositeIDSelectParametersMatch<tf_Table, tf_CCompositeID, tfp_CValues...>()
		)
	{
		return []<auto ...tfp_pMembers, typename ...tfp_CCallValues>
			(
				CSqlDatabaseClient *_pThis
				, TCSqlCompositeID<tfp_pMembers...> const *
				, tfp_CCallValues &&...p_CallValues
			)
			{
				return _pThis->f_QueryOptional<NPrivate::gc_SqlSelectByID<tf_Table, tfp_pMembers...>>(fg_Forward<tfp_CCallValues>(p_CallValues)...);
			}
			(this, static_cast<tf_CCompositeID const *>(nullptr), fg_Forward<tfp_CValues>(p_Values)...)
		;
	}

	template <auto &tf_Table, auto tf_pMember, auto ...tfp_pSetMembers, typename ...tfp_CValues>
	auto CSqlDatabaseClient::f_UpdateByID(tfp_CValues &&...p_Values) -> NConcurrency::TCFuture<umint>
		requires
		(
			sizeof...(tfp_pSetMembers) > 0
			&& NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
			&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
			&& NPrivate::fg_SqlPreparedUpdateValuesMatch<NPrivate::gc_SqlUpdateByID<tf_Table, tf_pMember, tfp_pSetMembers...>, tfp_CValues...>()
		)
	{
		return f_Update<NPrivate::gc_SqlUpdateByID<tf_Table, tf_pMember, tfp_pSetMembers...>>(fg_Forward<tfp_CValues>(p_Values)...);
	}

	template <auto &tf_Table, auto tf_pMember, typename tf_CValue>
	auto CSqlDatabaseClient::f_DeleteByID(tf_CValue &&_Value) -> NConcurrency::TCFuture<umint>
		requires
		(
			NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
			&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
			&& NPrivate::fg_SqlPreparedDeleteValuesMatch<NPrivate::gc_SqlDeleteByID<tf_Table, tf_pMember>, tf_CValue>()
		)
	{
		return f_Delete<NPrivate::gc_SqlDeleteByID<tf_Table, tf_pMember>>(fg_Forward<tf_CValue>(_Value));
	}

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	auto CSqlDatabaseClient::f_DeleteByCompositeID(tfp_CValues &&...p_Values) -> NConcurrency::TCFuture<umint>
		requires
		(
			NPrivate::fg_SqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>()
			&& NPrivate::fg_SqlCompositeIDDeleteValuesMatch<tf_Table, tf_CCompositeID, tfp_CValues...>()
		)
	{
		return []<auto ...tfp_pMembers, typename ...tfp_CCallValues>
			(
				CSqlDatabaseClient *_pThis
				, TCSqlCompositeID<tfp_pMembers...> const *
				, tfp_CCallValues &&...p_CallValues
			)
			{
				return _pThis->f_Delete<NPrivate::gc_SqlDeleteByID<tf_Table, tfp_pMembers...>>(fg_Forward<tfp_CCallValues>(p_CallValues)...);
			}
			(this, static_cast<tf_CCompositeID const *>(nullptr), fg_Forward<tfp_CValues>(p_Values)...)
		;
	}

	template <auto &tf_Table, auto tf_pIDMember, auto tf_pVersionMember, auto ...tfp_pSetMembers>
	auto CSqlDatabaseClient::f_Save(typename NTraits::TCDecay<decltype(tf_Table)>::CRow _Row)
		-> NConcurrency::TCFuture<TCSqlSaveResult<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
		requires
		(
			sizeof...(tfp_pSetMembers) > 0
			&& NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
			&& NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<tf_Table, tf_pIDMember>()
			&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pIDMember>()
		)
	{
		return NPrivate::fg_SqlSave<tf_Table, tf_pIDMember, tf_pVersionMember, tfp_pSetMembers...>(mp_Backend, fg_Move(_Row));
	}

	template <auto &tf_PreparedSelect, typename tf_CParam>
	auto CSqlDatabaseClient::f_Query(tf_CParam _Param, CSqlSelectSettings _Settings)
		requires (NPrivate::fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	{
		return fp_Query<tf_PreparedSelect>(fg_Move(_Param), _Settings);
	}

	template <auto &tf_PreparedSelect, typename tf_CParam0, typename tf_CParam1, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_Query(tf_CParam0 _Param0, tf_CParam1 _Param1, tfp_CParams ...p_Params)
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam0, tf_CParam1, tfp_CParams...>())
	{
		return fp_Query<tf_PreparedSelect>(CSqlSelectSettings{}, fg_Move(_Param0), fg_Move(_Param1), fg_Move(p_Params)...);
	}

	template <auto &tf_PreparedSelect, typename tf_CParam0, typename tf_CParam1, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_Query(CSqlSelectSettings _Settings, tf_CParam0 _Param0, tf_CParam1 _Param1, tfp_CParams ...p_Params)
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam0, tf_CParam1, tfp_CParams...>())
	{
		return fp_Query<tf_PreparedSelect>(_Settings, fg_Move(_Param0), fg_Move(_Param1), fg_Move(p_Params)...);
	}

	template <auto &tf_PreparedSelect>
	auto CSqlDatabaseClient::f_Query(CSqlSelectSettings _Settings)
		requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedSelect>())
	{
		return fp_Query<tf_PreparedSelect>(_Settings);
	}

	template <auto &tf_PreparedSelect, typename tf_CParam>
	auto CSqlDatabaseClient::fp_Query(tf_CParam _Param, CSqlSelectSettings _Settings)
		requires (NPrivate::fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	{
		return fsp_SelectRows<ICSqlDatabaseBackendActor, typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			(
				mp_Backend
				, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>(fg_Move(_Param), _Settings)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlDatabaseClient::fp_Query(CSqlSelectSettings _Settings, tfp_CParams ...p_Params)
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		return fsp_SelectRows<ICSqlDatabaseBackendActor, typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			(
				mp_Backend
				, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>(_Settings, fg_Move(p_Params)...)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect>
	auto CSqlDatabaseClient::fp_Query(CSqlSelectSettings _Settings)
		requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedSelect>())
	{
		return fsp_SelectRows<ICSqlDatabaseBackendActor, typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			(
				mp_Backend
				, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>(_Settings)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_QueryStream(tfp_CParams ...p_Params)
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		return f_Query<tf_PreparedSelect>(fg_Move(p_Params)...);
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_QueryVector(tfp_CParams ...p_Params)
		-> NConcurrency::TCFuture<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		return fsp_QueryVector<CRow>(f_Query<tf_PreparedSelect>(fg_Move(p_Params)...), m_nPipelineLength);
	}

	template <auto &tf_PreparedJoin, typename tf_CResult>
	auto CSqlDatabaseClient::f_QueryJoinedVectorAs() -> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
		requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedJoin>())
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedJoin)>::CRow;

		return fsp_QueryJoinedVectorAs<tf_CResult>
			(
				fsp_SelectRows<ICSqlDatabaseBackendActor, CRow>
					(
						mp_Backend
						, NPrivate::fg_SqlSelectOperation<tf_PreparedJoin>(CSqlSelectSettings{})
						, m_nPipelineLength
					)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_QueryVectorAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CProjectionRow = NPrivate::TCSqlProjectionBackendRow<tf_CResult, tfp_pMembers...>;

		return fsp_QueryVectorAs<tf_CResult, tfp_pMembers...>
			(
				fsp_SelectRows<ICSqlDatabaseBackendActor, CProjectionRow>
				(
					mp_Backend
					, NPrivate::fg_SqlProjectionSelectOperation<tf_PreparedSelect, tf_CResult, tfp_pMembers...>(CSqlSelectSettings{}, fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_QueryOptional(tfp_CParams ...p_Params)
		-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		return fsp_QueryOptional<CRow>
			(
				fsp_SelectRows<ICSqlDatabaseBackendActor, CRow>
				(
					mp_Backend
					, NPrivate::fg_SqlTwoRowSelectOperation<tf_PreparedSelect>(fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_QueryOptionalAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<NStorage::TCOptional<tf_CResult>>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CProjectionRow = NPrivate::TCSqlProjectionBackendRow<tf_CResult, tfp_pMembers...>;

		return fsp_QueryOptionalAs<tf_CResult, tfp_pMembers...>
			(
				fsp_SelectRows<ICSqlDatabaseBackendActor, CProjectionRow>
				(
					mp_Backend
					, NPrivate::fg_SqlTwoRowProjectionSelectOperation<tf_PreparedSelect, tf_CResult, tfp_pMembers...>(fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_QueryOne(tfp_CParams ...p_Params)
		-> NConcurrency::TCFuture<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		return fsp_QueryOne<CRow>
			(
				fsp_SelectRows<ICSqlDatabaseBackendActor, CRow>
				(
					mp_Backend
					, NPrivate::fg_SqlTwoRowSelectOperation<tf_PreparedSelect>(fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_QueryOneAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<tf_CResult>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CProjectionRow = NPrivate::TCSqlProjectionBackendRow<tf_CResult, tfp_pMembers...>;

		return fsp_QueryOneAs<tf_CResult, tfp_pMembers...>
			(
				fsp_SelectRows<ICSqlDatabaseBackendActor, CProjectionRow>
				(
					mp_Backend
					, NPrivate::fg_SqlTwoRowProjectionSelectOperation<tf_PreparedSelect, tf_CResult, tfp_pMembers...>(fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_Count(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<umint>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Count, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>({}, fg_Move(p_Params)...));
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlDatabaseClient::f_Exists(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<bool>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Exists, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>({}, fg_Move(p_Params)...));
	}

	template <typename tf_CActor, typename tf_CRow>
	auto CSqlDatabaseClient::fsp_SelectRows(NConcurrency::TCActor<tf_CActor> _Backend, CSqlSelectOperation _Operation, uint32 _nPipelineLength)
		-> NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>>
	{
		auto BackendBatches = co_await _Backend(&tf_CActor::f_Select, fg_Move(_Operation));
		for (auto iBatch = co_await fg_Move(BackendBatches).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
		{
			NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>> Batch;
			for (CSqlRowDataPointer &pRow : *iBatch)
				Batch.f_InsertLast(NStorage::TCUniquePointer<TCRowData<tf_CRow>>(static_cast<TCRowData<tf_CRow> *>(pRow.f_Detach())));

			co_yield fg_Move(Batch);
		}

		co_return {};
	}

	template <typename tf_CRow>
	auto CSqlDatabaseClient::fsp_QueryVector(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<NContainer::TCVector<tf_CRow>>
	{
		NContainer::TCVector<tf_CRow> Rows;
		for (auto iBatch = co_await fg_Move(_Rows).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
		{
			for (auto &pRow : *iBatch)
				Rows.f_InsertLast(fg_Move(pRow->m_Data));
		}

		co_return Rows;
	}

	template <typename tf_CResult, auto ...tfp_pMembers, typename tf_CRow>
	auto CSqlDatabaseClient::fsp_QueryVectorAs(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
	{
		NContainer::TCVector<tf_CResult> Rows;
		for (auto iBatch = co_await fg_Move(_Rows).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
		{
			for (auto &pRow : *iBatch)
			{
				Rows.f_InsertLast
					(
						NPrivate::fg_SqlProjectionResultFromRow<tf_CResult, tfp_pMembers...>(fg_Move(pRow->m_Data))
					)
				;
			}
		}

		co_return Rows;
	}

	template <typename tf_CResult, typename tf_CRow>
	auto CSqlDatabaseClient::fsp_QueryJoinedVectorAs(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
	{
		NContainer::TCVector<tf_CResult> Rows;
		for (auto iBatch = co_await fg_Move(_Rows).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
		{
			for (auto &pRow : *iBatch)
			{
				// A joined row carries one table row per joined table (two, three, or more), so construct the result
				// from every element of the joined tuple rather than only the first two.
				[&]<umint ...tfp_iTables>(std::index_sequence<tfp_iTables...>)
				{
					Rows.f_InsertLast(tf_CResult{fg_Move(fg_Get<tfp_iTables>(pRow->m_Data))...});
				}
				(std::make_index_sequence<NStorage::gc_Tuple_Len<tf_CRow>>{});
			}
		}

		co_return Rows;
	}

	template <typename tf_CRow>
	auto CSqlDatabaseClient::fsp_QueryOptional(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<NStorage::TCOptional<tf_CRow>>
	{
		NStorage::TCOptional<tf_CRow> Row;
		for (auto iBatch = co_await fg_Move(_Rows).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
		{
			for (auto &pRow : *iBatch)
			{
				if (Row)
				{
					co_return DMibErrorSqlInstance
						(
							"SQL query expected at most one row but returned multiple rows"
							, fg_SqlErrorData(ESqlErrorCategory::mc_TooManyRows)
						)
					;
				}

				Row = fg_Move(pRow->m_Data);
			}
		}

		co_return Row;
	}

	template <typename tf_CResult, auto ...tfp_pMembers, typename tf_CRow>
	auto CSqlDatabaseClient::fsp_QueryOptionalAs(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<NStorage::TCOptional<tf_CResult>>
	{
		NStorage::TCOptional<tf_CResult> Row;
		for (auto iBatch = co_await fg_Move(_Rows).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
		{
			for (auto &pRow : *iBatch)
			{
				if (Row)
				{
					co_return DMibErrorSqlInstance
						(
							"SQL query expected at most one row but returned multiple rows"
							, fg_SqlErrorData(ESqlErrorCategory::mc_TooManyRows)
						)
					;
				}

				Row = NPrivate::fg_SqlProjectionResultFromRow<tf_CResult, tfp_pMembers...>(fg_Move(pRow->m_Data));
			}
		}

		co_return Row;
	}

	template <typename tf_CRow>
	auto CSqlDatabaseClient::fsp_QueryOne(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<tf_CRow>
	{
		NStorage::TCOptional<tf_CRow> Row = co_await fsp_QueryOptional<tf_CRow>(fg_Move(_Rows), _nPipelineLength);
		if (!Row)
		{
			co_return DMibErrorSqlInstance
				(
					"SQL query expected one row but returned no rows"
					, fg_SqlErrorData(ESqlErrorCategory::mc_MissingRow)
				)
			;
		}

		co_return fg_Move(*Row);
	}

	template <typename tf_CResult, auto ...tfp_pMembers, typename tf_CRow>
	auto CSqlDatabaseClient::fsp_QueryOneAs(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<tf_CResult>
	{
		NStorage::TCOptional<tf_CResult> Row = co_await fsp_QueryOptionalAs<tf_CResult, tfp_pMembers...>(fg_Move(_Rows), _nPipelineLength);
		if (!Row)
		{
			co_return DMibErrorSqlInstance
				(
					"SQL query expected one row but returned no rows"
					, fg_SqlErrorData(ESqlErrorCategory::mc_MissingRow)
				)
			;
		}

		co_return fg_Move(*Row);
	}

	template <typename tf_CRow>
	auto CSqlDatabaseClient::fsp_Count(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<umint>
	{
		umint nRows = 0;
		for (auto iBatch = co_await fg_Move(_Rows).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
			nRows += (*iBatch).f_GetLen();

		co_return nRows;
	}

	template <typename tf_CRow>
	auto CSqlDatabaseClient::fsp_Exists(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<bool>
	{
		for (auto iBatch = co_await fg_Move(_Rows).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
			if ((*iBatch).f_GetLen() > 0)
				co_return true;

		co_return false;
	}

	template <typename tf_CFunction>
	auto CSqlDatabaseClient::f_WithTransaction(tf_CFunction _fFunction)
		-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue>
	{
		return fsp_WithTransaction(mp_Backend, m_nPipelineLength, fg_Move(_fFunction));
	}

	template <typename tf_CFunction>
	auto CSqlDatabaseClient::f_WithReadTransaction(tf_CFunction _fFunction)
		-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue>
	{
		return fsp_WithReadTransaction(mp_Backend, m_nPipelineLength, fg_Move(_fFunction));
	}

	template <typename tf_CFunction>
	auto CSqlDatabaseClient::fsp_WithTransaction(NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend, uint32 _nPipelineLength, tf_CFunction _fFunction)
		-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue>
	{
		using CReturn = typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue;

		auto Transaction = co_await fsp_BeginTransaction(fg_Move(_Backend), {}, _nPipelineLength);
		auto CallbackTransaction = Transaction.fp_CopyHandleForScope();
		auto Result = co_await NConcurrency::fg_CallSafe(fg_Move(_fFunction), fg_Move(CallbackTransaction)).f_Wrap();
		if (!Result)
		{
			co_await Transaction.f_Rollback();
			co_return Result.f_GetException();
		}

		co_await Transaction.f_Commit();

		if constexpr (NTraits::cIsVoid<CReturn>)
			co_return {};
		else
			co_return Result.f_Move();
	}

	template <typename tf_CFunction>
	auto CSqlDatabaseClient::fsp_WithReadTransaction(NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend, uint32 _nPipelineLength, tf_CFunction _fFunction)
		-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue>
	{
		using CReturn = typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue;

		auto Transaction = co_await fsp_BeginReadTransaction(fg_Move(_Backend), {}, _nPipelineLength);
		auto CallbackTransaction = Transaction.fp_CopyHandleForScope();
		auto Result = co_await NConcurrency::fg_CallSafe(fg_Move(_fFunction), fg_Move(CallbackTransaction)).f_Wrap();
		if (!Result)
		{
			co_await Transaction.f_Rollback();
			co_return Result.f_GetException();
		}

		co_await Transaction.f_Commit();

		if constexpr (NTraits::cIsVoid<CReturn>)
			co_return {};
		else
			co_return Result.f_Move();
	}

	template <auto &tf_Repository>
	auto CSqlTransaction::f_Repository()
		-> TCSqlRepositoryConnection<CSqlTransaction, tf_Repository>
	{
		return TCSqlRepositoryConnection<CSqlTransaction, tf_Repository>(this);
	}

	template <typename tf_CFunction>
	auto CSqlTransaction::f_WithTransaction(tf_CFunction _fFunction)
		-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CSqlTransaction *>(nullptr))))::CValue>
	{
		return fsp_WithTransaction(this, fg_Move(_fFunction));
	}

	template <typename tf_CFunction>
	auto CSqlTransaction::fsp_WithTransaction(CSqlTransaction *_pTransaction, tf_CFunction _fFunction)
		-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CSqlTransaction *>(nullptr))))::CValue>
	{
		using CReturn = typename decltype(_fFunction(fg_Move(*static_cast<CSqlTransaction *>(nullptr))))::CValue;

		auto Savepoint = co_await _pTransaction->f_CreateSavepoint();
		auto CallbackTransaction = _pTransaction->fp_CopyHandleForScope();

		auto Result = co_await NConcurrency::fg_CallSafe(fg_Move(_fFunction), fg_Move(CallbackTransaction)).f_Wrap();
		if (!Result)
		{
			co_await _pTransaction->f_RollbackToSavepoint(Savepoint);
			co_await _pTransaction->f_ReleaseSavepoint(fg_Move(Savepoint));
			co_return Result.f_GetException();
		}

		co_await _pTransaction->f_ReleaseSavepoint(fg_Move(Savepoint));

		if constexpr (NTraits::cIsVoid<CReturn>)
			co_return {};
		else
			co_return Result.f_Move();
	}

	template <typename tf_CTable>
	NConcurrency::TCFuture<void> CSqlTransaction::f_Insert(tf_CTable const &_Table, typename tf_CTable::CRow &&_Row)
	{
		return mp_Transaction(&ICSqlTransactionActor::f_Insert, NPrivate::fg_SqlInsertOperation(_Table, fg_Move(_Row)));
	}

	template <typename tf_CTable, typename ...tfp_CValues>
	NConcurrency::TCFuture<void> CSqlTransaction::f_Insert(tf_CTable const &_Table, tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlTableInsertValuesMatch<tf_CTable, tfp_CValues...>())
	{
		return mp_Transaction(&ICSqlTransactionActor::f_Insert, NPrivate::fg_SqlInsertOperation(_Table, fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	NConcurrency::TCFuture<void> CSqlTransaction::f_Insert(tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>())
	{
		return mp_Transaction(&ICSqlTransactionActor::f_Insert, NPrivate::fg_SqlInsertOperation<tf_PreparedInsert>(fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto &tf_PreparedInsert>
	auto CSqlTransaction::f_InsertMany
		(
			NConcurrency::TCAsyncGenerator<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedInsert)>::CRow>> _RowBatches
		)
		-> NConcurrency::TCFuture<umint>
	{
		CSqlBulkInsertOperation Operation;
		Operation.m_pDescription = &NPrivate::gc_SqlInsertOperationDescription<tf_PreparedInsert>;
		Operation.m_RowBatches = NPrivate::fg_SqlBulkInsertGenerator<tf_PreparedInsert>(fg_Move(_RowBatches));

		return mp_Transaction(&ICSqlTransactionActor::f_InsertMany, fg_Move(Operation));
	}

	template <auto tf_pMember, auto &tf_PreparedInsert, typename ...tfp_CValues>
	auto CSqlTransaction::f_InsertReturning(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
		requires
		(
			NPrivate::fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>()
			&& NPrivate::fg_SqlPreparedInsertGeneratedPrimaryKeyColumnCount<tf_PreparedInsert>() == 1
			&& NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<tf_PreparedInsert.m_Table, tf_pMember>()
		)
	{
		return NPrivate::fg_SqlInsertReturning<tf_pMember, tf_PreparedInsert>
			(
				mp_Transaction
				, NPrivate::fg_SqlInsertReturningOperation<tf_pMember, tf_PreparedInsert>(fg_Forward<tfp_CValues>(p_Values)...)
			)
		;
	}

	template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
	NConcurrency::TCFuture<umint> CSqlTransaction::f_Upsert(tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
	{
		return mp_Transaction(&ICSqlTransactionActor::f_Upsert, NPrivate::fg_SqlUpsertOperation<tf_PreparedUpsert>(fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto tf_pMember, auto &tf_PreparedUpsert, typename ...tfp_CValues>
	auto CSqlTransaction::f_UpsertReturning(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
		requires (NPrivate::fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
	{
		return NPrivate::fg_SqlMutationReturning<tf_pMember, tf_PreparedUpsert>
			(
				mp_Transaction
				, &ICSqlTransactionActor::f_UpsertReturning
				, NPrivate::fg_SqlUpsertReturningOperation<tf_pMember, tf_PreparedUpsert>(fg_Forward<tfp_CValues>(p_Values)...)
			)
		;
	}

	template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
	NConcurrency::TCFuture<umint> CSqlTransaction::f_Update(tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
	{
		return mp_Transaction(&ICSqlTransactionActor::f_Update, NPrivate::fg_SqlUpdateOperation<tf_PreparedUpdate>(fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto tf_pMember, auto &tf_PreparedUpdate, typename ...tfp_CValues>
	auto CSqlTransaction::f_UpdateReturning(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
		requires (NPrivate::fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
	{
		return NPrivate::fg_SqlMutationReturning<tf_pMember, tf_PreparedUpdate>
			(
				mp_Transaction
				, &ICSqlTransactionActor::f_UpdateReturning
				, NPrivate::fg_SqlUpdateReturningOperation<tf_pMember, tf_PreparedUpdate>(fg_Forward<tfp_CValues>(p_Values)...)
			)
		;
	}

	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	NConcurrency::TCFuture<umint> CSqlTransaction::f_Delete(tfp_CValues &&...p_Values)
		requires (NPrivate::fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
	{
		return mp_Transaction(&ICSqlTransactionActor::f_Delete, NPrivate::fg_SqlDeleteOperation<tf_PreparedDelete>(fg_Forward<tfp_CValues>(p_Values)...));
	}

	template <auto tf_pMember, auto &tf_PreparedDelete, typename ...tfp_CValues>
	auto CSqlTransaction::f_DeleteReturning(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
		requires (NPrivate::fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
	{
		return NPrivate::fg_SqlMutationReturning<tf_pMember, tf_PreparedDelete>
			(
				mp_Transaction
				, &ICSqlTransactionActor::f_DeleteReturning
				, NPrivate::fg_SqlDeleteReturningOperation<tf_pMember, tf_PreparedDelete>(fg_Forward<tfp_CValues>(p_Values)...)
			)
		;
	}

	template <auto &tf_Table, auto tf_pMember, typename tf_CValue>
	auto CSqlTransaction::f_GetByID(tf_CValue _Value)
		-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
		requires
		(
			NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
			&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
			&& NPrivate::fg_SqlPreparedSelectParameterMatches<NPrivate::gc_SqlSelectByID<tf_Table, tf_pMember>, tf_CValue>()
		)
	{
		return f_QueryOptional<NPrivate::gc_SqlSelectByID<tf_Table, tf_pMember>>(fg_Move(_Value));
	}

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	auto CSqlTransaction::f_GetByCompositeID(tfp_CValues &&...p_Values)
		-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
		requires
		(
			NPrivate::fg_SqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>()
			&& NPrivate::fg_SqlCompositeIDSelectParametersMatch<tf_Table, tf_CCompositeID, tfp_CValues...>()
		)
	{
		return []<auto ...tfp_pMembers, typename ...tfp_CCallValues>
			(
				CSqlTransaction *_pThis
				, TCSqlCompositeID<tfp_pMembers...> const *
				, tfp_CCallValues &&...p_CallValues
			)
			{
				return _pThis->f_QueryOptional<NPrivate::gc_SqlSelectByID<tf_Table, tfp_pMembers...>>(fg_Forward<tfp_CCallValues>(p_CallValues)...);
			}
			(this, static_cast<tf_CCompositeID const *>(nullptr), fg_Forward<tfp_CValues>(p_Values)...)
		;
	}

	template <auto &tf_Table, auto tf_pMember, auto ...tfp_pSetMembers, typename ...tfp_CValues>
	auto CSqlTransaction::f_UpdateByID(tfp_CValues &&...p_Values) -> NConcurrency::TCFuture<umint>
		requires
		(
			sizeof...(tfp_pSetMembers) > 0
			&& NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
			&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
			&& NPrivate::fg_SqlPreparedUpdateValuesMatch<NPrivate::gc_SqlUpdateByID<tf_Table, tf_pMember, tfp_pSetMembers...>, tfp_CValues...>()
		)
	{
		return f_Update<NPrivate::gc_SqlUpdateByID<tf_Table, tf_pMember, tfp_pSetMembers...>>(fg_Forward<tfp_CValues>(p_Values)...);
	}

	template <auto &tf_Table, auto tf_pMember, typename tf_CValue>
	auto CSqlTransaction::f_DeleteByID(tf_CValue &&_Value) -> NConcurrency::TCFuture<umint>
		requires
		(
			NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
			&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
			&& NPrivate::fg_SqlPreparedDeleteValuesMatch<NPrivate::gc_SqlDeleteByID<tf_Table, tf_pMember>, tf_CValue>()
		)
	{
		return f_Delete<NPrivate::gc_SqlDeleteByID<tf_Table, tf_pMember>>(fg_Forward<tf_CValue>(_Value));
	}

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	auto CSqlTransaction::f_DeleteByCompositeID(tfp_CValues &&...p_Values) -> NConcurrency::TCFuture<umint>
		requires
		(
			NPrivate::fg_SqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>()
			&& NPrivate::fg_SqlCompositeIDDeleteValuesMatch<tf_Table, tf_CCompositeID, tfp_CValues...>()
		)
	{
		return []<auto ...tfp_pMembers, typename ...tfp_CCallValues>
			(
				CSqlTransaction *_pThis
				, TCSqlCompositeID<tfp_pMembers...> const *
				, tfp_CCallValues &&...p_CallValues
			)
			{
				return _pThis->f_Delete<NPrivate::gc_SqlDeleteByID<tf_Table, tfp_pMembers...>>(fg_Forward<tfp_CCallValues>(p_CallValues)...);
			}
			(this, static_cast<tf_CCompositeID const *>(nullptr), fg_Forward<tfp_CValues>(p_Values)...)
		;
	}

	template <auto &tf_Table, auto tf_pIDMember, auto tf_pVersionMember, auto ...tfp_pSetMembers>
	auto CSqlTransaction::f_Save(typename NTraits::TCDecay<decltype(tf_Table)>::CRow _Row)
		-> NConcurrency::TCFuture<TCSqlSaveResult<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
		requires
		(
			sizeof...(tfp_pSetMembers) > 0
			&& NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
			&& NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<tf_Table, tf_pIDMember>()
			&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pIDMember>()
		)
	{
		return NPrivate::fg_SqlSave<tf_Table, tf_pIDMember, tf_pVersionMember, tfp_pSetMembers...>(mp_Transaction, fg_Move(_Row));
	}

	template <auto &tf_PreparedSelect, typename tf_CParam>
	auto CSqlTransaction::f_Query(tf_CParam _Param, CSqlSelectSettings _Settings)
		requires (NPrivate::fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	{
		return CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			(
				mp_Transaction.f_GetActor()
				, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>(fg_Move(_Param), _Settings)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename tf_CParam0, typename tf_CParam1, typename ...tfp_CParams>
	auto CSqlTransaction::f_Query(tf_CParam0 _Param0, tf_CParam1 _Param1, tfp_CParams ...p_Params)
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam0, tf_CParam1, tfp_CParams...>())
	{
		return CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			(
				mp_Transaction.f_GetActor()
				, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>(CSqlSelectSettings{}, fg_Move(_Param0), fg_Move(_Param1), fg_Move(p_Params)...)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename tf_CParam0, typename tf_CParam1, typename ...tfp_CParams>
	auto CSqlTransaction::f_Query(CSqlSelectSettings _Settings, tf_CParam0 _Param0, tf_CParam1 _Param1, tfp_CParams ...p_Params)
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam0, tf_CParam1, tfp_CParams...>())
	{
		return CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			(
				mp_Transaction.f_GetActor()
				, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>(_Settings, fg_Move(_Param0), fg_Move(_Param1), fg_Move(p_Params)...)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect>
	auto CSqlTransaction::f_Query(CSqlSelectSettings _Settings)
		requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedSelect>())
	{
		return CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			(
				mp_Transaction.f_GetActor()
				, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>(_Settings)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlTransaction::f_QueryStream(tfp_CParams ...p_Params)
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		return f_Query<tf_PreparedSelect>(fg_Move(p_Params)...);
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlTransaction::f_QueryVector(tfp_CParams ...p_Params)
		-> NConcurrency::TCFuture<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		return CSqlDatabaseClient::fsp_QueryVector<CRow>(f_Query<tf_PreparedSelect>(fg_Move(p_Params)...), m_nPipelineLength);
	}

	template <auto &tf_PreparedJoin, typename tf_CResult>
	auto CSqlTransaction::f_QueryJoinedVectorAs() -> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
		requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedJoin>())
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedJoin)>::CRow;

		return CSqlDatabaseClient::fsp_QueryJoinedVectorAs<tf_CResult>
			(
				CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, CRow>
					(
						mp_Transaction.f_GetActor()
						, NPrivate::fg_SqlSelectOperation<tf_PreparedJoin>(CSqlSelectSettings{})
						, m_nPipelineLength
					)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	auto CSqlTransaction::f_QueryVectorAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CProjectionRow = NPrivate::TCSqlProjectionBackendRow<tf_CResult, tfp_pMembers...>;

		return CSqlDatabaseClient::fsp_QueryVectorAs<tf_CResult, tfp_pMembers...>
			(
				CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, CProjectionRow>
				(
					mp_Transaction.f_GetActor()
					, NPrivate::fg_SqlProjectionSelectOperation<tf_PreparedSelect, tf_CResult, tfp_pMembers...>(CSqlSelectSettings{}, fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlTransaction::f_QueryOptional(tfp_CParams ...p_Params)
		-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		return CSqlDatabaseClient::fsp_QueryOptional<CRow>
			(
				CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, CRow>
				(
					mp_Transaction.f_GetActor()
					, NPrivate::fg_SqlTwoRowSelectOperation<tf_PreparedSelect>(fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	auto CSqlTransaction::f_QueryOptionalAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<NStorage::TCOptional<tf_CResult>>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CProjectionRow = NPrivate::TCSqlProjectionBackendRow<tf_CResult, tfp_pMembers...>;

		return CSqlDatabaseClient::fsp_QueryOptionalAs<tf_CResult, tfp_pMembers...>
			(
				CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, CProjectionRow>
				(
					mp_Transaction.f_GetActor()
					, NPrivate::fg_SqlTwoRowProjectionSelectOperation<tf_PreparedSelect, tf_CResult, tfp_pMembers...>(fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlTransaction::f_QueryOne(tfp_CParams ...p_Params)
		-> NConcurrency::TCFuture<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		return CSqlDatabaseClient::fsp_QueryOne<CRow>
			(
				CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, CRow>
				(
					mp_Transaction.f_GetActor()
					, NPrivate::fg_SqlTwoRowSelectOperation<tf_PreparedSelect>(fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	auto CSqlTransaction::f_QueryOneAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<tf_CResult>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CProjectionRow = NPrivate::TCSqlProjectionBackendRow<tf_CResult, tfp_pMembers...>;

		return CSqlDatabaseClient::fsp_QueryOneAs<tf_CResult, tfp_pMembers...>
			(
				CSqlDatabaseClient::fsp_SelectRows<ICSqlTransactionActor, CProjectionRow>
				(
					mp_Transaction.f_GetActor()
					, NPrivate::fg_SqlTwoRowProjectionSelectOperation<tf_PreparedSelect, tf_CResult, tfp_pMembers...>(fg_Move(p_Params)...)
					, m_nPipelineLength
				)
				, m_nPipelineLength
			)
		;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlTransaction::f_Count(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<umint>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		return mp_Transaction(&ICSqlTransactionActor::f_Count, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>({}, fg_Move(p_Params)...));
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	auto CSqlTransaction::f_Exists(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<bool>
		requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		return mp_Transaction(&ICSqlTransactionActor::f_Exists, NPrivate::fg_SqlSelectOperation<tf_PreparedSelect>({}, fg_Move(p_Params)...));
	}

	template <typename t_CConnection, auto &tf_Repository>
	TCSqlRepositoryConnection<t_CConnection, tf_Repository>::TCSqlRepositoryConnection(CConnection *_pConnection)
		: mp_pConnection(_pConnection)
	{
	}

	template <typename t_CConnection, auto &tf_Repository>
	auto TCSqlRepositoryConnection<t_CConnection, tf_Repository>::f_Get(CID _ID) -> NConcurrency::TCFuture<NStorage::TCOptional<CRow>>
	{
		return mp_pConnection->template f_GetByID<tf_Repository.m_Table, CRepository::mc_pIDMember>(fg_Move(_ID));
	}

	template <typename t_CConnection, auto &tf_Repository>
	auto TCSqlRepositoryConnection<t_CConnection, tf_Repository>::f_Save(CRow _Row) -> NConcurrency::TCFuture<TCSqlSaveResult<CRow>>
	{
		return []<auto ...tfp_pSaveMembers>
			(
				TCSqlRepository<CTable, CRepository::mc_pIDMember, CRepository::mc_pVersionMember, tfp_pSaveMembers...> const *
				, CConnection *_pConnection
				, CRow _Row
			)
		{
			return _pConnection->template f_Save<tf_Repository.m_Table, CRepository::mc_pIDMember, CRepository::mc_pVersionMember, tfp_pSaveMembers...>(fg_Move(_Row));
		}(static_cast<CRepository const *>(nullptr), mp_pConnection, fg_Move(_Row));
	}

	template <typename t_CConnection, auto &tf_Repository>
	auto TCSqlRepositoryConnection<t_CConnection, tf_Repository>::f_Delete(CID _ID) -> NConcurrency::TCFuture<umint>
	{
		return mp_pConnection->template f_DeleteByID<tf_Repository.m_Table, CRepository::mc_pIDMember>(fg_Move(_ID));
	}

}
