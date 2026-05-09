// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL::NPrivate
{
	template <ESqlJoinType tf_JoinType, typename tf_CJoinSelect, typename tf_CNextTable, typename tf_CJoinOn>
	consteval auto fg_SqlAppendPreparedJoin(tf_CJoinSelect const &_Select, tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn);

	template <auto &t_PreparedInsert>
	inline constexpr CSqlInsertOperationDescription gc_SqlInsertOperationDescription =
		{
			.m_pStatement = &t_PreparedInsert
			, .m_QueryID = t_PreparedInsert.m_QueryID
		}
	;
	template <auto &t_PreparedUpdate>
	inline constexpr CSqlUpdateOperationDescription gc_SqlUpdateOperationDescription =
		{
			.m_pStatement = &t_PreparedUpdate
			, .m_QueryID = t_PreparedUpdate.m_QueryID
		}
	;
	template <auto &t_PreparedDelete>
	inline constexpr CSqlDeleteOperationDescription gc_SqlDeleteOperationDescription =
		{
			.m_pStatement = &t_PreparedDelete
			, .m_QueryID = t_PreparedDelete.m_QueryID
		}
	;
	template <auto &t_PreparedUpsert>
	inline constexpr CSqlUpsertOperationDescription gc_SqlUpsertOperationDescription =
		{
			.m_pStatement = &t_PreparedUpsert
			, .m_QueryID = t_PreparedUpsert.m_QueryID
		}
	;

	template <auto t_pMember>
	struct TCSqlMemberPointerTraits;

	template <typename t_CRow, typename t_CMember, t_CMember t_CRow:: *t_pMember>
	struct TCSqlMemberPointerTraits<t_pMember>
	{
		using CRow = t_CRow;
		using CMember = t_CMember;
	};

	template <typename tf_CMember, typename tf_CValue>
	constexpr bool fg_SqlInsertValueMatchesMember();

	template <auto tf_pMember, typename tf_CValue>
	constexpr bool fg_SqlPreparedInsertValueMatchesMember();

	template <auto &tf_Table, typename tf_CColumn>
	constexpr bool fg_SqlTableColumnIsInPrimaryKeyConstraint(tf_CColumn const &_Column);

	template <auto &tf_Table, typename tf_CColumn>
	constexpr bool fg_SqlTableColumnIsPrimaryKey(tf_CColumn const &_Column);

	template <auto &tf_Table, auto tf_pMember>
	constexpr bool fg_SqlTableMemberIsPrimaryKey();

	template <auto &tf_Table, auto ...tfp_pMembers>
	constexpr bool fg_SqlTableMembersArePrimaryKey();

	template <auto ...tfp_pMembers>
	constexpr bool fg_SqlMembersAreDistinct();

	template <auto &tf_Table>
	constexpr umint fg_SqlTablePrimaryKeyColumnCount();
	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	constexpr bool fg_SqlPreparedSelectParametersMatch();
	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedDeleteValuesMatch();

	template <auto &t_Table, typename t_CCompositeID>
	struct TCSqlCompositeIDMatchesTable;

	template <auto &t_Table, auto ...tp_pMembers>
	struct TCSqlCompositeIDMatchesTable<t_Table, TCSqlCompositeID<tp_pMembers...>>
	{
		static constexpr bool fs_Matches();
	};

	template <auto &tf_Table, typename tf_CCompositeID>
	constexpr bool fg_SqlCompositeIDMatchesTable();

	template <auto &tf_Table, auto tf_pMember>
	constexpr bool fg_SqlTableMemberIsGeneratedPrimaryKey();

	template <auto &tf_Table>
	constexpr umint fg_SqlTableGeneratedPrimaryKeyColumnCount();

	template <auto &tf_PreparedInsert>
	constexpr umint fg_SqlPreparedInsertGeneratedPrimaryKeyColumnCount();

	template <auto tf_pMember>
	constexpr auto fg_SqlPrimaryKeyPredicate();

	template <auto tf_pMember0, auto tf_pMember1, auto ...tfp_pMembers>
	constexpr auto fg_SqlPrimaryKeyPredicate();

	template <auto &t_Table, auto ...tp_pMembers>
	inline constexpr auto gc_SqlSelectByID = fg_SqlPreparedSelect(t_Table).f_Where(fg_SqlPrimaryKeyPredicate<tp_pMembers...>());

	template <auto &t_Table, typename t_CCompositeID, typename ...tp_CValues>
	struct TCSqlCompositeIDSelectParametersMatch;

	template <auto &t_Table, auto ...tp_pMembers, typename ...tp_CValues>
	struct TCSqlCompositeIDSelectParametersMatch<t_Table, TCSqlCompositeID<tp_pMembers...>, tp_CValues...>
	{
		static constexpr bool fs_Matches();
	};

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	constexpr bool fg_SqlCompositeIDSelectParametersMatch();

	template <auto &t_Table, auto t_pMember, auto ...tp_pSetMembers>
	inline constexpr auto gc_SqlUpdateByID = fg_SqlPreparedUpdate(t_Table).f_Where(fg_SqlParamEq<t_pMember>()).template f_Set<tp_pSetMembers...>();

	template <auto &t_Table, auto ...tp_pMembers>
	inline constexpr auto gc_SqlDeleteByID = fg_SqlPreparedDelete(t_Table).f_Where(fg_SqlPrimaryKeyPredicate<tp_pMembers...>());

	template <auto &t_Table, typename t_CCompositeID, typename ...tp_CValues>
	struct TCSqlCompositeIDDeleteValuesMatch;

	template <auto &t_Table, auto ...tp_pMembers, typename ...tp_CValues>
	struct TCSqlCompositeIDDeleteValuesMatch<t_Table, TCSqlCompositeID<tp_pMembers...>, tp_CValues...>
	{
		static constexpr bool fs_Matches();
	};

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	constexpr bool fg_SqlCompositeIDDeleteValuesMatch();

	template <auto &t_Table>
	inline constexpr auto gc_SqlSaveInsert = fg_SqlPreparedInsert(t_Table);

	template <auto &t_Table, auto t_pIDMember, auto t_pVersionMember, auto ...tp_pSetMembers>
	inline constexpr auto gc_SqlSaveUpdate = fg_SqlPreparedUpdate(t_Table)
		.f_Where(fg_SqlAnd(fg_SqlParamEq<t_pIDMember>(), fg_SqlParamEq<t_pVersionMember>()))
		.template f_Set<tp_pSetMembers..., t_pVersionMember>()
	;

	template <typename t_CPredicate>
	struct TCSqlPredicateFirstParameterMember
	{
		using CMember = typename TCSqlMemberPointerTraits<t_CPredicate::mc_pMember>::CMember;
	};

	template <typename t_CLeft, typename t_CRight, ESqlPredicateType t_Type>
	struct TCSqlPredicateFirstParameterMember<TCSqlBinaryPredicate<t_CLeft, t_CRight, t_Type>> : public TCSqlPredicateFirstParameterMember<t_CLeft>
	{
	};

	template <typename t_CPredicate>
	struct TCSqlPredicateFirstParameterMember<TCSqlNotPredicate<t_CPredicate>> : public TCSqlPredicateFirstParameterMember<t_CPredicate>
	{
	};

	template <auto &t_PreparedSelect>
	struct TCSqlPreparedSelectParameterMember
	{
		using CPreparedSelect = NTraits::TCDecay<decltype(t_PreparedSelect)>;
		using CMember = typename TCSqlPredicateFirstParameterMember<typename CPreparedSelect::CPredicate>::CMember;
	};

	template <typename t_COrderBy, typename t_CTerm>
	struct TCSqlAppendOrderBy;

	template <typename t_CTerm>
	struct TCSqlAppendOrderBy<CSqlNoOrderBy, t_CTerm>
	{
		using CType = TCSqlOrderBy<t_CTerm>;
	};

	template <typename ...tp_CTerms, typename t_CTerm>
	struct TCSqlAppendOrderBy<TCSqlOrderBy<tp_CTerms...>, t_CTerm>
	{
		using CType = TCSqlOrderBy<tp_CTerms..., t_CTerm>;
	};

	template <auto ...tp_pMembers>
	struct TCSqlMemberList
	{
		static constexpr umint mc_nMembers = sizeof...(tp_pMembers);
	};

	template <typename ...tp_CLists>
	struct TCSqlMemberListConcat;

	template <auto ...tp_pMembers>
	struct TCSqlMemberListConcat<TCSqlMemberList<tp_pMembers...>>
	{
		using CType = TCSqlMemberList<tp_pMembers...>;
	};

	template <auto ...tp_pLeftMembers, auto ...tp_pRightMembers, typename ...tp_CLists>
	struct TCSqlMemberListConcat<TCSqlMemberList<tp_pLeftMembers...>, TCSqlMemberList<tp_pRightMembers...>, tp_CLists...>
	{
		using CType = typename TCSqlMemberListConcat<TCSqlMemberList<tp_pLeftMembers..., tp_pRightMembers...>, tp_CLists...>::CType;
	};

	template <auto t_pMember, umint t_nRemaining, auto ...tp_pMembers>
	struct TCSqlRepeatMemberList : public TCSqlRepeatMemberList<t_pMember, t_nRemaining - 1, t_pMember, tp_pMembers...>
	{
	};

	template <auto t_pMember, auto ...tp_pMembers>
	struct TCSqlRepeatMemberList<t_pMember, 0, tp_pMembers...>
	{
		using CType = TCSqlMemberList<tp_pMembers...>;
	};

	template <typename t_CPredicate>
	struct TCSqlPredicateParameterMembers;

	template <auto t_pMember, ESqlPredicateType t_Type>
	struct TCSqlPredicateParameterMembers<TCSqlParameterPredicate<t_pMember, t_Type>>
	{
		using CType = TCSqlMemberList<t_pMember>;
	};

	template <auto t_pMember, ESqlPredicateType t_Type>
	struct TCSqlPredicateParameterMembers<TCSqlNullPredicate<t_pMember, t_Type>>
	{
		using CType = TCSqlMemberList<>;
	};

	template <>
	struct TCSqlPredicateParameterMembers<CSqlAllRowsPredicate>
	{
		using CType = TCSqlMemberList<>;
	};

	template <auto t_pMember, umint t_nParameters>
	struct TCSqlPredicateParameterMembers<TCSqlInPredicate<t_pMember, t_nParameters>>
	{
		using CType = typename TCSqlRepeatMemberList<t_pMember, t_nParameters>::CType;
	};

	template <auto t_pMember, auto &t_Subquery>
	struct TCSqlPredicateParameterMembers<TCSqlInSubqueryPredicate<t_pMember, t_Subquery>>
	{
		using CSubquery = NTraits::TCDecay<decltype(t_Subquery)>;
		using CType = typename TCSqlPredicateParameterMembers<typename CSubquery::CPredicate>::CType;
	};

	template <auto &t_Subquery, ESqlPredicateType t_Type>
	struct TCSqlPredicateParameterMembers<TCSqlExistsPredicate<t_Subquery, t_Type>>
	{
		using CSubquery = NTraits::TCDecay<decltype(t_Subquery)>;
		using CType = typename TCSqlPredicateParameterMembers<typename CSubquery::CPredicate>::CType;
	};

	template <typename t_CLeft, typename t_CRight, ESqlPredicateType t_Type>
	struct TCSqlPredicateParameterMembers<TCSqlBinaryPredicate<t_CLeft, t_CRight, t_Type>>
	{
		using CType = typename TCSqlMemberListConcat<typename TCSqlPredicateParameterMembers<t_CLeft>::CType, typename TCSqlPredicateParameterMembers<t_CRight>::CType>::CType;
	};

	template <typename t_CPredicate>
	struct TCSqlPredicateParameterMembers<TCSqlNotPredicate<t_CPredicate>> : public TCSqlPredicateParameterMembers<t_CPredicate>
	{
	};

	template <typename t_CMemberList, typename ...tp_CParams>
	struct TCSqlParameterMembersMatch;

	template <typename ...tp_CTypes>
	struct TCSqlTypeList
	{
		static constexpr umint mc_nTypes = sizeof...(tp_CTypes);
	};

	template <typename ...tp_CLists>
	struct TCSqlTypeListConcat;

	template <typename ...tp_CTypes>
	struct TCSqlTypeListConcat<TCSqlTypeList<tp_CTypes...>>
	{
		using CType = TCSqlTypeList<tp_CTypes...>;
	};

	template <typename ...tp_CLeftTypes, typename ...tp_CRightTypes, typename ...tp_CLists>
	struct TCSqlTypeListConcat<TCSqlTypeList<tp_CLeftTypes...>, TCSqlTypeList<tp_CRightTypes...>, tp_CLists...>
	{
		using CType = typename TCSqlTypeListConcat<TCSqlTypeList<tp_CLeftTypes..., tp_CRightTypes...>, tp_CLists...>::CType;
	};

	template <>
	struct TCSqlParameterMembersMatch<TCSqlMemberList<>>
	{
		static constexpr bool fs_Match();
	};

	template <auto t_pMember, auto ...tp_pMembers, typename t_CParam, typename ...tp_CParams>
	struct TCSqlParameterMembersMatch<TCSqlMemberList<t_pMember, tp_pMembers...>, t_CParam, tp_CParams...>
	{
		static constexpr bool fs_Match();
	};

	template <typename t_CMemberList>
	struct TCSqlTypeListForMemberList;

	template <typename t_CPreparedSelect>
	struct TCSqlPreparedSelectParameterTypesList;

	template <typename t_CPreparedSelect>
	struct TCSqlPreparedSelectNestedParameterTypesList;

	template <auto ...tp_pMembers>
	struct TCSqlTypeListForMemberList<TCSqlMemberList<tp_pMembers...>>
	{
		using CType = TCSqlTypeList<typename TCSqlMemberPointerTraits<tp_pMembers>::CMember...>;
	};

	template <typename t_CPredicate>
	struct TCSqlPredicateParameterTypes
	{
		using CType = typename TCSqlTypeListForMemberList<typename TCSqlPredicateParameterMembers<t_CPredicate>::CType>::CType;
	};

	template <auto t_pMember, auto &t_Subquery>
	struct TCSqlPredicateParameterTypes<TCSqlInSubqueryPredicate<t_pMember, t_Subquery>>
	{
		using CSubquery = NTraits::TCDecay<decltype(t_Subquery)>;
		using CType = typename TCSqlPreparedSelectNestedParameterTypesList<CSubquery>::CType;
	};

	template <auto &t_Subquery, ESqlPredicateType t_Type>
	struct TCSqlPredicateParameterTypes<TCSqlExistsPredicate<t_Subquery, t_Type>>
	{
		using CSubquery = NTraits::TCDecay<decltype(t_Subquery)>;
		using CType = typename TCSqlPreparedSelectNestedParameterTypesList<CSubquery>::CType;
	};

	template <typename t_CLeft, typename t_CRight, ESqlPredicateType t_Type>
	struct TCSqlPredicateParameterTypes<TCSqlBinaryPredicate<t_CLeft, t_CRight, t_Type>>
	{
		using CType = typename TCSqlTypeListConcat<typename TCSqlPredicateParameterTypes<t_CLeft>::CType, typename TCSqlPredicateParameterTypes<t_CRight>::CType>::CType;
	};

	template <typename t_CPredicate>
	struct TCSqlPredicateParameterTypes<TCSqlNotPredicate<t_CPredicate>> : public TCSqlPredicateParameterTypes<t_CPredicate>
	{
	};

	template <typename t_CHaving>
	struct TCSqlHavingParameterTypes;

	template <>
	struct TCSqlHavingParameterTypes<CSqlNoHaving>
	{
		using CType = TCSqlTypeList<>;
	};

	template <typename t_CExpression, ESqlPredicateType t_Type>
	struct TCSqlHavingParameterTypes<TCSqlHavingAggregatePredicate<t_CExpression, t_Type>>
	{
		using CResult = typename TCSqlExpressionResultType<t_CExpression>::CType;
		using CType = TCSqlTypeList<NStorage::TCOptionalType<CResult>>;
	};

	template <typename t_CTypeList, typename ...tp_CParams>
	struct TCSqlParameterTypesMatch;

	template <typename t_CPreparedSelect>
	struct TCSqlPreparedSelectParameterTypesList;

	template <>
	struct TCSqlParameterTypesMatch<TCSqlTypeList<>>
	{
		static constexpr bool fs_Match();
	};

	template <typename t_CType, typename ...tp_CTypes, typename t_CParam, typename ...tp_CParams>
	struct TCSqlParameterTypesMatch<TCSqlTypeList<t_CType, tp_CTypes...>, t_CParam, tp_CParams...>
	{
		static constexpr bool fs_Match();
	};

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	constexpr bool fg_SqlPreparedSelectParametersMatch();

	template <auto &tf_PreparedSelect, typename tf_CParam>
	constexpr bool fg_SqlPreparedSelectParameterMatches();

	template <auto &tf_PreparedSelect>
	constexpr bool fg_SqlPreparedSelectHasNoParameters();

	template <typename tf_CColumn>
	constexpr bool fg_SqlColumnIsGenerated(tf_CColumn const &_Column);

	template <typename tf_CColumn>
	constexpr bool fg_SqlPreparedInsertColumnIsImplicitlySelected(tf_CColumn const &_Column);

	template <auto ...tp_pMembers>
	struct TCSqlPreparedInsertValueMatcher;

	template <>
	struct TCSqlPreparedInsertValueMatcher<>
	{
		template <typename ...tfp_CValues>
		static constexpr bool fs_Matches();
	};

	template <auto t_pMember, auto ...tp_pMembers>
	struct TCSqlPreparedInsertValueMatcher<t_pMember, tp_pMembers...>
	{
		static constexpr bool fs_Matches();

		template <typename tf_CValue, typename ...tfp_CValues>
		static constexpr bool fs_Matches();
	};

	template <auto &tf_PreparedInsert, umint tf_iColumn, umint tf_iValue, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedInsertImplicitValuesMatch();

	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedInsertImplicitRowValueMatches();

	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedInsertValuesMatch();

	template <typename t_CSet>
	struct TCSqlSetParameterMembers;

	template <auto ...tp_pMembers>
	struct TCSqlSetParameterMembers<TCSqlSelectedColumns<tp_pMembers...>>
	{
		using CType = TCSqlMemberList<tp_pMembers...>;
	};

	template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedUpdateValuesMatch();

	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedDeleteValuesMatch();

	template <typename tf_CTable, typename ...tfp_CValues>
	constexpr bool fg_SqlTableInsertValuesMatch();
	template <auto &tf_Table, typename ...tfp_CValues>
	constexpr bool fg_SqlTableImplicitInsertValuesMatch();

	template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedUpsertValuesMatch();

	template <typename tf_CTable, umint tf_iColumn, typename ...tfp_CValues>
	constexpr bool fg_SqlTableInsertValuesMatchFrom();

	template <typename tf_CTable, typename ...tfp_CValues>
	constexpr bool fg_SqlTableInsertValuesMatch();

	template <typename tf_CTable>
	consteval bool fg_SqlTableMembersAllNullable();

	template <auto &tf_Table, umint tf_iColumn, umint tf_iValue, typename ...tfp_CValues>
	constexpr bool fg_SqlTableImplicitInsertValuesMatchFrom();

	template <auto &tf_Table, typename ...tfp_CValues>
	constexpr bool fg_SqlTableImplicitInsertValuesMatch();

	template <typename tf_CTable>
	CSqlInsertOperation fg_SqlInsertOperation(tf_CTable const &_Table, typename tf_CTable::CRow &&_Row);
	template <typename tf_CTable, typename ...tfp_CValues>
	CSqlInsertOperation fg_SqlInsertOperation(tf_CTable const &_Table, tfp_CValues && ...p_Values)
		requires (fg_SqlTableInsertValuesMatch<tf_CTable, tfp_CValues...>())
	;
	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	CSqlInsertOperation fg_SqlInsertOperation(tfp_CValues && ...p_Values);
	template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
	CSqlUpdateOperation fg_SqlUpdateOperation(tfp_CValues && ...p_Values);
	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	CSqlDeleteOperation fg_SqlDeleteOperation(tfp_CValues && ...p_Values);
	template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
	CSqlUpsertOperation fg_SqlUpsertOperation(tfp_CValues && ...p_Values);

	template <auto &tf_PreparedSelect, typename tf_CParam>
	CSqlSelectOperation fg_SqlSelectOperation(tf_CParam &&_Param, CSqlSelectSettings _Settings)
		requires (fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	;
	template <auto &tf_PreparedSelect, typename tf_CParam>
	CSqlSelectOperation fg_SqlSelectOperation(CSqlSelectSettings _Settings, tf_CParam &&_Param)
		requires (fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	;
	template <auto ...tp_pMembers>
	using TCSqlProjectionRow = NStorage::TCTuple<typename TCSqlMemberPointerTraits<tp_pMembers>::CMember...>;
	template <typename t_CResult, auto ...tp_pMembers>
	struct TCSqlProjectionBackendRowType
	{
		using CType = TCSqlProjectionRow<tp_pMembers...>;
	};

	template <typename ...tp_CTypes, auto ...tp_pMembers>
	struct TCSqlProjectionBackendRowType<NStorage::TCTuple<tp_CTypes...>, tp_pMembers...>
	{
		static_assert(sizeof...(tp_CTypes) == sizeof...(tp_pMembers), "SQL tuple projection result must match selected member count");

		using CType = NStorage::TCTuple<tp_CTypes...>;
	};

	template <typename t_CType, auto t_pMember>
	struct TCSqlProjectionBackendRowType<NStorage::TCTuple<t_CType>, t_pMember>
	{
		using CType = NStorage::TCTuple<t_CType>;
	};

	template <typename t_CResult, auto t_pMember>
	struct TCSqlProjectionBackendRowType<t_CResult, t_pMember>
	{
		using CMember = typename TCSqlMemberPointerTraits<t_pMember>::CMember;
		using CResult = NTraits::TCRemoveReferenceAndQualifiers<t_CResult>;

		using CType = TCConditional<NTraits::cIsSame<CResult, CMember>, CResult, TCSqlProjectionRow<t_pMember>>;
	};

	template <typename t_CResult, auto ...tp_pMembers>
	using TCSqlProjectionBackendRow = typename TCSqlProjectionBackendRowType<t_CResult, tp_pMembers...>::CType;
	template <typename t_CProjectionRow, umint t_iMember>
	struct TCSqlProjectionMemberType
	{
		static_assert(t_iMember == 0, "SQL scalar projection row can only map one selected member");

		using CType = t_CProjectionRow;
	};

	template <typename ...tp_CTypes, umint t_iMember>
	struct TCSqlProjectionMemberType<NStorage::TCTuple<tp_CTypes...>, t_iMember>
	{
		using CType = NStorage::TCTuple_Get<t_iMember, NStorage::TCTuple<tp_CTypes...>>;
	};

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	CSqlSelectOperation fg_SqlProjectionSelectOperation(CSqlSelectSettings _Settings, tfp_CParams && ...p_Params)
		requires (fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	;

	int64 fg_SqlRowCountParameter(umint _nRows);

	// The savepoints a transaction actor created, oldest first. Releasing or rolling back to a savepoint also ends the ones
	// created after it. Savepoints that raw SQL creates are not tracked.
	struct CSqlSavepointStack
	{
		enum class EKind : uint8
		{
			mc_Manual
			, mc_Scope
		};

		NException::CExceptionPointer f_CheckScopeEnd(NStr::CStr const &_Name) const;
		void f_Created(NStr::CStr const &_Name, EKind _Kind);
		void f_Released(NStr::CStr const &_Name);
		void f_RolledBackTo(NStr::CStr const &_Name);

	private:
		struct CSavepoint
		{
			NStr::CStr m_Name;
			EKind m_Kind = EKind::mc_Manual;
		};

		umint fp_Find(NStr::CStr const &_Name) const;

		NContainer::TCVector<CSavepoint> mp_Savepoints;
	};

	// A row stream keeps the transaction it reads from busy, so a commit after the callback returned it would wait for a
	// stream only the commit's caller can read
	template <typename t_CType>
	concept cSqlTransactionRowStream = NTraits::cIsSame<t_CType, CSqlRawStream> || NConcurrency::NPrivate::TCIsAsyncGenerator<t_CType>::mc_Value;

	template <typename tf_FFunction, typename tf_CTransaction>
	auto fg_SqlCallScopeFunction(tf_FFunction &&_fFunction, tf_CTransaction &&_Transaction) -> decltype(_fFunction(fg_Forward<tf_CTransaction>(_Transaction)));
}
