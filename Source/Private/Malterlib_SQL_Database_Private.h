// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL::NPrivate
{
	template <ESqlJoinType tf_JoinType, typename tf_CJoinSelect, typename tf_CNextTable, typename tf_CJoinOn>
	consteval auto fg_SqlAppendPreparedJoin(tf_CJoinSelect const &_Select, tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn);

	template <auto &tf_PreparedInsert>
	inline constexpr CSqlInsertOperationDescription gc_SqlInsertOperationDescription =
		{
			.m_pStatement = &tf_PreparedInsert
			, .m_QueryID = tf_PreparedInsert.m_QueryID
		}
	;
	template <auto &tf_PreparedUpdate>
	inline constexpr CSqlUpdateOperationDescription gc_SqlUpdateOperationDescription =
		{
			.m_pStatement = &tf_PreparedUpdate
			, .m_QueryID = tf_PreparedUpdate.m_QueryID
		}
	;
	template <auto &tf_PreparedDelete>
	inline constexpr CSqlDeleteOperationDescription gc_SqlDeleteOperationDescription =
		{
			.m_pStatement = &tf_PreparedDelete
			, .m_QueryID = tf_PreparedDelete.m_QueryID
		}
	;
	template <auto &tf_PreparedUpsert>
	inline constexpr CSqlUpsertOperationDescription gc_SqlUpsertOperationDescription =
		{
			.m_pStatement = &tf_PreparedUpsert
			, .m_QueryID = tf_PreparedUpsert.m_QueryID
		}
	;

	template <auto tf_pMember>
	struct TCSqlMemberPointerTraits;

	template <typename tf_CRow, typename tf_CMember, tf_CMember tf_CRow::*tf_pMember>
	struct TCSqlMemberPointerTraits<tf_pMember>
	{
		using CRow = tf_CRow;
		using CMember = tf_CMember;
	};

	template <typename tf_CMember, typename tf_CValue>
	constexpr bool fg_SqlInsertValueMatchesMember()
	{
		using CMember = NTraits::TCRemoveReferenceAndQualifiers<tf_CMember>;
		using CValue = NTraits::TCRemoveReferenceAndQualifiers<tf_CValue>;
		using CStoredMember = NStorage::TCOptionalType<CMember>;
		using CSqlType = typename TCSqlTypeTraits<CStoredMember>::CSqlType;

		return
			NTraits::cIsConstructibleWith<CMember, tf_CValue &&>
			|| NTraits::cIsConstructibleWith<CSqlType, tf_CValue &&>
			|| NTraits::cIsConvertible<CValue, CMember>
			|| NTraits::cIsConvertible<CValue, CSqlType>
		;
	}

	template <auto tf_pMember, typename tf_CValue>
	constexpr bool fg_SqlPreparedInsertValueMatchesMember()
	{
		using CMember = typename TCSqlMemberPointerTraits<tf_pMember>::CMember;

		return fg_SqlInsertValueMatchesMember<CMember, tf_CValue>();
	}

	template <auto &tf_Table, typename tf_CColumn>
	constexpr bool fg_SqlTableColumnIsInPrimaryKeyConstraint(tf_CColumn const &_Column)
	{
		bool bPrimaryKey = false;
		tf_Table.f_ForEachConstraint
			(
				[&](auto const &...p_Constraints)
				{
					(
						[&]
						{
							if constexpr (requires { p_Constraints.mc_Type; })
							{
								if constexpr (p_Constraints.mc_Type == ESqlConstraintType::mc_PrimaryKey)
								{
									if constexpr (requires { p_Constraints.f_MemberIsInConstraint(_Column.m_pMember); })
										bPrimaryKey = bPrimaryKey || p_Constraints.f_MemberIsInConstraint(_Column.m_pMember);
									else
									{
										p_Constraints.f_ForEachColumnName
											(
												[&](auto const &...p_ColumnNames)
												{
													bPrimaryKey = bPrimaryKey || ((&_Column.f_Name() == &p_ColumnNames) || ...);
												}
											)
										;
									}
								}
							}
						}
						()
						, ...
					);
				}
			)
		;

		return bPrimaryKey;
	}

	template <auto &tf_Table, typename tf_CColumn>
	constexpr bool fg_SqlTableColumnIsPrimaryKey(tf_CColumn const &_Column)
	{
		return
			fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_PrimaryKey)
			|| fg_SqlTableColumnIsInPrimaryKeyConstraint<tf_Table>(_Column)
		;
	}

	template <auto &tf_Table, auto tf_pMember>
	constexpr bool fg_SqlTableMemberIsPrimaryKey()
	{
		bool bPrimaryKey = false;
		tf_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == tf_pMember; })
							{
								if (p_Columns.m_pMember == tf_pMember)
									bPrimaryKey = fg_SqlTableColumnIsPrimaryKey<tf_Table>(p_Columns);
							}
						}
						()
						, ...
					);
				}
			)
		;

		return bPrimaryKey;
	}

	template <auto &tf_Table, auto ...tfp_pMembers>
	constexpr bool fg_SqlTableMembersArePrimaryKey()
	{
		return (fg_SqlTableMemberIsPrimaryKey<tf_Table, tfp_pMembers>() && ...);
	}

	template <auto &tf_Table>
	constexpr umint fg_SqlTablePrimaryKeyColumnCount();
	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	constexpr bool fg_SqlPreparedSelectParametersMatch();
	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedDeleteValuesMatch();

	template <auto &tf_Table, typename tf_CCompositeID>
	struct TCSqlCompositeIDMatchesTable;

	template <auto &tf_Table, auto ...tfp_pMembers>
	struct TCSqlCompositeIDMatchesTable<tf_Table, TCSqlCompositeID<tfp_pMembers...>>
	{
		static constexpr bool fs_Matches()
		{
			return
				sizeof...(tfp_pMembers) > 1
				&& fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == sizeof...(tfp_pMembers)
				&& fg_SqlTableMembersArePrimaryKey<tf_Table, tfp_pMembers...>()
			;
		}
	};

	template <auto &tf_Table, typename tf_CCompositeID>
	constexpr bool fg_SqlCompositeIDMatchesTable()
	{
		return TCSqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>::fs_Matches();
	}

	template <auto &tf_Table, auto tf_pMember>
	constexpr bool fg_SqlTableMemberIsGeneratedPrimaryKey()
	{
		bool bGeneratedPrimaryKey = false;
		tf_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == tf_pMember; })
							{
								if (p_Columns.m_pMember == tf_pMember)
								{
									bGeneratedPrimaryKey =
										fg_IsSet(p_Columns.m_Flags, ESqlColumnFlag::mc_PrimaryKey)
										&& fg_IsSet(p_Columns.m_Flags, ESqlColumnFlag::mc_AutoIncrement)
									;
								}
							}
						}
						()
						, ...
					);
				}
			)
		;

		return bGeneratedPrimaryKey;
	}

	template <auto &tf_Table>
	constexpr umint fg_SqlTablePrimaryKeyColumnCount()
	{
		umint nPrimaryKeys = 0;
		tf_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if (fg_SqlTableColumnIsPrimaryKey<tf_Table>(p_Columns))
								++nPrimaryKeys;
						}
						()
						, ...
					);
				}
			)
		;

		return nPrimaryKeys;
	}

	template <auto &tf_Table>
	constexpr umint fg_SqlTableGeneratedPrimaryKeyColumnCount()
	{
		umint nGeneratedPrimaryKeys = 0;
		tf_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if
							(
								fg_IsSet(p_Columns.m_Flags, ESqlColumnFlag::mc_PrimaryKey)
								&& fg_IsSet(p_Columns.m_Flags, ESqlColumnFlag::mc_AutoIncrement)
							)
							{
								++nGeneratedPrimaryKeys;
							}
						}
						()
						, ...
					);
				}
			)
		;

		return nGeneratedPrimaryKeys;
	}

	template <auto &tf_PreparedInsert>
	constexpr umint fg_SqlPreparedInsertGeneratedPrimaryKeyColumnCount()
	{
		return fg_SqlTableGeneratedPrimaryKeyColumnCount<tf_PreparedInsert.m_Table>();
	}

	template <auto tf_pMember>
	constexpr auto fg_SqlPrimaryKeyPredicate()
	{
		return fg_SqlParamEq<tf_pMember>();
	}

	template <auto tf_pMember0, auto tf_pMember1, auto ...tfp_pMembers>
	constexpr auto fg_SqlPrimaryKeyPredicate()
	{
		return fg_SqlAnd(fg_SqlParamEq<tf_pMember0>(), fg_SqlPrimaryKeyPredicate<tf_pMember1, tfp_pMembers...>());
	}

	template <auto &tf_Table, auto ...tfp_pMembers>
	inline constexpr auto gc_SqlSelectByID = fg_SqlPreparedSelect(tf_Table)
		.f_Where(fg_SqlPrimaryKeyPredicate<tfp_pMembers...>())
	;

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	struct TCSqlCompositeIDSelectParametersMatch;

	template <auto &tf_Table, auto ...tfp_pMembers, typename ...tfp_CValues>
	struct TCSqlCompositeIDSelectParametersMatch<tf_Table, TCSqlCompositeID<tfp_pMembers...>, tfp_CValues...>
	{
		static constexpr bool fs_Matches()
		{
			return fg_SqlPreparedSelectParametersMatch<gc_SqlSelectByID<tf_Table, tfp_pMembers...>, tfp_CValues...>();
		}
	};

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	constexpr bool fg_SqlCompositeIDSelectParametersMatch()
	{
		return TCSqlCompositeIDSelectParametersMatch<tf_Table, tf_CCompositeID, tfp_CValues...>::fs_Matches();
	}

	template <auto &tf_Table, auto tf_pMember, auto ...tfp_pSetMembers>
	inline constexpr auto gc_SqlUpdateByID = fg_SqlPreparedUpdate(tf_Table)
		.f_Where(fg_SqlParamEq<tf_pMember>())
		.template f_Set<tfp_pSetMembers...>()
	;

	template <auto &tf_Table, auto ...tfp_pMembers>
	inline constexpr auto gc_SqlDeleteByID = fg_SqlPreparedDelete(tf_Table)
		.f_Where(fg_SqlPrimaryKeyPredicate<tfp_pMembers...>())
	;

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	struct TCSqlCompositeIDDeleteValuesMatch;

	template <auto &tf_Table, auto ...tfp_pMembers, typename ...tfp_CValues>
	struct TCSqlCompositeIDDeleteValuesMatch<tf_Table, TCSqlCompositeID<tfp_pMembers...>, tfp_CValues...>
	{
		static constexpr bool fs_Matches()
		{
			return fg_SqlPreparedDeleteValuesMatch<gc_SqlDeleteByID<tf_Table, tfp_pMembers...>, tfp_CValues...>();
		}
	};

	template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
	constexpr bool fg_SqlCompositeIDDeleteValuesMatch()
	{
		return TCSqlCompositeIDDeleteValuesMatch<tf_Table, tf_CCompositeID, tfp_CValues...>::fs_Matches();
	}

	template <auto &tf_Table>
	inline constexpr auto gc_SqlSaveInsert = fg_SqlPreparedInsert(tf_Table);

	template <auto &tf_Table, auto tf_pIDMember, auto tf_pVersionMember, auto ...tfp_pSetMembers>
	inline constexpr auto gc_SqlSaveUpdate = fg_SqlPreparedUpdate(tf_Table)
		.f_Where(fg_SqlAnd(fg_SqlParamEq<tf_pIDMember>(), fg_SqlParamEq<tf_pVersionMember>()))
		.template f_Set<tfp_pSetMembers..., tf_pVersionMember>()
	;

	template <typename tf_CPredicate>
	struct TCSqlPredicateFirstParameterMember
	{
		using CMember = typename TCSqlMemberPointerTraits<tf_CPredicate::mc_pMember>::CMember;
	};

	template <typename tf_CLeft, typename tf_CRight, ESqlPredicateType tf_Type>
	struct TCSqlPredicateFirstParameterMember<TCSqlBinaryPredicate<tf_CLeft, tf_CRight, tf_Type>> : public TCSqlPredicateFirstParameterMember<tf_CLeft>
	{
	};

	template <typename tf_CPredicate>
	struct TCSqlPredicateFirstParameterMember<TCSqlNotPredicate<tf_CPredicate>> : public TCSqlPredicateFirstParameterMember<tf_CPredicate>
	{
	};

	template <auto &tf_PreparedSelect>
	struct TCSqlPreparedSelectParameterMember
	{
		using CPreparedSelect = NTraits::TCDecay<decltype(tf_PreparedSelect)>;
		using CMember = typename TCSqlPredicateFirstParameterMember<typename CPreparedSelect::CPredicate>::CMember;
	};

	template <typename tf_COrderBy, typename tf_CTerm>
	struct TCSqlAppendOrderBy;

	template <typename tf_CTerm>
	struct TCSqlAppendOrderBy<CSqlNoOrderBy, tf_CTerm>
	{
		using CType = TCSqlOrderBy<tf_CTerm>;
	};

	template <typename ...tfp_CTerms, typename tf_CTerm>
	struct TCSqlAppendOrderBy<TCSqlOrderBy<tfp_CTerms...>, tf_CTerm>
	{
		using CType = TCSqlOrderBy<tfp_CTerms..., tf_CTerm>;
	};

	template <auto ...tfp_pMembers>
	struct TCSqlMemberList
	{
		static constexpr umint mc_nMembers = sizeof...(tfp_pMembers);
	};

	template <typename ...tfp_CLists>
	struct TCSqlMemberListConcat;

	template <auto ...tfp_pMembers>
	struct TCSqlMemberListConcat<TCSqlMemberList<tfp_pMembers...>>
	{
		using CType = TCSqlMemberList<tfp_pMembers...>;
	};

	template <auto ...tfp_pLeftMembers, auto ...tfp_pRightMembers, typename ...tfp_CLists>
	struct TCSqlMemberListConcat<TCSqlMemberList<tfp_pLeftMembers...>, TCSqlMemberList<tfp_pRightMembers...>, tfp_CLists...>
	{
		using CType = typename TCSqlMemberListConcat<TCSqlMemberList<tfp_pLeftMembers..., tfp_pRightMembers...>, tfp_CLists...>::CType;
	};

	template <auto tf_pMember, umint tf_nRemaining, auto ...tfp_pMembers>
	struct TCSqlRepeatMemberList : public TCSqlRepeatMemberList<tf_pMember, tf_nRemaining - 1, tf_pMember, tfp_pMembers...>
	{
	};

	template <auto tf_pMember, auto ...tfp_pMembers>
	struct TCSqlRepeatMemberList<tf_pMember, 0, tfp_pMembers...>
	{
		using CType = TCSqlMemberList<tfp_pMembers...>;
	};

	template <typename tf_CPredicate>
	struct TCSqlPredicateParameterMembers;

	template <auto tf_pMember, ESqlPredicateType tf_Type>
	struct TCSqlPredicateParameterMembers<TCSqlParameterPredicate<tf_pMember, tf_Type>>
	{
		using CType = TCSqlMemberList<tf_pMember>;
	};

	template <auto tf_pMember, ESqlPredicateType tf_Type>
	struct TCSqlPredicateParameterMembers<TCSqlNullPredicate<tf_pMember, tf_Type>>
	{
		using CType = TCSqlMemberList<>;
	};

	template <>
	struct TCSqlPredicateParameterMembers<CSqlAllRowsPredicate>
	{
		using CType = TCSqlMemberList<>;
	};

	template <auto tf_pMember, umint tf_nParameters>
	struct TCSqlPredicateParameterMembers<TCSqlInPredicate<tf_pMember, tf_nParameters>>
	{
		using CType = typename TCSqlRepeatMemberList<tf_pMember, tf_nParameters>::CType;
	};

	template <auto tf_pMember, auto &tf_Subquery>
	struct TCSqlPredicateParameterMembers<TCSqlInSubqueryPredicate<tf_pMember, tf_Subquery>>
	{
		using CSubquery = NTraits::TCDecay<decltype(tf_Subquery)>;
		using CType = typename TCSqlPredicateParameterMembers<typename CSubquery::CPredicate>::CType;
	};

	template <auto &tf_Subquery, ESqlPredicateType tf_Type>
	struct TCSqlPredicateParameterMembers<TCSqlExistsPredicate<tf_Subquery, tf_Type>>
	{
		using CSubquery = NTraits::TCDecay<decltype(tf_Subquery)>;
		using CType = typename TCSqlPredicateParameterMembers<typename CSubquery::CPredicate>::CType;
	};

	template <typename tf_CLeft, typename tf_CRight, ESqlPredicateType tf_Type>
	struct TCSqlPredicateParameterMembers<TCSqlBinaryPredicate<tf_CLeft, tf_CRight, tf_Type>>
	{
		using CType = typename TCSqlMemberListConcat<typename TCSqlPredicateParameterMembers<tf_CLeft>::CType, typename TCSqlPredicateParameterMembers<tf_CRight>::CType>::CType;
	};

	template <typename tf_CPredicate>
	struct TCSqlPredicateParameterMembers<TCSqlNotPredicate<tf_CPredicate>> : public TCSqlPredicateParameterMembers<tf_CPredicate>
	{
	};

	template <typename tf_CMemberList, typename ...tfp_CParams>
	struct TCSqlParameterMembersMatch;

	template <typename ...tfp_CTypes>
	struct TCSqlTypeList
	{
		static constexpr umint mc_nTypes = sizeof...(tfp_CTypes);
	};

	template <typename ...tfp_CLists>
	struct TCSqlTypeListConcat;

	template <typename ...tfp_CTypes>
	struct TCSqlTypeListConcat<TCSqlTypeList<tfp_CTypes...>>
	{
		using CType = TCSqlTypeList<tfp_CTypes...>;
	};

	template <typename ...tfp_CLeftTypes, typename ...tfp_CRightTypes, typename ...tfp_CLists>
	struct TCSqlTypeListConcat<TCSqlTypeList<tfp_CLeftTypes...>, TCSqlTypeList<tfp_CRightTypes...>, tfp_CLists...>
	{
		using CType = typename TCSqlTypeListConcat<TCSqlTypeList<tfp_CLeftTypes..., tfp_CRightTypes...>, tfp_CLists...>::CType;
	};

	template <>
	struct TCSqlParameterMembersMatch<TCSqlMemberList<>>
	{
		static constexpr bool fs_Match()
		{
			return true;
		}
	};

	template <auto tf_pMember, auto ...tfp_pMembers, typename tf_CParam, typename ...tfp_CParams>
	struct TCSqlParameterMembersMatch<TCSqlMemberList<tf_pMember, tfp_pMembers...>, tf_CParam, tfp_CParams...>
	{
		static constexpr bool fs_Match()
		{
			return fg_SqlPreparedInsertValueMatchesMember<tf_pMember, tf_CParam>() && TCSqlParameterMembersMatch<TCSqlMemberList<tfp_pMembers...>, tfp_CParams...>::fs_Match();
		}
	};

	template <typename tf_CMemberList>
	struct TCSqlTypeListForMemberList;

	template <typename tf_CPreparedSelect>
	struct TCSqlPreparedSelectParameterTypesList;

	template <typename tf_CPreparedSelect>
	struct TCSqlPreparedSelectNestedParameterTypesList;

	template <auto ...tfp_pMembers>
	struct TCSqlTypeListForMemberList<TCSqlMemberList<tfp_pMembers...>>
	{
		using CType = TCSqlTypeList<typename TCSqlMemberPointerTraits<tfp_pMembers>::CMember...>;
	};

	template <typename tf_CPredicate>
	struct TCSqlPredicateParameterTypes
	{
		using CType = typename TCSqlTypeListForMemberList<typename TCSqlPredicateParameterMembers<tf_CPredicate>::CType>::CType;
	};

	template <auto tf_pMember, auto &tf_Subquery>
	struct TCSqlPredicateParameterTypes<TCSqlInSubqueryPredicate<tf_pMember, tf_Subquery>>
	{
		using CSubquery = NTraits::TCDecay<decltype(tf_Subquery)>;
		using CType = typename TCSqlPreparedSelectNestedParameterTypesList<CSubquery>::CType;
	};

	template <auto &tf_Subquery, ESqlPredicateType tf_Type>
	struct TCSqlPredicateParameterTypes<TCSqlExistsPredicate<tf_Subquery, tf_Type>>
	{
		using CSubquery = NTraits::TCDecay<decltype(tf_Subquery)>;
		using CType = typename TCSqlPreparedSelectNestedParameterTypesList<CSubquery>::CType;
	};

	template <typename tf_CLeft, typename tf_CRight, ESqlPredicateType tf_Type>
	struct TCSqlPredicateParameterTypes<TCSqlBinaryPredicate<tf_CLeft, tf_CRight, tf_Type>>
	{
		using CType = typename TCSqlTypeListConcat<typename TCSqlPredicateParameterTypes<tf_CLeft>::CType, typename TCSqlPredicateParameterTypes<tf_CRight>::CType>::CType;
	};

	template <typename tf_CPredicate>
	struct TCSqlPredicateParameterTypes<TCSqlNotPredicate<tf_CPredicate>> : public TCSqlPredicateParameterTypes<tf_CPredicate>
	{
	};

	template <typename tf_CHaving>
	struct TCSqlHavingParameterTypes;

	template <>
	struct TCSqlHavingParameterTypes<CSqlNoHaving>
	{
		using CType = TCSqlTypeList<>;
	};

	template <typename tf_CExpression, ESqlPredicateType tf_Type>
	struct TCSqlHavingParameterTypes<TCSqlHavingAggregatePredicate<tf_CExpression, tf_Type>>
	{
		using CResult = typename TCSqlExpressionResultType<tf_CExpression>::CType;
		using CType = TCSqlTypeList<NStorage::TCOptionalType<CResult>>;
	};

	template <typename tf_CTypeList, typename ...tfp_CParams>
	struct TCSqlParameterTypesMatch;

	template <typename tf_CPreparedSelect>
	struct TCSqlPreparedSelectParameterTypesList;

	template <>
	struct TCSqlParameterTypesMatch<TCSqlTypeList<>>
	{
		static constexpr bool fs_Match()
		{
			return true;
		}
	};

	template <typename tf_CType, typename ...tfp_CTypes, typename tf_CParam, typename ...tfp_CParams>
	struct TCSqlParameterTypesMatch<TCSqlTypeList<tf_CType, tfp_CTypes...>, tf_CParam, tfp_CParams...>
	{
		static constexpr bool fs_Match()
		{
			return fg_SqlInsertValueMatchesMember<tf_CType, tf_CParam>() && TCSqlParameterTypesMatch<TCSqlTypeList<tfp_CTypes...>, tfp_CParams...>::fs_Match();
		}
	};

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	constexpr bool fg_SqlPreparedSelectParametersMatch()
	{
		using CPreparedSelect = NTraits::TCDecay<decltype(tf_PreparedSelect)>;
		using CParameterTypes = typename TCSqlPreparedSelectParameterTypesList<CPreparedSelect>::CType;
		if constexpr (CParameterTypes::mc_nTypes != sizeof...(tfp_CParams))
			return false;
		else
			return TCSqlParameterTypesMatch<CParameterTypes, tfp_CParams...>::fs_Match();
	}

	template <auto &tf_PreparedSelect, typename tf_CParam>
	constexpr bool fg_SqlPreparedSelectParameterMatches()
	{
		return fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam>();
	}

	template <auto &tf_PreparedSelect>
	constexpr bool fg_SqlPreparedSelectHasNoParameters()
	{
		return fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect>();
	}

	template <typename tf_CColumn>
	constexpr bool fg_SqlColumnIsGenerated(tf_CColumn const &_Column)
	{
		constexpr NStr::CStr const *c_pEmptyStr = &NStr::gc_Str<"">.m_Str;
		for (umint iOption = 0; iOption < _Column.m_Options.m_nNonPortableOptions; ++iOption)
		{
			if (_Column.m_Options.m_NonPortableOptions[iOption].m_pGeneratedSql != c_pEmptyStr)
				return true;
		}

		return false;
	}

	template <typename tf_CColumn>
	constexpr bool fg_SqlPreparedInsertColumnIsImplicitlySelected(tf_CColumn const &_Column)
	{
		// A generated column (GENERATED ALWAYS) computes its own value and cannot be inserted, and an autoincrement
		// primary key is assigned by the database, so neither belongs in an implicit insert/upsert column list.
		if (fg_SqlColumnIsGenerated(_Column))
			return false;

		return !(fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_PrimaryKey) && fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement));
	}

	template <auto ...tfp_pMembers>
	struct TCSqlPreparedInsertValueMatcher;

	template <>
	struct TCSqlPreparedInsertValueMatcher<>
	{
		template <typename ...tfp_CValues>
		static constexpr bool fs_Matches()
		{
			return sizeof...(tfp_CValues) == 0;
		}
	};

	template <auto tf_pMember, auto ...tfp_pMembers>
	struct TCSqlPreparedInsertValueMatcher<tf_pMember, tfp_pMembers...>
	{
		static constexpr bool fs_Matches()
		{
			return false;
		}

		template <typename tf_CValue, typename ...tfp_CValues>
		static constexpr bool fs_Matches()
		{
			if constexpr (sizeof...(tfp_pMembers) != sizeof...(tfp_CValues))
				return false;
			else
				return fg_SqlPreparedInsertValueMatchesMember<tf_pMember, tf_CValue>() && TCSqlPreparedInsertValueMatcher<tfp_pMembers...>::template fs_Matches<tfp_CValues...>();
		}
	};

	template <auto &tf_PreparedInsert, umint tf_iColumn, umint tf_iValue, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedInsertImplicitValuesMatch()
	{
		using CPreparedInsert = NTraits::TCDecay<decltype(tf_PreparedInsert)>;
		if constexpr (tf_iColumn == CPreparedInsert::CTable::mc_nColumns)
			return tf_iValue == sizeof...(tfp_CValues);
		else
		{
			constexpr auto const &Column = std::get<tf_iColumn>(tf_PreparedInsert.m_Table.m_Columns.m_Columns);
			if constexpr (fg_SqlPreparedInsertColumnIsImplicitlySelected(Column))
			{
				if constexpr (tf_iValue == sizeof...(tfp_CValues))
					return false;
				else
				{
					using CMember = typename NTraits::TCRemoveReferenceAndQualifiers<decltype(Column)>::CMember;
					using CValue = NStorage::TCTuple_Get<tf_iValue, NStorage::TCTuple<tfp_CValues...>>;

					return
						fg_SqlInsertValueMatchesMember<CMember, CValue>()
						&& fg_SqlPreparedInsertImplicitValuesMatch<tf_PreparedInsert, tf_iColumn + 1, tf_iValue + 1, tfp_CValues...>()
					;
				}
			}
			else
				return fg_SqlPreparedInsertImplicitValuesMatch<tf_PreparedInsert, tf_iColumn + 1, tf_iValue, tfp_CValues...>();
		}
	}

	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedInsertImplicitRowValueMatches()
	{
		if constexpr (sizeof...(tfp_CValues) != 1)
			return false;
		else
		{
			using CPreparedInsert = NTraits::TCDecay<decltype(tf_PreparedInsert)>;
			using CValue = NStorage::TCTuple_Get<0, NStorage::TCTuple<tfp_CValues...>>;
			return !NTraits::cIsReference<CValue> && NTraits::cIsSame<NTraits::TCRemoveReferenceAndQualifiers<CValue>, typename CPreparedInsert::CRow>;
		}
	}

	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedInsertValuesMatch()
	{
		using CPreparedInsert = NTraits::TCDecay<decltype(tf_PreparedInsert)>;
		return []<auto ...tfp_pMembers>(TCSqlPreparedInsert<typename CPreparedInsert::CTable, tfp_pMembers...> const *)
		{
			if constexpr (sizeof...(tfp_pMembers) == 0)
				return fg_SqlPreparedInsertImplicitRowValueMatches<tf_PreparedInsert, tfp_CValues...>() || fg_SqlPreparedInsertImplicitValuesMatch<tf_PreparedInsert, 0, 0, tfp_CValues...>();
			else
				return TCSqlPreparedInsertValueMatcher<tfp_pMembers...>::template fs_Matches<tfp_CValues...>();
		}(static_cast<CPreparedInsert const *>(nullptr));
	}

	template <typename tf_CSet>
	struct TCSqlSetParameterMembers;

	template <auto ...tfp_pMembers>
	struct TCSqlSetParameterMembers<TCSqlSelectedColumns<tfp_pMembers...>>
	{
		using CType = TCSqlMemberList<tfp_pMembers...>;
	};

	template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedUpdateValuesMatch()
	{
		// Use predicate parameter types rather than members so a subquery predicate's nested parameters (HAVING,
		// LIMIT/OFFSET, set operands) are counted - the generated SQL emits placeholders for all of them.
		using CPreparedUpdate = NTraits::TCDecay<decltype(tf_PreparedUpdate)>;
		using CSetTypes = typename TCSqlTypeListForMemberList<typename TCSqlSetParameterMembers<typename CPreparedUpdate::CSet>::CType>::CType;
		using CPredicateTypes = typename TCSqlPredicateParameterTypes<typename CPreparedUpdate::CPredicate>::CType;
		using CParameterTypes = typename TCSqlTypeListConcat<CSetTypes, CPredicateTypes>::CType;
		if constexpr (CParameterTypes::mc_nTypes != sizeof...(tfp_CValues))
			return false;
		else
			return TCSqlParameterTypesMatch<CParameterTypes, tfp_CValues...>::fs_Match();
	}

	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedDeleteValuesMatch()
	{
		using CPreparedDelete = NTraits::TCDecay<decltype(tf_PreparedDelete)>;
		using CParameterTypes = typename TCSqlPredicateParameterTypes<typename CPreparedDelete::CPredicate>::CType;
		if constexpr (CParameterTypes::mc_nTypes != sizeof...(tfp_CValues))
			return false;
		else
			return TCSqlParameterTypesMatch<CParameterTypes, tfp_CValues...>::fs_Match();
	}

	template <typename tf_CTable, typename ...tfp_CValues>
	constexpr bool fg_SqlTableInsertValuesMatch();
	template <auto &tf_Table, typename ...tfp_CValues>
	constexpr bool fg_SqlTableImplicitInsertValuesMatch();

	template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
	constexpr bool fg_SqlPreparedUpsertValuesMatch()
	{
		// An upsert describes its INSERT column list through the implicit-column path, which omits an auto-generated
		// first column, so the values it accepts must match those implicit columns - not the full row.
		return fg_SqlTableImplicitInsertValuesMatch<tf_PreparedUpsert.m_Table, tfp_CValues...>();
	}

	template <typename tf_CTable, umint tf_iColumn, typename ...tfp_CValues>
	constexpr bool fg_SqlTableInsertValuesMatchFrom()
	{
		if constexpr (sizeof...(tfp_CValues) + tf_iColumn != tf_CTable::mc_nColumns)
			return false;
		else
			return []<umint ...tfp_iValues>(std::index_sequence<tfp_iValues...>)
			{
				return
					(
						[]<umint tf_iValue>()
						{
							using CColumn = NStorage::TCTuple_Get<tf_iColumn + tf_iValue, typename tf_CTable::CColumns::CColumns>;
							using CMember = typename CColumn::CMember;
							using CValue = NStorage::TCTuple_Get<tf_iValue, NStorage::TCTuple<tfp_CValues...>>;

							return
								fg_SqlInsertValueMatchesMember<CMember, CValue>()
							;
						}.template operator()<tfp_iValues>()
						&& ...
					)
				;
			}(std::make_index_sequence<sizeof...(tfp_CValues)>())
		;
	}

	template <typename tf_CTable, typename ...tfp_CValues>
	constexpr bool fg_SqlTableInsertValuesMatch()
	{
		if constexpr (sizeof...(tfp_CValues) == 0)
			return false;
		else
			return fg_SqlTableInsertValuesMatchFrom<tf_CTable, 0, tfp_CValues...>();
	}

	// True when every column of the table maps to a nullable (TCOptional) member. The right side of a LEFT JOIN must
	// satisfy this: an unmatched left row returns NULL for every right-side column, and the row mapper rejects NULL for
	// a non-nullable member.
	template <typename tf_CTable>
	consteval bool fg_SqlTableMembersAllNullable()
	{
		return []<umint ...tfp_iColumns>(std::index_sequence<tfp_iColumns...>)
		{
			return
				(
					NStorage::cIsOptional<typename NStorage::TCTuple_Get<tfp_iColumns, typename tf_CTable::CColumns::CColumns>::CMember>
					&& ...
				)
			;
		}(std::make_index_sequence<tf_CTable::mc_nColumns>{});
	}

	template <auto &tf_Table, umint tf_iColumn, umint tf_iValue, typename ...tfp_CValues>
	constexpr bool fg_SqlTableImplicitInsertValuesMatchFrom()
	{
		using CTable = NTraits::TCDecay<decltype(tf_Table)>;
		if constexpr (tf_iColumn == CTable::mc_nColumns)
			return tf_iValue == sizeof...(tfp_CValues);
		else
		{
			constexpr auto const &Column = std::get<tf_iColumn>(tf_Table.m_Columns.m_Columns);
			if constexpr (!fg_SqlPreparedInsertColumnIsImplicitlySelected(Column))
				return fg_SqlTableImplicitInsertValuesMatchFrom<tf_Table, tf_iColumn + 1, tf_iValue, tfp_CValues...>();
			else if constexpr (tf_iValue == sizeof...(tfp_CValues))
				return false;
			else
			{
				using CMember = typename NTraits::TCRemoveReferenceAndQualifiers<decltype(Column)>::CMember;
				using CValue = NStorage::TCTuple_Get<tf_iValue, NStorage::TCTuple<tfp_CValues...>>;
				return
					fg_SqlInsertValueMatchesMember<CMember, CValue>()
					&& fg_SqlTableImplicitInsertValuesMatchFrom<tf_Table, tf_iColumn + 1, tf_iValue + 1, tfp_CValues...>()
				;
			}
		}
	}

	template <auto &tf_Table, typename ...tfp_CValues>
	constexpr bool fg_SqlTableImplicitInsertValuesMatch()
	{
		if constexpr (sizeof...(tfp_CValues) == 0)
			return false;
		else
		{
			// Match the implicit insert columns exactly: the columns the database fills (an autoincrement primary key
			// and any generated column) are omitted from the column list wherever they appear, so their values must be
			// omitted too. Accepting the full row would bind more values than the statement has placeholders.
			return fg_SqlTableImplicitInsertValuesMatchFrom<tf_Table, 0, 0, tfp_CValues...>();
		}
	}

	template <typename tf_CTable>
	CSqlInsertOperation fg_SqlInsertOperation(tf_CTable const &_Table, typename tf_CTable::CRow &&_Row);
	template <typename tf_CTable, typename ...tfp_CValues>
	CSqlInsertOperation fg_SqlInsertOperation(tf_CTable const &_Table, tfp_CValues &&...p_Values)
		requires (fg_SqlTableInsertValuesMatch<tf_CTable, tfp_CValues...>())
	;
	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	CSqlInsertOperation fg_SqlInsertOperation(tfp_CValues &&...p_Values);
	template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
	CSqlUpdateOperation fg_SqlUpdateOperation(tfp_CValues &&...p_Values);
	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	CSqlDeleteOperation fg_SqlDeleteOperation(tfp_CValues &&...p_Values);
	template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
	CSqlUpsertOperation fg_SqlUpsertOperation(tfp_CValues &&...p_Values);

	template <auto &tf_PreparedSelect, typename tf_CParam>
	CSqlSelectOperation fg_SqlSelectOperation(tf_CParam _Param, CSqlSelectSettings _Settings)
		requires (fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	;
	template <auto &tf_PreparedSelect, typename tf_CParam>
	CSqlSelectOperation fg_SqlSelectOperation(CSqlSelectSettings _Settings, tf_CParam _Param)
		requires (fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	;
	template <auto ...tfp_pMembers>
	using TCSqlProjectionRow = NStorage::TCTuple<typename TCSqlMemberPointerTraits<tfp_pMembers>::CMember...>;
	template <typename tf_CResult, auto ...tfp_pMembers>
	struct TCSqlProjectionBackendRowType
	{
		using CType = TCSqlProjectionRow<tfp_pMembers...>;
	};
	template <typename ...tfp_CTypes, auto ...tfp_pMembers>
	struct TCSqlProjectionBackendRowType<NStorage::TCTuple<tfp_CTypes...>, tfp_pMembers...>
	{
		static_assert(sizeof...(tfp_CTypes) == sizeof...(tfp_pMembers), "SQL tuple projection result must match selected member count");

		using CType = NStorage::TCTuple<tfp_CTypes...>;
	};
	template <typename tf_CResult, auto tf_pMember>
	struct TCSqlProjectionBackendRowType<tf_CResult, tf_pMember>
	{
		using CMember = typename TCSqlMemberPointerTraits<tf_pMember>::CMember;
		using CResult = NTraits::TCRemoveReferenceAndQualifiers<tf_CResult>;

		using CType = TCConditional<NTraits::cIsSame<CResult, CMember>, CResult, TCSqlProjectionRow<tf_pMember>>;
	};
	template <typename tf_CResult, auto ...tfp_pMembers>
	using TCSqlProjectionBackendRow = typename TCSqlProjectionBackendRowType<tf_CResult, tfp_pMembers...>::CType;
	template <typename tf_CProjectionRow, umint tf_iMember>
	struct TCSqlProjectionMemberType
	{
		static_assert(tf_iMember == 0, "SQL scalar projection row can only map one selected member");

		using CType = tf_CProjectionRow;
	};
	template <typename ...tfp_CTypes, umint tf_iMember>
	struct TCSqlProjectionMemberType<NStorage::TCTuple<tfp_CTypes...>, tf_iMember>
	{
		using CType = NStorage::TCTuple_Get<tf_iMember, NStorage::TCTuple<tfp_CTypes...>>;
	};
	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	CSqlSelectOperation fg_SqlProjectionSelectOperation(CSqlSelectSettings _Settings, tfp_CParams ...p_Params)
		requires (fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	;
}
