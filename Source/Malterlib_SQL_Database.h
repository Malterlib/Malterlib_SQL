// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/Concurrency/AsyncGenerator>
#include <Mib/Concurrency/ActorInterface>
#include <Mib/Concurrency/ConcurrencyManager>
#include <Mib/Function/Function>
#include <Mib/SQL/DatabaseSchema>
#include <Mib/SQL/SQL>
#include <Mib/Storage/Tuple>
#include <Mib/Storage/UniquePointer>
#include <Mib/Storage/Variant>

namespace NMib::NSQL
{
	using CSqlValue = NStorage::TCStreamableVariant
		<
			ESqlValueType
			, NStorage::TCMember<void, ESqlValueType::mc_Null>
			, NStorage::TCMember<int8, ESqlValueType::mc_Integer8>
			, NStorage::TCMember<int16, ESqlValueType::mc_Integer16>
			, NStorage::TCMember<int32, ESqlValueType::mc_Integer32>
			, NStorage::TCMember<int64, ESqlValueType::mc_Integer64>
			, NStorage::TCMember<uint8, ESqlValueType::mc_UnsignedInteger8>
			, NStorage::TCMember<uint16, ESqlValueType::mc_UnsignedInteger16>
			, NStorage::TCMember<uint32, ESqlValueType::mc_UnsignedInteger32>
			, NStorage::TCMember<uint64, ESqlValueType::mc_UnsignedInteger64>
			, NStorage::TCMember<fp32, ESqlValueType::mc_Float32>
			, NStorage::TCMember<fp64, ESqlValueType::mc_Float64>
			, NStorage::TCMember<NStr::CStr, ESqlValueType::mc_Text>
			, NStorage::TCMember<NContainer::CIOByteVector, ESqlValueType::mc_Blob>
			, NStorage::TCMember<bool, ESqlValueType::mc_Boolean>
			, NStorage::TCMember<NTime::CTime, ESqlValueType::mc_Time>
			, NStorage::TCMember<NCryptography::CUniversallyUniqueIdentifier, ESqlValueType::mc_UUID>
			, NStorage::TCMember<CSqlDate, ESqlValueType::mc_Date>
			, NStorage::TCMember<CSqlTimeOfDay, ESqlValueType::mc_TimeOfDay>
			, NStorage::TCMember<CSqlTimestamp, ESqlValueType::mc_Timestamp>
			, NStorage::TCMember<CSqlTimestampTz, ESqlValueType::mc_TimestampTz>
			, NStorage::TCMember<CSqlInterval, ESqlValueType::mc_Interval>
			, NStorage::TCMember<NEncoding::CJsonOrdered, ESqlValueType::mc_Json>
			, NStorage::TCMember<NEncoding::CJsonSorted, ESqlValueType::mc_Jsonb>
			, NStorage::TCMember<CSqlUnrecognizedBackendValue, ESqlValueType::mc_UnrecognizedBackend>
			, NStorage::TCMember<TCSqlArray<int16>, ESqlValueType::mc_Array_Integer16>
			, NStorage::TCMember<TCSqlArray<int32>, ESqlValueType::mc_Array_Integer32>
			, NStorage::TCMember<TCSqlArray<int64>, ESqlValueType::mc_Array_Integer64>
			, NStorage::TCMember<TCSqlArray<fp32>, ESqlValueType::mc_Array_Float32>
			, NStorage::TCMember<TCSqlArray<fp64>, ESqlValueType::mc_Array_Float64>
			, NStorage::TCMember<TCSqlArray<NStr::CStr>, ESqlValueType::mc_Array_Text>
			, NStorage::TCMember<TCSqlArray<bool>, ESqlValueType::mc_Array_Boolean>
			, NStorage::TCMember<TCSqlArray<NContainer::CIOByteVector>, ESqlValueType::mc_Array_Bytes>
			, NStorage::TCMember<TCSqlArray<CSqlDate>, ESqlValueType::mc_Array_Date>
			, NStorage::TCMember<TCSqlArray<CSqlTimeOfDay>, ESqlValueType::mc_Array_TimeOfDay>
			, NStorage::TCMember<TCSqlArray<CSqlTimestamp>, ESqlValueType::mc_Array_Timestamp>
			, NStorage::TCMember<TCSqlArray<CSqlTimestampTz>, ESqlValueType::mc_Array_TimestampTz>
			, NStorage::TCMember<TCSqlArray<NCryptography::CUniversallyUniqueIdentifier>, ESqlValueType::mc_Array_UUID>
			, NStorage::TCMember<TCSqlArray<NEncoding::CJsonOrdered>, ESqlValueType::mc_Array_Json>
			, NStorage::TCMember<TCSqlArray<NEncoding::CJsonSorted>, ESqlValueType::mc_Array_Jsonb>
			, NStorage::TCMember<TCSqlArray<CSqlInterval>, ESqlValueType::mc_Array_Interval>
		>
	;

	struct CSqlColumnValue
	{
		NStr::CStr m_ColumnName;
		CSqlValue m_Value;
	};

	struct ICRowData
	{
		virtual ~ICRowData() = default;
	};

	template <typename t_CData>
	struct TCRowData : public ICRowData
	{
		t_CData m_Data;
	};

	using CSqlRowDataPointer = NStorage::TCUniquePointer<ICRowData>;
	using CSqlRowDataBatch = NContainer::TCVector<CSqlRowDataPointer>;

	struct CSqlRowFieldMapping
	{
		NStr::CStr m_ColumnName;
		ESqlValueType m_ValueType = ESqlValueType::mc_Text;
		bool m_bNullable = false;
		umint m_Offset = 0;
		NException::CExceptionPointer (*m_fSetValue)(void *, CSqlValue &&, NStr::CStr const &) = nullptr;
	};

	struct CSqlRowMapping
	{
		CSqlRowDataPointer f_CreateRow() const;

		NContainer::TCVector<CSqlRowFieldMapping> m_Fields;
		CSqlRowDataPointer (*m_fCreateRow)() = nullptr;
	};

	struct CSqlQueryID
	{
		constexpr auto operator <=> (CSqlQueryID const &_Other) const noexcept = default;
		constexpr bool operator == (CSqlQueryID const &_Other) const = default;

		uint64 m_Value = 0;
	};

	struct ICSqlPreparedSelectStatement;
	struct ICSqlPreparedInsertStatement;

	struct CSqlParameterTypesDescription
	{
		constexpr ESqlValueType f_GetType(umint _iParameter) const;

		CSqlQueryID m_QueryID;
		ESqlValueType const *m_pTypes = nullptr;
		umint m_nTypes = 0;
	};

	struct CSqlSelectOperationDescription
	{
		ICSqlPreparedSelectStatement const *m_pStatement = nullptr;
		CSqlQueryID m_QueryID;
		CSqlParameterTypesDescription m_ParameterTypes;
	};

	enum class ESqlPredicateType : uint8
	{
		mc_EqualParameter
		, mc_NotEqualParameter
		, mc_LessParameter
		, mc_LessEqualParameter
		, mc_GreaterParameter
		, mc_GreaterEqualParameter
		, mc_LikeParameter
		, mc_IsNull
		, mc_IsNotNull
		, mc_InParameters
		, mc_InSubquery
		, mc_Exists
		, mc_NotExists
		, mc_And
		, mc_Or
		, mc_Not
		, mc_AllRows
	};

	enum class ESqlSelectExpressionType : uint8
	{
		mc_Column
		, mc_Count
		, mc_Sum
		, mc_Avg
		, mc_Min
		, mc_Max
		, mc_Add
		, mc_Subtract
		, mc_Multiply
		, mc_Divide
		, mc_Lower
		, mc_Upper
		, mc_Length
		, mc_CastFloat
		, mc_BackendFunction
	};

	struct CSqlSelectExpressionDescription
	{
		ESqlSelectExpressionType m_Type = ESqlSelectExpressionType::mc_Count;
		NStr::CStr m_ColumnName;
		NStr::CStr m_LeftColumnName;
		NStr::CStr m_RightColumnName;
		NStr::CStr m_FunctionName;
		ESqlValueType m_ResultType = ESqlValueType::mc_Null;
	};

	struct CSqlPredicateDescription
	{
		ESqlPredicateType m_Type = ESqlPredicateType::mc_EqualParameter;
		NStr::CStr m_ColumnName;
		CSqlSelectExpressionDescription m_Expression;
		bool m_bExpression = false;
		umint m_iParameter = 0;
		umint m_nParameters = 1;
		ICSqlPreparedSelectStatement const *m_pSubqueryStatement = nullptr;
		NContainer::TCVector<CSqlPredicateDescription> m_Children;
	};

	struct CSqlOrderByDescription
	{
		NStr::CStr m_ColumnName;
		bool m_bDescending = false;
	};

	struct CSqlGroupByDescription
	{
		NStr::CStr m_ColumnName;
	};

	struct CSqlQualifiedColumnDescription
	{
		umint m_iTable = 0;
		NStr::CStr m_ColumnName;
	};

	struct CSqlJoinOnDescription
	{
		ESqlPredicateType m_Type = ESqlPredicateType::mc_EqualParameter;
		umint m_iLeftTable = 0;
		umint m_iRightTable = 1;
		NStr::CStr m_LeftColumnName;
		NStr::CStr m_RightColumnName;
	};

	enum class ESqlJoinType : uint8
	{
		mc_Inner
		, mc_Left
	};

	enum class ESqlSetOperationType : uint8
	{
		mc_Union
		, mc_UnionAll
		, mc_Intersect
		, mc_Except
	};

	struct CSqlSetOperationDescription
	{
		ESqlSetOperationType m_Type = ESqlSetOperationType::mc_Union;
		ICSqlPreparedSelectStatement const *m_pStatement = nullptr;
	};

	struct CSqlJoinDescription
	{
		ESqlJoinType m_Type = ESqlJoinType::mc_Inner;
		NStr::CStr m_TableName;
		NContainer::TCVector<CSqlJoinOnDescription> m_On;
	};

	struct CSqlLimitOffsetDescription
	{
		bool m_bHasLimit = false;
		bool m_bHasOffset = false;
	};

	struct CSqlPreparedSelectStatementDescription
	{
		CSqlQueryID m_QueryID;
		NStr::CStr m_TableName;
		bool m_bDistinct = false;
		NContainer::TCVector<NStr::CStr> m_SelectColumns;
		NContainer::TCVector<CSqlQualifiedColumnDescription> m_QualifiedSelectColumns;
		NContainer::TCVector<CSqlSelectExpressionDescription> m_SelectExpressions;
		CSqlPredicateDescription m_Predicate;
		NContainer::TCVector<CSqlJoinDescription> m_Joins;
		NContainer::TCVector<CSqlSetOperationDescription> m_SetOperations;
		NContainer::TCVector<CSqlGroupByDescription> m_GroupBy;
		CSqlPredicateDescription m_Having;
		bool m_bHasHaving = false;
		NContainer::TCVector<CSqlOrderByDescription> m_OrderBy;
		CSqlLimitOffsetDescription m_LimitOffset;
		CSqlRowMapping m_RowMapping;
	};

	inline bool fg_SqlSelectExpressionIsAggregate(ESqlSelectExpressionType _Type)
	{
		switch (_Type)
		{
		case ESqlSelectExpressionType::mc_Count:
		case ESqlSelectExpressionType::mc_Sum:
		case ESqlSelectExpressionType::mc_Avg:
		case ESqlSelectExpressionType::mc_Min:
		case ESqlSelectExpressionType::mc_Max:
			return true;
		default:
			return false;
		}
	}

	inline bool fg_SqlSelectIsUngroupedAggregate(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		// An aggregate projection with no GROUP BY collapses the input to a single result row (e.g. SELECT COUNT(...)),
		// so a SELECT 1 existence rewrite would count the base rows instead of the single aggregate row and would make
		// EXISTS false over empty input even though the aggregate still returns a row. Such a projection must be kept.
		if (!_Statement.m_GroupBy.f_IsEmpty())
			return false;

		for (auto const &Expression : _Statement.m_SelectExpressions)
		{
			if (fg_SqlSelectExpressionIsAggregate(Expression.m_Type))
				return true;
		}

		return false;
	}

	struct CSqlPreparedInsertStatementDescription
	{
		CSqlQueryID m_QueryID;
		NStr::CStr m_TableName;
		NContainer::TCVector<NStr::CStr> m_InsertColumns;
		NContainer::TCVector<ESqlValueType> m_InsertColumnTypes;
	};

	struct CSqlPreparedUpdateStatementDescription
	{
		CSqlQueryID m_QueryID;
		NStr::CStr m_TableName;
		NContainer::TCVector<NStr::CStr> m_UpdateColumns;
		NContainer::TCVector<ESqlValueType> m_UpdateColumnTypes;
		CSqlPredicateDescription m_Predicate;
	};

	struct CSqlPreparedDeleteStatementDescription
	{
		CSqlQueryID m_QueryID;
		NStr::CStr m_TableName;
		CSqlPredicateDescription m_Predicate;
	};

	struct CSqlPreparedUpsertStatementDescription
	{
		CSqlQueryID m_QueryID;
		NStr::CStr m_TableName;
		NContainer::TCVector<NStr::CStr> m_InsertColumns;
		NContainer::TCVector<ESqlValueType> m_InsertColumnTypes;
		NContainer::TCVector<NStr::CStr> m_ConflictColumns;
		NContainer::TCVector<NStr::CStr> m_UpdateColumns;
	};

	struct CSqlInsertOperationDescription
	{
		ICSqlPreparedInsertStatement const *m_pStatement = nullptr;
		CSqlQueryID m_QueryID;
	};

	struct ICSqlPreparedUpdateStatement;
	struct ICSqlPreparedDeleteStatement;
	struct ICSqlPreparedUpsertStatement;

	struct CSqlUpdateOperationDescription
	{
		ICSqlPreparedUpdateStatement const *m_pStatement = nullptr;
		CSqlQueryID m_QueryID;
	};

	struct CSqlDeleteOperationDescription
	{
		ICSqlPreparedDeleteStatement const *m_pStatement = nullptr;
		CSqlQueryID m_QueryID;
	};

	struct CSqlUpsertOperationDescription
	{
		ICSqlPreparedUpsertStatement const *m_pStatement = nullptr;
		CSqlQueryID m_QueryID;
	};

	struct ICSqlPreparedSelectStatement
	{
		constexpr ICSqlPreparedSelectStatement() = default;
		constexpr ICSqlPreparedSelectStatement(CSqlQueryID _QueryID);

		virtual CSqlPreparedSelectStatementDescription f_Describe() const = 0;

		CSqlQueryID m_QueryID;
	};

	struct ICSqlPreparedInsertStatement
	{
		constexpr ICSqlPreparedInsertStatement() = default;
		constexpr ICSqlPreparedInsertStatement(CSqlQueryID _QueryID);

		virtual CSqlPreparedInsertStatementDescription f_Describe() const = 0;

		CSqlQueryID m_QueryID;
	};

	struct ICSqlPreparedUpdateStatement
	{
		constexpr ICSqlPreparedUpdateStatement() = default;
		constexpr ICSqlPreparedUpdateStatement(CSqlQueryID _QueryID);

		virtual CSqlPreparedUpdateStatementDescription f_Describe() const = 0;

		CSqlQueryID m_QueryID;
	};

	struct ICSqlPreparedDeleteStatement
	{
		constexpr ICSqlPreparedDeleteStatement() = default;
		constexpr ICSqlPreparedDeleteStatement(CSqlQueryID _QueryID);

		virtual CSqlPreparedDeleteStatementDescription f_Describe() const = 0;

		CSqlQueryID m_QueryID;
	};

	struct ICSqlPreparedUpsertStatement
	{
		constexpr ICSqlPreparedUpsertStatement() = default;
		constexpr ICSqlPreparedUpsertStatement(CSqlQueryID _QueryID);

		virtual CSqlPreparedUpsertStatementDescription f_Describe() const = 0;

		CSqlQueryID m_QueryID;
	};

	struct CSqlInsertOperation
	{
		CSqlInsertOperationDescription const *m_pDescription = nullptr;
		NStr::CStr m_TableName;
		NContainer::TCVector<CSqlColumnValue> m_Values;
		NStr::CStr m_ReturningColumnName;
		ESqlValueType m_ReturningValueType = ESqlValueType::mc_Null;
		bool m_bReturning = false;
	};

	using CSqlBulkInsertRow = NContainer::TCVector<CSqlColumnValue>;
	using CSqlBulkInsertRowBatch = NContainer::TCVector<CSqlBulkInsertRow>;

	struct CSqlBulkInsertOperation
	{
		CSqlInsertOperationDescription const *m_pDescription = nullptr;
		NConcurrency::TCAsyncGenerator<CSqlBulkInsertRowBatch> m_RowBatches;
	};

	struct CSqlSelectOperation
	{
		CSqlSelectOperationDescription const *m_pDescription = nullptr;
		NContainer::TCVector<CSqlValue> m_Parameters;
		umint m_nRowsPerBatch = 0;
		umint m_nResultRowLimit = 0;
		umint m_nResultRowOffset = 0;
	};

	struct CSqlUpdateOperation
	{
		CSqlUpdateOperationDescription const *m_pDescription = nullptr;
		NContainer::TCVector<CSqlValue> m_Values;
		NStr::CStr m_ReturningColumnName;
		ESqlValueType m_ReturningValueType = ESqlValueType::mc_Null;
		bool m_bReturning = false;
	};

	struct CSqlDeleteOperation
	{
		CSqlDeleteOperationDescription const *m_pDescription = nullptr;
		NContainer::TCVector<CSqlValue> m_Values;
		NStr::CStr m_ReturningColumnName;
		ESqlValueType m_ReturningValueType = ESqlValueType::mc_Null;
		bool m_bReturning = false;
	};

	struct CSqlUpsertOperation
	{
		CSqlUpsertOperationDescription const *m_pDescription = nullptr;
		NContainer::TCVector<CSqlColumnValue> m_Values;
		NStr::CStr m_ReturningColumnName;
		ESqlValueType m_ReturningValueType = ESqlValueType::mc_Null;
		bool m_bReturning = false;
	};

	struct CSqlSelectSettings
	{
		umint m_nRowsPerBatch = 0;
		umint m_nResultRowLimit = 0;
		umint m_nResultRowOffset = 0;
	};

	enum class ESqlRawBackend : uint8
	{
		mc_Any
		, mc_SQLite
		, mc_Postgres
	};

	struct CSqlRawOperation
	{
		NStr::CStr m_Sql;
		NContainer::TCVector<CSqlValue> m_Parameters;
		ESqlRawBackend m_BackendRequirement = ESqlRawBackend::mc_Any;
		umint m_nRowsPerBatch = 0;
	};

	struct CSqlRawColumnDescription
	{
		NStr::CStr m_Name;
		ESqlValueType m_ValueType = ESqlValueType::mc_Null;
		uint32 m_BackendTypeID = 0;
	};

	struct CSqlRawRow
	{
		NContainer::TCVector<CSqlValue> m_Values;
	};

	using CSqlRawRowBatch = NContainer::TCVector<CSqlRawRow>;

	struct CSqlRawResult
	{
		NContainer::TCVector<CSqlRawColumnDescription> m_Columns;
		NContainer::TCVector<CSqlRawRow> m_Rows;
	};

	struct CSqlRawStream
	{
		NContainer::TCVector<CSqlRawColumnDescription> m_Columns;
		NConcurrency::TCAsyncGenerator<CSqlRawRowBatch> m_Rows;
	};

	CSqlRawOperation fg_SqlRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});
	CSqlRawOperation fg_SqlPostgresRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});
	CSqlRawOperation fg_SqlSqliteRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});

	enum class ESqlSaveResult : uint8
	{
		mc_Inserted
		, mc_Updated
		, mc_StaleOrMissing
	};

	template <typename t_CRow>
	struct TCSqlSaveResult
	{
		ESqlSaveResult m_Result = ESqlSaveResult::mc_StaleOrMissing;
		t_CRow m_Row;
	};

	template <auto ...tfp_pMembers>
	struct TCSqlCompositeID
	{
		static constexpr umint mc_nMembers = sizeof...(tfp_pMembers);
	};

	struct ICSqlTransactionActor : public NConcurrency::CActor
	{
		virtual NConcurrency::TCFuture<void> f_Insert(CSqlInsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_InsertMany(CSqlBulkInsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlValue> f_InsertReturning(CSqlInsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_Upsert(CSqlUpsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlValue> f_UpsertReturning(CSqlUpsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_Update(CSqlUpdateOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlValue> f_UpdateReturning(CSqlUpdateOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_Delete(CSqlDeleteOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlValue> f_DeleteReturning(CSqlDeleteOperation _Operation) = 0;
		virtual NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> f_Select(CSqlSelectOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_Count(CSqlSelectOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<bool> f_Exists(CSqlSelectOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_ExecuteRaw(CSqlRawOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlRawResult> f_QueryRaw(CSqlRawOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlRawStream> f_QueryRawStream(CSqlRawOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<NStr::CStr> f_CreateSavepoint() = 0;
		virtual NConcurrency::TCFuture<void> f_ReleaseSavepoint(NStr::CStr _Name) = 0;
		virtual NConcurrency::TCFuture<void> f_RollbackToSavepoint(NStr::CStr _Name) = 0;
		virtual NConcurrency::TCFuture<void> f_CommitTransaction() = 0;
		virtual NConcurrency::TCFuture<void> f_RollbackTransaction() = 0;
	};

	using CSqlTransactionInterface = NConcurrency::TCActorInterface<ICSqlTransactionActor>;

	struct CSqlDatabaseBackendCapabilities
	{
		ESqlDialect m_Dialect = ESqlDialect::mc_None;
		bool m_bReadTransactions = false;
		bool m_bTransactionalDDL = false;
		bool m_bTableRename = false;
		bool m_bColumnRename = false;
		bool m_bTableRebuild = false;
		bool m_bDropColumn = false;
		bool m_bForeignKeyEnforcement = false;
		bool m_bNumberedPlaceholders = false;
		bool m_bMutationReturning = false;
		bool m_bUUID = false;
		bool m_bDate = false;
		bool m_bTimeOfDay = false;
		bool m_bTimestamp = false;
		bool m_bTimestampTz = false;
		bool m_bInterval = false;
		bool m_bJSON = false;
		bool m_bJSONB = false;
		bool m_bArrays = false;
		bool m_bUnrecognizedBackend = false;
		bool m_bIsolationReadCommitted = false;
		bool m_bIsolationRepeatableRead = false;
		bool m_bIsolationSerializable = false;

		bool f_SupportsColumnType(ESqlColumnType _Type) const;
		bool f_SupportsValueType(ESqlValueType _Type) const;
	};

	NStr::CStr fg_SqlColumnTypeName(ESqlColumnType _Type);
	NStr::CStr fg_SqlValueTypeName(ESqlValueType _Type);

	struct ICSqlDatabaseBackendActor : public NConcurrency::CActor
	{
		virtual CSqlDatabaseBackendCapabilities f_Capabilities() const = 0;
		virtual NConcurrency::TCFuture<void> f_Open() = 0;
		virtual NConcurrency::TCFuture<void> f_ApplySchema() = 0;
		virtual NConcurrency::TCFuture<void> f_Insert(CSqlInsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_InsertMany(CSqlBulkInsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlValue> f_InsertReturning(CSqlInsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_Upsert(CSqlUpsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlValue> f_UpsertReturning(CSqlUpsertOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_Update(CSqlUpdateOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlValue> f_UpdateReturning(CSqlUpdateOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_Delete(CSqlDeleteOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlValue> f_DeleteReturning(CSqlDeleteOperation _Operation) = 0;
		virtual NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> f_Select(CSqlSelectOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_Count(CSqlSelectOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<bool> f_Exists(CSqlSelectOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<umint> f_ExecuteRaw(CSqlRawOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlRawResult> f_QueryRaw(CSqlRawOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlRawStream> f_QueryRawStream(CSqlRawOperation _Operation) = 0;
		virtual NConcurrency::TCFuture<CSqlTransactionInterface> f_BeginTransaction(CSqlTransactionSettings _Settings = {}) = 0;
		virtual NConcurrency::TCFuture<CSqlTransactionInterface> f_BeginReadTransaction(CSqlTransactionSettings _Settings = {}) = 0;
	};

	template <auto t_pMember, ESqlPredicateType t_Type>
	struct TCSqlParameterPredicate
	{
		static constexpr auto mc_pMember = t_pMember;
		static constexpr ESqlPredicateType mc_Type = t_Type;
		static constexpr umint mc_nParameters = 1;
	};

	struct CSqlAllRowsPredicate
	{
		static constexpr umint mc_nParameters = 0;
	};

	template <auto t_pLeftMember, auto t_pRightMember>
	struct TCSqlJoinOnEqual
	{
		static constexpr auto mc_pLeftMember = t_pLeftMember;
		static constexpr auto mc_pRightMember = t_pRightMember;
		static constexpr ESqlPredicateType mc_Type = ESqlPredicateType::mc_EqualParameter;
	};

	template <auto t_pLeftMember, auto t_pRightMember, ESqlPredicateType t_Type>
	struct TCSqlJoinOnCompare
	{
		static constexpr auto mc_pLeftMember = t_pLeftMember;
		static constexpr auto mc_pRightMember = t_pRightMember;
		static constexpr ESqlPredicateType mc_Type = t_Type;
	};

	template <typename ...t_CPredicates>
	struct TCSqlJoinOnAll
	{
	};

	template <auto t_pMember, ESqlPredicateType t_Type>
	struct TCSqlNullPredicate
	{
		static constexpr auto mc_pMember = t_pMember;
		static constexpr ESqlPredicateType mc_Type = t_Type;
		static constexpr umint mc_nParameters = 0;
	};

	template <auto t_pMember, umint t_nParameters>
	struct TCSqlInPredicate
	{
		static constexpr auto mc_pMember = t_pMember;
		static constexpr ESqlPredicateType mc_Type = ESqlPredicateType::mc_InParameters;
		static constexpr umint mc_nParameters = t_nParameters;
	};

	template <auto t_pMember, auto &t_Subquery>
	struct TCSqlInSubqueryPredicate
	{
		static constexpr auto mc_pMember = t_pMember;
		static constexpr auto &mc_Subquery = t_Subquery;
		static constexpr ESqlPredicateType mc_Type = ESqlPredicateType::mc_InSubquery;
		static constexpr umint mc_nParameters = 0;
	};

	template <auto &t_Subquery, ESqlPredicateType t_Type>
	struct TCSqlExistsPredicate
	{
		static constexpr auto &mc_Subquery = t_Subquery;
		static constexpr ESqlPredicateType mc_Type = t_Type;
		static constexpr umint mc_nParameters = 0;
	};

	template <typename t_CLeft, typename t_CRight, ESqlPredicateType t_Type>
	struct TCSqlBinaryPredicate
	{
		using CLeft = t_CLeft;
		using CRight = t_CRight;

		static constexpr ESqlPredicateType mc_Type = t_Type;
		static constexpr umint mc_nParameters = CLeft::mc_nParameters + CRight::mc_nParameters;

		CLeft m_Left;
		CRight m_Right;
	};

	template <typename t_CPredicate>
	struct TCSqlNotPredicate
	{
		using CPredicate = t_CPredicate;

		static constexpr ESqlPredicateType mc_Type = ESqlPredicateType::mc_Not;
		static constexpr umint mc_nParameters = CPredicate::mc_nParameters;

		CPredicate m_Predicate;
	};

	template <auto t_pMember>
	using TCSqlEqualParameterPredicate = TCSqlParameterPredicate<t_pMember, ESqlPredicateType::mc_EqualParameter>;

	struct CSqlNoOrderBy
	{
	};

	struct CSqlNoGroupBy
	{
	};

	struct CSqlNoHaving
	{
		static constexpr umint mc_nParameters = 0;
	};

	struct CSqlNoLimitOffset
	{
		static constexpr bool mc_bHasLimit = false;
		static constexpr bool mc_bHasOffset = false;
	};

	struct CSqlNoDistinct
	{
		static constexpr bool mc_bDistinct = false;
	};

	struct CSqlDistinct
	{
		static constexpr bool mc_bDistinct = true;
	};

	struct CSqlAllColumns
	{
	};

	template <auto t_pMember>
	struct TCSqlMemberPointerTraits;

	template <typename t_CRow, typename t_CMember, t_CMember t_CRow::*t_pMember>
	struct TCSqlMemberPointerTraits<t_pMember>
	{
		using CRow = t_CRow;
		using CMember = t_CMember;
	};

	template <auto ...tp_pMembers>
	struct TCSqlSelectedColumns
	{
		using CRow = NStorage::TCTuple<typename TCSqlMemberPointerTraits<tp_pMembers>::CMember...>;
	};

	template <ESqlSelectExpressionType t_Type, auto t_pMember = nullptr>
	struct TCSqlAggregateExpression
	{
		static constexpr ESqlSelectExpressionType mc_Type = t_Type;
		static constexpr auto mc_pMember = t_pMember;
	};

	template <auto t_pMember>
	struct TCSqlColumnExpression
	{
		static constexpr auto mc_pMember = t_pMember;
	};

	template <ESqlSelectExpressionType t_Type, auto t_pLeftMember, auto t_pRightMember>
	struct TCSqlBinaryColumnExpression
	{
		static constexpr ESqlSelectExpressionType mc_Type = t_Type;
		static constexpr auto mc_pLeftMember = t_pLeftMember;
		static constexpr auto mc_pRightMember = t_pRightMember;
	};

	template <ESqlSelectExpressionType t_Type, auto t_pMember>
	struct TCSqlUnaryColumnExpression
	{
		static constexpr ESqlSelectExpressionType mc_Type = t_Type;
		static constexpr auto mc_pMember = t_pMember;
	};

	template <auto &t_FunctionName, auto t_pMember>
	struct TCSqlBackendFunctionExpression
	{
		static constexpr auto &mc_FunctionName = t_FunctionName;
		static constexpr auto mc_pMember = t_pMember;
	};

	template <auto t_pResultMember, typename t_CExpression>
	struct TCSqlAliasedExpression
	{
		using CExpression = t_CExpression;

		static constexpr auto mc_pResultMember = t_pResultMember;

		CExpression m_Expression;
	};

	template <typename t_CExpression>
	constexpr bool gc_SqlIsAliasedExpression = false;
	template <auto t_pResultMember, typename t_CExpression>
	constexpr bool gc_SqlIsAliasedExpression<TCSqlAliasedExpression<t_pResultMember, t_CExpression>> = true;

	// A selected-expressions list must be either all aliased (mapping to a named result struct) or all unaliased
	// (mapping to a tuple row). Mixing them picks the tuple row type via the leading unaliased entry, yet still maps
	// the aliased entries through their struct member offsets - writing decoded values to the wrong location - so the
	// mix is rejected at f_Select.
	template <typename ...tp_CExpressions>
	constexpr bool gc_SqlExpressionsConsistentAliasing =
		(gc_SqlIsAliasedExpression<NTraits::TCRemoveReferenceAndQualifiers<tp_CExpressions>> && ...)
		|| (!gc_SqlIsAliasedExpression<NTraits::TCRemoveReferenceAndQualifiers<tp_CExpressions>> && ...);

	template <typename t_CExpression>
	struct TCSqlExpressionResultType;

	template <>
	struct TCSqlExpressionResultType<TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Count, nullptr>>
	{
		using CType = int64;
	};

	template <auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlColumnExpression<t_pMember>>
	{
		using CType = typename TCSqlMemberPointerTraits<t_pMember>::CMember;
	};

	template <typename t_CLeftMember, typename t_CRightMember>
	struct TCSqlArithmeticExpressionResultType
	{
		using CLeftMember = NStorage::TCOptionalType<t_CLeftMember>;
		using CRightMember = NStorage::TCOptionalType<t_CRightMember>;
		// SQL arithmetic yields NULL whenever an operand is NULL, so the mapped result must be optional when either
		// operand is nullable; otherwise a row with a NULL operand fails the non-nullable row mapper.
		static constexpr bool mc_bNullable = NStorage::cIsOptional<t_CLeftMember> || NStorage::cIsOptional<t_CRightMember>;
		using CValueType = TCConditional<NTraits::cIsFloat<CLeftMember> || NTraits::cIsFloat<CRightMember>, fp64, int64>;
		using CType = TCConditional<mc_bNullable, NStorage::TCOptional<CValueType>, CValueType>;
	};

	template <ESqlSelectExpressionType t_Type, auto t_pLeftMember, auto t_pRightMember>
	struct TCSqlExpressionResultType<TCSqlBinaryColumnExpression<t_Type, t_pLeftMember, t_pRightMember>>
	{
		using CLeftMember = typename TCSqlMemberPointerTraits<t_pLeftMember>::CMember;
		using CRightMember = typename TCSqlMemberPointerTraits<t_pRightMember>::CMember;
		using CArithmetic = TCSqlArithmeticExpressionResultType<CLeftMember, CRightMember>;
		using CValueType = TCConditional<t_Type == ESqlSelectExpressionType::mc_Divide, fp64, typename CArithmetic::CValueType>;
		using CType = TCConditional<CArithmetic::mc_bNullable, NStorage::TCOptional<CValueType>, CValueType>;
	};

	template <auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlUnaryColumnExpression<ESqlSelectExpressionType::mc_Lower, t_pMember>>
	{
		using CType = typename TCSqlMemberPointerTraits<t_pMember>::CMember;
	};

	template <auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlUnaryColumnExpression<ESqlSelectExpressionType::mc_Upper, t_pMember>>
	{
		using CType = typename TCSqlMemberPointerTraits<t_pMember>::CMember;
	};

	template <auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlUnaryColumnExpression<ESqlSelectExpressionType::mc_Length, t_pMember>>
	{
		using CType = int64;
	};

	template <auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlUnaryColumnExpression<ESqlSelectExpressionType::mc_CastFloat, t_pMember>>
	{
		using CType = fp64;
	};

	template <auto &t_FunctionName, auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlBackendFunctionExpression<t_FunctionName, t_pMember>>
	{
		using CType = typename TCSqlMemberPointerTraits<t_pMember>::CMember;
	};

	template <auto t_pResultMember, typename t_CExpression>
	struct TCSqlExpressionResultType<TCSqlAliasedExpression<t_pResultMember, t_CExpression>>
	{
		using CType = typename TCSqlMemberPointerTraits<t_pResultMember>::CMember;
	};

	template <typename ...tp_CExpressions>
	struct TCSqlSelectedExpressionsRow
	{
		using CRow = NStorage::TCTuple<typename TCSqlExpressionResultType<tp_CExpressions>::CType...>;
	};

	template <auto t_pResultMember, typename t_CExpression, typename ...tp_CExpressions>
	struct TCSqlSelectedExpressionsRow<TCSqlAliasedExpression<t_pResultMember, t_CExpression>, tp_CExpressions...>
	{
		using CRow = typename TCSqlMemberPointerTraits<t_pResultMember>::CRow;

		static_assert
			(
				(NTraits::cIsSame<CRow, typename TCSqlMemberPointerTraits<tp_CExpressions::mc_pResultMember>::CRow> && ...)
				, "SQL aliased expression selections must all target members of the same result row type"
			)
		;
	};

	template <auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Sum, t_pMember>>
	{
		using CMember = NStorage::TCOptionalType<typename TCSqlMemberPointerTraits<t_pMember>::CMember>;
		using CType = TCConditional<NTraits::cIsFloat<CMember>, NStorage::TCOptional<fp64>, NStorage::TCOptional<int64>>;
	};

	template <auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Avg, t_pMember>>
	{
		using CType = NStorage::TCOptional<fp64>;
	};

	template <ESqlSelectExpressionType t_Type, auto t_pMember>
	struct TCSqlExpressionResultType<TCSqlAggregateExpression<t_Type, t_pMember>>
	{
		using CMember = NStorage::TCOptionalType<typename TCSqlMemberPointerTraits<t_pMember>::CMember>;
		using CType = NStorage::TCOptional<CMember>;
	};

	template <typename ...tp_CExpressions>
	struct TCSqlSelectedExpressions
	{
		using CRow = typename TCSqlSelectedExpressionsRow<tp_CExpressions...>::CRow;
	};

	template <typename t_CTable, typename t_CSelection>
	struct TCSqlSelectResultRow
	{
		using CRow = typename t_CSelection::CRow;
	};

	template <typename t_CTable>
	struct TCSqlSelectResultRow<t_CTable, CSqlAllColumns>
	{
		using CRow = typename t_CTable::CRow;
	};

	template <auto t_pMember, bool t_bDescending>
	struct TCSqlOrderByTerm
	{
		static constexpr auto mc_pMember = t_pMember;
		static constexpr bool mc_bDescending = t_bDescending;
	};

	template <typename ...tp_CTerms>
	struct TCSqlOrderBy
	{
	};

	template <auto ...tp_pMembers>
	struct TCSqlGroupBy
	{
	};

	template <typename t_CExpression, ESqlPredicateType t_Type>
	struct TCSqlHavingAggregatePredicate
	{
		using CExpression = t_CExpression;

		static constexpr ESqlPredicateType mc_Type = t_Type;
		static constexpr umint mc_nParameters = 1;

		CExpression m_Expression;
	};

	template <bool t_bHasLimit, bool t_bHasOffset>
	struct TCSqlLimitOffset
	{
		static constexpr bool mc_bHasLimit = t_bHasLimit;
		static constexpr bool mc_bHasOffset = t_bHasOffset;
	};

	template <ESqlValueType ...tp_Types>
	struct TCSqlParameterTypes
	{
		static constexpr CSqlQueryID fs_QueryID();
		static constexpr CSqlParameterTypesDescription fs_Describe();

		static constexpr umint mc_nTypes = sizeof...(tp_Types);
		// A no-parameter prepared select instantiates this with an empty pack (a projection query always builds the
		// parameter-types description, even when there are zero parameters). A zero-length array is a non-standard
		// extension some compilers reject, so size it to at least one; fs_Describe reports a null pointer with
		// mc_nTypes == 0 so the dummy element is never read.
		static constexpr ESqlValueType mc_Types[mc_nTypes == 0 ? 1 : mc_nTypes] = {tp_Types...};
	};

	template
	<
		typename t_CTable
		, typename t_CPredicate
		, typename t_COrderBy = CSqlNoOrderBy
		, typename t_CLimitOffset = CSqlNoLimitOffset
		, typename t_CDistinct = CSqlNoDistinct
		, typename t_CSelection = CSqlAllColumns
		, typename t_CGroupBy = CSqlNoGroupBy
		, typename t_CHaving = CSqlNoHaving
	>
	struct TCSqlPreparedSelect : public ICSqlPreparedSelectStatement
	{
		using CTable = t_CTable;
		using CPredicate = t_CPredicate;
		using COrderBy = t_COrderBy;
		using CLimitOffset = t_CLimitOffset;
		using CDistinct = t_CDistinct;
		using CSelection = t_CSelection;
		using CGroupBy = t_CGroupBy;
		using CHaving = t_CHaving;
		using CRow = typename TCSqlSelectResultRow<CTable, CSelection>::CRow;

		constexpr TCSqlPreparedSelect() = default;
		constexpr TCSqlPreparedSelect
			(
				CTable const &_Table
				, CPredicate _Predicate
				, COrderBy _OrderBy = {}
				, CLimitOffset _LimitOffset = {}
				, CDistinct _Distinct = {}
				, CSelection _Selection = {}
				, CGroupBy _GroupBy = {}
				, CHaving _Having = {}
			)
		;
		TCSqlPreparedSelect(TCSqlPreparedSelect const &) = delete;
		TCSqlPreparedSelect(TCSqlPreparedSelect &&) = delete;
		TCSqlPreparedSelect &operator = (TCSqlPreparedSelect const &) = delete;
		TCSqlPreparedSelect &operator = (TCSqlPreparedSelect &&) = delete;

		CSqlPreparedSelectStatementDescription f_Describe() const override;
		template <auto tf_pMember>
		consteval auto f_OrderByAscending() const;
		template <auto tf_pMember>
		consteval auto f_OrderByDescending() const;
		consteval auto f_WithLimit() const;
		consteval auto f_WithOffset() const;
		consteval auto f_Distinct() const;
		template <auto ...tfp_pMembers>
		consteval auto f_Select() const;
		template <typename ...tfp_CExpressions>
		consteval auto f_Select(tfp_CExpressions ...p_Expressions) const
			requires (gc_SqlExpressionsConsistentAliasing<tfp_CExpressions...>)
		;
		template <auto ...tfp_pMembers>
		consteval auto f_GroupBy() const;
		template <typename tf_CHaving>
		consteval auto f_Having(tf_CHaving _Having) const;

		CTable const &m_Table;
		CPredicate m_Predicate;
		COrderBy m_OrderBy;
		CLimitOffset m_LimitOffset;
		CDistinct m_Distinct;
		CSelection m_Selection;
		CGroupBy m_GroupBy;
		CHaving m_Having;
	};

	template <typename t_CTable>
	struct TCSqlPreparedSelectBuilder
	{
		using CTable = t_CTable;

		template <typename tf_CPredicate>
		consteval auto f_Where(tf_CPredicate _Predicate) const;

		CTable const &m_Table;
	};

	template <typename ...tp_CTables>
	struct TCSqlJoinedTables
	{
	};

	template <typename t_CRightTable, typename t_CJoinOn, ESqlJoinType t_JoinType>
	struct TCSqlJoinTerm
	{
		using CRightTable = t_CRightTable;
		using CJoinOn = t_CJoinOn;

		static constexpr ESqlJoinType mc_JoinType = t_JoinType;
	};

	template <typename ...tp_CTerms>
	struct TCSqlJoinTerms
	{
	};

	template <typename t_CTables>
	struct TCSqlJoinedRow;

	template <typename ...tp_CTables>
	struct TCSqlJoinedRow<TCSqlJoinedTables<tp_CTables...>>
	{
		using CRow = NStorage::TCTuple<typename tp_CTables::CRow...>;
	};

	template <typename t_CTables, typename t_CTerms>
	struct TCSqlPreparedJoinNSelect;

	template <typename ...tp_CTables, typename ...tp_CTerms>
	struct TCSqlPreparedJoinNSelect<TCSqlJoinedTables<tp_CTables...>, TCSqlJoinTerms<tp_CTerms...>> : public ICSqlPreparedSelectStatement
	{
		using CTables = TCSqlJoinedTables<tp_CTables...>;
		using CTerms = TCSqlJoinTerms<tp_CTerms...>;
		using CPredicate = CSqlAllRowsPredicate;
		using CRow = typename TCSqlJoinedRow<CTables>::CRow;

		static_assert(sizeof...(tp_CTables) >= 2);
		static_assert(sizeof...(tp_CTerms) == sizeof...(tp_CTables) - 1);

		constexpr TCSqlPreparedJoinNSelect() = default;
		constexpr TCSqlPreparedJoinNSelect(tp_CTables const &...p_Tables, typename tp_CTerms::CJoinOn ...p_JoinOns);
		TCSqlPreparedJoinNSelect(TCSqlPreparedJoinNSelect const &) = delete;
		TCSqlPreparedJoinNSelect(TCSqlPreparedJoinNSelect &&) = delete;
		TCSqlPreparedJoinNSelect &operator = (TCSqlPreparedJoinNSelect const &) = delete;
		TCSqlPreparedJoinNSelect &operator = (TCSqlPreparedJoinNSelect &&) = delete;

		CSqlPreparedSelectStatementDescription f_Describe() const override;
		template <typename tf_CNextTable, typename tf_CJoinOn>
		consteval auto f_InnerJoin(tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn) const;
		template <typename tf_CNextTable, typename tf_CJoinOn>
		consteval auto f_LeftJoin(tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn) const;

		NStorage::TCTuple<tp_CTables const &...> m_Tables;
		NStorage::TCTuple<typename tp_CTerms::CJoinOn...> m_JoinOns;
	};

	template <typename t_CLeftTable, typename t_CRightTable, typename t_CJoinOn, ESqlJoinType t_JoinType>
	struct TCSqlPreparedJoinSelect : public ICSqlPreparedSelectStatement
	{
		using CLeftTable = t_CLeftTable;
		using CRightTable = t_CRightTable;
		using CJoinOn = t_CJoinOn;
		using CPredicate = CSqlAllRowsPredicate;
		using CRow = NStorage::TCTuple<typename CLeftTable::CRow, typename CRightTable::CRow>;

		static constexpr ESqlJoinType mc_JoinType = t_JoinType;

		constexpr TCSqlPreparedJoinSelect() = default;
		constexpr TCSqlPreparedJoinSelect(CLeftTable const &_LeftTable, CRightTable const &_RightTable, CJoinOn _JoinOn);
		TCSqlPreparedJoinSelect(TCSqlPreparedJoinSelect const &) = delete;
		TCSqlPreparedJoinSelect(TCSqlPreparedJoinSelect &&) = delete;
		TCSqlPreparedJoinSelect &operator = (TCSqlPreparedJoinSelect const &) = delete;
		TCSqlPreparedJoinSelect &operator = (TCSqlPreparedJoinSelect &&) = delete;

		CSqlPreparedSelectStatementDescription f_Describe() const override;
		template <typename tf_CNextTable, typename tf_CJoinOn>
		consteval auto f_InnerJoin(tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn) const;
		template <typename tf_CNextTable, typename tf_CJoinOn>
		consteval auto f_LeftJoin(tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn) const;

		CLeftTable const &m_LeftTable;
		CRightTable const &m_RightTable;
		CJoinOn m_JoinOn;
	};

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
	struct TCSqlPreparedJoin3Select : public ICSqlPreparedSelectStatement
	{
		using CLeftTable = t_CLeftTable;
		using CMiddleTable = t_CMiddleTable;
		using CRightTable = t_CRightTable;
		using CFirstJoinOn = t_CFirstJoinOn;
		using CSecondJoinOn = t_CSecondJoinOn;
		using CPredicate = CSqlAllRowsPredicate;
		using CRow = NStorage::TCTuple<typename CLeftTable::CRow, typename CMiddleTable::CRow, typename CRightTable::CRow>;

		static constexpr ESqlJoinType mc_FirstJoinType = t_FirstJoinType;
		static constexpr ESqlJoinType mc_SecondJoinType = t_SecondJoinType;

		constexpr TCSqlPreparedJoin3Select() = default;
		constexpr TCSqlPreparedJoin3Select
			(
				CLeftTable const &_LeftTable
				, CMiddleTable const &_MiddleTable
				, CRightTable const &_RightTable
				, CFirstJoinOn _FirstJoinOn
				, CSecondJoinOn _SecondJoinOn
			)
		;
		TCSqlPreparedJoin3Select(TCSqlPreparedJoin3Select const &) = delete;
		TCSqlPreparedJoin3Select(TCSqlPreparedJoin3Select &&) = delete;
		TCSqlPreparedJoin3Select &operator = (TCSqlPreparedJoin3Select const &) = delete;
		TCSqlPreparedJoin3Select &operator = (TCSqlPreparedJoin3Select &&) = delete;

		CSqlPreparedSelectStatementDescription f_Describe() const override;

		CLeftTable const &m_LeftTable;
		CMiddleTable const &m_MiddleTable;
		CRightTable const &m_RightTable;
		CFirstJoinOn m_FirstJoinOn;
		CSecondJoinOn m_SecondJoinOn;
	};

	template <auto &t_LeftSelect, auto &t_RightSelect, ESqlSetOperationType t_Type>
	struct TCSqlPreparedSetSelect : public ICSqlPreparedSelectStatement
	{
		using CLeftSelect = NTraits::TCDecay<decltype(t_LeftSelect)>;
		using CRightSelect = NTraits::TCDecay<decltype(t_RightSelect)>;
		using CPredicate = CSqlAllRowsPredicate;
		using CRow = typename CLeftSelect::CRow;

		static_assert(NTraits::cIsSame<CRow, typename CRightSelect::CRow>, "SQL set operation selects must have the same result row type");

		static constexpr ESqlSetOperationType mc_Type = t_Type;

		constexpr TCSqlPreparedSetSelect();
		TCSqlPreparedSetSelect(TCSqlPreparedSetSelect const &) = delete;
		TCSqlPreparedSetSelect(TCSqlPreparedSetSelect &&) = delete;
		TCSqlPreparedSetSelect &operator = (TCSqlPreparedSetSelect const &) = delete;
		TCSqlPreparedSetSelect &operator = (TCSqlPreparedSetSelect &&) = delete;

		CSqlPreparedSelectStatementDescription f_Describe() const override;
	};

	template <typename t_CTable, auto ...tp_pMembers>
	struct TCSqlPreparedInsert : public ICSqlPreparedInsertStatement
	{
		using CTable = t_CTable;
		using CRow = typename CTable::CRow;

		constexpr TCSqlPreparedInsert() = default;
		constexpr TCSqlPreparedInsert(CTable const &_Table);
		TCSqlPreparedInsert(TCSqlPreparedInsert const &) = delete;
		TCSqlPreparedInsert(TCSqlPreparedInsert &&) = delete;
		TCSqlPreparedInsert &operator = (TCSqlPreparedInsert const &) = delete;
		TCSqlPreparedInsert &operator = (TCSqlPreparedInsert &&) = delete;

		CSqlPreparedInsertStatementDescription f_Describe() const override;

		template <auto ...tfp_pMembers>
		consteval auto f_Columns() const;

		CTable const &m_Table;
	};

	template <typename t_CTable, typename t_CPredicate, typename t_CSet>
	struct TCSqlPreparedUpdate : public ICSqlPreparedUpdateStatement
	{
		using CTable = t_CTable;
		using CPredicate = t_CPredicate;
		using CSet = t_CSet;
		using CRow = typename CTable::CRow;

		constexpr TCSqlPreparedUpdate() = default;
		constexpr TCSqlPreparedUpdate(CTable const &_Table, CPredicate _Predicate, CSet _Set);
		TCSqlPreparedUpdate(TCSqlPreparedUpdate const &) = delete;
		TCSqlPreparedUpdate(TCSqlPreparedUpdate &&) = delete;
		TCSqlPreparedUpdate &operator = (TCSqlPreparedUpdate const &) = delete;
		TCSqlPreparedUpdate &operator = (TCSqlPreparedUpdate &&) = delete;

		CSqlPreparedUpdateStatementDescription f_Describe() const override;

		CTable const &m_Table;
		CPredicate m_Predicate;
		CSet m_Set;
	};

	template <typename t_CTable, typename t_CPredicate>
	struct TCSqlPreparedUpdateSetBuilder
	{
		using CTable = t_CTable;
		using CPredicate = t_CPredicate;

		// An empty set list would render "UPDATE ... SET  WHERE ..." (invalid SQL on every backend), so require at
		// least one column, matching the upsert f_Update and f_UpdateByID helpers.
		template <auto ...tfp_pMembers>
		consteval auto f_Set() const
			requires (sizeof...(tfp_pMembers) > 0);

		CTable const &m_Table;
		CPredicate m_Predicate;
	};

	template <typename t_CTable>
	struct TCSqlPreparedUpdateBuilder
	{
		using CTable = t_CTable;

		template <typename tf_CPredicate>
		consteval auto f_Where(tf_CPredicate _Predicate) const;
		consteval auto f_AllRows() const;

		CTable const &m_Table;
	};

	template <typename t_CTable, typename t_CPredicate>
	struct TCSqlPreparedDelete : public ICSqlPreparedDeleteStatement
	{
		using CTable = t_CTable;
		using CPredicate = t_CPredicate;
		using CRow = typename CTable::CRow;

		constexpr TCSqlPreparedDelete() = default;
		constexpr TCSqlPreparedDelete(CTable const &_Table, CPredicate _Predicate);
		TCSqlPreparedDelete(TCSqlPreparedDelete const &) = delete;
		TCSqlPreparedDelete(TCSqlPreparedDelete &&) = delete;
		TCSqlPreparedDelete &operator = (TCSqlPreparedDelete const &) = delete;
		TCSqlPreparedDelete &operator = (TCSqlPreparedDelete &&) = delete;

		CSqlPreparedDeleteStatementDescription f_Describe() const override;

		CTable const &m_Table;
		CPredicate m_Predicate;
	};

	template <typename t_CTable>
	struct TCSqlPreparedDeleteBuilder
	{
		using CTable = t_CTable;

		template <typename tf_CPredicate>
		consteval auto f_Where(tf_CPredicate _Predicate) const;
		consteval auto f_AllRows() const;

		CTable const &m_Table;
	};

	template <typename t_CTable, typename t_CConflict, typename t_CUpdate>
	struct TCSqlPreparedUpsert : public ICSqlPreparedUpsertStatement
	{
		using CTable = t_CTable;
		using CConflict = t_CConflict;
		using CUpdate = t_CUpdate;
		using CRow = typename CTable::CRow;

		constexpr TCSqlPreparedUpsert() = default;
		constexpr TCSqlPreparedUpsert(CTable const &_Table, CConflict _Conflict, CUpdate _Update);
		TCSqlPreparedUpsert(TCSqlPreparedUpsert const &) = delete;
		TCSqlPreparedUpsert(TCSqlPreparedUpsert &&) = delete;
		TCSqlPreparedUpsert &operator = (TCSqlPreparedUpsert const &) = delete;
		TCSqlPreparedUpsert &operator = (TCSqlPreparedUpsert &&) = delete;

		CSqlPreparedUpsertStatementDescription f_Describe() const override;

		CTable const &m_Table;
		CConflict m_Conflict;
		CUpdate m_Update;
	};

	template <typename t_CTable, typename t_CConflict>
	struct TCSqlPreparedUpsertUpdateBuilder
	{
		using CTable = t_CTable;
		using CConflict = t_CConflict;

		template <auto ...tfp_pMembers>
		consteval auto f_Update() const
			requires (sizeof...(tfp_pMembers) > 0)
		;

		CTable const &m_Table;
		CConflict m_Conflict;
	};

	template <typename t_CTable>
	struct TCSqlPreparedUpsertBuilder
	{
		using CTable = t_CTable;

		template <auto ...tfp_pMembers>
		consteval auto f_OnConflict() const
			requires (sizeof...(tfp_pMembers) > 0)
		;

		CTable const &m_Table;
	};

	template <typename tf_CTable>
	consteval auto fg_SqlPreparedSelect(tf_CTable const &_Table);
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnEq();
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnNe();
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnLt();
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnLe();
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnGt();
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlJoinOnGe();
	template <typename ...tfp_CPredicates>
	consteval auto fg_SqlJoinOnAll(tfp_CPredicates ...p_Predicates);
	template <typename tf_CLeftTable, typename tf_CRightTable, typename tf_CJoinOn>
	consteval auto fg_SqlPreparedInnerJoin(tf_CLeftTable const &_LeftTable, tf_CRightTable const &_RightTable, tf_CJoinOn _JoinOn);
	template <typename tf_CLeftTable, typename tf_CRightTable, typename tf_CJoinOn>
	consteval auto fg_SqlPreparedLeftJoin(tf_CLeftTable const &_LeftTable, tf_CRightTable const &_RightTable, tf_CJoinOn _JoinOn);
	template <auto &tf_LeftSelect, auto &tf_RightSelect>
	consteval auto fg_SqlUnion();
	template <auto &tf_LeftSelect, auto &tf_RightSelect>
	consteval auto fg_SqlUnionAll();
	template <auto &tf_LeftSelect, auto &tf_RightSelect>
	consteval auto fg_SqlIntersect();
	template <auto &tf_LeftSelect, auto &tf_RightSelect>
	consteval auto fg_SqlExcept();
	template <typename tf_CTable>
	consteval auto fg_SqlPreparedInsert(tf_CTable const &_Table);
	template <typename tf_CTable>
	consteval auto fg_SqlPreparedUpdate(tf_CTable const &_Table);
	template <typename tf_CTable>
	consteval auto fg_SqlPreparedDelete(tf_CTable const &_Table);
	template <typename tf_CTable>
	consteval auto fg_SqlPreparedUpsert(tf_CTable const &_Table);
	template <auto tf_pMember>
	consteval auto fg_SqlParamEq();
	template <auto tf_pMember>
	consteval auto fg_SqlParamNe();
	template <auto tf_pMember>
	consteval auto fg_SqlParamLt();
	template <auto tf_pMember>
	consteval auto fg_SqlParamLe();
	template <auto tf_pMember>
	consteval auto fg_SqlParamGt();
	template <auto tf_pMember>
	consteval auto fg_SqlParamGe();
	template <auto tf_pMember>
	consteval auto fg_SqlParamLike();
	template <auto tf_pMember>
	consteval auto fg_SqlIsNull();
	template <auto tf_pMember>
	consteval auto fg_SqlIsNotNull();
	template <auto tf_pMember, umint tf_nParameters>
	consteval auto fg_SqlParamIn()
		requires (tf_nParameters > 0)
	;
	template <auto tf_pMember, auto &tf_Subquery>
	consteval auto fg_SqlInSubquery();
	template <auto &tf_Subquery>
	consteval auto fg_SqlExists();
	template <auto &tf_Subquery>
	consteval auto fg_SqlNotExists();
	template <typename tf_CLeft, typename tf_CRight>
	consteval auto fg_SqlAnd(tf_CLeft _Left, tf_CRight _Right);
	template <typename tf_CLeft, typename tf_CRight>
	consteval auto fg_SqlOr(tf_CLeft _Left, tf_CRight _Right);
	template <typename tf_CPredicate>
	consteval auto fg_SqlNot(tf_CPredicate _Predicate);
	consteval auto fg_SqlCount();
	template <auto tf_pMember>
	consteval auto fg_SqlColumn();
	template <auto tf_pResultMember, typename tf_CExpression>
	consteval auto fg_SqlAlias(tf_CExpression _Expression);
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlAdd();
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlSubtract();
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlMultiply();
	template <auto tf_pLeftMember, auto tf_pRightMember>
	consteval auto fg_SqlDivide();
	template <auto tf_pMember>
	consteval auto fg_SqlLower();
	template <auto tf_pMember>
	consteval auto fg_SqlUpper();
	template <auto tf_pMember>
	consteval auto fg_SqlLength();
	template <auto tf_pMember>
	consteval auto fg_SqlCastFloat();
	template <auto &tf_FunctionName, auto tf_pMember>
	consteval auto fg_SqlBackendFunction();
	template <auto tf_pMember>
	consteval auto fg_SqlSum();
	template <auto tf_pMember>
	consteval auto fg_SqlAvg();
	template <auto tf_pMember>
	consteval auto fg_SqlMin();
	template <auto tf_pMember>
	consteval auto fg_SqlMax();
	template <typename tf_CExpression>
	consteval auto fg_SqlHavingGt(tf_CExpression _Expression);
	template <typename tf_CExpression>
	consteval auto fg_SqlHavingGe(tf_CExpression _Expression);
	template <typename tf_CExpression>
	consteval auto fg_SqlHavingLt(tf_CExpression _Expression);
	template <typename tf_CExpression>
	consteval auto fg_SqlHavingLe(tf_CExpression _Expression);
	template <typename tf_CExpression>
	consteval auto fg_SqlHavingEq(tf_CExpression _Expression);
}

#include "Private/Malterlib_SQL_Database_Private.h"

namespace NMib::NSQL
{
	struct CSqlTransaction;
	template <typename t_CConnection, auto &tf_Repository>
	struct TCSqlRepositoryConnection;

	struct CSqlDatabaseClient
	{
		using CTransaction = CSqlTransaction;

		CSqlDatabaseClient(NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend);

		NConcurrency::TCFuture<void> f_Open();
		NConcurrency::TCFuture<void> f_ApplySchema();
		template <auto &tf_Repository>
		auto f_Repository()
			-> TCSqlRepositoryConnection<CSqlDatabaseClient, tf_Repository>
		;

		template <typename tf_CTable>
		NConcurrency::TCFuture<void> f_Insert(tf_CTable const &_Table, typename tf_CTable::CRow &&_Row);
		template <typename tf_CTable, typename ...tfp_CValues>
		NConcurrency::TCFuture<void> f_Insert(tf_CTable const &_Table, tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlTableInsertValuesMatch<tf_CTable, tfp_CValues...>())
		;

		template <auto &tf_PreparedInsert, typename ...tfp_CValues>
		NConcurrency::TCFuture<void> f_Insert(tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>())
		;
		template <auto &tf_PreparedInsert>
		auto f_InsertMany(NConcurrency::TCAsyncGenerator<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedInsert)>::CRow>> _RowBatches)
			-> NConcurrency::TCFuture<umint>
		;
		template <auto tf_pMember, auto &tf_PreparedInsert, typename ...tfp_CValues>
		auto f_InsertReturning(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
			requires
			(
				NPrivate::fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>()
				&& NPrivate::fg_SqlPreparedInsertGeneratedPrimaryKeyColumnCount<tf_PreparedInsert>() == 1
				&& NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<tf_PreparedInsert.m_Table, tf_pMember>()
			)
		;
		template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
		NConcurrency::TCFuture<umint> f_Upsert(tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
		;
		template <auto tf_pMember, auto &tf_PreparedUpsert, typename ...tfp_CValues>
		auto f_UpsertReturning(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
			requires (NPrivate::fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
		;
		template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
		NConcurrency::TCFuture<umint> f_Update(tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
		;
		template <auto tf_pMember, auto &tf_PreparedUpdate, typename ...tfp_CValues>
		auto f_UpdateReturning(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
			requires (NPrivate::fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
		;
		template <auto &tf_PreparedDelete, typename ...tfp_CValues>
		NConcurrency::TCFuture<umint> f_Delete(tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
		;
		template <auto tf_pMember, auto &tf_PreparedDelete, typename ...tfp_CValues>
		auto f_DeleteReturning(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
			requires (NPrivate::fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
		;
		template <auto &tf_Table, auto tf_pMember, typename tf_CValue>
		auto f_GetByID(tf_CValue _Value)
			-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
			requires
			(
				NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
				&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
				&& NPrivate::fg_SqlPreparedSelectParameterMatches<NPrivate::gc_SqlSelectByID<tf_Table, tf_pMember>, tf_CValue>()
			)
		;
		template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
		auto f_GetByCompositeID(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
			requires
			(
				NPrivate::fg_SqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>()
				&& NPrivate::fg_SqlCompositeIDSelectParametersMatch<tf_Table, tf_CCompositeID, tfp_CValues...>()
			)
		;
		template <auto &tf_Table, auto tf_pMember, auto ...tfp_pSetMembers, typename ...tfp_CValues>
		auto f_UpdateByID(tfp_CValues &&...p_Values) -> NConcurrency::TCFuture<umint>
			requires
			(
				sizeof...(tfp_pSetMembers) > 0
				&& NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
				&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
				&& NPrivate::fg_SqlPreparedUpdateValuesMatch<NPrivate::gc_SqlUpdateByID<tf_Table, tf_pMember, tfp_pSetMembers...>, tfp_CValues...>()
			)
		;
		template <auto &tf_Table, auto tf_pMember, typename tf_CValue>
		auto f_DeleteByID(tf_CValue &&_Value) -> NConcurrency::TCFuture<umint>
			requires
			(
				NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
				&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
				&& NPrivate::fg_SqlPreparedDeleteValuesMatch<NPrivate::gc_SqlDeleteByID<tf_Table, tf_pMember>, tf_CValue>()
			)
		;
		template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
		auto f_DeleteByCompositeID(tfp_CValues &&...p_Values) -> NConcurrency::TCFuture<umint>
			requires
			(
				NPrivate::fg_SqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>()
				&& NPrivate::fg_SqlCompositeIDDeleteValuesMatch<tf_Table, tf_CCompositeID, tfp_CValues...>()
			)
		;
		template <auto &tf_Table, auto tf_pIDMember, auto tf_pVersionMember, auto ...tfp_pSetMembers>
		auto f_Save(typename NTraits::TCDecay<decltype(tf_Table)>::CRow _Row)
			-> NConcurrency::TCFuture<TCSqlSaveResult<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
			requires
			(
				sizeof...(tfp_pSetMembers) > 0
				&& NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
				&& NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<tf_Table, tf_pIDMember>()
				&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pIDMember>()
			)
		;

		template <auto &tf_PreparedSelect, typename tf_CParam>
		auto f_Query(tf_CParam _Param, CSqlSelectSettings _Settings = {})
			requires (NPrivate::fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
		;
		template <auto &tf_PreparedSelect, typename tf_CParam0, typename tf_CParam1, typename ...tfp_CParams>
		auto f_Query(tf_CParam0 _Param0, tf_CParam1 _Param1, tfp_CParams ...p_Params)
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam0, tf_CParam1, tfp_CParams...>())
		;
		// Settings come first for multi-parameter selects: a trailing CSqlSelectSettings cannot be distinguished from a
		// bound parameter after the parameter pack, so the single-parameter settings-last form has no variadic analogue.
		template <auto &tf_PreparedSelect, typename tf_CParam0, typename tf_CParam1, typename ...tfp_CParams>
		auto f_Query(CSqlSelectSettings _Settings, tf_CParam0 _Param0, tf_CParam1 _Param1, tfp_CParams ...p_Params)
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam0, tf_CParam1, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect>
		auto f_Query(CSqlSelectSettings _Settings = {})
			requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedSelect>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_QueryStream(tfp_CParams ...p_Params)
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_QueryVector(tfp_CParams ...p_Params)
			-> NConcurrency::TCFuture<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedJoin, typename tf_CResult>
		auto f_QueryJoinedVectorAs() -> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
			requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedJoin>())
		;
		template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
		auto f_QueryVectorAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_QueryOptional(tfp_CParams ...p_Params)
			-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
		auto f_QueryOptionalAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<NStorage::TCOptional<tf_CResult>>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_QueryOne(tfp_CParams ...p_Params)
			-> NConcurrency::TCFuture<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
		auto f_QueryOneAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<tf_CResult>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_Count(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<umint>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_Exists(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<bool>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;

		NConcurrency::TCFuture<umint> f_ExecuteRaw(CSqlRawOperation _Operation);
		NConcurrency::TCFuture<umint> f_ExecuteRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});
		NConcurrency::TCFuture<CSqlRawResult> f_QueryRaw(CSqlRawOperation _Operation);
		NConcurrency::TCFuture<CSqlRawResult> f_QueryRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});
		NConcurrency::TCFuture<CSqlRawStream> f_QueryRawStream(CSqlRawOperation _Operation);
		NConcurrency::TCFuture<CSqlRawStream> f_QueryRawStream(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});

		NConcurrency::TCFuture<CTransaction> f_BeginTransaction(CSqlTransactionSettings _Settings = {});
		NConcurrency::TCFuture<CTransaction> f_BeginReadTransaction(CSqlTransactionSettings _Settings = {});
		template <typename tf_CFunction>
		auto f_WithTransaction(tf_CFunction _fFunction)
			-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue>
		;
		template <typename tf_CFunction>
		auto f_WithReadTransaction(tf_CFunction _fFunction)
			-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue>
		;

	private:
		friend struct CSqlTransaction;

		template <auto &tf_PreparedSelect, typename tf_CParam>
		auto fp_Query(tf_CParam _Param, CSqlSelectSettings _Settings)
			requires (NPrivate::fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto fp_Query(CSqlSelectSettings _Settings, tfp_CParams ...p_Params)
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect>
		auto fp_Query(CSqlSelectSettings _Settings)
			requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedSelect>())
		;
		template <typename tf_CActor, typename tf_CRow>
		static auto fsp_SelectRows(NConcurrency::TCActor<tf_CActor> _Backend, CSqlSelectOperation _Operation, uint32 _nPipelineLength)
			-> NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>>
		;
		template <typename tf_CRow>
		static auto fsp_QueryVector(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
			-> NConcurrency::TCFuture<NContainer::TCVector<tf_CRow>>
		;
		template <typename tf_CResult, auto ...tfp_pMembers, typename tf_CRow>
		static auto fsp_QueryVectorAs(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
			-> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
		;
		template <typename tf_CResult, typename tf_CRow>
		static auto fsp_QueryJoinedVectorAs(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
			-> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
		;
		template <typename tf_CRow>
		static auto fsp_QueryOptional(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
			-> NConcurrency::TCFuture<NStorage::TCOptional<tf_CRow>>
		;
		template <typename tf_CResult, auto ...tfp_pMembers, typename tf_CRow>
		static auto fsp_QueryOptionalAs(NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows, uint32 _nPipelineLength)
			-> NConcurrency::TCFuture<NStorage::TCOptional<tf_CResult>>
		;
		template <typename tf_CRow>
		static auto fsp_QueryOne
			(
				NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows
				, uint32 _nPipelineLength
			)
			-> NConcurrency::TCFuture<tf_CRow>
		;
		template <typename tf_CResult, auto ...tfp_pMembers, typename tf_CRow>
		static auto fsp_QueryOneAs
			(
				NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows
				, uint32 _nPipelineLength
			)
			-> NConcurrency::TCFuture<tf_CResult>
		;
		template <typename tf_CRow>
		static auto fsp_Count
			(
				NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows
				, uint32 _nPipelineLength
			)
			-> NConcurrency::TCFuture<umint>
		;
		template <typename tf_CRow>
		static auto fsp_Exists
			(
				NConcurrency::TCAsyncGenerator<NContainer::TCVector<NStorage::TCUniquePointer<TCRowData<tf_CRow>>>> _Rows
				, uint32 _nPipelineLength
			)
			-> NConcurrency::TCFuture<bool>
		;
		static auto fsp_BeginTransaction
			(
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend
				, CSqlTransactionSettings _Settings
				, uint32 _nPipelineLength
			)
			-> NConcurrency::TCFuture<CTransaction>
		;
		static auto fsp_BeginReadTransaction
			(
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend
				, CSqlTransactionSettings _Settings
				, uint32 _nPipelineLength
			)
			-> NConcurrency::TCFuture<CTransaction>
		;
		template <typename tf_CFunction>
		static auto fsp_WithTransaction(NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend, uint32 _nPipelineLength, tf_CFunction _fFunction)
			-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue>
		;
		template <typename tf_CFunction>
		static auto fsp_WithReadTransaction(NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend, uint32 _nPipelineLength, tf_CFunction _fFunction)
			-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CTransaction *>(nullptr))))::CValue>
		;

		NConcurrency::TCActor<ICSqlDatabaseBackendActor> mp_Backend;
	public:
		uint32 m_nPipelineLength = 5;
	};

	struct CSqlTransaction
	{
		CSqlTransaction(CSqlTransactionInterface _Transaction);

		template <auto &tf_Repository>
		auto f_Repository()
			-> TCSqlRepositoryConnection<CSqlTransaction, tf_Repository>
		;

		template <typename tf_CTable>
		NConcurrency::TCFuture<void> f_Insert(tf_CTable const &_Table, typename tf_CTable::CRow &&_Row);
		template <typename tf_CTable, typename ...tfp_CValues>
		NConcurrency::TCFuture<void> f_Insert(tf_CTable const &_Table, tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlTableInsertValuesMatch<tf_CTable, tfp_CValues...>())
		;

		template <auto &tf_PreparedInsert, typename ...tfp_CValues>
		NConcurrency::TCFuture<void> f_Insert(tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>())
		;
		template <auto &tf_PreparedInsert>
		auto f_InsertMany(NConcurrency::TCAsyncGenerator<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedInsert)>::CRow>> _RowBatches)
			-> NConcurrency::TCFuture<umint>
		;
		template <auto tf_pMember, auto &tf_PreparedInsert, typename ...tfp_CValues>
		auto f_InsertReturning(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
			requires
			(
				NPrivate::fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>()
				&& NPrivate::fg_SqlPreparedInsertGeneratedPrimaryKeyColumnCount<tf_PreparedInsert>() == 1
				&& NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<tf_PreparedInsert.m_Table, tf_pMember>()
			)
		;
		template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
		NConcurrency::TCFuture<umint> f_Upsert(tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
		;
		template <auto tf_pMember, auto &tf_PreparedUpsert, typename ...tfp_CValues>
		auto f_UpsertReturning(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
			requires (NPrivate::fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
		;
		template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
		NConcurrency::TCFuture<umint> f_Update(tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
		;
		template <auto tf_pMember, auto &tf_PreparedUpdate, typename ...tfp_CValues>
		auto f_UpdateReturning(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
			requires (NPrivate::fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
		;
		template <auto &tf_PreparedDelete, typename ...tfp_CValues>
		NConcurrency::TCFuture<umint> f_Delete(tfp_CValues &&...p_Values)
			requires (NPrivate::fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
		;
		template <auto tf_pMember, auto &tf_PreparedDelete, typename ...tfp_CValues>
		auto f_DeleteReturning(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<typename NPrivate::TCSqlMemberPointerTraits<tf_pMember>::CMember>
			requires (NPrivate::fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
		;
		template <auto &tf_Table, auto tf_pMember, typename tf_CValue>
		auto f_GetByID(tf_CValue _Value)
			-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
			requires
			(
				NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
				&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
				&& NPrivate::fg_SqlPreparedSelectParameterMatches<NPrivate::gc_SqlSelectByID<tf_Table, tf_pMember>, tf_CValue>()
			)
		;
		template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
		auto f_GetByCompositeID(tfp_CValues &&...p_Values)
			-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
			requires
			(
				NPrivate::fg_SqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>()
				&& NPrivate::fg_SqlCompositeIDSelectParametersMatch<tf_Table, tf_CCompositeID, tfp_CValues...>()
			)
		;
		template <auto &tf_Table, auto tf_pMember, auto ...tfp_pSetMembers, typename ...tfp_CValues>
		auto f_UpdateByID(tfp_CValues &&...p_Values) -> NConcurrency::TCFuture<umint>
			requires
			(
				sizeof...(tfp_pSetMembers) > 0
				&& NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
				&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
				&& NPrivate::fg_SqlPreparedUpdateValuesMatch<NPrivate::gc_SqlUpdateByID<tf_Table, tf_pMember, tfp_pSetMembers...>, tfp_CValues...>()
			)
		;
		template <auto &tf_Table, auto tf_pMember, typename tf_CValue>
		auto f_DeleteByID(tf_CValue &&_Value) -> NConcurrency::TCFuture<umint>
			requires
			(
				NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
				&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pMember>()
				&& NPrivate::fg_SqlPreparedDeleteValuesMatch<NPrivate::gc_SqlDeleteByID<tf_Table, tf_pMember>, tf_CValue>()
			)
		;
		template <auto &tf_Table, typename tf_CCompositeID, typename ...tfp_CValues>
		auto f_DeleteByCompositeID(tfp_CValues &&...p_Values) -> NConcurrency::TCFuture<umint>
			requires
			(
				NPrivate::fg_SqlCompositeIDMatchesTable<tf_Table, tf_CCompositeID>()
				&& NPrivate::fg_SqlCompositeIDDeleteValuesMatch<tf_Table, tf_CCompositeID, tfp_CValues...>()
			)
		;
		template <auto &tf_Table, auto tf_pIDMember, auto tf_pVersionMember, auto ...tfp_pSetMembers>
		auto f_Save(typename NTraits::TCDecay<decltype(tf_Table)>::CRow _Row)
			-> NConcurrency::TCFuture<TCSqlSaveResult<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
			requires
			(
				sizeof...(tfp_pSetMembers) > 0
				&& NPrivate::fg_SqlTablePrimaryKeyColumnCount<tf_Table>() == 1
				&& NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<tf_Table, tf_pIDMember>()
				&& NPrivate::fg_SqlTableMemberIsPrimaryKey<tf_Table, tf_pIDMember>()
			)
		;

		template <auto &tf_PreparedSelect, typename tf_CParam>
		auto f_Query(tf_CParam _Param, CSqlSelectSettings _Settings = {})
			requires (NPrivate::fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
		;
		template <auto &tf_PreparedSelect, typename tf_CParam0, typename tf_CParam1, typename ...tfp_CParams>
		auto f_Query(tf_CParam0 _Param0, tf_CParam1 _Param1, tfp_CParams ...p_Params)
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam0, tf_CParam1, tfp_CParams...>())
		;
		// Settings come first for multi-parameter selects: a trailing CSqlSelectSettings cannot be distinguished from a
		// bound parameter after the parameter pack, so the single-parameter settings-last form has no variadic analogue.
		template <auto &tf_PreparedSelect, typename tf_CParam0, typename tf_CParam1, typename ...tfp_CParams>
		auto f_Query(CSqlSelectSettings _Settings, tf_CParam0 _Param0, tf_CParam1 _Param1, tfp_CParams ...p_Params)
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tf_CParam0, tf_CParam1, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect>
		auto f_Query(CSqlSelectSettings _Settings = {})
			requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedSelect>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_QueryStream(tfp_CParams ...p_Params)
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_QueryVector(tfp_CParams ...p_Params)
			-> NConcurrency::TCFuture<NContainer::TCVector<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedJoin, typename tf_CResult>
		auto f_QueryJoinedVectorAs() -> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
			requires (NPrivate::fg_SqlPreparedSelectHasNoParameters<tf_PreparedJoin>())
		;
		template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
		auto f_QueryVectorAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<NContainer::TCVector<tf_CResult>>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_QueryOptional(tfp_CParams ...p_Params)
			-> NConcurrency::TCFuture<NStorage::TCOptional<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
		auto f_QueryOptionalAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<NStorage::TCOptional<tf_CResult>>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_QueryOne(tfp_CParams ...p_Params)
			-> NConcurrency::TCFuture<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
		auto f_QueryOneAs(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<tf_CResult>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_Count(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<umint>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;
		template <auto &tf_PreparedSelect, typename ...tfp_CParams>
		auto f_Exists(tfp_CParams ...p_Params) -> NConcurrency::TCFuture<bool>
			requires (NPrivate::fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
		;

		NConcurrency::TCFuture<umint> f_ExecuteRaw(CSqlRawOperation _Operation);
		NConcurrency::TCFuture<umint> f_ExecuteRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});
		NConcurrency::TCFuture<CSqlRawResult> f_QueryRaw(CSqlRawOperation _Operation);
		NConcurrency::TCFuture<CSqlRawResult> f_QueryRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});
		NConcurrency::TCFuture<CSqlRawStream> f_QueryRawStream(CSqlRawOperation _Operation);
		NConcurrency::TCFuture<CSqlRawStream> f_QueryRawStream(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters = {});

		NConcurrency::TCFuture<void> f_Commit();
		NConcurrency::TCFuture<void> f_Rollback();
		NConcurrency::TCFuture<NStr::CStr> f_CreateSavepoint();
		NConcurrency::TCFuture<void> f_ReleaseSavepoint(NStr::CStr _Name);
		NConcurrency::TCFuture<void> f_RollbackToSavepoint(NStr::CStr _Name);
		template <typename tf_CFunction>
		auto f_WithTransaction(tf_CFunction _fFunction)
			-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CSqlTransaction *>(nullptr))))::CValue>
		;

		uint32 m_nPipelineLength = 5;

	private:
		friend struct CSqlDatabaseClient;

		template <typename tf_CFunction>
		static auto fsp_WithTransaction(CSqlTransaction *_pTransaction, tf_CFunction _fFunction)
			-> NConcurrency::TCFuture<typename decltype(_fFunction(fg_Move(*static_cast<CSqlTransaction *>(nullptr))))::CValue>
		;
		CSqlTransaction fp_CopyHandleForScope() const;

		CSqlTransactionInterface mp_Transaction;
	};

	template <typename t_CConnection, auto &tf_Repository>
	struct TCSqlRepositoryConnection
	{
		using CConnection = t_CConnection;
		using CRepository = NTraits::TCDecay<decltype(tf_Repository)>;
		using CTable = typename CRepository::CTable;
		using CRow = typename CRepository::CRow;
		using CID = typename NPrivate::TCSqlMemberPointerTraits<CRepository::mc_pIDMember>::CMember;

		TCSqlRepositoryConnection(CConnection *_pConnection);

		auto f_Get(CID _ID) -> NConcurrency::TCFuture<NStorage::TCOptional<CRow>>;
		auto f_Save(CRow _Row) -> NConcurrency::TCFuture<TCSqlSaveResult<CRow>>;
		auto f_Delete(CID _ID) -> NConcurrency::TCFuture<umint>;

	private:
		CConnection *mp_pConnection = nullptr;
	};
}

#include "Malterlib_SQL_Database.hpp"

#ifndef DMibPNoShortCuts
using namespace NMib::NSQL;
#endif
