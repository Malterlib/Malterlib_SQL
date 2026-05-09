// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/Database>
#include <Mib/Test/Exception>

#include "Test_Malterlib_SQL_DatabaseSchemaMigrations.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	using FCreateBackend = NFunction::TCFunctionMovable<NConcurrency::TCActor<ICSqlDatabaseBackendActor> (ICSqlSchemaVersions const *_pSchemaVersions)>;

	struct CPersonRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Email;
	};

	struct CProfileRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Email;
		NStr::CStr m_DisplayName;
	};

	struct CProfileProjection
	{
		NStr::CStr m_Email;
		NStr::CStr m_DisplayName;
	};

	struct CPersonProfileJoin
	{
		CPersonRow m_Person;
		CProfileRow m_Profile;
	};

	struct CFullMutationRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Label;
	};

	struct CUpsertRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Email;
		NStr::CStr m_DisplayName;
	};

	struct CPersonProfileUpsertJoin
	{
		CPersonRow m_Person;
		CProfileRow m_Profile;
		CUpsertRow m_Upsert;
	};

	struct CLeftJoinParentRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Key;
	};

	struct CLeftJoinChildRow
	{
		NStorage::TCOptional<NStr::CStr> m_Key;
		NStorage::TCOptional<NStr::CStr> m_Value;
	};

	struct CMappedAmount
	{
		fp64 m_Value = 0.0;
	};

	struct CMappedAmountRow
	{
		int64 m_ID = 0;
		CMappedAmount m_Amount;
	};

	struct CAccountID
	{
		constexpr CAccountID() = default;
		constexpr CAccountID(uint64 _Value);

		constexpr operator uint64 () const;

		constexpr auto operator <=> (CAccountID const &_Other) const noexcept = default;
		constexpr bool operator == (CAccountID const &_Other) const noexcept = default;

		uint64 m_Value = 0;
	};

	struct CStorageOnlyID
	{
		constexpr CStorageOnlyID() = default;
		constexpr CStorageOnlyID(uint64 _Value) = delete;

		constexpr auto operator <=> (CStorageOnlyID const &_Other) const noexcept = default;
		constexpr bool operator == (CStorageOnlyID const &_Other) const noexcept = default;

		uint64 m_Value = 0;
	};

	struct CInvalidMappedBackendWrapper
	{
		void *m_pValue = nullptr;
	};

	enum class EAccountState : uint8
	{
		mc_Disabled  = 1
		, mc_Enabled = 2
	};

	// Its conversions throw for a value outside 0 to 1000000, in both directions
	struct CCheckedValue
	{
		auto operator <=> (CCheckedValue const &_Other) const noexcept = default;
		bool operator == (CCheckedValue const &_Other) const noexcept = default;

		int64 m_Value = 0;
	};
}

namespace NMib::NSQL
{
	template <>
	struct TCSqlTypeTraits<NTest::NDatabaseBackend::CAccountID> : public TCSqlMappedTypeTraits<NTest::NDatabaseBackend::CAccountID, uint64>
	{
	};

	template <>
	struct TCSqlTypeTraits<NTest::NDatabaseBackend::CMappedAmount> : public TCSqlMappedTypeTraits<NTest::NDatabaseBackend::CMappedAmount, fp64>
	{
	};

	// A member mapped to floating-point storage sums as floating point, whatever its C++ type
	static_assert
		(
			NTraits::cIsSame
			<
				typename TCSqlExpressionResultType<TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Sum, &NTest::NDatabaseBackend::CMappedAmountRow::m_Amount>>::CType
				, NStorage::TCOptional<fp64>
			>
		)
	;

	template <>
	struct TCSqlTypeTraits<NTest::NDatabaseBackend::CStorageOnlyID>
	{
		using CSqlType = uint64;

		static constexpr CSqlType fs_ToSqlStorage(NTest::NDatabaseBackend::CStorageOnlyID const &_Value);
		static constexpr NTest::NDatabaseBackend::CStorageOnlyID fs_FromSqlStorage(CSqlType _Value);

		static constexpr bool mc_bSupported = true;
		static constexpr ESqlColumnType mc_ColumnType = ESqlColumnType::mc_UnsignedInteger64;
		static constexpr ESqlValueType mc_ValueType = ESqlValueType::mc_UnsignedInteger64;
	};

	template <>
	struct TCSqlTypeTraits<NTest::NDatabaseBackend::CInvalidMappedBackendWrapper> : public TCSqlMappedTypeTraits<NTest::NDatabaseBackend::CInvalidMappedBackendWrapper, void *>
	{
	};

	template <>
	struct TCSqlTypeTraits<NTest::NDatabaseBackend::CCheckedValue>
	{
		using CSqlType = int64;

		static CSqlType fs_ToSqlStorage(NTest::NDatabaseBackend::CCheckedValue const &_Value);
		static NTest::NDatabaseBackend::CCheckedValue fs_FromSqlStorage(CSqlType _Value);

		static constexpr bool mc_bSupported = true;
		static constexpr ESqlColumnType mc_ColumnType = ESqlColumnType::mc_Integer64;
		static constexpr ESqlValueType mc_ValueType = ESqlValueType::mc_Integer64;
	};
}

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	struct CCheckedRow
	{
		CCheckedValue m_ID;
		int64 m_Version = 0;
		CCheckedValue m_Value;
	};

	struct CStorageOnlyInsertRow
	{
		CStorageOnlyID m_ID;
	};

	struct CValueTypesRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Key;
		int8 m_Int8 = 0;
		int16 m_Int16 = 0;
		int32 m_Int32 = 0;
		int64 m_Int64 = 0;
		uint8 m_UInt8 = 0;
		uint16 m_UInt16 = 0;
		uint32 m_UInt32 = 0;
		uint64 m_UInt64 = 0;
		bool m_bFlag = false;
		fp32 m_Float32 = 0;
		fp64 m_Float64 = 0;
		NContainer::CIOByteVector m_Blob;
		NTime::CTime m_Time;
		EAccountState m_State = EAccountState::mc_Disabled;
		CAccountID m_AccountID;
	};

	struct CValueExpressionProjectionRow
	{
		NStr::CStr m_UpperKey;
		int64 m_KeyLength = 0;
		int64 m_Sum = 0;
		fp64 m_FloatValue = 0;
	};

	// Two same-typed members so that two selects aliasing the same expression into different members produce identical
	// SQL (hence the same content QueryID) yet require distinct row mappings - one writes m_First, the other m_Second.
	// The -1 sentinels make a value decoded into the wrong member observable.
	struct CAliasReuseRow
	{
		int64 m_First = -1;
		int64 m_Second = -1;
	};

	struct CNullableTypesRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Key;
		NStorage::TCOptional<int32> m_Integer;
		NStorage::TCOptional<NStr::CStr> m_Text;
		NStorage::TCOptional<bool> m_Boolean;
		NStorage::TCOptional<NContainer::CIOByteVector> m_Blob;
		NStorage::TCOptional<NTime::CTime> m_Time;
	};

	struct CNullableProjection
	{
		NStorage::TCOptional<int32> m_Integer;
		NStorage::TCOptional<NStr::CStr> m_Text;
	};

	struct CDefaultValuesRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Key;
		bool m_bEnabled = false;
		uint16 m_RetryCount = 0;
		NStr::CStr m_Label;
	};

	struct CConversionFailureRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Key;
		uint8 m_Tiny = 0;
	};

	struct CSaveRow
	{
		int64 m_ID = 0;
		int64 m_Version = 0;
		NStr::CStr m_Email;
		NStr::CStr m_DisplayName;
	};

	struct CCompositeThreeRow
	{
		uint64 m_TenantID = 0;
		NStr::CStr m_Category;
		NStr::CStr m_Key;
		NStr::CStr m_Value;
	};

	constexpr auto gc_PersonTable = fg_SqlTable<CPersonRow>
		(
			NStr::gc_Str<"people">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CPersonRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CPersonRow::m_Email)
			)
		)
	;

	constexpr auto gc_ProfileTable = fg_SqlTable<CProfileRow>
		(
			NStr::gc_Str<"profiles">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CProfileRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CProfileRow::m_Email)
				, fg_SqlColumn(NStr::gc_Str<"display_name">, &CProfileRow::m_DisplayName)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"idx_profiles_email">, NStr::gc_Str<"email">))
		)
	;

	constexpr auto gc_FullMutationTable = fg_SqlTable<CFullMutationRow>
		(
			NStr::gc_Str<"full_mutation_rows">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CFullMutationRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"label">, &CFullMutationRow::m_Label)
			)
		)
	;

	constexpr auto gc_UpsertTable = fg_SqlTable<CUpsertRow>
		(
			NStr::gc_Str<"upsert_people">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CUpsertRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CUpsertRow::m_Email)
				, fg_SqlColumn(NStr::gc_Str<"display_name">, &CUpsertRow::m_DisplayName)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"idx_upsert_people_email">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"email">))
		)
	;

	constexpr auto gc_LeftJoinParentTable = fg_SqlTable<CLeftJoinParentRow>
		(
			NStr::gc_Str<"left_join_parents">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CLeftJoinParentRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"key_value">, &CLeftJoinParentRow::m_Key)
			)
		)
	;

	constexpr auto gc_LeftJoinChildTable = fg_SqlTable<CLeftJoinChildRow>
		(
			NStr::gc_Str<"left_join_children">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"key_value">, &CLeftJoinChildRow::m_Key), fg_SqlColumn(NStr::gc_Str<"child_value">, &CLeftJoinChildRow::m_Value))
		)
	;

	constexpr auto gc_ValueTypesTable = fg_SqlTable<CValueTypesRow>
		(
			NStr::gc_Str<"value_types">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CValueTypesRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"key">, &CValueTypesRow::m_Key)
				, fg_SqlColumn(NStr::gc_Str<"int8_value">, &CValueTypesRow::m_Int8)
				, fg_SqlColumn(NStr::gc_Str<"int16_value">, &CValueTypesRow::m_Int16)
				, fg_SqlColumn(NStr::gc_Str<"int32_value">, &CValueTypesRow::m_Int32)
				, fg_SqlColumn(NStr::gc_Str<"int64_value">, &CValueTypesRow::m_Int64)
				, fg_SqlColumn(NStr::gc_Str<"uint8_value">, &CValueTypesRow::m_UInt8)
				, fg_SqlColumn(NStr::gc_Str<"uint16_value">, &CValueTypesRow::m_UInt16)
				, fg_SqlColumn(NStr::gc_Str<"uint32_value">, &CValueTypesRow::m_UInt32)
				, fg_SqlColumn(NStr::gc_Str<"uint64_value">, &CValueTypesRow::m_UInt64)
				, fg_SqlColumn(NStr::gc_Str<"flag">, &CValueTypesRow::m_bFlag)
				, fg_SqlColumn(NStr::gc_Str<"float32_value">, &CValueTypesRow::m_Float32)
				, fg_SqlColumn(NStr::gc_Str<"float64_value">, &CValueTypesRow::m_Float64)
				, fg_SqlColumn(NStr::gc_Str<"blob_value">, &CValueTypesRow::m_Blob)
				, fg_SqlColumn(NStr::gc_Str<"time_value">, &CValueTypesRow::m_Time)
				, fg_SqlColumn(NStr::gc_Str<"state">, &CValueTypesRow::m_State)
				, fg_SqlColumn(NStr::gc_Str<"account_id">, &CValueTypesRow::m_AccountID)
			)
		)
	;

	struct CIdOnlyRow
	{
		int64 m_ID = 0;
	};

	constexpr auto gc_IdOnlyTable = fg_SqlTable<CIdOnlyRow>
		(
			NStr::gc_Str<"id_only_default">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CIdOnlyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;

	struct CUniqueColumnRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
	};

	constexpr auto gc_UniqueColumnTable = fg_SqlTable<CUniqueColumnRow>
		(
			NStr::gc_Str<"unique_column_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CUniqueColumnRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CUniqueColumnRow::m_Code, ESqlColumnFlag::mc_Unique)
			)
		)
	;

	constexpr auto gc_NullableTypesTable = fg_SqlTable<CNullableTypesRow>
		(
			NStr::gc_Str<"nullable_types">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CNullableTypesRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"key">, &CNullableTypesRow::m_Key)
				, fg_SqlColumn(NStr::gc_Str<"integer_value">, &CNullableTypesRow::m_Integer)
				, fg_SqlColumn(NStr::gc_Str<"text_value">, &CNullableTypesRow::m_Text)
				, fg_SqlColumn(NStr::gc_Str<"boolean_value">, &CNullableTypesRow::m_Boolean)
				, fg_SqlColumn(NStr::gc_Str<"blob_value">, &CNullableTypesRow::m_Blob)
				, fg_SqlColumn(NStr::gc_Str<"time_value">, &CNullableTypesRow::m_Time)
			)
		)
	;

	constexpr auto gc_DefaultValuesTable = fg_SqlTable<CDefaultValuesRow>
		(
			NStr::gc_Str<"default_values">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDefaultValuesRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"key">, &CDefaultValuesRow::m_Key)
				, fg_SqlColumn(NStr::gc_Str<"enabled">, &CDefaultValuesRow::m_bEnabled, ESqlColumnFlag::mc_None, NStr::gc_Str<"1">)
				, fg_SqlColumn(NStr::gc_Str<"retry_count">, &CDefaultValuesRow::m_RetryCount, ESqlColumnFlag::mc_None, NStr::gc_Str<"7">)
				, fg_SqlColumn(NStr::gc_Str<"label">, &CDefaultValuesRow::m_Label, ESqlColumnFlag::mc_None, NStr::gc_Str<"'default label'">)
			)
		)
	;

	constexpr auto gc_ConversionFailureTable = fg_SqlTable<CConversionFailureRow>
		(
			NStr::gc_Str<"conversion_failures">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CConversionFailureRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"key">, &CConversionFailureRow::m_Key)
				, fg_SqlColumn(NStr::gc_Str<"tiny_value">, &CConversionFailureRow::m_Tiny, ESqlColumnFlag::mc_None, NStr::gc_Str<"300">)
			)
		)
	;

	constexpr auto gc_SaveTable = fg_SqlTable<CSaveRow>
		(
			NStr::gc_Str<"save_people">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CSaveRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"version">, &CSaveRow::m_Version)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CSaveRow::m_Email)
				, fg_SqlColumn(NStr::gc_Str<"display_name">, &CSaveRow::m_DisplayName)
			)
		)
	;
	constexpr auto gc_SaveRepository = fg_SqlRepository<&CSaveRow::m_ID, &CSaveRow::m_Version, &CSaveRow::m_Email, &CSaveRow::m_DisplayName>(gc_SaveTable);
	constexpr auto gc_CheckedTable = fg_SqlTable<CCheckedRow>
		(
			NStr::gc_Str<"checked_values">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CCheckedRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"version">, &CCheckedRow::m_Version)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CCheckedRow::m_Value)
			)
		)
	;
	constexpr auto gc_CheckedDatabase = fg_SqlDatabase(NStr::gc_Str<"checked_database">, gc_CheckedTable);
	constexpr auto gc_CheckedSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"checked_0001">, gc_CheckedDatabase);
	constexpr auto gc_CheckedSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_CheckedSchemaVersion));
	constexpr auto gc_CheckedRepository = fg_SqlRepository<&CCheckedRow::m_ID, &CCheckedRow::m_Version, &CCheckedRow::m_Value>(gc_CheckedTable);
	constexpr auto gc_InsertCheckedRow = fg_SqlPreparedInsert(gc_CheckedTable);
	constexpr auto gc_UpdateCheckedVersionByID = fg_SqlPreparedUpdate(gc_CheckedTable).f_Where(fg_SqlParamEq<&CCheckedRow::m_ID>()).f_Set<&CCheckedRow::m_Version>();
	constexpr auto gc_CompositeThreeTable = fg_SqlTable<CCompositeThreeRow>
		(
			NStr::gc_Str<"composite_three">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"tenant_id">, &CCompositeThreeRow::m_TenantID)
				, fg_SqlColumn(NStr::gc_Str<"category">, &CCompositeThreeRow::m_Category)
				, fg_SqlColumn(NStr::gc_Str<"key_value">, &CCompositeThreeRow::m_Key)
				, fg_SqlColumn(NStr::gc_Str<"stored_value">, &CCompositeThreeRow::m_Value)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlPrimaryKey(NStr::gc_Str<"composite_three_pk">, NStr::gc_Str<"tenant_id">, NStr::gc_Str<"category">, NStr::gc_Str<"key_value">))
		)
	;

	constexpr auto gc_Database = fg_SqlDatabase
		(
			NStr::gc_Str<"database_backend_test">
			, gc_PersonTable
			, gc_ProfileTable
			, gc_FullMutationTable
			, gc_UpsertTable
			, gc_LeftJoinParentTable
			, gc_LeftJoinChildTable
			, gc_ValueTypesTable
			, gc_NullableTypesTable
			, gc_DefaultValuesTable
			, gc_ConversionFailureTable
			, gc_SaveTable
			, NMib::NSQL::NTest::gc_UserTable
			, NMib::NSQL::NTest::gc_UserRoleTable
			, gc_CompositeThreeTable
		)
	;

	constexpr auto gc_SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"0001">, gc_Database);
	constexpr auto gc_SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_SchemaVersion));
	constexpr auto gc_Version1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(NMib::NSQL::NTest::NVersion1::gc_SchemaVersion));
	constexpr auto gc_StorageOnlyInsertTable = fg_SqlTable<CStorageOnlyInsertRow>
		(
			NStr::gc_Str<"storage_only_insert">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CStorageOnlyInsertRow::m_ID))
		)
	;
	constexpr auto gc_StorageOnlyPreparedInsert = fg_SqlPreparedInsert(gc_StorageOnlyInsertTable);
	constexpr auto gc_SelectStorageOnlyByID = fg_SqlPreparedSelect(gc_StorageOnlyInsertTable).f_Where(fg_SqlParamEq<&CStorageOnlyInsertRow::m_ID>());

	static_assert(!NTraits::cIsConstructibleWith<CStorageOnlyID, uint64>);
	static_assert(NPrivate::fg_SqlTableInsertValuesMatch<decltype(gc_StorageOnlyInsertTable), uint64>());
	static_assert(NPrivate::fg_SqlPreparedInsertValuesMatch<gc_StorageOnlyPreparedInsert, uint64>());
	static_assert(NPrivate::fg_SqlPreparedSelectParameterMatches<gc_SelectStorageOnlyByID, uint64>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParameterMatches<gc_SelectStorageOnlyByID, NStr::CStr>());

	constexpr auto gc_SelectPersonByEmail = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlParamEq<&CPersonRow::m_Email>());
	// This aggregate returns a row even when the filter matches nothing
	constexpr auto gc_SelectPersonCountByEmailLike = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlParamLike<&CPersonRow::m_Email>()).f_Select(fg_SqlCount());
	constexpr auto gc_SelectPersonBackendMaxIDByEmailLike = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlParamLike<&CPersonRow::m_Email>())
		.f_Select(fg_SqlBackendFunction<NStr::gc_Str<"MAX">, &CPersonRow::m_ID>())
	;
	constexpr auto gc_UpdatePersonEmailByEmail = fg_SqlPreparedUpdate(gc_PersonTable).f_Where(fg_SqlParamEq<&CPersonRow::m_Email>()).f_Set<&CPersonRow::m_Email>();
	constexpr auto gc_DeletePersonByEmail = fg_SqlPreparedDelete(gc_PersonTable).f_Where(fg_SqlParamEq<&CPersonRow::m_Email>());
	static_assert(NPrivate::fg_SqlPreparedUpdateValuesMatch<gc_UpdatePersonEmailByEmail, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedUpdateValuesMatch<gc_UpdatePersonEmailByEmail, int64, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedUpdateValuesMatch<gc_UpdatePersonEmailByEmail, NStr::CStr>());

	static_assert(NPrivate::fg_SqlPreparedDeleteValuesMatch<gc_DeletePersonByEmail, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedDeleteValuesMatch<gc_DeletePersonByEmail, int64>());
	static_assert(!NPrivate::fg_SqlPreparedDeleteValuesMatch<gc_DeletePersonByEmail>());
	constexpr auto gc_SelectPersonEmailOnlyByEmail = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlParamEq<&CPersonRow::m_Email>()).f_Select<&CPersonRow::m_Email>();
	constexpr auto gc_SelectPersonRepeatedSelectByEmail = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlParamEq<&CPersonRow::m_Email>())
		.f_Select<&CPersonRow::m_ID>()
		.f_Select<&CPersonRow::m_Email>()
	;
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectPersonEmailOnlyByEmail)>::CRow, NStorage::TCTuple<NStr::CStr>>);
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectPersonRepeatedSelectByEmail)>::CRow, NStorage::TCTuple<NStr::CStr>>);
	constexpr auto gc_SelectPersonByEmailGeAndLike = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlAnd(fg_SqlParamGe<&CPersonRow::m_Email>(), fg_SqlParamLike<&CPersonRow::m_Email>()));
	constexpr auto gc_SelectPersonByEmailEqOrEq = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlOr(fg_SqlParamEq<&CPersonRow::m_Email>(), fg_SqlParamEq<&CPersonRow::m_Email>()))
		.f_OrderByAscending<&CPersonRow::m_Email>()
	;
	constexpr auto gc_SelectPersonByEmailIn = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlParamIn<&CPersonRow::m_Email, 2>());
	constexpr auto gc_SelectPersonByNestedAndOr = fg_SqlPreparedSelect(gc_PersonTable).f_Where
		(
			fg_SqlAnd(fg_SqlParamGe<&CPersonRow::m_Email>(), fg_SqlOr(fg_SqlParamEq<&CPersonRow::m_Email>(), fg_SqlParamEq<&CPersonRow::m_Email>()))
		)
	;
	constexpr auto gc_SelectPersonByNotComposite = fg_SqlPreparedSelect(gc_PersonTable).f_Where
		(
			fg_SqlAnd(fg_SqlParamGe<&CPersonRow::m_Email>(), fg_SqlNot(fg_SqlOr(fg_SqlParamEq<&CPersonRow::m_Email>(), fg_SqlParamEq<&CPersonRow::m_Email>())))
		)
	;
	constexpr auto gc_SelectPersonByEmailGe = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlParamGe<&CPersonRow::m_Email>());
	constexpr auto gc_SelectPersonByEmailGeDescending = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlParamGe<&CPersonRow::m_Email>()).f_OrderByDescending<&CPersonRow::m_Email>();
	constexpr auto gc_SelectPersonByEmailGeDescendingLimitOffset = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlParamGe<&CPersonRow::m_Email>())
		.f_OrderByDescending<&CPersonRow::m_Email>()
		.f_WithLimit()
		.f_WithOffset()
	;
	constexpr auto gc_SelectPersonByEmailGeAscendingOffsetOnly = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlParamGe<&CPersonRow::m_Email>())
		.f_OrderByAscending<&CPersonRow::m_Email>()
		.f_WithOffset()
	;
	constexpr auto gc_SelectPersonByEmailGeDescendingRepeatedLimitOffset = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlParamGe<&CPersonRow::m_Email>())
		.f_OrderByDescending<&CPersonRow::m_Email>()
		.f_WithLimit()
		.f_WithLimit()
		.f_WithOffset()
		.f_WithOffset()
	;
	static_assert(gc_SelectPersonByEmail.m_QueryID != gc_SelectPersonEmailOnlyByEmail.m_QueryID);
	static_assert(gc_SelectPersonByEmail.m_QueryID != gc_SelectPersonByEmailGe.m_QueryID);
	static_assert(gc_SelectPersonByEmailGe.m_QueryID != gc_SelectPersonByEmailGeDescending.m_QueryID);
	static_assert(gc_SelectPersonByEmailGeDescending.m_QueryID != gc_SelectPersonByEmailGeDescendingLimitOffset.m_QueryID);
	static_assert(gc_SelectPersonByEmailGeDescendingLimitOffset.m_QueryID == gc_SelectPersonByEmailGeDescendingRepeatedLimitOffset.m_QueryID);
	static_assert(gc_SelectPersonByEmailEqOrEq.m_QueryID != gc_SelectPersonByEmailIn.m_QueryID);
	static_assert(gc_SelectPersonByEmailEqOrEq.m_QueryID != gc_SelectPersonByNestedAndOr.m_QueryID);
	constexpr auto gc_SelectDistinctPersonByEmailGeDescendingFirst = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlParamGe<&CPersonRow::m_Email>())
		.f_Distinct()
		.f_OrderByDescending<&CPersonRow::m_Email>()
		.f_WithLimit()
	;
	constexpr auto gc_SelectDistinctPersonEmailOnlyByEmailLike = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlParamLike<&CPersonRow::m_Email>())
		.f_Distinct()
		.f_Select<&CPersonRow::m_Email>()
		.f_OrderByAscending<&CPersonRow::m_Email>()
	;
	static_assert(gc_SelectDistinctPersonByEmailGeDescendingFirst.m_QueryID != gc_SelectDistinctPersonEmailOnlyByEmailLike.m_QueryID);
	constexpr auto gc_SelectPersonByEmailLike = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlParamLike<&CPersonRow::m_Email>());
	constexpr auto gc_InsertPerson = fg_SqlPreparedInsert(gc_PersonTable);
	constexpr auto gc_InsertPersonEmail = fg_SqlPreparedInsert(gc_PersonTable).f_Columns<&CPersonRow::m_Email>();
	constexpr auto gc_InsertSchemaUser = fg_SqlPreparedInsert(NMib::NSQL::NTest::gc_UserTable);
	constexpr auto gc_InsertUserRole = fg_SqlPreparedInsert(NMib::NSQL::NTest::gc_UserRoleTable);
	constexpr auto gc_InsertCompositeThree = fg_SqlPreparedInsert(gc_CompositeThreeTable);
	static_assert(NPrivate::fg_SqlPreparedInsertGeneratedPrimaryKeyColumnCount<gc_InsertPerson>() == 1);
	static_assert(NPrivate::fg_SqlTableMemberIsGeneratedPrimaryKey<gc_InsertPerson.m_Table, &CPersonRow::m_ID>());
	static_assert(NPrivate::fg_SqlTablePrimaryKeyColumnCount<NMib::NSQL::NTest::gc_UserRoleTable>() == 2);
	static_assert(NPrivate::fg_SqlTableMembersArePrimaryKey<NMib::NSQL::NTest::gc_UserRoleTable, &NMib::NSQL::NTest::CUserRoleRow::m_UserID, &NMib::NSQL::NTest::CUserRoleRow::m_Role>());
	static_assert
		(
			NPrivate::fg_SqlCompositeIDSelectParametersMatch
			<
				NMib::NSQL::NTest::gc_UserRoleTable
				, TCSqlCompositeID<&NMib::NSQL::NTest::CUserRoleRow::m_UserID, &NMib::NSQL::NTest::CUserRoleRow::m_Role>
				, uint64
				, NStr::CStr
			>
			()
		)
	;
	static_assert
		(
			NPrivate::fg_SqlCompositeIDDeleteValuesMatch
			<
				NMib::NSQL::NTest::gc_UserRoleTable
				, TCSqlCompositeID<&NMib::NSQL::NTest::CUserRoleRow::m_UserID, &NMib::NSQL::NTest::CUserRoleRow::m_Role>
				, uint64
				, NStr::CStr
			>
			()
		)
	;
	static_assert(NPrivate::fg_SqlTablePrimaryKeyColumnCount<gc_CompositeThreeTable>() == 3);
	static_assert
		(
			NPrivate::fg_SqlCompositeIDMatchesTable<gc_CompositeThreeTable, TCSqlCompositeID<&CCompositeThreeRow::m_TenantID, &CCompositeThreeRow::m_Category, &CCompositeThreeRow::m_Key>>()
		)
	;
	// A repeated member would leave part of the key unconstrained
	static_assert
		(
			!NPrivate::fg_SqlCompositeIDMatchesTable<gc_CompositeThreeTable, TCSqlCompositeID<&CCompositeThreeRow::m_TenantID, &CCompositeThreeRow::m_TenantID, &CCompositeThreeRow::m_Key>>()
		)
	;
	static_assert
		(
			NPrivate::fg_SqlCompositeIDSelectParametersMatch
			<
				gc_CompositeThreeTable
				, TCSqlCompositeID<&CCompositeThreeRow::m_TenantID, &CCompositeThreeRow::m_Category, &CCompositeThreeRow::m_Key>
				, uint64
				, NStr::CStr
				, NStr::CStr
			>
			()
		)
	;
	constexpr auto gc_UpdateAllFullMutationLabels = fg_SqlPreparedUpdate(gc_FullMutationTable).f_AllRows().f_Set<&CFullMutationRow::m_Label>();
	constexpr auto gc_DeleteAllFullMutationRows = fg_SqlPreparedDelete(gc_FullMutationTable).f_AllRows();
	constexpr auto gc_UpsertPersonDisplayNameByEmail = fg_SqlPreparedUpsert(gc_UpsertTable).f_OnConflict<&CUpsertRow::m_Email>().f_Update<&CUpsertRow::m_DisplayName>();
	constexpr auto gc_SelectUpsertPersonByEmail = fg_SqlPreparedSelect(gc_UpsertTable).f_Where(fg_SqlParamEq<&CUpsertRow::m_Email>());
	constexpr auto gc_UpdateUpsertPersonDisplayNameByEmail = fg_SqlPreparedUpdate(gc_UpsertTable).f_Where(fg_SqlParamEq<&CUpsertRow::m_Email>()).f_Set<&CUpsertRow::m_DisplayName>();
	constexpr auto gc_DeleteUpsertPersonByEmail = fg_SqlPreparedDelete(gc_UpsertTable).f_Where(fg_SqlParamEq<&CUpsertRow::m_Email>());
	static_assert(NPrivate::fg_SqlPreparedUpdateValuesMatch<gc_UpdateAllFullMutationLabels, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedUpdateValuesMatch<gc_UpdateAllFullMutationLabels>());
	static_assert(NPrivate::fg_SqlPreparedDeleteValuesMatch<gc_DeleteAllFullMutationRows>());
	static_assert(!NPrivate::fg_SqlPreparedDeleteValuesMatch<gc_DeleteAllFullMutationRows, NStr::CStr>());
	static_assert(NPrivate::fg_SqlPreparedUpsertValuesMatch<gc_UpsertPersonDisplayNameByEmail, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedUpsertValuesMatch<gc_UpsertPersonDisplayNameByEmail, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedUpsertValuesMatch<gc_UpsertPersonDisplayNameByEmail, int64, NStr::CStr>());
	// The upsert omits the auto-generated id column, so passing the full row (id, email, display_name) must be
	// rejected at compile time rather than binding one value more than the generated SQL has placeholders.
	static_assert(!NPrivate::fg_SqlPreparedUpsertValuesMatch<gc_UpsertPersonDisplayNameByEmail, int64, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlTableInsertValuesMatch<NTraits::TCDecay<decltype(gc_UserRoleTable)>, NStr::CStr>());
	constexpr auto gc_InsertProfile = fg_SqlPreparedInsert(gc_ProfileTable);
	constexpr auto gc_InsertProfileEmailAndDisplayName = fg_SqlPreparedInsert(gc_ProfileTable).f_Columns<&CProfileRow::m_Email, &CProfileRow::m_DisplayName>();
	// Reverse of the table's column order: each value must still bind to its own column
	constexpr auto gc_InsertProfileDisplayNameAndEmail = fg_SqlPreparedInsert(gc_ProfileTable).f_Columns<&CProfileRow::m_DisplayName, &CProfileRow::m_Email>();
	constexpr auto gc_InsertFullMutation = fg_SqlPreparedInsert(gc_FullMutationTable);
	constexpr auto gc_SelectProfileByEmail = fg_SqlPreparedSelect(gc_ProfileTable).f_Where(fg_SqlParamEq<&CProfileRow::m_Email>());
	constexpr auto gc_SelectProfileByEmailLikeOrderEmailDisplay = fg_SqlPreparedSelect(gc_ProfileTable)
		.f_Where(fg_SqlParamLike<&CProfileRow::m_Email>())
		.f_OrderByAscending<&CProfileRow::m_Email>()
		.f_OrderByDescending<&CProfileRow::m_DisplayName>()
	;
	constexpr auto gc_SelectProfileEmailAndDisplayNameByEmail = fg_SqlPreparedSelect(gc_ProfileTable)
		.f_Where(fg_SqlParamEq<&CProfileRow::m_Email>())
		.f_Select<&CProfileRow::m_Email, &CProfileRow::m_DisplayName>()
	;
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectProfileEmailAndDisplayNameByEmail)>::CRow, NStorage::TCTuple<NStr::CStr, NStr::CStr>>);
	constexpr auto gc_JoinPersonProfilesByEmail = fg_SqlPreparedInnerJoin(gc_PersonTable, gc_ProfileTable, fg_SqlJoinOnEq<&CPersonRow::m_Email, &CProfileRow::m_Email>());
	constexpr auto gc_JoinPersonProfilesToUpserts = gc_JoinPersonProfilesByEmail.f_InnerJoin(gc_UpsertTable, fg_SqlJoinOnEq<&CProfileRow::m_DisplayName, &CUpsertRow::m_DisplayName>());
	constexpr auto gc_JoinPersonProfilesToUpsertsToProfiles = gc_JoinPersonProfilesToUpserts.f_InnerJoin(gc_ProfileTable, fg_SqlJoinOnEq<&CUpsertRow::m_Email, &CProfileRow::m_Email>());

	// PostgreSQL's prepared statement caches fall back to the query ID, so statements generating different SQL must not share one
	constexpr auto gc_JoinPersonProfilesToUpsertsByPersonEmail = gc_JoinPersonProfilesByEmail.f_InnerJoin(gc_UpsertTable, fg_SqlJoinOnEq<&CPersonRow::m_Email, &CUpsertRow::m_Email>());
	constexpr auto gc_JoinPersonProfilesToUpsertsByProfileEmail = gc_JoinPersonProfilesByEmail.f_InnerJoin(gc_UpsertTable, fg_SqlJoinOnEq<&CProfileRow::m_Email, &CUpsertRow::m_Email>());
	static_assert(gc_JoinPersonProfilesToUpsertsByPersonEmail.m_QueryID != gc_JoinPersonProfilesToUpsertsByProfileEmail.m_QueryID);

	constexpr auto gc_UpsertConflictIDUpdateEmailAndDisplayName = fg_SqlPreparedUpsert(gc_UpsertTable)
		.f_OnConflict<&CUpsertRow::m_ID>()
		.f_Update<&CUpsertRow::m_Email, &CUpsertRow::m_DisplayName>()
	;
	constexpr auto gc_UpsertConflictIDAndEmailUpdateDisplayName = fg_SqlPreparedUpsert(gc_UpsertTable)
		.f_OnConflict<&CUpsertRow::m_ID, &CUpsertRow::m_Email>()
		.f_Update<&CUpsertRow::m_DisplayName>()
	;
	static_assert(gc_UpsertConflictIDUpdateEmailAndDisplayName.m_QueryID != gc_UpsertConflictIDAndEmailUpdateDisplayName.m_QueryID);

	constexpr auto gc_PersonTableReorderedColumns = fg_SqlTable<CPersonRow>
		(
			NStr::gc_Str<"people">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"email">, &CPersonRow::m_Email)
				, fg_SqlColumn(NStr::gc_Str<"id">, &CPersonRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
			)
		)
	;
	static_assert(fg_SqlPreparedInsert(gc_PersonTable).m_QueryID != fg_SqlPreparedInsert(gc_PersonTableReorderedColumns).m_QueryID);
	static_assert(NTraits::cIsSame<NPrivate::TCSqlProjectionBackendRow<NStorage::TCTuple<int64>, &CPersonRow::m_ID>, NStorage::TCTuple<int64>>);

	struct CPersonNarrowKeyRow
	{
		int32 m_ID = 0;
		NStr::CStr m_Email;
	};

	constexpr auto gc_PersonTableNarrowKey = fg_SqlTable<CPersonNarrowKeyRow>
		(
			NStr::gc_Str<"people">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CPersonNarrowKeyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CPersonNarrowKeyRow::m_Email)
			)
		)
	;
	static_assert
		(
			fg_SqlPreparedDelete(gc_PersonTable).f_Where(fg_SqlParamEq<&CPersonRow::m_ID>()).m_QueryID
			!= fg_SqlPreparedDelete(gc_PersonTableNarrowKey).f_Where(fg_SqlParamEq<&CPersonNarrowKeyRow::m_ID>()).m_QueryID
		)
	;
	static_assert
		(
			fg_SqlPreparedSelect(gc_PersonTable).f_Where(CSqlAllRowsPredicate{}).m_QueryID != fg_SqlPreparedSelect(gc_PersonTableReorderedColumns).f_Where(CSqlAllRowsPredicate{}).m_QueryID
		)
	;
	static_assert
		(
			fg_SqlPreparedUpsert(gc_PersonTable).f_OnConflict<&CPersonRow::m_ID>().f_Update<&CPersonRow::m_Email>().m_QueryID
			!= fg_SqlPreparedUpsert(gc_PersonTableReorderedColumns).f_OnConflict<&CPersonRow::m_ID>().f_Update<&CPersonRow::m_Email>().m_QueryID
		)
	;
	static_assert
		(
			fg_SqlPreparedInnerJoin(gc_PersonTable, gc_ProfileTable, fg_SqlJoinOnEq<&CPersonRow::m_Email, &CProfileRow::m_Email>()).m_QueryID
			!= fg_SqlPreparedInnerJoin(gc_PersonTableReorderedColumns, gc_ProfileTable, fg_SqlJoinOnEq<&CPersonRow::m_Email, &CProfileRow::m_Email>()).m_QueryID
		)
	;
	constexpr auto gc_SelectProfileEmails = fg_SqlPreparedSelect(gc_ProfileTable).f_Where(CSqlAllRowsPredicate{}).f_Select<&CProfileRow::m_Email>();
	constexpr auto gc_SelectProfileEmailsByDisplayName = fg_SqlPreparedSelect(gc_ProfileTable).f_Where(fg_SqlParamEq<&CProfileRow::m_DisplayName>()).f_Select<&CProfileRow::m_Email>();
	constexpr auto gc_SelectProfileEmailsByDisplayNameLimitOffset = fg_SqlPreparedSelect(gc_ProfileTable)
		.f_Where(fg_SqlParamEq<&CProfileRow::m_DisplayName>())
		.f_Select<&CProfileRow::m_Email>()
		.f_WithLimit()
		.f_WithOffset()
	;
	constexpr auto gc_SelectProfileEmailsByDisplayNameAndEmailLikeLimited = fg_SqlPreparedSelect(gc_ProfileTable)
		.f_Where(fg_SqlAnd(fg_SqlParamEq<&CProfileRow::m_DisplayName>(), fg_SqlParamLike<&CProfileRow::m_Email>()))
		.f_Select<&CProfileRow::m_Email>()
		.f_OrderByAscending<&CProfileRow::m_Email>()
		.f_WithLimit()
	;
	constexpr auto gc_SelectProfileEmailsByEmailLikeHavingLimitOffset = fg_SqlPreparedSelect(gc_ProfileTable)
		.f_Where(fg_SqlParamLike<&CProfileRow::m_Email>())
		.f_GroupBy<&CProfileRow::m_Email>()
		.f_Having(fg_SqlHavingGt(fg_SqlCount()))
		.f_Select(fg_SqlColumn<&CProfileRow::m_Email>())
		.f_WithLimit()
		.f_WithOffset()
	;
	constexpr auto gc_SelectPersonEmails = fg_SqlPreparedSelect(gc_PersonTable).f_Where(CSqlAllRowsPredicate{}).f_Select<&CPersonRow::m_Email>();
	constexpr auto gc_SelectPersonEmailsByEmailLike = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlParamLike<&CPersonRow::m_Email>()).f_Select<&CPersonRow::m_Email>();
	constexpr auto gc_SelectPersonEmailsOrderedLimited = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(CSqlAllRowsPredicate{})
		.f_Select<&CPersonRow::m_Email>()
		.f_OrderByAscending<&CPersonRow::m_Email>()
		.f_WithLimit()
	;
	constexpr auto gc_SelectProfileEmailsByEmailLike = fg_SqlPreparedSelect(gc_ProfileTable).f_Where(fg_SqlParamLike<&CProfileRow::m_Email>()).f_Select<&CProfileRow::m_Email>();
	constexpr auto gc_SelectProfileEmailsOrdered = fg_SqlPreparedSelect(gc_ProfileTable)
		.f_Where(CSqlAllRowsPredicate{})
		.f_Select<&CProfileRow::m_Email>()
		.f_OrderByAscending<&CProfileRow::m_Email>()
	;
	constexpr auto gc_UnionPersonAndProfileEmails = fg_SqlUnion<gc_SelectPersonEmails, gc_SelectProfileEmails>();
	constexpr auto gc_UnionAllPersonAndProfileEmails = fg_SqlUnionAll<gc_SelectPersonEmails, gc_SelectProfileEmails>();
	constexpr auto gc_IntersectPersonAndProfileEmails = fg_SqlIntersect<gc_SelectPersonEmails, gc_SelectProfileEmails>();
	constexpr auto gc_ExceptPersonProfileEmails = fg_SqlExcept<gc_SelectPersonEmails, gc_SelectProfileEmails>();
	constexpr auto gc_UnionParameterizedPersonAndProfileEmails = fg_SqlUnion<gc_SelectPersonEmailsByEmailLike, gc_SelectProfileEmailsByEmailLike>();
	constexpr auto gc_UnionWithModifiedLeftOperand = fg_SqlUnion<gc_SelectPersonEmailsOrderedLimited, gc_SelectProfileEmails>();
	constexpr auto gc_UnionWithModifiedRightOperand = fg_SqlUnion<gc_SelectPersonEmails, gc_SelectProfileEmailsOrdered>();
	constexpr auto gc_UnionThreeParameterizedEmailSelects = fg_SqlUnion<gc_UnionParameterizedPersonAndProfileEmails, gc_SelectProfileEmailsByDisplayName>();
	constexpr auto gc_IntersectUnionedEmailsWithProfileDisplayName = fg_SqlIntersect<gc_UnionParameterizedPersonAndProfileEmails, gc_SelectProfileEmailsByDisplayName>();
	constexpr auto gc_UnionProfileEmailsLikeOrDisplayName = fg_SqlUnion<gc_SelectProfileEmailsByEmailLike, gc_SelectProfileEmailsByDisplayName>();
	constexpr auto gc_ExceptPersonEmailsFromUnionedProfileEmails = fg_SqlExcept<gc_SelectPersonEmailsByEmailLike, gc_UnionProfileEmailsLikeOrDisplayName>();
	constexpr auto gc_SelectLeftJoinChildKeys = fg_SqlPreparedSelect(gc_LeftJoinChildTable).f_Where(CSqlAllRowsPredicate{}).f_Select<&CLeftJoinChildRow::m_Key>();
	constexpr auto gc_SelectPeopleWithProfileEmailSubquery = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlInSubquery<&CPersonRow::m_Email, gc_SelectProfileEmails>());
	constexpr auto gc_SelectPeopleWithProfileDisplayNameSubquery = fg_SqlPreparedSelect(gc_PersonTable).f_Where
		(
			fg_SqlInSubquery<&CPersonRow::m_Email, gc_SelectProfileEmailsByDisplayName>()
		)
	;
	constexpr auto gc_SelectPeopleByEmailLikeWithProfileDisplayNameSubquery = fg_SqlPreparedSelect(gc_PersonTable).f_Where
		(
			fg_SqlAnd(fg_SqlParamLike<&CPersonRow::m_Email>(), fg_SqlInSubquery<&CPersonRow::m_Email, gc_SelectProfileEmailsByDisplayName>())
		)
	;
	constexpr auto gc_SelectPeopleByEmailLikeWithLimitedProfileDisplayNameSubquery = fg_SqlPreparedSelect(gc_PersonTable).f_Where
		(
			fg_SqlAnd(fg_SqlParamLike<&CPersonRow::m_Email>(), fg_SqlInSubquery<&CPersonRow::m_Email, gc_SelectProfileEmailsByDisplayNameLimitOffset>())
		)
	;
	constexpr auto gc_SelectPeopleByEmailLikeWithHavingLimitedProfileEmailSubquery = fg_SqlPreparedSelect(gc_PersonTable).f_Where
		(
			fg_SqlAnd(fg_SqlParamLike<&CPersonRow::m_Email>(), fg_SqlInSubquery<&CPersonRow::m_Email, gc_SelectProfileEmailsByEmailLikeHavingLimitOffset>())
		)
	;
	constexpr auto gc_SelectPeopleWithParameterizedUnionSubquery = fg_SqlPreparedSelect(gc_PersonTable).f_Where
		(
			fg_SqlInSubquery<&CPersonRow::m_Email, gc_UnionParameterizedPersonAndProfileEmails>()
		)
	;
	constexpr auto gc_SelectPeopleWhenProfilesExist = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlExists<gc_SelectProfileEmails>());
	constexpr auto gc_SelectPeopleWhenJoinedProfilesExist = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlExists<gc_JoinPersonProfilesByEmail>());
	constexpr auto gc_SelectPeopleWhenProfileDisplayNameExists = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlExists<gc_SelectProfileEmailsByDisplayName>());
	constexpr auto gc_SelectPeopleWhenNoLeftJoinChildrenExist = fg_SqlPreparedSelect(gc_PersonTable).f_Where(fg_SqlNotExists<gc_SelectLeftJoinChildKeys>());
	// A parameterized subquery inside a compound predicate, followed by an outer LIMIT/OFFSET, exercises the binary
	// predicate parameter count: the subquery's parameter must be counted so the outer placeholders do not reuse it.
	constexpr auto gc_SelectPeopleInProfileSubqueryAndEmailLikeLimited = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlAnd(fg_SqlInSubquery<&CPersonRow::m_Email, gc_SelectProfileEmailsByDisplayName>(), fg_SqlParamLike<&CPersonRow::m_Email>()))
		.f_WithLimit()
		.f_WithOffset()
	;
	// The HAVING placeholder must be numbered after the WHERE subquery's parameter
	constexpr auto gc_SelectPeopleInProfileSubqueryGroupedHaving = fg_SqlPreparedSelect(gc_PersonTable)
		.f_Where(fg_SqlInSubquery<&CPersonRow::m_Email, gc_SelectProfileEmailsByDisplayName>())
		.f_GroupBy<&CPersonRow::m_Email>()
		.f_Having(fg_SqlHavingGt(fg_SqlCount()))
	;
	constexpr auto gc_DeletePeopleInLimitedProfileSubquery = fg_SqlPreparedDelete(gc_PersonTable).f_Where
		(
			fg_SqlInSubquery<&CPersonRow::m_Email, gc_SelectProfileEmailsByDisplayNameLimitOffset>()
		)
	;
	static_assert(NPrivate::fg_SqlPreparedDeleteValuesMatch<gc_DeletePeopleInLimitedProfileSubquery, NStr::CStr, int64, int64>());
	static_assert(!NPrivate::fg_SqlPreparedDeleteValuesMatch<gc_DeletePeopleInLimitedProfileSubquery, NStr::CStr>());
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_JoinPersonProfilesByEmail)>::CRow, NStorage::TCTuple<CPersonRow, CProfileRow>>);
	constexpr auto gc_LeftJoinParentsToChildren = fg_SqlPreparedLeftJoin
		(
			gc_LeftJoinParentTable
			, gc_LeftJoinChildTable
			, fg_SqlJoinOnEq<&CLeftJoinParentRow::m_Key, &CLeftJoinChildRow::m_Key>()
		)
	;
	constexpr auto gc_LeftJoinParentsToChildChain = gc_LeftJoinParentsToChildren.f_LeftJoin(gc_LeftJoinChildTable, fg_SqlJoinOnEq<&CLeftJoinChildRow::m_Key, &CLeftJoinChildRow::m_Key>());
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_LeftJoinParentsToChildren)>::CRow, NStorage::TCTuple<CLeftJoinParentRow, CLeftJoinChildRow>>);
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_JoinPersonProfilesToUpserts)>::CRow, NStorage::TCTuple<CPersonRow, CProfileRow, CUpsertRow>>);
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_JoinPersonProfilesToUpsertsToProfiles)>::CRow, NStorage::TCTuple<CPersonRow, CProfileRow, CUpsertRow, CProfileRow>>);
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_LeftJoinParentsToChildChain)>::CRow, NStorage::TCTuple<CLeftJoinParentRow, CLeftJoinChildRow, CLeftJoinChildRow>>);
	// An unmatched row reads NULL for every right-hand column, so f_LeftJoin requires them all to be nullable
	static_assert(NPrivate::fg_SqlTableMembersAllNullable<NTraits::TCDecay<decltype(gc_LeftJoinChildTable)>>());
	static_assert(!NPrivate::fg_SqlTableMembersAllNullable<NTraits::TCDecay<decltype(gc_PersonTable)>>());
	constexpr auto gc_InsertLeftJoinParent = fg_SqlPreparedInsert(gc_LeftJoinParentTable);
	constexpr auto gc_InsertLeftJoinChild = fg_SqlPreparedInsert(gc_LeftJoinChildTable);
	constexpr auto gc_InsertValueTypes = fg_SqlPreparedInsert(gc_ValueTypesTable);
	constexpr auto gc_JoinValueTypesByKeyAndInt32Less = fg_SqlPreparedInnerJoin
		(
			gc_ValueTypesTable
			, gc_ValueTypesTable
			, fg_SqlJoinOnAll(fg_SqlJoinOnEq<&CValueTypesRow::m_Key, &CValueTypesRow::m_Key>(), fg_SqlJoinOnLt<&CValueTypesRow::m_Int32, &CValueTypesRow::m_Int32>())
		)
	;
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_JoinValueTypesByKeyAndInt32Less)>::CRow, NStorage::TCTuple<CValueTypesRow, CValueTypesRow>>);
	constexpr auto gc_SelectValueTypesByKey = fg_SqlPreparedSelect(gc_ValueTypesTable).f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>());
	constexpr auto gc_SelectValueTypesByKeyIn = fg_SqlPreparedSelect(gc_ValueTypesTable).f_Where(fg_SqlParamIn<&CValueTypesRow::m_Key, 2>());
	constexpr auto gc_SelectValueTypesByKeyAndInt32 = fg_SqlPreparedSelect(gc_ValueTypesTable).f_Where
		(
			fg_SqlAnd(fg_SqlParamEq<&CValueTypesRow::m_Key>(), fg_SqlParamEq<&CValueTypesRow::m_Int32>())
		)
	;
	constexpr auto gc_SelectValueTypeAggregatesByKeyLike = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamLike<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlCount(), fg_SqlSum<&CValueTypesRow::m_Int32>(), fg_SqlAvg<&CValueTypesRow::m_Int32>(), fg_SqlMin<&CValueTypesRow::m_Int32>(), fg_SqlMax<&CValueTypesRow::m_Int32>())
	;
	// PostgreSQL has no MIN or MAX over boolean or bytea
	constexpr auto gc_SelectValueTypeFlagAndBlobMinMaxByKeyLike = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamLike<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlMin<&CValueTypesRow::m_bFlag>(), fg_SqlMax<&CValueTypesRow::m_bFlag>(), fg_SqlMin<&CValueTypesRow::m_Blob>(), fg_SqlMax<&CValueTypesRow::m_Blob>())
	;
	// SUM over a 64-bit column (BIGINT on PostgreSQL) and SUM over a floating-point column exercise both result
	// paths of the aggregate generator: PostgreSQL widens SUM(bigint) to NUMERIC, which must be cast back to
	// BIGINT for the int64 projection, while SUM(double precision) keeps its type and needs no cast.
	constexpr auto gc_SelectValueTypeInt64SumByKeyLike = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamLike<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlSum<&CValueTypesRow::m_Int64>())
	;
	constexpr auto gc_SelectValueTypeFloat64SumByKeyLike = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamLike<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlSum<&CValueTypesRow::m_Float64>())
	;
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectValueTypeInt64SumByKeyLike)>::CRow, NStorage::TCTuple<NStorage::TCOptional<int64>>>);
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectValueTypeFloat64SumByKeyLike)>::CRow, NStorage::TCTuple<NStorage::TCOptional<fp64>>>);
	constexpr auto gc_SelectValueTypeExpressionsByKey = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>())
		.f_Select
		(
			fg_SqlAdd<&CValueTypesRow::m_Int32, &CValueTypesRow::m_Int16>()
			, fg_SqlUpper<&CValueTypesRow::m_Key>()
			, fg_SqlLength<&CValueTypesRow::m_Key>()
			, fg_SqlCastFloat<&CValueTypesRow::m_Int32>()
			, fg_SqlBackendFunction<NStr::gc_Str<"ABS">, &CValueTypesRow::m_Int32>()
		)
	;
	constexpr auto gc_SelectValueTypeFloat32Sum = fg_SqlPreparedSelect(gc_ValueTypesTable).f_Where(CSqlAllRowsPredicate{}).f_Select(fg_SqlSum<&CValueTypesRow::m_Float32>());
	constexpr auto gc_SelectValueTypeInt64DoubledByKey = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlAdd<&CValueTypesRow::m_Int64, &CValueTypesRow::m_Int64>())
	;
	constexpr auto gc_SelectValueTypeInt16DoubledByKey = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlAdd<&CValueTypesRow::m_Int16, &CValueTypesRow::m_Int16>())
	;
	constexpr auto gc_SelectDistinctValueTypeExpressionsByKeyLike = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamLike<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlAdd<&CValueTypesRow::m_Int32, &CValueTypesRow::m_Int16>(), fg_SqlUpper<&CValueTypesRow::m_Key>())
		.f_Distinct()
	;
	constexpr auto gc_SelectValueTypeAliasedExpressionsByKey = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>())
		.f_Select
		(
			fg_SqlAlias<&CValueExpressionProjectionRow::m_Sum>(fg_SqlAdd<&CValueTypesRow::m_Int32, &CValueTypesRow::m_Int16>())
			, fg_SqlAlias<&CValueExpressionProjectionRow::m_UpperKey>(fg_SqlUpper<&CValueTypesRow::m_Key>())
			, fg_SqlAlias<&CValueExpressionProjectionRow::m_FloatValue>(fg_SqlCastFloat<&CValueTypesRow::m_Int32>())
			, fg_SqlAlias<&CValueExpressionProjectionRow::m_KeyLength>(fg_SqlLength<&CValueTypesRow::m_Key>())
		)
	;
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectValueTypeExpressionsByKey)>::CRow, NStorage::TCTuple<int64, NStr::CStr, int64, fp64, int32>>);
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectValueTypeAliasedExpressionsByKey)>::CRow, CValueExpressionProjectionRow>);
	// Aliased and unaliased expressions use different row mappings, so a selection can't mix them
	constexpr auto gc_TestUnaliasedExpr = fg_SqlAdd<&CValueTypesRow::m_Int32, &CValueTypesRow::m_Int16>();
	constexpr auto gc_TestAliasedExpr = fg_SqlAlias<&CValueExpressionProjectionRow::m_Sum>(fg_SqlAdd<&CValueTypesRow::m_Int32, &CValueTypesRow::m_Int16>());
	static_assert(gc_bSqlExpressionsConsistentAliasing<decltype(gc_TestUnaliasedExpr)>);
	static_assert(gc_bSqlExpressionsConsistentAliasing<decltype(gc_TestAliasedExpr)>);
	static_assert(gc_bSqlExpressionsConsistentAliasing<decltype(gc_TestUnaliasedExpr), decltype(gc_TestUnaliasedExpr)>);
	static_assert(gc_bSqlExpressionsConsistentAliasing<decltype(gc_TestAliasedExpr), decltype(gc_TestAliasedExpr)>);
	static_assert(!gc_bSqlExpressionsConsistentAliasing<decltype(gc_TestUnaliasedExpr), decltype(gc_TestAliasedExpr)>);
	static_assert(!gc_bSqlExpressionsConsistentAliasing<decltype(gc_TestAliasedExpr), decltype(gc_TestUnaliasedExpr)>);
	// Aliases change the client-side row mapping without changing the SQL, so these selects share a QueryID
	constexpr auto gc_SelectAliasReuseFirst = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlAlias<&CAliasReuseRow::m_First>(fg_SqlAdd<&CValueTypesRow::m_Int32, &CValueTypesRow::m_Int16>()))
	;
	constexpr auto gc_SelectAliasReuseSecond = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlAlias<&CAliasReuseRow::m_Second>(fg_SqlAdd<&CValueTypesRow::m_Int32, &CValueTypesRow::m_Int16>()))
	;
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectAliasReuseFirst)>::CRow, CAliasReuseRow>);
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectAliasReuseSecond)>::CRow, CAliasReuseRow>);
	static_assert(gc_SelectAliasReuseFirst.m_QueryID.m_Value == gc_SelectAliasReuseSecond.m_QueryID.m_Value);
	constexpr auto gc_SelectAliasReuseUnion = fg_SqlUnionAll<gc_SelectAliasReuseFirst, gc_SelectAliasReuseSecond>();
	constexpr auto gc_SelectValueTypeAbsInt32ByKey = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlBackendFunction<NStr::gc_Str<"ABS">, &CValueTypesRow::m_Int32>())
	;
	constexpr auto gc_SelectValueTypeSignInt32ByKey = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamEq<&CValueTypesRow::m_Key>())
		.f_Select(fg_SqlBackendFunction<NStr::gc_Str<"SIGN">, &CValueTypesRow::m_Int32>())
	;
	static_assert(gc_SelectValueTypeAbsInt32ByKey.m_QueryID != gc_SelectValueTypeSignInt32ByKey.m_QueryID);

	constexpr NPrivate::TCSqlSelectProjection
		<
			gc_SelectPersonByEmail
			, NPrivate::TCSqlProjectionBackendRow<NStorage::TCTuple<int64, NStr::CStr>, &CPersonRow::m_ID, &CPersonRow::m_Email>
			, &CPersonRow::m_ID
			, &CPersonRow::m_Email
		>
		gc_PersonProjectionTuple
	;
	constexpr NPrivate::TCSqlSelectProjection
		<
			gc_SelectPersonByEmail
			, NPrivate::TCSqlProjectionBackendRow<NStorage::TCTuple<int64, NStorage::TCOptional<NStr::CStr>>, &CPersonRow::m_ID, &CPersonRow::m_Email>
			, &CPersonRow::m_ID
			, &CPersonRow::m_Email
		>
		gc_PersonProjectionTupleNullable
	;
	static_assert(gc_PersonProjectionTuple.m_QueryID != gc_PersonProjectionTupleNullable.m_QueryID);
	static_assert
		(
			NTraits::cIsSame
			<
				typename NTraits::TCDecay<decltype(gc_SelectValueTypeAggregatesByKeyLike)>::CRow
				, NStorage::TCTuple<int64, NStorage::TCOptional<int64>, NStorage::TCOptional<fp64>, NStorage::TCOptional<int32>, NStorage::TCOptional<int32>>
			>
		)
	;
	constexpr auto gc_SelectValueTypeGroupedAggregatesByKeyLike = fg_SqlPreparedSelect(gc_ValueTypesTable)
		.f_Where(fg_SqlParamLike<&CValueTypesRow::m_Key>())
		.f_GroupBy<&CValueTypesRow::m_bFlag>()
		.f_Having(fg_SqlHavingGt(fg_SqlCount()))
		.f_Select(fg_SqlColumn<&CValueTypesRow::m_bFlag>(), fg_SqlCount(), fg_SqlSum<&CValueTypesRow::m_Int32>())
		.f_OrderByAscending<&CValueTypesRow::m_bFlag>()
	;
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypesByKeyAndInt32, NStr::CStr, int32>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleWithProfileDisplayNameSubquery, NStr::CStr>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleByEmailLikeWithProfileDisplayNameSubquery, NStr::CStr, NStr::CStr>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleByEmailLikeWithLimitedProfileDisplayNameSubquery, NStr::CStr, NStr::CStr, int64, int64>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleByEmailLikeWithHavingLimitedProfileEmailSubquery, NStr::CStr, NStr::CStr, int64, int64, int64>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleWithParameterizedUnionSubquery, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleWithProfileDisplayNameSubquery>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleWithProfileDisplayNameSubquery, int32>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleByEmailLikeWithProfileDisplayNameSubquery, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleByEmailLikeWithLimitedProfileDisplayNameSubquery, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectPeopleByEmailLikeWithProfileDisplayNameSubquery, int32, NStr::CStr>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_UnionParameterizedPersonAndProfileEmails, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_UnionParameterizedPersonAndProfileEmails, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_UnionParameterizedPersonAndProfileEmails, int32, NStr::CStr>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_UnionThreeParameterizedEmailSelects, NStr::CStr, NStr::CStr, NStr::CStr>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypeGroupedAggregatesByKeyLike, NStr::CStr, int64>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypeGroupedAggregatesByKeyLike, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypeGroupedAggregatesByKeyLike, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypesByKeyAndInt32, int32, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypesByKeyAndInt32, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypesByKeyAndInt32, NStr::CStr, int32, int32>());
	static_assert(NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypesByKeyIn, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypesByKeyIn, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedSelectParametersMatch<gc_SelectValueTypesByKeyIn, NStr::CStr, int32>());
	constexpr auto gc_InsertNullableTypes = fg_SqlPreparedInsert(gc_NullableTypesTable);
	constexpr auto gc_InsertNullableTypesKeyAndInteger = fg_SqlPreparedInsert(gc_NullableTypesTable).f_Columns<&CNullableTypesRow::m_Key, &CNullableTypesRow::m_Integer>();
	constexpr auto gc_SelectNullableTypesByKey = fg_SqlPreparedSelect(gc_NullableTypesTable).f_Where(fg_SqlParamEq<&CNullableTypesRow::m_Key>());
	constexpr auto gc_SelectNullableTypesWithNullInteger = fg_SqlPreparedSelect(gc_NullableTypesTable).f_Where(fg_SqlIsNull<&CNullableTypesRow::m_Integer>());
	constexpr auto gc_SelectNullableTypesWithInteger = fg_SqlPreparedSelect(gc_NullableTypesTable).f_Where(fg_SqlIsNotNull<&CNullableTypesRow::m_Integer>());
	// A non-aliased arithmetic expression over a nullable column: the result must be optional, because a row whose
	// operand is NULL yields NULL, which the row mapper would reject for a non-nullable field.
	constexpr auto gc_SelectNullableIntegerDoubledByKey = fg_SqlPreparedSelect(gc_NullableTypesTable)
		.f_Where(fg_SqlParamEq<&CNullableTypesRow::m_Key>())
		.f_Select(fg_SqlAdd<&CNullableTypesRow::m_Integer, &CNullableTypesRow::m_Integer>())
	;
	static_assert(NTraits::cIsSame<typename NTraits::TCDecay<decltype(gc_SelectNullableIntegerDoubledByKey)>::CRow, NStorage::TCTuple<NStorage::TCOptional<int64>>>);
	constexpr auto gc_SelectNullableTextLengthAndIntegerFloatByKey = fg_SqlPreparedSelect(gc_NullableTypesTable)
		.f_Where(fg_SqlParamEq<&CNullableTypesRow::m_Key>())
		.f_Select(fg_SqlLength<&CNullableTypesRow::m_Text>(), fg_SqlCastFloat<&CNullableTypesRow::m_Integer>())
	;
	static_assert
		(
			NTraits::cIsSame
			<
				typename NTraits::TCDecay<decltype(gc_SelectNullableTextLengthAndIntegerFloatByKey)>::CRow
				, NStorage::TCTuple<NStorage::TCOptional<int64>, NStorage::TCOptional<fp64>>
			>
		)
	;
	// An IS NULL predicate binds no value, so a following HAVING parameter must keep its placeholder numbering ($1).
	constexpr auto gc_SelectNullableNullIntegerGroupedHaving = fg_SqlPreparedSelect(gc_NullableTypesTable)
		.f_Where(fg_SqlIsNull<&CNullableTypesRow::m_Integer>())
		.f_GroupBy<&CNullableTypesRow::m_Key>()
		.f_Having(fg_SqlHavingGt(fg_SqlCount()))
	;
	constexpr auto gc_DeleteNullableByKey = fg_SqlPreparedDelete(gc_NullableTypesTable).f_Where(fg_SqlParamEq<&CNullableTypesRow::m_Key>());
	constexpr auto gc_InsertDefaultValuesKey = fg_SqlPreparedInsert(gc_DefaultValuesTable).f_Columns<&CDefaultValuesRow::m_Key>();
	constexpr auto gc_SelectDefaultValuesByKey = fg_SqlPreparedSelect(gc_DefaultValuesTable).f_Where(fg_SqlParamEq<&CDefaultValuesRow::m_Key>());
	constexpr auto gc_InsertConversionFailureKey = fg_SqlPreparedInsert(gc_ConversionFailureTable).f_Columns<&CConversionFailureRow::m_Key>();
	constexpr auto gc_SelectConversionFailureByKey = fg_SqlPreparedSelect(gc_ConversionFailureTable).f_Where(fg_SqlParamEq<&CConversionFailureRow::m_Key>());

	constexpr auto gc_SelectVersion1SessionByToken = fg_SqlPreparedSelect(NMib::NSQL::NTest::NVersion1::gc_SessionTable).f_Where
		(
			fg_SqlParamEq<&NMib::NSQL::NTest::NVersion1::CSessionRow::m_Token>()
		)
	;

	constexpr auto gc_SelectLatestSessionByToken = fg_SqlPreparedSelect(NMib::NSQL::NTest::gc_SessionTable).f_Where(fg_SqlParamEq<&NMib::NSQL::NTest::CSessionRow::m_Token>());
	constexpr auto gc_InsertVersion1User = fg_SqlPreparedInsert(NMib::NSQL::NTest::NVersion1::gc_UserTable);

	struct CRenameVersion1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_OldToken;
	};

	struct CRenameVersion2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
	};

	constexpr auto gc_RenameVersion1Table = fg_SqlTable<CRenameVersion1Row>
		(
			NStr::gc_Str<"legacy_sessions">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRenameVersion1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"old_token">, &CRenameVersion1Row::m_OldToken)
			)
		)
	;

	constexpr auto gc_RenameVersion2Table = fg_SqlTable<CRenameVersion2Row>
		(
			NStr::gc_Str<"sessions_renamed">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRenameVersion2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRenameVersion2Row::m_Token)
			)
		)
	;

	constexpr auto gc_RenameVersion1Database = fg_SqlDatabase(NStr::gc_Str<"database_backend_rename_test">, gc_RenameVersion1Table);

	constexpr auto gc_RenameVersion2Database = fg_SqlDatabase(NStr::gc_Str<"database_backend_rename_test">, gc_RenameVersion2Table);

	constexpr auto gc_RenameVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rename_0001">, gc_RenameVersion1Database);
	constexpr auto gc_RenameVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rename_0002">, gc_RenameVersion2Database);
	constexpr auto gc_RenameVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RenameVersion1SchemaVersion));
	constexpr auto gc_RenameSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RenameVersion1SchemaVersion)
			, fg_SqlSchemaMigration
			(
				gc_RenameVersion2SchemaVersion
				, fg_SqlRenameTable(NStr::gc_Str<"legacy_sessions">, NStr::gc_Str<"sessions_renamed">)
				, fg_SqlRenameColumn(NStr::gc_Str<"sessions_renamed">, NStr::gc_Str<"old_token">, NStr::gc_Str<"token">)
			)
		)
	;
	constexpr auto gc_RenameRebuildSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RenameVersion1SchemaVersion)
			, fg_SqlSchemaMigration
			(
				gc_RenameVersion2SchemaVersion
				, fg_SqlRenameTable(NStr::gc_Str<"legacy_sessions">, NStr::gc_Str<"sessions_renamed">)
				, fg_SqlRenameColumn(NStr::gc_Str<"sessions_renamed">, NStr::gc_Str<"old_token">, NStr::gc_Str<"token">)
				, fg_SqlRebuildTable(NStr::gc_Str<"sessions_renamed">)
			)
		)
	;
	// A rebuild before the rename creates the table without old_token, so the migration is rejected
	constexpr auto gc_RebuildThenRenameSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RenameVersion1SchemaVersion)
			, fg_SqlSchemaMigration
			(
				gc_RenameVersion2SchemaVersion
				, fg_SqlRenameTable(NStr::gc_Str<"legacy_sessions">, NStr::gc_Str<"sessions_renamed">)
				, fg_SqlRebuildTable(NStr::gc_Str<"sessions_renamed">)
				, fg_SqlRenameColumn(NStr::gc_Str<"sessions_renamed">, NStr::gc_Str<"old_token">, NStr::gc_Str<"token">)
			)
		)
	;

	constexpr auto gc_SelectRenameVersion1ByToken = fg_SqlPreparedSelect(gc_RenameVersion1Table).f_Where(fg_SqlParamEq<&CRenameVersion1Row::m_OldToken>());

	constexpr auto gc_SelectRenameVersion2ByToken = fg_SqlPreparedSelect(gc_RenameVersion2Table).f_Where(fg_SqlParamEq<&CRenameVersion2Row::m_Token>());

	struct CRenameConstrainedVersion1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
	};

	struct CRenameConstrainedVersion2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
	};

	constexpr auto gc_RenameConstrainedVersion1Table = fg_SqlTable<CRenameConstrainedVersion1Row>
		(
			NStr::gc_Str<"legacy_constrained_sessions">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRenameConstrainedVersion1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRenameConstrainedVersion1Row::m_Token)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlCheck(NStr::gc_Str<"constrained_sessions_token_check">, NStr::gc_Str<"length(token) > 0">))
		)
	;

	constexpr auto gc_RenameConstrainedVersion2Table = fg_SqlTable<CRenameConstrainedVersion2Row>
		(
			NStr::gc_Str<"constrained_sessions_renamed">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRenameConstrainedVersion2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRenameConstrainedVersion2Row::m_Token)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlCheck(NStr::gc_Str<"constrained_sessions_token_check">, NStr::gc_Str<"length(token) > 0">))
		)
	;

	constexpr auto gc_RenameConstrainedVersion1Database = fg_SqlDatabase(NStr::gc_Str<"database_backend_constrained_rename_test">, gc_RenameConstrainedVersion1Table);
	constexpr auto gc_RenameConstrainedVersion2Database = fg_SqlDatabase(NStr::gc_Str<"database_backend_constrained_rename_test">, gc_RenameConstrainedVersion2Table);
	constexpr auto gc_RenameConstrainedVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rename_constrained_0001">, gc_RenameConstrainedVersion1Database);
	constexpr auto gc_RenameConstrainedVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rename_constrained_0002">, gc_RenameConstrainedVersion2Database);
	constexpr auto gc_RenameConstrainedVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RenameConstrainedVersion1SchemaVersion));
	constexpr auto gc_RenameConstrainedSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RenameConstrainedVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RenameConstrainedVersion2SchemaVersion, fg_SqlRenameTable(NStr::gc_Str<"legacy_constrained_sessions">, NStr::gc_Str<"constrained_sessions_renamed">))
		)
	;

	constexpr auto gc_SelectRenameConstrainedVersion1ByToken = fg_SqlPreparedSelect(gc_RenameConstrainedVersion1Table).f_Where(fg_SqlParamEq<&CRenameConstrainedVersion1Row::m_Token>());

	constexpr auto gc_SelectRenameConstrainedVersion2ByToken = fg_SqlPreparedSelect(gc_RenameConstrainedVersion2Table).f_Where(fg_SqlParamEq<&CRenameConstrainedVersion2Row::m_Token>());

	struct CChecksumMismatchRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Email;
		NStr::CStr m_Name;
	};

	constexpr auto gc_ChecksumMismatchTable = fg_SqlTable<CChecksumMismatchRow>
		(
			NStr::gc_Str<"people">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CChecksumMismatchRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CChecksumMismatchRow::m_Email)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CChecksumMismatchRow::m_Name)
			)
		)
	;

	constexpr auto gc_ChecksumMismatchDatabase = fg_SqlDatabase(NStr::gc_Str<"database_backend_test">, gc_ChecksumMismatchTable);

	constexpr auto gc_ChecksumMismatchSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"0001">, gc_ChecksumMismatchDatabase);
	constexpr auto gc_ChecksumMismatchSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_ChecksumMismatchSchemaVersion));

	struct CRequiredColumnVersion1Row
	{
		int64 m_ID = 0;
	};

	struct CRequiredColumnVersion2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_RequiredValue;
	};

	constexpr auto gc_RequiredColumnVersion1Table = fg_SqlTable<CRequiredColumnVersion1Row>
		(
			NStr::gc_Str<"required_column_test">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CRequiredColumnVersion1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;

	constexpr auto gc_RequiredColumnVersion2Table = fg_SqlTable<CRequiredColumnVersion2Row>
		(
			NStr::gc_Str<"required_column_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRequiredColumnVersion2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"required_value">, &CRequiredColumnVersion2Row::m_RequiredValue)
			)
		)
	;

	constexpr auto gc_RequiredColumnVersion1Database = fg_SqlDatabase(NStr::gc_Str<"required_column_database">, gc_RequiredColumnVersion1Table);
	constexpr auto gc_RequiredColumnVersion2Database = fg_SqlDatabase(NStr::gc_Str<"required_column_database">, gc_RequiredColumnVersion2Table);
	constexpr auto gc_RequiredColumnVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"required_0001">, gc_RequiredColumnVersion1Database);
	constexpr auto gc_RequiredColumnVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"required_0002">, gc_RequiredColumnVersion2Database);
	constexpr auto gc_RequiredColumnVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RequiredColumnVersion1SchemaVersion));
	constexpr auto gc_RequiredColumnSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RequiredColumnVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RequiredColumnVersion2SchemaVersion)
		)
	;

	struct CBackendDefaultVersion1Row
	{
		int64 m_ID = 0;
	};

	struct CBackendDefaultVersion2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_BackendValue;
	};

	constexpr auto gc_BackendDefaultVersion1Table = fg_SqlTable<CBackendDefaultVersion1Row>
		(
			NStr::gc_Str<"backend_default_test">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CBackendDefaultVersion1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;

	constexpr auto gc_BackendDefaultVersion2Table = fg_SqlTable<CBackendDefaultVersion2Row>
		(
			NStr::gc_Str<"backend_default_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CBackendDefaultVersion2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn
				(
					NStr::gc_Str<"backend_value">
					, &CBackendDefaultVersion2Row::m_BackendValue
					, fg_SqlColumnOptions
					(
						ESqlColumnFlag::mc_None
						, NStr::gc_Str<"">
						,
						{
							{
								.m_pBackendID = &NStr::gc_Str<"sqlite">.m_Str
								, .m_pDefaultSql = &NStr::gc_Str<"'sqlite-default'">.m_Str
							}
							,
							{
								.m_pBackendID = &NStr::gc_Str<"postgres">.m_Str
								, .m_pDefaultSql = &NStr::gc_Str<"'postgres-default'">.m_Str
							}
						}
					)
				)
			)
		)
	;

	constexpr auto gc_BackendDefaultVersion1Database = fg_SqlDatabase(NStr::gc_Str<"backend_default_database">, gc_BackendDefaultVersion1Table);
	constexpr auto gc_BackendDefaultVersion2Database = fg_SqlDatabase(NStr::gc_Str<"backend_default_database">, gc_BackendDefaultVersion2Table);
	constexpr auto gc_BackendDefaultVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"backend_default_0001">, gc_BackendDefaultVersion1Database);
	constexpr auto gc_BackendDefaultVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"backend_default_0002">, gc_BackendDefaultVersion2Database);
	constexpr auto gc_BackendDefaultVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_BackendDefaultVersion1SchemaVersion));
	constexpr auto gc_BackendDefaultSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_BackendDefaultVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_BackendDefaultVersion2SchemaVersion)
		)
	;

	// SQLite cannot add a UNIQUE column in place, so this upgrade exercises the rebuild path
	struct CUniqueAddV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
	};

	struct CUniqueAddV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
		NStorage::TCOptional<NStr::CStr> m_Code;
	};

	constexpr auto gc_UniqueAddV1Table = fg_SqlTable<CUniqueAddV1Row>
		(
			NStr::gc_Str<"unique_add_migration">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CUniqueAddV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CUniqueAddV1Row::m_Name)
			)
		)
	;

	constexpr auto gc_UniqueAddV2Table = fg_SqlTable<CUniqueAddV2Row>
		(
			NStr::gc_Str<"unique_add_migration">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CUniqueAddV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CUniqueAddV2Row::m_Name)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CUniqueAddV2Row::m_Code, ESqlColumnFlag::mc_Unique)
			)
		)
	;

	constexpr auto gc_UniqueAddV1Database = fg_SqlDatabase(NStr::gc_Str<"unique_add_database">, gc_UniqueAddV1Table);
	constexpr auto gc_UniqueAddV2Database = fg_SqlDatabase(NStr::gc_Str<"unique_add_database">, gc_UniqueAddV2Table);
	constexpr auto gc_UniqueAddV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"unique_add_0001">, gc_UniqueAddV1Database);
	constexpr auto gc_UniqueAddV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"unique_add_0002">, gc_UniqueAddV2Database);
	constexpr auto gc_UniqueAddV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_UniqueAddV1SchemaVersion));
	constexpr auto gc_UniqueAddSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_UniqueAddV1SchemaVersion), fg_SqlSchemaMigration(gc_UniqueAddV2SchemaVersion));

	constexpr auto gc_InsertUniqueAddV1 = fg_SqlPreparedInsert(gc_UniqueAddV1Table);
	constexpr auto gc_InsertUniqueAddV2 = fg_SqlPreparedInsert(gc_UniqueAddV2Table);
	constexpr auto gc_SelectUniqueAddByName = fg_SqlPreparedSelect(gc_UniqueAddV2Table).f_Where(fg_SqlParamEq<&CUniqueAddV2Row::m_Name>());

	struct CGenColV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
		NStorage::TCOptional<NStr::CStr> m_ValueLower;
	};

	struct CGenColV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
		NStorage::TCOptional<NStr::CStr> m_ValueLower;
		NStorage::TCOptional<NStr::CStr> m_Note;
	};

	constexpr auto gc_GenColValueLowerOptions = fg_SqlColumnOptions
		(
			ESqlColumnFlag::mc_None
			, NStr::gc_Str<"">
			,
			{
				{
					.m_pBackendID = &NStr::gc_Str<"sqlite">.m_Str
					, .m_pGeneratedSql = &NStr::gc_Str<"lower(value)">.m_Str
				}
				,
				{
					.m_pBackendID = &NStr::gc_Str<"postgres">.m_Str
					, .m_pGeneratedSql = &NStr::gc_Str<"lower(value)">.m_Str
				}
			}
		)
	;

	constexpr auto gc_GenColV1Table = fg_SqlTable<CGenColV1Row>
		(
			NStr::gc_Str<"gen_col_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CGenColV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CGenColV1Row::m_Value)
				, fg_SqlColumn(NStr::gc_Str<"value_lower">, &CGenColV1Row::m_ValueLower, gc_GenColValueLowerOptions)
			)
		)
	;

	constexpr auto gc_GenColV2Table = fg_SqlTable<CGenColV2Row>
		(
			NStr::gc_Str<"gen_col_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CGenColV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CGenColV2Row::m_Value)
				, fg_SqlColumn(NStr::gc_Str<"value_lower">, &CGenColV2Row::m_ValueLower, gc_GenColValueLowerOptions)
				, fg_SqlColumn(NStr::gc_Str<"note">, &CGenColV2Row::m_Note)
			)
		)
	;

	constexpr auto gc_InsertGenCol = fg_SqlPreparedInsert(gc_GenColV1Table);
	constexpr auto gc_SelectGenColV1ByValue = fg_SqlPreparedSelect(gc_GenColV1Table).f_Where(fg_SqlParamEq<&CGenColV1Row::m_Value>());
	constexpr auto gc_SelectGenColV2ByValue = fg_SqlPreparedSelect(gc_GenColV2Table).f_Where(fg_SqlParamEq<&CGenColV2Row::m_Value>());

	constexpr auto gc_GenColV1Database = fg_SqlDatabase(NStr::gc_Str<"gen_col_database">, gc_GenColV1Table);
	constexpr auto gc_GenColV2Database = fg_SqlDatabase(NStr::gc_Str<"gen_col_database">, gc_GenColV2Table);
	constexpr auto gc_GenColV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"gen_col_0001">, gc_GenColV1Database);
	constexpr auto gc_GenColV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"gen_col_0002">, gc_GenColV2Database);
	constexpr auto gc_GenColV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_GenColV1SchemaVersion));
	constexpr auto gc_GenColSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_GenColV1SchemaVersion), fg_SqlSchemaMigration(gc_GenColV2SchemaVersion));

	constexpr auto gc_GenColOtherBackendTable = fg_SqlTable<CGenColV1Row>
		(
			NStr::gc_Str<"gen_col_other_backend">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CGenColV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CGenColV1Row::m_Value)
				, fg_SqlColumn
				(
					NStr::gc_Str<"value_lower">
					, &CGenColV1Row::m_ValueLower
					, fg_SqlColumnOptions
					(
						ESqlColumnFlag::mc_None
						, NStr::gc_Str<"">
						,
						{
							{
								.m_pBackendID = &NStr::gc_Str<"other">.m_Str
								, .m_pGeneratedSql = &NStr::gc_Str<"lower(value)">.m_Str
							}
						}
					)
				)
			)
		)
	;
	constexpr auto gc_GenColOtherBackendDatabase = fg_SqlDatabase(NStr::gc_Str<"gen_col_other_backend_database">, gc_GenColOtherBackendTable);
	constexpr auto gc_GenColOtherBackendSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"gen_col_other_backend_0001">, gc_GenColOtherBackendDatabase);
	constexpr auto gc_GenColOtherBackendSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_GenColOtherBackendSchemaVersion));

	struct CGenColUpsertRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
		NStorage::TCOptional<NStr::CStr> m_ValueLower;
		NStr::CStr m_Note;
	};

	constexpr auto gc_GenColUpsertTable = fg_SqlTable<CGenColUpsertRow>
		(
			NStr::gc_Str<"gen_col_upsert">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CGenColUpsertRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CGenColUpsertRow::m_Value)
				, fg_SqlColumn(NStr::gc_Str<"value_lower">, &CGenColUpsertRow::m_ValueLower, gc_GenColValueLowerOptions)
				, fg_SqlColumn(NStr::gc_Str<"note">, &CGenColUpsertRow::m_Note)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"idx_gen_col_upsert_value">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"value">))
		)
	;

	constexpr auto gc_UpsertGenColNoteByValue = fg_SqlPreparedUpsert(gc_GenColUpsertTable).f_OnConflict<&CGenColUpsertRow::m_Value>().f_Update<&CGenColUpsertRow::m_Note>();
	constexpr auto gc_SelectGenColUpsertByValue = fg_SqlPreparedSelect(gc_GenColUpsertTable).f_Where(fg_SqlParamEq<&CGenColUpsertRow::m_Value>());

	// The upsert binds the implicit insert columns (value, note), skipping the auto-increment id and the generated
	// value_lower. Passing a value for the generated column too must be rejected at compile time rather than binding
	// one value more than the generated SQL has placeholders.
	static_assert(NPrivate::fg_SqlPreparedUpsertValuesMatch<gc_UpsertGenColNoteByValue, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedUpsertValuesMatch<gc_UpsertGenColNoteByValue, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlPreparedUpsertValuesMatch<gc_UpsertGenColNoteByValue, NStr::CStr, NStr::CStr, NStr::CStr>());
	static_assert(NPrivate::fg_SqlTableImplicitInsertValuesMatch<gc_GenColUpsertTable, NStr::CStr, NStr::CStr>());
	static_assert(!NPrivate::fg_SqlTableImplicitInsertValuesMatch<gc_GenColUpsertTable, NStr::CStr, NStr::CStr, NStr::CStr>());

	constexpr auto gc_GenColUpsertDatabase = fg_SqlDatabase(NStr::gc_Str<"gen_col_upsert_database">, gc_GenColUpsertTable);
	constexpr auto gc_GenColUpsertSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"gen_col_upsert_0001">, gc_GenColUpsertDatabase);
	constexpr auto gc_GenColUpsertSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_GenColUpsertSchemaVersion));

	// v2 adds the stored generated column, and the rebuild in v3 must recompute it rather than copy it
	struct CGenColMigrateV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
	};

	struct CGenColMigrateV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
		NStorage::TCOptional<NStr::CStr> m_ValueLower;
	};

	constexpr auto gc_GenColMigrateV1Table = fg_SqlTable<CGenColMigrateV1Row>
		(
			NStr::gc_Str<"gen_col_migrate">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CGenColMigrateV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CGenColMigrateV1Row::m_Value)
			)
		)
	;

	constexpr auto gc_GenColMigrateV2Table = fg_SqlTable<CGenColMigrateV2Row>
		(
			NStr::gc_Str<"gen_col_migrate">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CGenColMigrateV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CGenColMigrateV2Row::m_Value)
				, fg_SqlColumn(NStr::gc_Str<"value_lower">, &CGenColMigrateV2Row::m_ValueLower, gc_GenColValueLowerOptions)
			)
		)
	;

	constexpr auto gc_InsertGenColMigrateV1 = fg_SqlPreparedInsert(gc_GenColMigrateV1Table);
	constexpr auto gc_SelectGenColMigrateV2ByValue = fg_SqlPreparedSelect(gc_GenColMigrateV2Table).f_Where(fg_SqlParamEq<&CGenColMigrateV2Row::m_Value>());

	constexpr auto gc_GenColMigrateV1Database = fg_SqlDatabase(NStr::gc_Str<"gen_col_migrate_database">, gc_GenColMigrateV1Table);
	constexpr auto gc_GenColMigrateV2Database = fg_SqlDatabase(NStr::gc_Str<"gen_col_migrate_database">, gc_GenColMigrateV2Table);
	constexpr auto gc_GenColMigrateV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"gen_col_migrate_0001">, gc_GenColMigrateV1Database);
	constexpr auto gc_GenColMigrateV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"gen_col_migrate_0002">, gc_GenColMigrateV2Database);
	constexpr auto gc_GenColMigrateV3SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"gen_col_migrate_0003">, gc_GenColMigrateV2Database);
	constexpr auto gc_GenColMigrateV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_GenColMigrateV1SchemaVersion));
	constexpr auto gc_GenColMigrateV2SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_GenColMigrateV1SchemaVersion), fg_SqlSchemaMigration(gc_GenColMigrateV2SchemaVersion));
	constexpr auto gc_GenColMigrateV3SchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_GenColMigrateV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_GenColMigrateV2SchemaVersion)
			, fg_SqlSchemaMigration(gc_GenColMigrateV3SchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"gen_col_migrate">))
		)
	;

	// An autoincrement key narrower than 64 bits, whose prepared key parameters must match the column's width
	struct CNarrowKeyRow
	{
		int32 m_ID = 0;
		NStr::CStr m_Value;
	};

	constexpr auto gc_NarrowKeyTable = fg_SqlTable<CNarrowKeyRow>
		(
			NStr::gc_Str<"narrow_keys">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CNarrowKeyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CNarrowKeyRow::m_Value)
			)
		)
	;
	constexpr auto gc_InsertNarrowKey = fg_SqlPreparedInsert(gc_NarrowKeyTable);

	// Every column is generated, so a prepared insert takes no values
	struct CKeyOnlyRow
	{
		int64 m_ID = 0;
	};

	constexpr auto gc_KeyOnlyTable = fg_SqlTable<CKeyOnlyRow>
		(
			NStr::gc_Str<"key_only">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CKeyOnlyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;
	constexpr auto gc_InsertKeyOnly = fg_SqlPreparedInsert(gc_KeyOnlyTable);

	constexpr auto gc_NarrowKeyDatabase = fg_SqlDatabase(NStr::gc_Str<"narrow_key_database">, gc_NarrowKeyTable, gc_KeyOnlyTable);
	constexpr auto gc_NarrowKeySchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"narrow_key_0001">, gc_NarrowKeyDatabase);
	constexpr auto gc_NarrowKeySchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_NarrowKeySchemaVersion));

	struct CRebuildKeepV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
	};

	struct CRebuildKeepV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
		NStr::CStr m_CodeLower;
	};

	struct CRebuildRenameAllV1Row
	{
		int64 m_OldID = 0;
	};

	struct CRebuildRenameAllV2Row
	{
		int64 m_NewID = 0;
	};

	constexpr auto gc_RebuildKeepCodeLowerOptions = fg_SqlColumnOptions
		(
			ESqlColumnFlag::mc_None
			, NStr::gc_Str<"">
			,
			{
				{
					.m_pBackendID = &NStr::gc_Str<"sqlite">.m_Str
					, .m_pGeneratedSql = &NStr::gc_Str<"lower(code)">.m_Str
				}
				,
				{
					.m_pBackendID = &NStr::gc_Str<"postgres">.m_Str
					, .m_pGeneratedSql = &NStr::gc_Str<"lower(code)">.m_Str
				}
			}
		)
	;

	constexpr auto gc_RebuildKeepV1Table = fg_SqlTable<CRebuildKeepV1Row>
		(
			NStr::gc_Str<"rebuild_keep">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildKeepV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CRebuildKeepV1Row::m_Code)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlUnique<&CRebuildKeepV1Row::m_Code>(gc_SqlUniqueName<"rebuild_keep", "code">))
		)
	;

	constexpr auto gc_RebuildKeepV2Table = fg_SqlTable<CRebuildKeepV2Row>
		(
			NStr::gc_Str<"rebuild_keep">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildKeepV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CRebuildKeepV2Row::m_Code)
				, fg_SqlColumn(NStr::gc_Str<"code_lower">, &CRebuildKeepV2Row::m_CodeLower, gc_RebuildKeepCodeLowerOptions)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlUnique<&CRebuildKeepV2Row::m_Code>(gc_SqlUniqueName<"rebuild_keep", "code">))
		)
	;

	constexpr auto gc_RebuildRenameAllV1Table = fg_SqlTable<CRebuildRenameAllV1Row>
		(
			NStr::gc_Str<"rebuild_rename_all">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"old_id">, &CRebuildRenameAllV1Row::m_OldID, ESqlColumnFlag::mc_PrimaryKey))
		)
	;

	constexpr auto gc_RebuildRenameAllV2Table = fg_SqlTable<CRebuildRenameAllV2Row>
		(
			NStr::gc_Str<"rebuild_rename_all">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"new_id">, &CRebuildRenameAllV2Row::m_NewID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;

	// rebuild_keep_child references a column of rebuild_keep_parent that is unique through an index, not a constraint
	struct CRebuildKeepParentRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
	};

	struct CRebuildKeepChildRow
	{
		int64 m_ID = 0;
		NStr::CStr m_ParentCode;
	};

	constexpr auto gc_RebuildKeepParentTable = fg_SqlTable<CRebuildKeepParentRow>
		(
			NStr::gc_Str<"rebuild_keep_parent">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildKeepParentRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CRebuildKeepParentRow::m_Code)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"idx_rebuild_keep_parent_code">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"code">))
		)
	;

	constexpr auto gc_RebuildKeepChildTable = fg_SqlTable<CRebuildKeepChildRow>
		(
			NStr::gc_Str<"rebuild_keep_child">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildKeepChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"parent_code">, &CRebuildKeepChildRow::m_ParentCode)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CRebuildKeepChildRow::m_ParentCode>
				(
					NStr::gc_Str<"rebuild_keep_child_parent_fk">
					, fg_SqlReferences<&CRebuildKeepParentRow::m_Code>(gc_RebuildKeepParentTable)
				)
			)
		)
	;

	struct CRebuildKeepLongNameRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
	};

	constexpr auto gc_RebuildKeepLongNameTable = fg_SqlTable<CRebuildKeepLongNameRow>
		(
			// Fits in PostgreSQL's 63-byte identifier limit, but not with the rebuild prefix added
			NStr::gc_Str<"rebuild_keep_table_with_a_long_name_near_the_identifier_limit">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildKeepLongNameRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CRebuildKeepLongNameRow::m_Value)
			)
		)
	;
	constexpr auto gc_InsertRebuildKeepLongName = fg_SqlPreparedInsert(gc_RebuildKeepLongNameTable);
	constexpr auto gc_SelectRebuildKeepLongNameByValue = fg_SqlPreparedSelect(gc_RebuildKeepLongNameTable).f_Where(fg_SqlParamEq<&CRebuildKeepLongNameRow::m_Value>());

	constexpr auto gc_InsertRebuildKeepParent = fg_SqlPreparedInsert(gc_RebuildKeepParentTable);
	constexpr auto gc_InsertRebuildKeepChild = fg_SqlPreparedInsert(gc_RebuildKeepChildTable);

	constexpr auto gc_InsertRebuildKeepV1 = fg_SqlPreparedInsert(gc_RebuildKeepV1Table);
	constexpr auto gc_InsertRebuildKeepV2 = fg_SqlPreparedInsert(gc_RebuildKeepV2Table);
	constexpr auto gc_SelectRebuildKeepV2ByCode = fg_SqlPreparedSelect(gc_RebuildKeepV2Table).f_Where(fg_SqlParamEq<&CRebuildKeepV2Row::m_Code>());

	constexpr auto gc_RebuildKeepV1Database = fg_SqlDatabase
		(
			NStr::gc_Str<"rebuild_keep_database">
			, gc_RebuildKeepV1Table
			, gc_RebuildRenameAllV1Table
			, gc_RebuildKeepParentTable
			, gc_RebuildKeepChildTable
			, gc_RebuildKeepLongNameTable
		)
	;
	constexpr auto gc_RebuildKeepV2Database = fg_SqlDatabase
		(
			NStr::gc_Str<"rebuild_keep_database">
			, gc_RebuildKeepV2Table
			, gc_RebuildRenameAllV1Table
			, gc_RebuildKeepParentTable
			, gc_RebuildKeepChildTable
			, gc_RebuildKeepLongNameTable
		)
	;
	constexpr auto gc_RebuildKeepV3Database = fg_SqlDatabase
		(
			NStr::gc_Str<"rebuild_keep_database">
			, gc_RebuildKeepV2Table
			, gc_RebuildRenameAllV2Table
			, gc_RebuildKeepParentTable
			, gc_RebuildKeepChildTable
			, gc_RebuildKeepLongNameTable
		)
	;
	constexpr auto gc_RebuildKeepV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_keep_0001">, gc_RebuildKeepV1Database);
	constexpr auto gc_RebuildKeepV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_keep_0002">, gc_RebuildKeepV2Database);
	constexpr auto gc_RebuildKeepV3SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_keep_0003">, gc_RebuildKeepV3Database);
	constexpr auto gc_RebuildKeepV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RebuildKeepV1SchemaVersion));
	constexpr auto gc_RebuildKeepV2SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RebuildKeepV1SchemaVersion), fg_SqlSchemaMigration(gc_RebuildKeepV2SchemaVersion));
	constexpr auto gc_RebuildKeepV3SchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RebuildKeepV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RebuildKeepV2SchemaVersion)
			, fg_SqlSchemaMigration
			(
				gc_RebuildKeepV3SchemaVersion
				, fg_SqlRebuildTable(NStr::gc_Str<"rebuild_keep">)
				, fg_SqlRebuildTable(NStr::gc_Str<"rebuild_rename_all">)
				, fg_SqlRebuildTable(NStr::gc_Str<"rebuild_keep_parent">)
				, fg_SqlRebuildTable(NStr::gc_Str<"rebuild_keep_table_with_a_long_name_near_the_identifier_limit">)
			)
		)
	;

	struct CAdoptConstraintRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
	};

	constexpr auto gc_AdoptConstraintV1Table = fg_SqlTable<CAdoptConstraintRow>
		(
			NStr::gc_Str<"adopt_constraint">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptConstraintRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CAdoptConstraintRow::m_Code)
			)
		)
	;

	constexpr auto gc_AdoptConstraintV2Table = fg_SqlTable<CAdoptConstraintRow>
		(
			NStr::gc_Str<"adopt_constraint">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptConstraintRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CAdoptConstraintRow::m_Code)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlUnique(NStr::gc_Str<"adopt_constraint_code_key">, NStr::gc_Str<"code">))
		)
	;

	constexpr auto gc_InsertAdoptConstraintV1 = fg_SqlPreparedInsert(gc_AdoptConstraintV1Table);
	constexpr auto gc_InsertAdoptConstraintV2 = fg_SqlPreparedInsert(gc_AdoptConstraintV2Table);

	constexpr auto gc_AdoptConstraintV1Database = fg_SqlDatabase(NStr::gc_Str<"adopt_constraint_database">, gc_AdoptConstraintV1Table);
	constexpr auto gc_AdoptConstraintV2Database = fg_SqlDatabase(NStr::gc_Str<"adopt_constraint_database">, gc_AdoptConstraintV2Table);
	constexpr auto gc_AdoptConstraintV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"adopt_constraint_0001">, gc_AdoptConstraintV1Database);
	constexpr auto gc_AdoptConstraintV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"adopt_constraint_0002">, gc_AdoptConstraintV2Database);
	constexpr auto gc_AdoptConstraintV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptConstraintV1SchemaVersion));
	constexpr auto gc_AdoptConstraintV2SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptConstraintV2SchemaVersion));

	struct CIndexChangeRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
	};

	constexpr auto gc_IndexChangeV1Table = fg_SqlTable<CIndexChangeRow>
		(
			NStr::gc_Str<"index_change">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CIndexChangeRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CIndexChangeRow::m_Code)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"idx_index_change_code">, ESqlIndexFlag::mc_None, NStr::gc_Str<"code">))
		)
	;

	constexpr auto gc_IndexChangeV2Table = fg_SqlTable<CIndexChangeRow>
		(
			NStr::gc_Str<"index_change">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CIndexChangeRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CIndexChangeRow::m_Code)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"idx_index_change_code">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"code">))
		)
	;

	constexpr auto gc_InsertIndexChangeV1 = fg_SqlPreparedInsert(gc_IndexChangeV1Table);
	constexpr auto gc_InsertIndexChangeV2 = fg_SqlPreparedInsert(gc_IndexChangeV2Table);
	constexpr auto gc_IndexChangeV1Database = fg_SqlDatabase(NStr::gc_Str<"index_change_database">, gc_IndexChangeV1Table);
	constexpr auto gc_IndexChangeV2Database = fg_SqlDatabase(NStr::gc_Str<"index_change_database">, gc_IndexChangeV2Table);
	constexpr auto gc_IndexChangeV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"index_change_0001">, gc_IndexChangeV1Database);
	constexpr auto gc_IndexChangeV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"index_change_0002">, gc_IndexChangeV2Database);
	constexpr auto gc_IndexChangeV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_IndexChangeV1SchemaVersion));
	constexpr auto gc_IndexChangeV2SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_IndexChangeV1SchemaVersion), fg_SqlSchemaMigration(gc_IndexChangeV2SchemaVersion));

	// SQLite treats the two table names as one
	constexpr auto gc_CaseCollisionUpperTable = fg_SqlTable<CKeyOnlyRow>
		(
			NStr::gc_Str<"Case_Collision">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CKeyOnlyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;
	constexpr auto gc_CaseCollisionLowerTable = fg_SqlTable<CKeyOnlyRow>
		(
			NStr::gc_Str<"case_collision">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CKeyOnlyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;
	constexpr auto gc_CaseCollisionDatabase = fg_SqlDatabase(NStr::gc_Str<"case_collision_database">, gc_CaseCollisionUpperTable, gc_CaseCollisionLowerTable);
	constexpr auto gc_CaseCollisionSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"case_collision_0001">, gc_CaseCollisionDatabase);
	constexpr auto gc_CaseCollisionSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_CaseCollisionSchemaVersion));

	// 64 bytes, one more than PostgreSQL keeps of an identifier
	constexpr auto gc_LongIdentifierTable = fg_SqlTable<CKeyOnlyRow>
		(
			NStr::gc_Str<"long_identifier_table_name_that_exceeds_the_postgresql_limit_xyz">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CKeyOnlyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;
	constexpr auto gc_LongIdentifierDatabase = fg_SqlDatabase(NStr::gc_Str<"long_identifier_database">, gc_LongIdentifierTable);
	constexpr auto gc_LongIdentifierSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"long_identifier_0001">, gc_LongIdentifierDatabase);
	constexpr auto gc_LongIdentifierSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_LongIdentifierSchemaVersion));

	// PostgreSQL names the primary key's index index_collision_pkey, the name the declared email index uses
	struct CIndexCollisionRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Email;
	};

	constexpr auto gc_IndexCollisionTable = fg_SqlTable<CIndexCollisionRow>
		(
			NStr::gc_Str<"index_collision">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CIndexCollisionRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CIndexCollisionRow::m_Email)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"index_collision_pkey">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"email">))
		)
	;
	constexpr auto gc_IndexCollisionDatabase = fg_SqlDatabase(NStr::gc_Str<"index_collision_database">, gc_IndexCollisionTable);
	constexpr auto gc_IndexCollisionSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"index_collision_0001">, gc_IndexCollisionDatabase);
	constexpr auto gc_IndexCollisionSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_IndexCollisionSchemaVersion));

	// v2 adds copy and backfills it in the same version, before the additive pass has added the column
	struct CBackfillV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
	};

	struct CBackfillV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
		NStorage::TCOptional<NStr::CStr> m_Copy;
	};

	constexpr auto gc_BackfillV1Table = fg_SqlTable<CBackfillV1Row>
		(
			NStr::gc_Str<"backfill_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CBackfillV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CBackfillV1Row::m_Value)
			)
		)
	;

	constexpr auto gc_BackfillV2Table = fg_SqlTable<CBackfillV2Row>
		(
			NStr::gc_Str<"backfill_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CBackfillV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CBackfillV2Row::m_Value)
				, fg_SqlColumn(NStr::gc_Str<"copy">, &CBackfillV2Row::m_Copy)
			)
		)
	;

	constexpr auto gc_InsertBackfillV1 = fg_SqlPreparedInsert(gc_BackfillV1Table);
	constexpr auto gc_BackfillV1Database = fg_SqlDatabase(NStr::gc_Str<"backfill_database">, gc_BackfillV1Table);
	constexpr auto gc_BackfillV2Database = fg_SqlDatabase(NStr::gc_Str<"backfill_database">, gc_BackfillV2Table);
	constexpr auto gc_BackfillV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"backfill_0001">, gc_BackfillV1Database);
	constexpr auto gc_BackfillV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"backfill_0002">, gc_BackfillV2Database);
	constexpr auto gc_BackfillV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_BackfillV1SchemaVersion));
	constexpr auto gc_BackfillV2SchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_BackfillV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_BackfillV2SchemaVersion, fg_SqlUpdateColumnSql(NStr::gc_Str<"backfill_test">, NStr::gc_Str<"copy">, NStr::gc_Str<"value">))
		)
	;

	constexpr auto gc_AdoptColumnConstraintTable = fg_SqlTable<CAdoptConstraintRow>
		(
			NStr::gc_Str<"adopt_constraint">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptConstraintRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CAdoptConstraintRow::m_Code, ESqlColumnFlag::mc_Unique)
			)
		)
	;
	constexpr auto gc_InsertAdoptColumnConstraint = fg_SqlPreparedInsert(gc_AdoptColumnConstraintTable);
	constexpr auto gc_AdoptColumnConstraintDatabase = fg_SqlDatabase(NStr::gc_Str<"adopt_constraint_database">, gc_AdoptColumnConstraintTable);
	constexpr auto gc_AdoptColumnConstraintSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"adopt_column_constraint_0001">, gc_AdoptColumnConstraintDatabase);
	constexpr auto gc_AdoptColumnConstraintSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptColumnConstraintSchemaVersion));

	struct CAddDefaultV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
	};

	struct CAddDefaultV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
		NStorage::TCOptional<NStr::CStr> m_CreatedAt;
	};

	constexpr auto gc_AddDefaultV1Table = fg_SqlTable<CAddDefaultV1Row>
		(
			NStr::gc_Str<"add_default_expr">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAddDefaultV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CAddDefaultV1Row::m_Name)
			)
		)
	;

	constexpr auto gc_AddDefaultV2Table = fg_SqlTable<CAddDefaultV2Row>
		(
			NStr::gc_Str<"add_default_expr">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAddDefaultV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CAddDefaultV2Row::m_Name)
				, fg_SqlColumn(NStr::gc_Str<"created_at">, &CAddDefaultV2Row::m_CreatedAt, ESqlColumnFlag::mc_None, NStr::gc_Str<"CURRENT_TIMESTAMP">)
			)
		)
	;

	constexpr auto gc_InsertAddDefaultV1 = fg_SqlPreparedInsert(gc_AddDefaultV1Table);
	constexpr auto gc_SelectAddDefaultV2ByName = fg_SqlPreparedSelect(gc_AddDefaultV2Table).f_Where(fg_SqlParamEq<&CAddDefaultV2Row::m_Name>());

	constexpr auto gc_AddDefaultV1Database = fg_SqlDatabase(NStr::gc_Str<"add_default_database">, gc_AddDefaultV1Table);
	constexpr auto gc_AddDefaultV2Database = fg_SqlDatabase(NStr::gc_Str<"add_default_database">, gc_AddDefaultV2Table);
	constexpr auto gc_AddDefaultV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"add_default_0001">, gc_AddDefaultV1Database);
	constexpr auto gc_AddDefaultV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"add_default_0002">, gc_AddDefaultV2Database);
	constexpr auto gc_AddDefaultV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AddDefaultV1SchemaVersion));
	constexpr auto gc_AddDefaultV2SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AddDefaultV1SchemaVersion), fg_SqlSchemaMigration(gc_AddDefaultV2SchemaVersion));

	// v2 reuses the name of the UNIQUE constraint that PostgreSQL drops with the old column, so the additive sync must recreate it
	struct CDropConstraintV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
		NStr::CStr m_Name;
	};

	struct CDropConstraintV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
	};

	constexpr auto gc_DropConstraintV1Table = fg_SqlTable<CDropConstraintV1Row>
		(
			NStr::gc_Str<"drop_constraint_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CDropConstraintV1Row::m_Code)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropConstraintV1Row::m_Name)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlUnique(NStr::gc_Str<"dct_unique">, NStr::gc_Str<"code">))
		)
	;

	constexpr auto gc_DropConstraintV2Table = fg_SqlTable<CDropConstraintV2Row>
		(
			NStr::gc_Str<"drop_constraint_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropConstraintV2Row::m_Name)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlUnique(NStr::gc_Str<"dct_unique">, NStr::gc_Str<"name">))
		)
	;

	constexpr auto gc_InsertDropConstraintV1 = fg_SqlPreparedInsert(gc_DropConstraintV1Table);
	constexpr auto gc_InsertDropConstraintV2 = fg_SqlPreparedInsert(gc_DropConstraintV2Table);

	constexpr auto gc_DropConstraintV1Database = fg_SqlDatabase(NStr::gc_Str<"drop_constraint_database">, gc_DropConstraintV1Table);
	constexpr auto gc_DropConstraintV2Database = fg_SqlDatabase(NStr::gc_Str<"drop_constraint_database">, gc_DropConstraintV2Table);
	constexpr auto gc_DropConstraintV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"drop_constraint_0001">, gc_DropConstraintV1Database);
	constexpr auto gc_DropConstraintV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"drop_constraint_0002">, gc_DropConstraintV2Database);
	constexpr auto gc_DropConstraintV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_DropConstraintV1SchemaVersion));
	constexpr auto gc_DropConstraintV2SchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_DropConstraintV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_DropConstraintV2SchemaVersion, fg_SqlDropColumn(NStr::gc_Str<"drop_constraint_test">, NStr::gc_Str<"code">))
		)
	;

	constexpr auto gc_ImplicitDropV1Table = fg_SqlTable<CDropConstraintV1Row>
		(
			NStr::gc_Str<"implicit_drop_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CDropConstraintV1Row::m_Code)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropConstraintV1Row::m_Name)
			)
		)
	;
	constexpr auto gc_ImplicitDropV2Table = fg_SqlTable<CDropConstraintV2Row>
		(
			NStr::gc_Str<"implicit_drop_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropConstraintV2Row::m_Name, ESqlColumnFlag::mc_Unique)
			)
		)
	;
	constexpr auto gc_ImplicitDropV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"implicit_drop_0001">, fg_SqlDatabase(NStr::gc_Str<"implicit_drop_database">, gc_ImplicitDropV1Table));
	constexpr auto gc_ImplicitDropV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"implicit_drop_0002">, fg_SqlDatabase(NStr::gc_Str<"implicit_drop_database">, gc_ImplicitDropV2Table));
	constexpr auto gc_ImplicitDropV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_ImplicitDropV1SchemaVersion));
	constexpr auto gc_ImplicitDropSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_ImplicitDropV1SchemaVersion), fg_SqlSchemaMigration(gc_ImplicitDropV2SchemaVersion));

	constexpr auto gc_RenamedCheckV1Table = fg_SqlTable<CDropConstraintV2Row>
		(
			NStr::gc_Str<"renamed_check_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"old_name">, &CDropConstraintV2Row::m_Name)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlCheck(NStr::gc_Str<"renamed_check">, NStr::gc_Str<"length(old_name) > 3">))
		)
	;

	constexpr auto gc_RenamedCheckV2Table = fg_SqlTable<CDropConstraintV2Row>
		(
			NStr::gc_Str<"renamed_check_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"new_name">, &CDropConstraintV2Row::m_Name)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlCheck(NStr::gc_Str<"renamed_check">, NStr::gc_Str<"length(new_name) > 3">))
		)
	;

	constexpr auto gc_InsertRenamedCheckV2 = fg_SqlPreparedInsert(gc_RenamedCheckV2Table);
	constexpr auto gc_RenamedCheckV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"renamed_check_0001">, fg_SqlDatabase(NStr::gc_Str<"renamed_check_database">, gc_RenamedCheckV1Table));
	constexpr auto gc_RenamedCheckV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"renamed_check_0002">, fg_SqlDatabase(NStr::gc_Str<"renamed_check_database">, gc_RenamedCheckV2Table));
	constexpr auto gc_RenamedCheckV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RenamedCheckV1SchemaVersion));
	constexpr auto gc_RenamedCheckSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RenamedCheckV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RenamedCheckV2SchemaVersion, fg_SqlRenameColumn(NStr::gc_Str<"renamed_check_test">, NStr::gc_Str<"old_name">, NStr::gc_Str<"new_name">))
		)
	;

	constexpr auto gc_ChangedCheckV1Table = fg_SqlTable<CDropConstraintV2Row>
		(
			NStr::gc_Str<"changed_check_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropConstraintV2Row::m_Name)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlCheck(NStr::gc_Str<"changed_check">, NStr::gc_Str<"length(name) > 0">))
		)
	;

	constexpr auto gc_ChangedCheckV2Table = fg_SqlTable<CDropConstraintV2Row>
		(
			NStr::gc_Str<"changed_check_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropConstraintV2Row::m_Name)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlCheck(NStr::gc_Str<"changed_check">, NStr::gc_Str<"length(name) > 3">))
		)
	;

	constexpr auto gc_InsertChangedCheckV2 = fg_SqlPreparedInsert(gc_ChangedCheckV2Table);
	constexpr auto gc_ChangedCheckV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"changed_check_0001">, fg_SqlDatabase(NStr::gc_Str<"changed_check_database">, gc_ChangedCheckV1Table));
	constexpr auto gc_ChangedCheckV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"changed_check_0002">, fg_SqlDatabase(NStr::gc_Str<"changed_check_database">, gc_ChangedCheckV2Table));
	constexpr auto gc_ChangedCheckV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_ChangedCheckV1SchemaVersion));
	constexpr auto gc_ChangedCheckSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_ChangedCheckV1SchemaVersion), fg_SqlSchemaMigration(gc_ChangedCheckV2SchemaVersion));

	constexpr auto gc_DropCheckV1Table = fg_SqlTable<CDropConstraintV1Row>
		(
			NStr::gc_Str<"drop_check_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CDropConstraintV1Row::m_Code)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropConstraintV1Row::m_Name)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlCheck(NStr::gc_Str<"drop_check_test_check">, NStr::gc_Str<"length(code) > 0">))
		)
	;

	constexpr auto gc_DropCheckV2Table = fg_SqlTable<CDropConstraintV2Row>
		(
			NStr::gc_Str<"drop_check_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropConstraintV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropConstraintV2Row::m_Name)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlCheck(NStr::gc_Str<"drop_check_test_check">, NStr::gc_Str<"length(name) > 0">))
		)
	;

	constexpr auto gc_DropCheckV1Database = fg_SqlDatabase(NStr::gc_Str<"drop_check_database">, gc_DropCheckV1Table);
	constexpr auto gc_DropCheckV2Database = fg_SqlDatabase(NStr::gc_Str<"drop_check_database">, gc_DropCheckV2Table);
	constexpr auto gc_DropCheckV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"drop_check_0001">, gc_DropCheckV1Database);
	constexpr auto gc_DropCheckV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"drop_check_0002">, gc_DropCheckV2Database);
	constexpr auto gc_DropCheckSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_DropCheckV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_DropCheckV2SchemaVersion, fg_SqlDropColumn(NStr::gc_Str<"drop_check_test">, NStr::gc_Str<"code">))
		)
	;

	// v2 drops code and adds note, and SQLite drops code by rebuilding from the v2 target, which already has note
	struct CDropAddV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
		NStr::CStr m_Name;
	};

	struct CDropAddV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
		NStorage::TCOptional<NStr::CStr> m_Note;
	};

	constexpr auto gc_DropAddV1Table = fg_SqlTable<CDropAddV1Row>
		(
			NStr::gc_Str<"drop_add_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropAddV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CDropAddV1Row::m_Code)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropAddV1Row::m_Name)
			)
		)
	;

	constexpr auto gc_DropAddV2Table = fg_SqlTable<CDropAddV2Row>
		(
			NStr::gc_Str<"drop_add_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropAddV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CDropAddV2Row::m_Name)
				, fg_SqlColumn(NStr::gc_Str<"note">, &CDropAddV2Row::m_Note)
			)
		)
	;

	constexpr auto gc_InsertDropAddV1 = fg_SqlPreparedInsert(gc_DropAddV1Table);
	constexpr auto gc_InsertDropAddV2 = fg_SqlPreparedInsert(gc_DropAddV2Table);
	constexpr auto gc_SelectDropAddV2ByName = fg_SqlPreparedSelect(gc_DropAddV2Table).f_Where(fg_SqlParamEq<&CDropAddV2Row::m_Name>());

	constexpr auto gc_DropAddV1Database = fg_SqlDatabase(NStr::gc_Str<"drop_add_database">, gc_DropAddV1Table);
	constexpr auto gc_DropAddV2Database = fg_SqlDatabase(NStr::gc_Str<"drop_add_database">, gc_DropAddV2Table);
	constexpr auto gc_DropAddV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"drop_add_0001">, gc_DropAddV1Database);
	constexpr auto gc_DropAddV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"drop_add_0002">, gc_DropAddV2Database);
	constexpr auto gc_DropAddV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_DropAddV1SchemaVersion));
	constexpr auto gc_DropAddV2SchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_DropAddV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_DropAddV2SchemaVersion, fg_SqlDropColumn(NStr::gc_Str<"drop_add_test">, NStr::gc_Str<"code">))
		)
	;

	// Identical column layouts with different table names: their prepared statements must stay distinct
	struct CSameShapeRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
	};

	constexpr auto gc_SameShapeATable = fg_SqlTable<CSameShapeRow>
		(
			NStr::gc_Str<"same_shape_a">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CSameShapeRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CSameShapeRow::m_Value)
			)
		)
	;

	constexpr auto gc_SameShapeBTable = fg_SqlTable<CSameShapeRow>
		(
			NStr::gc_Str<"same_shape_b">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CSameShapeRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CSameShapeRow::m_Value)
			)
		)
	;

	constexpr auto gc_InsertSameShapeA = fg_SqlPreparedInsert(gc_SameShapeATable);
	constexpr auto gc_InsertSameShapeB = fg_SqlPreparedInsert(gc_SameShapeBTable);

	constexpr auto gc_SameShapeDatabase = fg_SqlDatabase(NStr::gc_Str<"same_shape_database">, gc_SameShapeATable, gc_SameShapeBTable);
	constexpr auto gc_SameShapeSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"same_shape_0001">, gc_SameShapeDatabase);
	constexpr auto gc_SameShapeSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_SameShapeSchemaVersion));

	constexpr auto gc_ExcludedNameTable = fg_SqlTable<CUpsertRow>
		(
			NStr::gc_Str<"excluded">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CUpsertRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CUpsertRow::m_Email)
				, fg_SqlColumn(NStr::gc_Str<"display_name">, &CUpsertRow::m_DisplayName)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"idx_excluded_email">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"email">))
		)
	;

	constexpr auto gc_UpsertExcludedNameByEmail = fg_SqlPreparedUpsert(gc_ExcludedNameTable).f_OnConflict<&CUpsertRow::m_Email>().f_Update<&CUpsertRow::m_DisplayName>();
	constexpr auto gc_SelectExcludedNameByEmail = fg_SqlPreparedSelect(gc_ExcludedNameTable).f_Where(fg_SqlParamEq<&CUpsertRow::m_Email>());

	constexpr auto gc_ExcludedNameDatabase = fg_SqlDatabase(NStr::gc_Str<"excluded_name_database">, gc_ExcludedNameTable);
	constexpr auto gc_ExcludedNameSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"excluded_name_0001">, gc_ExcludedNameDatabase);
	constexpr auto gc_ExcludedNameSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_ExcludedNameSchemaVersion));

	struct CAdoptRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
	};

	constexpr auto gc_AdoptTable = fg_SqlTable<CAdoptRow>
		(
			NStr::gc_Str<"adopt_existing">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CAdoptRow::m_Value)
			)
		)
	;

	constexpr auto gc_InsertAdopt = fg_SqlPreparedInsert(gc_AdoptTable);

	constexpr auto gc_AdoptDatabase = fg_SqlDatabase(NStr::gc_Str<"adopt_database">, gc_AdoptTable);
	constexpr auto gc_AdoptSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"adopt_0001">, gc_AdoptDatabase);
	constexpr auto gc_AdoptSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptSchemaVersion));

	// Deleting a parent sets both key columns of its children to NULL
	struct CAdoptForeignKeyParentRow
	{
		int64 m_TenantID = 0;
		int64 m_ID = 0;
	};

	struct CAdoptForeignKeyChildRow
	{
		int64 m_ID = 0;
		int64 m_TenantID = 0;
		NStorage::TCOptional<int64> m_ParentID;
	};

	constexpr auto gc_AdoptForeignKeyParentTable = fg_SqlTable<CAdoptForeignKeyParentRow>
		(
			NStr::gc_Str<"adopt_fk_parent">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"tenant_id">, &CAdoptForeignKeyParentRow::m_TenantID), fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptForeignKeyParentRow::m_ID))
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlPrimaryKey(NStr::gc_Str<"adopt_fk_parent_pk">, NStr::gc_Str<"tenant_id">, NStr::gc_Str<"id">))
		)
	;

	constexpr auto gc_AdoptForeignKeyChildTable = fg_SqlTable<CAdoptForeignKeyChildRow>
		(
			NStr::gc_Str<"adopt_fk_child">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptForeignKeyChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey)
				, fg_SqlColumn(NStr::gc_Str<"tenant_id">, &CAdoptForeignKeyChildRow::m_TenantID)
				, fg_SqlColumn(NStr::gc_Str<"parent_id">, &CAdoptForeignKeyChildRow::m_ParentID)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints
			(
				fg_SqlForeignKey
				(
					NStr::gc_Str<"adopt_fk_child_parent_fk">
					, fg_SqlColumnNames(NStr::gc_Str<"tenant_id">, NStr::gc_Str<"parent_id">)
					, fg_SqlReferences(NStr::gc_Str<"adopt_fk_parent">, NStr::gc_Str<"tenant_id">, NStr::gc_Str<"id">)
					, ESqlForeignKeyAction::mc_SetNull
				)
			)
		)
	;

	constexpr auto gc_AdoptForeignKeyDatabase = fg_SqlDatabase(NStr::gc_Str<"adopt_fk_database">, gc_AdoptForeignKeyParentTable, gc_AdoptForeignKeyChildTable);
	constexpr auto gc_AdoptForeignKeySchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"adopt_fk_0001">, gc_AdoptForeignKeyDatabase);
	constexpr auto gc_AdoptForeignKeySchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptForeignKeySchemaVersion));

	constexpr auto gc_AdoptLaterMarkerTable = fg_SqlTable<CAdoptRow>
		(
			NStr::gc_Str<"adopt_later_marker">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"value">, &CAdoptRow::m_Value)
			)
		)
	;

	// The second version first declares adopt_existing, so applying it adopts a live table of that name
	constexpr auto gc_AdoptLaterV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"adopt_later_0001">, fg_SqlDatabase(NStr::gc_Str<"adopt_database">, gc_AdoptLaterMarkerTable));
	constexpr auto gc_AdoptLaterV2SchemaVersion = fg_SqlSchemaVersion
		(
			NStr::gc_Str<"adopt_later_0002">
			, fg_SqlDatabase(NStr::gc_Str<"adopt_database">, gc_AdoptLaterMarkerTable, gc_AdoptTable)
		)
	;
	constexpr auto gc_AdoptLaterV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptLaterV1SchemaVersion));
	constexpr auto gc_AdoptLaterSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptLaterV1SchemaVersion), fg_SqlSchemaMigration(gc_AdoptLaterV2SchemaVersion));

	struct CAdoptNullableParentRow
	{
		NStr::CStr m_Code;
	};

	struct CAdoptNullableRow
	{
		int64 m_ID = 0;
		NStorage::TCOptional<NStr::CStr> m_Code;
		NStr::CStr m_ParentCode;
	};

	constexpr auto gc_AdoptNullableParentTable = fg_SqlTable<CAdoptNullableParentRow>
		(
			NStr::gc_Str<"adopt_nullable_parent">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"code">, &CAdoptNullableParentRow::m_Code, ESqlColumnFlag::mc_PrimaryKey))
		)
	;

	constexpr auto gc_AdoptNullableTable = fg_SqlTable<CAdoptNullableRow>
		(
			NStr::gc_Str<"adopt_nullable">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptNullableRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CAdoptNullableRow::m_Code)
				, fg_SqlColumn(NStr::gc_Str<"parent_code">, &CAdoptNullableRow::m_ParentCode)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"adopt_nullable_code">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"code">))
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CAdoptNullableRow::m_ParentCode>(NStr::gc_Str<"adopt_nullable_parent_fk">, fg_SqlReferences<&CAdoptNullableParentRow::m_Code>(gc_AdoptNullableParentTable))
			)
		)
	;

	constexpr auto gc_AdoptNullableDatabase = fg_SqlDatabase(NStr::gc_Str<"adopt_nullable_database">, gc_AdoptNullableParentTable, gc_AdoptNullableTable);
	constexpr auto gc_AdoptNullableSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"adopt_nullable_0001">, gc_AdoptNullableDatabase);
	constexpr auto gc_AdoptNullableSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptNullableSchemaVersion));

	struct CAdoptUniqueRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Code;
	};

	constexpr auto gc_AdoptUniqueTable = fg_SqlTable<CAdoptUniqueRow>
		(
			NStr::gc_Str<"adopt_unique">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CAdoptUniqueRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"code">, &CAdoptUniqueRow::m_Code)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlUnique(NStr::gc_Str<"adopt_unique_code">, NStr::gc_Str<"code">))
		)
	;

	constexpr auto gc_InsertAdoptUnique = fg_SqlPreparedInsert(gc_AdoptUniqueTable);

	constexpr auto gc_AdoptUniqueDatabase = fg_SqlDatabase(NStr::gc_Str<"adopt_unique_database">, gc_AdoptUniqueTable);
	constexpr auto gc_AdoptUniqueSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"adopt_unique_0001">, gc_AdoptUniqueDatabase);
	constexpr auto gc_AdoptUniqueSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_AdoptUniqueSchemaVersion));

	struct CRebuildVersion1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
		NStr::CStr m_LegacyValue;
	};

	struct CRebuildVersion2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
	};

	constexpr auto gc_RebuildVersion1Table = fg_SqlTable<CRebuildVersion1Row>
		(
			NStr::gc_Str<"rebuild_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildVersion1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRebuildVersion1Row::m_Token)
				, fg_SqlColumn(NStr::gc_Str<"legacy_value">, &CRebuildVersion1Row::m_LegacyValue)
			)
		)
	;

	constexpr auto gc_RebuildVersion2Table = fg_SqlTable<CRebuildVersion2Row>
		(
			NStr::gc_Str<"rebuild_test">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildVersion2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRebuildVersion2Row::m_Token)
			)
		)
	;

	constexpr auto gc_RebuildVersion1Database = fg_SqlDatabase(NStr::gc_Str<"rebuild_database">, gc_RebuildVersion1Table);
	constexpr auto gc_RebuildVersion2Database = fg_SqlDatabase(NStr::gc_Str<"rebuild_database">, gc_RebuildVersion2Table);
	constexpr auto gc_RebuildVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_0001">, gc_RebuildVersion1Database);
	constexpr auto gc_RebuildVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_0002">, gc_RebuildVersion2Database);
	constexpr auto gc_RebuildVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RebuildVersion1SchemaVersion));
	constexpr auto gc_RebuildSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RebuildVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RebuildVersion2SchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"rebuild_test">))
		)
	;
	constexpr auto gc_DropColumnSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RebuildVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RebuildVersion2SchemaVersion, fg_SqlDropColumn(NStr::gc_Str<"rebuild_test">, NStr::gc_Str<"legacy_value">))
		)
	;

	constexpr auto gc_SelectRebuildVersion1ByToken = fg_SqlPreparedSelect(gc_RebuildVersion1Table).f_Where(fg_SqlParamEq<&CRebuildVersion1Row::m_Token>());

	constexpr auto gc_SelectRebuildVersion2ByToken = fg_SqlPreparedSelect(gc_RebuildVersion2Table).f_Where(fg_SqlParamEq<&CRebuildVersion2Row::m_Token>());

	constexpr auto gc_TransformVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"transform_0001">, gc_RebuildVersion2Database);
	constexpr auto gc_TransformVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"transform_0002">, gc_RebuildVersion2Database);
	constexpr auto gc_TransformVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_TransformVersion1SchemaVersion));
	constexpr auto gc_TransformSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_TransformVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_TransformVersion2SchemaVersion, fg_SqlUpdateColumnSql(NStr::gc_Str<"rebuild_test">, NStr::gc_Str<"token">, NStr::gc_Str<"'transformed-token'">))
		)
	;

	struct CFkParentRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
	};

	struct CFkChildRow
	{
		int64 m_ID = 0;
		int64 m_ParentID = 0;
	};

	constexpr auto gc_FkParentTable = fg_SqlTable<CFkParentRow>
		(
			NStr::gc_Str<"fk_parent">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CFkParentRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CFkParentRow::m_Name)
			)
		)
	;

	constexpr auto gc_FkChildTable = fg_SqlTable<CFkChildRow>
		(
			NStr::gc_Str<"fk_child">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CFkChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"parent_id">, &CFkChildRow::m_ParentID)
			)
			, fg_SqlIndexes(fg_SqlIndex<&CFkChildRow::m_ParentID>(NStr::gc_Str<"fk_child_parent_id">))
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CFkChildRow::m_ParentID>(NStr::gc_Str<"fk_child_parent_fk">, fg_SqlReferences<&CFkParentRow::m_ID>(gc_FkParentTable), ESqlForeignKeyAction::mc_Cascade)
			)
		)
	;

	constexpr auto gc_InsertFkParent = fg_SqlPreparedInsert(gc_FkParentTable);
	constexpr auto gc_InsertFkChild = fg_SqlPreparedInsert(gc_FkChildTable);
	constexpr auto gc_SelectFkChildByParent = fg_SqlPreparedSelect(gc_FkChildTable).f_Where(fg_SqlParamEq<&CFkChildRow::m_ParentID>());

	constexpr auto gc_FkDatabase = fg_SqlDatabase(NStr::gc_Str<"fk_database">, gc_FkParentTable, gc_FkChildTable);
	constexpr auto gc_FkVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"fk_0001">, gc_FkDatabase);
	constexpr auto gc_FkVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"fk_0002">, gc_FkDatabase);
	constexpr auto gc_FkVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion));
	constexpr auto gc_FkParentRebuildSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_FkVersion2SchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"fk_parent">))
		)
	;
	constexpr auto gc_FkUnchangedUpgradeSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion), fg_SqlSchemaMigration(gc_FkVersion2SchemaVersion));
	constexpr auto gc_FkViolationSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_FkVersion2SchemaVersion, fg_SqlUpdateColumnSql(NStr::gc_Str<"fk_child">, NStr::gc_Str<"parent_id">, NStr::gc_Str<"999999">))
		)
	;

	constexpr auto gc_FkForwardReferenceDatabase = fg_SqlDatabase(NStr::gc_Str<"fk_forward_database">, gc_FkChildTable, gc_FkParentTable);
	constexpr auto gc_FkForwardReferenceSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"fk_forward_0001">, gc_FkForwardReferenceDatabase);
	constexpr auto gc_FkForwardReferenceSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_FkForwardReferenceSchemaVersion));

	// v2 no longer declares the child foreign key that the parent rebuild drops, and changes its child after the rebuild
	struct CFkChildRenamedRow
	{
		int64 m_ID = 0;
		int64 m_OwnerID = 0;
	};

	struct CFkChildDroppedRow
	{
		int64 m_ID = 0;
	};

	constexpr auto gc_FkChildRenamedTable = fg_SqlTable<CFkChildRenamedRow>
		(
			NStr::gc_Str<"fk_child">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CFkChildRenamedRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"owner_id">, &CFkChildRenamedRow::m_OwnerID)
			)
		)
	;

	constexpr auto gc_FkChildDroppedTable = fg_SqlTable<CFkChildDroppedRow>
		(
			NStr::gc_Str<"fk_child">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CFkChildDroppedRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement))
		)
	;

	constexpr auto gc_FkChildRenamedDatabase = fg_SqlDatabase(NStr::gc_Str<"fk_database">, gc_FkParentTable, gc_FkChildRenamedTable);
	constexpr auto gc_FkChildDroppedColumnDatabase = fg_SqlDatabase(NStr::gc_Str<"fk_database">, gc_FkParentTable, gc_FkChildDroppedTable);
	constexpr auto gc_FkChildDroppedTableDatabase = fg_SqlDatabase(NStr::gc_Str<"fk_database">, gc_FkParentTable);
	constexpr auto gc_FkChildRenamedSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"fk_0002">, gc_FkChildRenamedDatabase);
	constexpr auto gc_FkChildDroppedColumnSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"fk_0002">, gc_FkChildDroppedColumnDatabase);
	constexpr auto gc_FkChildDroppedTableSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"fk_0002">, gc_FkChildDroppedTableDatabase);
	constexpr auto gc_FkChildRenameAfterParentRebuildSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion)
			, fg_SqlSchemaMigration
			(
				gc_FkChildRenamedSchemaVersion
				, fg_SqlRebuildTable(NStr::gc_Str<"fk_parent">)
				, fg_SqlRenameColumn(NStr::gc_Str<"fk_child">, NStr::gc_Str<"parent_id">, NStr::gc_Str<"owner_id">)
			)
		)
	;
	constexpr auto gc_FkChildDropColumnAfterParentRebuildSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion)
			, fg_SqlSchemaMigration
			(
				gc_FkChildDroppedColumnSchemaVersion
				, fg_SqlRebuildTable(NStr::gc_Str<"fk_parent">)
				, fg_SqlDropColumn(NStr::gc_Str<"fk_child">, NStr::gc_Str<"parent_id">)
			)
		)
	;
	constexpr auto gc_FkChildDropTableAfterParentRebuildSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_FkChildDroppedTableSchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"fk_parent">), fg_SqlDropTable(NStr::gc_Str<"fk_child">))
		)
	;

	// v2 renames the child table, which keeps declaring its foreign key under the same name
	constexpr auto gc_FkChildMovedTable = fg_SqlTable<CFkChildRow>
		(
			NStr::gc_Str<"fk_child_moved">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CFkChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"parent_id">, &CFkChildRow::m_ParentID)
			)
			, fg_SqlIndexes(fg_SqlIndex<&CFkChildRow::m_ParentID>(NStr::gc_Str<"fk_child_parent_id">))
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CFkChildRow::m_ParentID>(NStr::gc_Str<"fk_child_parent_fk">, fg_SqlReferences<&CFkParentRow::m_ID>(gc_FkParentTable), ESqlForeignKeyAction::mc_Cascade)
			)
		)
	;

	constexpr auto gc_FkChildMovedDatabase = fg_SqlDatabase(NStr::gc_Str<"fk_database">, gc_FkParentTable, gc_FkChildMovedTable);
	constexpr auto gc_FkChildMovedSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"fk_0002">, gc_FkChildMovedDatabase);
	constexpr auto gc_FkChildTableRenameAfterParentRebuildSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_FkChildMovedSchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"fk_parent">), fg_SqlRenameTable(NStr::gc_Str<"fk_child">, NStr::gc_Str<"fk_child_moved">))
		)
	;

	// v2 drops fk_parent and no longer declares the child's foreign key, which the live child keeps
	constexpr auto gc_FkChildWithoutForeignKeyTable = fg_SqlTable<CFkChildRow>
		(
			NStr::gc_Str<"fk_child">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CFkChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"parent_id">, &CFkChildRow::m_ParentID)
			)
			, fg_SqlIndexes(fg_SqlIndex<&CFkChildRow::m_ParentID>(NStr::gc_Str<"fk_child_parent_id">))
		)
	;

	constexpr auto gc_FkParentDroppedDatabase = fg_SqlDatabase(NStr::gc_Str<"fk_database">, gc_FkChildWithoutForeignKeyTable);
	constexpr auto gc_FkParentDroppedSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"fk_0002">, gc_FkParentDroppedDatabase);
	constexpr auto gc_FkParentDropSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_FkVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_FkParentDroppedSchemaVersion, fg_SqlDropTable(NStr::gc_Str<"fk_parent">))
		)
	;

	// A rebuild must restore a self-referencing foreign key once, as a key of the rebuilt table, not as a key from another table
	struct CSelfRefRow
	{
		int64 m_ID = 0;
		NStorage::TCOptional<int64> m_ParentID;
	};

	constexpr auto gc_SelfRefTable = fg_SqlTable<CSelfRefRow>
		(
			NStr::gc_Str<"self_ref">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CSelfRefRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"parent_id">, &CSelfRefRow::m_ParentID)
			)
			, fg_SqlIndexes(fg_SqlIndex<&CSelfRefRow::m_ParentID>(NStr::gc_Str<"self_ref_parent_id">))
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CSelfRefRow::m_ParentID>
				(
					NStr::gc_Str<"self_ref_parent_fk">
					, fg_SqlReferences(NStr::gc_Str<"self_ref">, NStr::gc_Str<"id">)
					, ESqlForeignKeyAction::mc_Cascade
				)
			)
		)
	;

	constexpr auto gc_InsertSelfRef = fg_SqlPreparedInsert(gc_SelfRefTable);
	constexpr auto gc_SelectSelfRefByParent = fg_SqlPreparedSelect(gc_SelfRefTable).f_Where(fg_SqlParamEq<&CSelfRefRow::m_ParentID>());

	constexpr auto gc_SelfRefDatabase = fg_SqlDatabase(NStr::gc_Str<"self_ref_database">, gc_SelfRefTable);
	constexpr auto gc_SelfRefVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"self_ref_0001">, gc_SelfRefDatabase);
	constexpr auto gc_SelfRefVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"self_ref_0002">, gc_SelfRefDatabase);
	constexpr auto gc_SelfRefVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_SelfRefVersion1SchemaVersion));
	constexpr auto gc_SelfRefRebuildSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_SelfRefVersion1SchemaVersion)
			, fg_SqlSchemaMigration(gc_SelfRefVersion2SchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"self_ref">))
		)
	;

	// v2 rebuilds the item table with a new foreign key to a table that v2 also introduces
	struct CRebuildFkItemV1Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
	};

	struct CRebuildFkCategoryRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
	};

	struct CRebuildFkItemV2Row
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
		NStorage::TCOptional<int64> m_CategoryID;
	};

	constexpr auto gc_RebuildFkItemV1Table = fg_SqlTable<CRebuildFkItemV1Row>
		(
			NStr::gc_Str<"rebuild_fk_item">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildFkItemV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRebuildFkItemV1Row::m_Token)
			)
		)
	;

	constexpr auto gc_RebuildFkCategoryTable = fg_SqlTable<CRebuildFkCategoryRow>
		(
			NStr::gc_Str<"rebuild_fk_category">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildFkCategoryRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CRebuildFkCategoryRow::m_Name)
			)
		)
	;

	constexpr auto gc_RebuildFkItemV2Table = fg_SqlTable<CRebuildFkItemV2Row>
		(
			NStr::gc_Str<"rebuild_fk_item">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildFkItemV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRebuildFkItemV2Row::m_Token)
				, fg_SqlColumn(NStr::gc_Str<"category_id">, &CRebuildFkItemV2Row::m_CategoryID)
			)
			, fg_SqlIndexes(fg_SqlIndex<&CRebuildFkItemV2Row::m_CategoryID>(NStr::gc_Str<"rebuild_fk_item_category_id">))
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CRebuildFkItemV2Row::m_CategoryID>
				(
					NStr::gc_Str<"rebuild_fk_item_category_fk">
					, fg_SqlReferences<&CRebuildFkCategoryRow::m_ID>(gc_RebuildFkCategoryTable)
					, ESqlForeignKeyAction::mc_Cascade
				)
			)
		)
	;

	constexpr auto gc_SelectRebuildFkItemByToken = fg_SqlPreparedSelect(gc_RebuildFkItemV2Table).f_Where(fg_SqlParamEq<&CRebuildFkItemV2Row::m_Token>());
	constexpr auto gc_InsertRebuildFkItemV1 = fg_SqlPreparedInsert(gc_RebuildFkItemV1Table);
	constexpr auto gc_InsertRebuildFkCategory = fg_SqlPreparedInsert(gc_RebuildFkCategoryTable);
	constexpr auto gc_InsertRebuildFkItemV2 = fg_SqlPreparedInsert(gc_RebuildFkItemV2Table);

	constexpr auto gc_RebuildFkV1Database = fg_SqlDatabase(NStr::gc_Str<"rebuild_fk_database">, gc_RebuildFkItemV1Table);
	constexpr auto gc_RebuildFkV2Database = fg_SqlDatabase(NStr::gc_Str<"rebuild_fk_database">, gc_RebuildFkCategoryTable, gc_RebuildFkItemV2Table);
	constexpr auto gc_RebuildFkV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_fk_0001">, gc_RebuildFkV1Database);
	constexpr auto gc_RebuildFkV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_fk_0002">, gc_RebuildFkV2Database);
	constexpr auto gc_RebuildFkV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RebuildFkV1SchemaVersion));
	constexpr auto gc_RebuildFkSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RebuildFkV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RebuildFkV2SchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"rebuild_fk_item">))
		)
	;

	// The rebuild already creates note, so a separate ADD COLUMN would be a duplicate
	struct CRebuildAddColumnRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
		NStorage::TCOptional<NStr::CStr> m_Note;
	};

	constexpr auto gc_RebuildAddColumnV1Table = fg_SqlTable<CRebuildVersion2Row>
		(
			NStr::gc_Str<"rebuild_add_column">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildVersion2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRebuildVersion2Row::m_Token)
			)
		)
	;

	constexpr auto gc_RebuildAddColumnV2Table = fg_SqlTable<CRebuildAddColumnRow>
		(
			NStr::gc_Str<"rebuild_add_column">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRebuildAddColumnRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CRebuildAddColumnRow::m_Token)
				, fg_SqlColumn(NStr::gc_Str<"note">, &CRebuildAddColumnRow::m_Note)
			)
		)
	;

	constexpr auto gc_RebuildAddColumnV1Database = fg_SqlDatabase(NStr::gc_Str<"rebuild_add_database">, gc_RebuildAddColumnV1Table);
	constexpr auto gc_RebuildAddColumnV2Database = fg_SqlDatabase(NStr::gc_Str<"rebuild_add_database">, gc_RebuildAddColumnV2Table);
	constexpr auto gc_RebuildAddColumnV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_add_0001">, gc_RebuildAddColumnV1Database);
	constexpr auto gc_RebuildAddColumnV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rebuild_add_0002">, gc_RebuildAddColumnV2Database);
	constexpr auto gc_RebuildAddColumnV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RebuildAddColumnV1SchemaVersion));
	constexpr auto gc_RebuildAddColumnSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RebuildAddColumnV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RebuildAddColumnV2SchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"rebuild_add_column">))
		)
	;

	constexpr auto gc_InsertRebuildAddColumnV1 = fg_SqlPreparedInsert(gc_RebuildAddColumnV1Table);
	constexpr auto gc_SelectRebuildAddColumnByToken = fg_SqlPreparedSelect(gc_RebuildAddColumnV2Table).f_Where(fg_SqlParamEq<&CRebuildAddColumnRow::m_Token>());

	// Renaming the parent must not force a rebuild of the child that references it
	struct CRenameRefParentRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Name;
	};

	struct CRenameRefChildRow
	{
		int64 m_ID = 0;
		NStorage::TCOptional<int64> m_ParentID;
	};

	constexpr auto gc_RenameRefParentV1Table = fg_SqlTable<CRenameRefParentRow>
		(
			NStr::gc_Str<"rename_ref_parent">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRenameRefParentRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CRenameRefParentRow::m_Name)
			)
		)
	;

	constexpr auto gc_RenameRefParentV2Table = fg_SqlTable<CRenameRefParentRow>
		(
			NStr::gc_Str<"rename_ref_parent_renamed">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRenameRefParentRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"name">, &CRenameRefParentRow::m_Name)
			)
		)
	;

	constexpr auto gc_RenameRefChildV1Table = fg_SqlTable<CRenameRefChildRow>
		(
			NStr::gc_Str<"rename_ref_child">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRenameRefChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"parent_id">, &CRenameRefChildRow::m_ParentID)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CRenameRefChildRow::m_ParentID>(NStr::gc_Str<"rename_ref_child_parent_fk">, fg_SqlReferences(NStr::gc_Str<"rename_ref_parent">, NStr::gc_Str<"id">))
			)
		)
	;

	constexpr auto gc_RenameRefChildV2Table = fg_SqlTable<CRenameRefChildRow>
		(
			NStr::gc_Str<"rename_ref_child">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CRenameRefChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"parent_id">, &CRenameRefChildRow::m_ParentID)
			)
			, fg_SqlIndexes()
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CRenameRefChildRow::m_ParentID>(NStr::gc_Str<"rename_ref_child_parent_fk">, fg_SqlReferences(NStr::gc_Str<"rename_ref_parent_renamed">, NStr::gc_Str<"id">))
			)
		)
	;

	constexpr auto gc_RenameRefV1Database = fg_SqlDatabase(NStr::gc_Str<"rename_ref_database">, gc_RenameRefParentV1Table, gc_RenameRefChildV1Table);
	constexpr auto gc_RenameRefV2Database = fg_SqlDatabase(NStr::gc_Str<"rename_ref_database">, gc_RenameRefParentV2Table, gc_RenameRefChildV2Table);
	constexpr auto gc_RenameRefV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rename_ref_0001">, gc_RenameRefV1Database);
	constexpr auto gc_RenameRefV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"rename_ref_0002">, gc_RenameRefV2Database);
	constexpr auto gc_RenameRefV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RenameRefV1SchemaVersion));
	constexpr auto gc_RenameRefSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RenameRefV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RenameRefV2SchemaVersion, fg_SqlRenameTable(NStr::gc_Str<"rename_ref_parent">, NStr::gc_Str<"rename_ref_parent_renamed">))
		)
	;

	struct CIndexedRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Email;
	};

	constexpr auto gc_IndexVersion1Table = fg_SqlTable<CIndexedRow>
		(
			NStr::gc_Str<"indexed_people">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CIndexedRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CIndexedRow::m_Email)
			)
		)
	;

	constexpr auto gc_IndexVersion2Table = fg_SqlTable<CIndexedRow>
		(
			NStr::gc_Str<"indexed_people">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CIndexedRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CIndexedRow::m_Email)
			)
			, fg_SqlIndexes(fg_SqlIndex(NStr::gc_Str<"idx_indexed_people_email">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"email">))
		)
	;

	constexpr auto gc_IndexVersion1Database = fg_SqlDatabase(NStr::gc_Str<"index_database">, gc_IndexVersion1Table);
	constexpr auto gc_IndexVersion2Database = fg_SqlDatabase(NStr::gc_Str<"index_database">, gc_IndexVersion2Table);
	constexpr auto gc_IndexVersion1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"index_0001">, gc_IndexVersion1Database);
	constexpr auto gc_IndexVersion2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"index_0002">, gc_IndexVersion2Database);
	constexpr auto gc_IndexVersion1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_IndexVersion1SchemaVersion));
	constexpr auto gc_IndexSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_IndexVersion1SchemaVersion), fg_SqlSchemaMigration(gc_IndexVersion2SchemaVersion));

	// A version inserted before an applied one leaves the applied one's checksum unchanged, which must not hide that it never ran
	constexpr auto gc_HistoryGapV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"history_gap_0001">, gc_RebuildVersion2Database);
	constexpr auto gc_HistoryGapV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"history_gap_0002">, gc_RebuildVersion2Database);
	constexpr auto gc_HistoryGapV3SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"history_gap_0003">, gc_RebuildVersion2Database);
	constexpr auto gc_HistoryGapAppliedSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_HistoryGapV1SchemaVersion), fg_SqlSchemaMigration(gc_HistoryGapV3SchemaVersion));
	constexpr auto gc_HistoryGapSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_HistoryGapV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_HistoryGapV2SchemaVersion, fg_SqlUpdateColumnSql(NStr::gc_Str<"rebuild_test">, NStr::gc_Str<"token">, NStr::gc_Str<"'history-gap-backfilled'">))
			, fg_SqlSchemaMigration(gc_HistoryGapV3SchemaVersion)
		)
	;

	// A column a migration drops after an operation that still uses it
	struct CDropLaterV1Row
	{
		int64 m_ID = 0;
		NStorage::TCOptional<NStr::CStr> m_First;
		NStorage::TCOptional<NStr::CStr> m_Second;
		NStorage::TCOptional<NStr::CStr> m_Kept;
	};

	struct CDropLaterV2Row
	{
		int64 m_ID = 0;
		NStorage::TCOptional<NStr::CStr> m_Kept;
	};

	constexpr auto gc_DropLaterV1Table = fg_SqlTable<CDropLaterV1Row>
		(
			NStr::gc_Str<"drop_later">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropLaterV1Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"first">, &CDropLaterV1Row::m_First)
				, fg_SqlColumn(NStr::gc_Str<"second">, &CDropLaterV1Row::m_Second)
				, fg_SqlColumn(NStr::gc_Str<"kept">, &CDropLaterV1Row::m_Kept)
			)
		)
	;

	constexpr auto gc_DropLaterV2Table = fg_SqlTable<CDropLaterV2Row>
		(
			NStr::gc_Str<"drop_later">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CDropLaterV2Row::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"kept">, &CDropLaterV2Row::m_Kept)
			)
		)
	;

	constexpr auto gc_DropLaterV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"drop_later_0001">, fg_SqlDatabase(NStr::gc_Str<"drop_later_database">, gc_DropLaterV1Table));
	constexpr auto gc_DropLaterV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"drop_later_0002">, fg_SqlDatabase(NStr::gc_Str<"drop_later_database">, gc_DropLaterV2Table));
	constexpr auto gc_DropLaterV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_DropLaterV1SchemaVersion));
	constexpr auto gc_DropLaterSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_DropLaterV1SchemaVersion)
			, fg_SqlSchemaMigration
			(
				gc_DropLaterV2SchemaVersion
				, fg_SqlDropColumn(NStr::gc_Str<"drop_later">, NStr::gc_Str<"first">)
				, fg_SqlUpdateColumnSql(NStr::gc_Str<"drop_later">, NStr::gc_Str<"kept">, NStr::gc_Str<"second">)
				, fg_SqlDropColumn(NStr::gc_Str<"drop_later">, NStr::gc_Str<"second">)
			)
		)
	;

	constexpr auto gc_RebuildDropLaterSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_DropLaterV1SchemaVersion)
			, fg_SqlSchemaMigration
			(
				gc_DropLaterV2SchemaVersion
				, fg_SqlRebuildTable(NStr::gc_Str<"drop_later">)
				, fg_SqlUpdateColumnSql(NStr::gc_Str<"drop_later">, NStr::gc_Str<"kept">, NStr::gc_Str<"second">)
				, fg_SqlDropColumn(NStr::gc_Str<"drop_later">, NStr::gc_Str<"second">)
			)
		)
	;

	constexpr auto gc_InsertDropLaterV1 = fg_SqlPreparedInsert(gc_DropLaterV1Table);

	// SQLite matches column names ignoring ASCII case, so a rebuild keeps the data of a column whose name changes only in case
	struct CCaseRenameRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Token;
	};

	constexpr auto gc_CaseRenameV1Table = fg_SqlTable<CCaseRenameRow>
		(
			NStr::gc_Str<"case_rename">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CCaseRenameRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"Token">, &CCaseRenameRow::m_Token)
			)
		)
	;

	constexpr auto gc_CaseRenameV2Table = fg_SqlTable<CCaseRenameRow>
		(
			NStr::gc_Str<"case_rename">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CCaseRenameRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CCaseRenameRow::m_Token)
			)
		)
	;

	constexpr auto gc_CaseRenameV1SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"case_rename_0001">, fg_SqlDatabase(NStr::gc_Str<"case_rename_database">, gc_CaseRenameV1Table));
	constexpr auto gc_CaseRenameV2SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"case_rename_0002">, fg_SqlDatabase(NStr::gc_Str<"case_rename_database">, gc_CaseRenameV2Table));
	constexpr auto gc_CaseRenameSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_CaseRenameV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_CaseRenameV2SchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"case_rename">))
		)
	;

	NConcurrency::TCFuture<void> fg_TestSqlDatabase(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseMigration(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRenameMigration(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRenameConstrainedMigration(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseUnknownVersionValidation(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseChecksumValidation(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRequiredColumnValidation(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildMigration(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildScratchNameCollision(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildAddsForeignKeyToNewTable(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAddUniqueColumnMigration(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseGeneratedColumn(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseGeneratedColumnInsertPaths(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseGeneratedColumnRebuildMigration(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildKeepsTableGuarantees(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseNarrowAutoIncrementKey(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptExistingTableConstraint(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptExistingColumnConstraint(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseBackfillOfAddedColumn(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseIndexDefinitionChange(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAddColumnWithExpressionDefault(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnWithAddedColumn(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRenameColumnWithCheck(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseSameShapedTablePreparedCache(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseUpsertIntoTableNamedExcluded(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptUntrackedExistingTable(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptRejectsUndeclaredConstraints(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptKeepsForeignKeyBehavior(FCreateBackend _fCreateBackend, bool _bDeleteSetColumns);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRejectsViewNamedLikeTable(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnReusedConstraint(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseForeignKeyRebuildMigration(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRecreatedTableForeignKeys(FCreateBackend _fCreateBackend, NStr::CStr _DropParentSql);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropReferencedTable(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseForeignKeyOrderingSchema(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseSelfReferentialForeignKeyRebuildMigration(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnMigration(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnKeepsUndeclaredColumn(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildKeepsUndeclaredColumn(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnKeepsLaterDroppedColumn(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildKeepsLaterDroppedColumn(FCreateBackend _fCreateBackend);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseHistoryGapValidation(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTransformMigration(FCreateBackend _fCreateBackend);

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseIndexMigration(FCreateBackend _fCreateBackend);

	struct CSqlBenchmarkBackend
	{
		NStr::CStr m_Name;
		CSqlDatabaseClient *m_pDatabase = nullptr;
	};

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseBenchmarks(NContainer::TCVector<CSqlBenchmarkBackend> _Backends, umint _nRows);

	template <typename tf_FFunction>
	auto fg_CallOnOtherActor(tf_FFunction _fFunction) -> decltype(_fFunction());
}

#include "Test_Malterlib_SQL_DatabaseBackendShared.hpp"
