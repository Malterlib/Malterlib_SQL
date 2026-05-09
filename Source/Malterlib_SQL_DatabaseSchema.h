// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/Container/Vector>
#include <Mib/Core/Core>
#include <Mib/Cryptography/UUID>
#include <Mib/Encoding/Json>
#include <Mib/Storage/Optional>
#include <Mib/Storage/Tuple>
#include <Mib/String/String>
#include <Mib/Time/Time>
#include <Mib/Type/Traits>

namespace NMib::NSQL
{
	enum class ESqlDialect : uint32
	{
		mc_None = 0
		, mc_SQL1999 = DMibBit(0)
		, mc_SQL2011 = DMibBit(1)
		, mc_SQL2016 = DMibBit(2)
		, mc_SQL2023 = DMibBit(3)
		, mc_SQLite = DMibBit(4)
		, mc_Postgres = DMibBit(5)
		, mc_Dynamic = DMibBit(6)
	};

	struct CSqlArrayDimension
	{
		constexpr auto operator <=> (CSqlArrayDimension const &_Other) const noexcept = default;

		int32 m_Length = 0;
		int32 m_LowerBound = 1;
	};

	template <typename t_CValue>
	struct TCSqlArray
	{
		constexpr auto operator <=> (TCSqlArray const &_Other) const noexcept = default;

		NContainer::TCVector<CSqlArrayDimension> m_Dimensions;
		NContainer::TCVector<NStorage::TCOptional<t_CValue>> m_Values;
	};

	struct CSqlDate
	{
		constexpr auto operator <=> (CSqlDate const &_Other) const noexcept = default;

		NTime::CTime m_Time;
	};

	struct CSqlTimeOfDay
	{
		constexpr auto operator <=> (CSqlTimeOfDay const &_Other) const noexcept = default;

		NTime::CTime m_Time;
	};

	struct CSqlTimestamp
	{
		constexpr auto operator <=> (CSqlTimestamp const &_Other) const noexcept = default;

		NTime::CTime m_Time;
	};

	struct CSqlTimestampTz
	{
		constexpr auto operator <=> (CSqlTimestampTz const &_Other) const noexcept = default;

		NTime::CTime m_Time;
	};

	struct CSqlInterval
	{
		constexpr auto operator <=> (CSqlInterval const &_Other) const noexcept = default;

		int32 m_Months = 0;
		int32 m_Days = 0;
		NTime::CTimeSpan m_Time;
	};

	struct CSqlUnrecognizedBackendValue
	{
		constexpr auto operator <=> (CSqlUnrecognizedBackendValue const &_Other) const noexcept = default;

		uint32 m_TypeID = 0;
		NContainer::CIOByteVector m_Bytes;
	};

	enum class ESqlColumnType : uint8
	{
		mc_Invalid
		, mc_Integer8
		, mc_Integer16
		, mc_Integer32
		, mc_Integer64
		, mc_UnsignedInteger8
		, mc_UnsignedInteger16
		, mc_UnsignedInteger32
		, mc_UnsignedInteger64
		, mc_Float32
		, mc_Float64
		, mc_Text
		, mc_Blob
		, mc_Boolean
		, mc_Time
		, mc_UUID
		, mc_Date
		, mc_TimeOfDay
		, mc_Timestamp
		, mc_TimestampTz
		, mc_Interval
		, mc_Json
		, mc_Jsonb
		, mc_Array_Integer16
		, mc_Array_Integer32
		, mc_Array_Integer64
		, mc_Array_Float32
		, mc_Array_Float64
		, mc_Array_Text
		, mc_Array_Boolean
		, mc_Array_Bytes
		, mc_Array_Date
		, mc_Array_TimeOfDay
		, mc_Array_Timestamp
		, mc_Array_TimestampTz
		, mc_Array_UUID
		, mc_Array_Json
		, mc_Array_Jsonb
		, mc_Array_Interval
	};

	enum class ESqlValueType : uint8
	{
		mc_Null
		, mc_Integer8
		, mc_Integer16
		, mc_Integer32
		, mc_Integer64
		, mc_UnsignedInteger8
		, mc_UnsignedInteger16
		, mc_UnsignedInteger32
		, mc_UnsignedInteger64
		, mc_Float32
		, mc_Float64
		, mc_Text
		, mc_Blob
		, mc_Boolean
		, mc_Time
		, mc_UUID
		, mc_Date
		, mc_TimeOfDay
		, mc_Timestamp
		, mc_TimestampTz
		, mc_Interval
		, mc_Json
		, mc_Jsonb
		, mc_UnrecognizedBackend
		, mc_Array_Integer16
		, mc_Array_Integer32
		, mc_Array_Integer64
		, mc_Array_Float32
		, mc_Array_Float64
		, mc_Array_Text
		, mc_Array_Boolean
		, mc_Array_Bytes
		, mc_Array_Date
		, mc_Array_TimeOfDay
		, mc_Array_Timestamp
		, mc_Array_TimestampTz
		, mc_Array_UUID
		, mc_Array_Json
		, mc_Array_Jsonb
		, mc_Array_Interval
	};

	enum class ESqlColumnFlag : uint32
	{
		mc_None = 0
		, mc_Nullable = DMibBit(0)
		, mc_PrimaryKey = DMibBit(1)
		, mc_AutoIncrement = DMibBit(2)
		, mc_Unique = DMibBit(3)
	};

	enum class ESqlIndexFlag : uint32
	{
		mc_None = 0
		, mc_Unique = DMibBit(0)
	};

	enum class ESqlConstraintType : uint8
	{
		mc_PrimaryKey
		, mc_Unique
		, mc_Check
		, mc_ForeignKey
	};

	enum class ESqlForeignKeyAction : uint8
	{
		mc_Default
		, mc_Restrict
		, mc_Cascade
		, mc_SetNull
		, mc_SetDefault
		, mc_NoAction
	};

	enum class ESqlSchemaMigrationOperationType : uint8
	{
		mc_RenameTable
		, mc_RenameColumn
		, mc_RebuildTable
		, mc_DropTable
		, mc_DropColumn
		, mc_UpdateColumnSql
	};

	enum class ESqlSchemaMigrationPlanStepType : uint8
	{
		mc_AlreadyCurrent
		, mc_CreateInitialSchema
		, mc_ApplyMigrationOperations
		, mc_SyncAdditiveSchema
		, mc_MarkSchemaVersionApplied
	};

	struct ICSqlColumn;
	struct ICSqlIndex;
	struct ICSqlConstraint;
	struct ICSqlTable;
	struct ICSqlDatabase;
	struct ICSqlSchema;
	struct ICSqlSchemaVersions;

	struct CSqlColumnDescription
	{
		constexpr NStr::CStr const &f_Name() const;
		constexpr NStr::CStr const &f_DefaultSql() const;
		constexpr NStr::CStr const &f_Comment() const;

		struct CNonPortableColumnOptions const *f_NonPortableOptions(NStr::CStr const &_BackendID) const;

		constexpr bool f_IsNullable() const;
		constexpr bool f_IsPrimaryKey() const;

		ICSqlColumn const *m_pColumn = nullptr;
		NStr::CStr const *m_pName = nullptr;
		ESqlColumnType m_Type = ESqlColumnType::mc_Invalid;
		ESqlColumnFlag m_Flags = ESqlColumnFlag::mc_None;
		NStr::CStr const *m_pDefaultSql = nullptr;
		NStr::CStr const *m_pComment = nullptr;
		struct CNonPortableColumnOptions const *m_pNonPortableOptions = nullptr;
		umint m_nNonPortableOptions = 0;
	};

	struct CNonPortableColumnOptions
	{
		NStr::CStr const *m_pBackendID = &NStr::gc_Str<"">.m_Str;
		NStr::CStr const *m_pDefaultSql = &NStr::gc_Str<"">.m_Str;
		NStr::CStr const *m_pCollationSql = &NStr::gc_Str<"">.m_Str;
		NStr::CStr const *m_pGeneratedSql = &NStr::gc_Str<"">.m_Str;
		NStr::CStr const *m_pCustomSql = &NStr::gc_Str<"">.m_Str;
	};

	struct CSqlColumnOptions
	{
		static constexpr umint mc_MaxNonPortableOptions = 8;

		ESqlColumnFlag m_Flags = ESqlColumnFlag::mc_None;
		NStr::CStr const *m_pDefaultSql = &NStr::gc_Str<"">.m_Str;
		NStr::CStr const *m_pComment = &NStr::gc_Str<"">.m_Str;
		CNonPortableColumnOptions m_NonPortableOptions[mc_MaxNonPortableOptions];
		umint m_nNonPortableOptions = 0;
	};

	struct CSqlIndexDescription
	{
		constexpr NStr::CStr const &f_Name() const;

		constexpr bool f_IsUnique() const;

		ICSqlIndex const *m_pIndex = nullptr;
		NStr::CStr const *m_pName = nullptr;
		ESqlIndexFlag m_Flags = ESqlIndexFlag::mc_None;
		NContainer::TCVector<NStr::CStr const *> m_Columns;
	};

	struct CSqlConstraintDescription
	{
		constexpr NStr::CStr const &f_Name() const;
		constexpr NStr::CStr const &f_ReferencedTable() const;
		constexpr NStr::CStr const &f_CheckSql() const;

		ICSqlConstraint const *m_pConstraint = nullptr;
		NStr::CStr const *m_pName = nullptr;
		ESqlConstraintType m_Type = ESqlConstraintType::mc_Check;
		NContainer::TCVector<NStr::CStr const *> m_Columns;
		NStr::CStr const *m_pReferencedTable = nullptr;
		NContainer::TCVector<NStr::CStr const *> m_ReferencedColumns;
		NStr::CStr const *m_pCheckSql = nullptr;
		ESqlForeignKeyAction m_OnDelete = ESqlForeignKeyAction::mc_Default;
		ESqlForeignKeyAction m_OnUpdate = ESqlForeignKeyAction::mc_Default;
	};

	struct CSqlTableDescription
	{
		constexpr NStr::CStr const &f_Name() const;

		ICSqlTable const *m_pTable = nullptr;
		NStr::CStr const *m_pName = nullptr;
		umint m_nColumns = 0;
		umint m_nIndexes = 0;
		umint m_nConstraints = 0;
		NContainer::TCVector<CSqlColumnDescription> m_Columns;
		NContainer::TCVector<CSqlIndexDescription> m_Indexes;
		NContainer::TCVector<CSqlConstraintDescription> m_Constraints;
	};

	struct CSqlDatabaseDescription
	{
		constexpr NStr::CStr const &f_Name() const;

		ICSqlDatabase const *m_pDatabase = nullptr;
		NStr::CStr const *m_pName = nullptr;
		umint m_nTables = 0;
		NContainer::TCVector<CSqlTableDescription> m_Tables;
	};

	struct CSqlSchemaVersionDescription
	{
		constexpr NStr::CStr const &f_ID() const;
		constexpr NStr::CStr const &f_DatabaseName() const;

		ICSqlSchema const *m_pSchema = nullptr;
		NStr::CStr const *m_pID = nullptr;
		NStr::CStr const *m_pDatabaseName = nullptr;
		NStr::CStr m_Checksum;
		umint m_nTables = 0;
		CSqlDatabaseDescription m_Database;
	};

	struct CSqlSchemaMigrationOperationDescription
	{
		ESqlSchemaMigrationOperationType m_Type = ESqlSchemaMigrationOperationType::mc_RenameTable;
		NStr::CStr const *m_pTableName = nullptr;
		NStr::CStr const *m_pOldName = nullptr;
		NStr::CStr const *m_pNewName = nullptr;
		NStr::CStr const *m_pSql = nullptr;
	};

	struct CSqlSchemaMigrationDescription
	{
		NStr::CStr const *m_pFromVersionID = nullptr;
		NStr::CStr const *m_pToVersionID = nullptr;
		NContainer::TCVector<CSqlSchemaMigrationOperationDescription> m_Operations;
	};

	struct CSqlSchemaMigrationPlanStep
	{
		ESqlSchemaMigrationPlanStepType m_Type = ESqlSchemaMigrationPlanStepType::mc_AlreadyCurrent;
		NStr::CStr m_FromVersionID;
		NStr::CStr m_ToVersionID;
		umint m_nOperations = 0;
	};

	struct CSqlSchemaMigrationPlan
	{
		NContainer::TCVector<CSqlSchemaMigrationPlanStep> m_Steps;
		NContainer::TCVector<NStr::CStr> m_Statements;
		NContainer::TCVector<NStr::CStr> m_Warnings;
	};

	struct ICSqlColumn
	{
		virtual CSqlColumnDescription f_Describe() const = 0;
	};

	struct ICSqlIndex
	{
		virtual CSqlIndexDescription f_Describe() const = 0;
	};

	struct ICSqlConstraint
	{
		virtual CSqlConstraintDescription f_Describe() const = 0;
	};

	struct ICSqlTable
	{
		virtual CSqlTableDescription f_Describe() const = 0;
	};

	struct ICSqlDatabase
	{
		virtual CSqlDatabaseDescription f_Describe() const = 0;
	};

	struct ICSqlSchema
	{
		virtual CSqlSchemaVersionDescription f_Describe() const = 0;
	};

	struct ICSqlSchemaVersions
	{
		virtual CSqlSchemaVersionDescription f_Describe() const = 0;
		virtual NContainer::TCVector<CSqlSchemaVersionDescription> f_DescribeVersions() const = 0;
		virtual NContainer::TCVector<CSqlSchemaMigrationDescription> f_DescribeMigrations() const = 0;
	};

	struct CSqlSchemaRenameTable
	{
		constexpr CSqlSchemaRenameTable() = default;
		constexpr CSqlSchemaRenameTable(NStr::CStr const *_pOldName, NStr::CStr const *_pNewName);

		CSqlSchemaMigrationOperationDescription f_Describe() const;

		NStr::CStr const *m_pOldName = nullptr;
		NStr::CStr const *m_pNewName = nullptr;
	};

	struct TCSqlSchemaRenameColumn
	{
		constexpr TCSqlSchemaRenameColumn() = default;
		constexpr TCSqlSchemaRenameColumn(NStr::CStr const *_pTableName, NStr::CStr const *_pOldName, NStr::CStr const *_pNewName);

		CSqlSchemaMigrationOperationDescription f_Describe() const;

		NStr::CStr const *m_pTableName = nullptr;
		NStr::CStr const *m_pOldName = nullptr;
		NStr::CStr const *m_pNewName = nullptr;
	};

	struct CSqlSchemaRebuildTable
	{
		constexpr CSqlSchemaRebuildTable() = default;
		constexpr CSqlSchemaRebuildTable(NStr::CStr const *_pTableName);

		CSqlSchemaMigrationOperationDescription f_Describe() const;

		NStr::CStr const *m_pTableName = nullptr;
	};

	struct CSqlSchemaDropTable
	{
		constexpr CSqlSchemaDropTable() = default;
		constexpr CSqlSchemaDropTable(NStr::CStr const *_pTableName);

		CSqlSchemaMigrationOperationDescription f_Describe() const;

		NStr::CStr const *m_pTableName = nullptr;
	};

	struct CSqlSchemaDropColumn
	{
		constexpr CSqlSchemaDropColumn() = default;
		constexpr CSqlSchemaDropColumn(NStr::CStr const *_pTableName, NStr::CStr const *_pColumnName);

		CSqlSchemaMigrationOperationDescription f_Describe() const;

		NStr::CStr const *m_pTableName = nullptr;
		NStr::CStr const *m_pColumnName = nullptr;
	};

	struct CSqlSchemaUpdateColumnSql
	{
		constexpr CSqlSchemaUpdateColumnSql() = default;
		constexpr CSqlSchemaUpdateColumnSql(NStr::CStr const *_pTableName, NStr::CStr const *_pColumnName, NStr::CStr const *_pSql);

		CSqlSchemaMigrationOperationDescription f_Describe() const;

		NStr::CStr const *m_pTableName = nullptr;
		NStr::CStr const *m_pColumnName = nullptr;
		NStr::CStr const *m_pSql = nullptr;
	};

	template <typename t_CSchemaVersion, typename ...tp_COperations>
	struct TCSqlSchemaMigration
	{
		using CSchemaVersion = t_CSchemaVersion;
		using COperations = NStorage::TCTuple<tp_COperations...>;

		static constexpr umint mc_nOperations = sizeof...(tp_COperations);

		constexpr TCSqlSchemaMigration() = default;
		constexpr TCSqlSchemaMigration(CSchemaVersion _SchemaVersion, COperations _Operations);

		CSqlSchemaVersionDescription f_DescribeVersion() const;
		CSqlSchemaMigrationDescription f_DescribeMigration(NStr::CStr const *_pFromVersionID) const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachOperation(tf_FFunctor &&_fFunctor) const;

		CSchemaVersion m_SchemaVersion;
		COperations m_Operations;
	};

	template <typename ...tp_CMigrations>
	struct TCSqlSchemaMigrations
	{
		using CMigrations = NStorage::TCTuple<tp_CMigrations...>;

		static constexpr umint mc_nMigrations = sizeof...(tp_CMigrations);

		constexpr TCSqlSchemaMigrations() = default;
		constexpr TCSqlSchemaMigrations(CMigrations _Migrations);

		NContainer::TCVector<CSqlSchemaVersionDescription> f_DescribeVersions() const;
		NContainer::TCVector<CSqlSchemaMigrationDescription> f_DescribeMigrations() const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachMigration(tf_FFunctor &&_fFunctor) const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachVersion(tf_FFunctor &&_fFunctor) const;

		CMigrations m_Migrations;
	};

	template <typename t_CType>
	struct TCSqlTypeTraits;

	template <typename t_CType, ESqlColumnType t_ColumnType, ESqlValueType t_ValueType>
	struct TCSqlPrimitiveTypeTraits
	{
		using CSqlType = t_CType;

		static constexpr bool mc_bSupported = true;
		static constexpr ESqlColumnType mc_ColumnType = t_ColumnType;
		static constexpr ESqlValueType mc_ValueType = t_ValueType;

		static CSqlType const &fs_ToSqlStorage(CSqlType const &_Value);
		static CSqlType &&fs_ToSqlStorage(CSqlType &&_Value);
		static CSqlType const &fs_FromSqlStorage(CSqlType const &_Value);
		static CSqlType &&fs_FromSqlStorage(CSqlType &&_Value);
	};

	template <typename t_CType, typename t_CSqlType>
	struct TCSqlMappedTypeTraits
	{
		using CSqlType = t_CSqlType;

		static constexpr bool mc_bSupported = TCSqlTypeTraits<CSqlType>::mc_bSupported;
		static constexpr ESqlColumnType mc_ColumnType = TCSqlTypeTraits<CSqlType>::mc_ColumnType;
		static constexpr ESqlValueType mc_ValueType = TCSqlTypeTraits<CSqlType>::mc_ValueType;

		static CSqlType fs_ToSqlStorage(t_CType const &_Value);
		static CSqlType fs_ToSqlStorage(t_CType &&_Value);
		static t_CType fs_FromSqlStorage(CSqlType const &_Value);
		static t_CType fs_FromSqlStorage(CSqlType &&_Value);
	};

	namespace NPrivate
	{
		template <typename t_CType, bool t_bEnum = NTraits::cIsEnum<t_CType>>
		struct TCSqlTypeTraits_Default
		{
			using CSqlType = void;

			static constexpr bool mc_bSupported = false;
			static constexpr ESqlColumnType mc_ColumnType = ESqlColumnType::mc_Invalid;
			static constexpr ESqlValueType mc_ValueType = ESqlValueType::mc_Null;
		};

		template <typename t_CType>
		struct TCSqlTypeTraits_Default<t_CType, true> : public TCSqlMappedTypeTraits<t_CType, NTraits::TCEnumUnderlyingType<t_CType>>
		{
		};
	}

	template <typename t_CType>
	struct TCSqlTypeTraits : public NPrivate::TCSqlTypeTraits_Default<t_CType>
	{
	};

	template <typename t_CType>
	struct TCSqlTypeTraits<NStorage::TCOptional<t_CType>> : public TCSqlTypeTraits<t_CType>
	{
	};

	template <typename t_CType>
	struct TCSqlColumnType
	{
		static constexpr ESqlColumnType mc_Type = TCSqlTypeTraits<t_CType>::mc_ColumnType;
	};

	// Content-based hashing for prepared-statement QueryIDs.
	//
	// A prepared-statement QueryID must be a constexpr, build-stable value that uniquely identifies a distinct
	// SQL statement. It must not depend on non-portable compiler internals (__PRETTY_FUNCTION__) or on runtime
	// pointers, both of which break across builds/compilers. Table and column names are not part of the
	// descriptor TYPE (they are runtime CStr members), so identically shaped tables would otherwise produce the
	// same QueryID and collide. We therefore fold the name CONTENT into the hash at consteval time, reading the
	// characters through the TCStrConst::m_pStr data captured by the schema factory functions (fg_SqlColumn,
	// fg_SqlTable). The mixers below are the shared FNV-1a primitives used by both the schema descriptors and
	// the QueryID functions.
	constexpr uint64 gc_SqlHashSeed = 14695981039346656037ull;
	constexpr uint64 gc_SqlHashPrime = 1099511628211ull;

	constexpr uint64 fg_SqlHashMixByte(uint64 _Hash, uint8 _Byte)
	{
		_Hash ^= _Byte;
		_Hash *= gc_SqlHashPrime;

		return _Hash;
	}

	constexpr uint64 fg_SqlHashMixValue(uint64 _Hash, uint64 _Value)
	{
		for (umint i = 0; i < sizeof(_Value); ++i)
			_Hash = fg_SqlHashMixByte(_Hash, uint8(_Value >> (i * 8)));

		return _Hash;
	}

	constexpr uint64 fg_SqlHashMixString(uint64 _Hash, NStr::TCStrConst<NStr::CStr> _Name)
	{
		umint nLength = _Name.m_Str.f_GetLen();
		for (umint i = 0; i < nLength; ++i)
			_Hash = fg_SqlHashMixByte(_Hash, uint8(_Name.m_pStr[i]));

		// Fold the length so that differently split concatenations of the same characters cannot alias.
		return fg_SqlHashMixValue(_Hash, nLength);
	}

	template <typename t_CRow, typename t_CMember>
	struct TCSqlColumn : public ICSqlColumn
	{
		using CRow = t_CRow;
		using CMember = t_CMember;
		using CStoredMember = NStorage::TCOptionalType<CMember>;

		static_assert(TCSqlTypeTraits<CStoredMember>::mc_bSupported, "Unsupported SQL column member type");
		static constexpr ESqlColumnType mc_Type = TCSqlColumnType<CMember>::mc_Type;

		constexpr TCSqlColumn() = default;
		constexpr TCSqlColumn(NStr::CStr const *_pName, t_CMember t_CRow::*_pMember, ESqlColumnType _Type, CSqlColumnOptions _Options);

		constexpr NStr::CStr const &f_Name() const;
		constexpr NStr::CStr const &f_DefaultSql() const;

		constexpr bool f_IsNullable() const;

		CSqlColumnDescription f_Describe() const override;

		constexpr CMember &f_Value(t_CRow &_Row) const;
		constexpr CMember const &f_Value(t_CRow const &_Row) const;

		NStr::CStr const *m_pName = nullptr;
		t_CMember t_CRow::*m_pMember = nullptr;
		ESqlColumnType m_Type = TCSqlColumnType<CMember>::mc_Type;
		ESqlColumnFlag m_Flags = ESqlColumnFlag::mc_None;
		NStr::CStr const *m_pDefaultSql = &NStr::gc_Str<"">.m_Str;
		CSqlColumnOptions m_Options;
		// Content hash of the column name, computed at consteval time in fg_SqlColumn (the name characters are
		// only constexpr-readable there, via TCStrConst::m_pStr). Used to build build-stable prepared-statement
		// QueryIDs without depending on the descriptor type.
		uint64 m_NameHash = 0;
	};

	template <typename ...tp_CColumns>
	struct TCSqlColumns
	{
		using CColumns = NStorage::TCTuple<tp_CColumns...>;

		static constexpr umint mc_nColumns = sizeof...(tp_CColumns);

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumn(tf_FFunctor &&_fFunctor) const;

		CColumns m_Columns;
	};

	template <umint t_nColumns>
	struct TCSqlIndex : public ICSqlIndex
	{
		static constexpr umint mc_nColumns = t_nColumns;

		constexpr TCSqlIndex() = default;
		constexpr TCSqlIndex(NStr::CStr const *_pName, ESqlIndexFlag _Flags, NStr::CStr const *const (&_ColumnNames)[t_nColumns]);

		constexpr NStr::CStr const &f_Name() const;

		constexpr bool f_IsUnique() const;

		CSqlIndexDescription f_Describe() const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumnName(tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_pName = nullptr;
		ESqlIndexFlag m_Flags = ESqlIndexFlag::mc_None;
		NStr::CStr const *m_ColumnNames[t_nColumns] = {};
	};

	template <auto ...tp_pMembers>
	struct TCSqlMemberIndex
	{
		static constexpr umint mc_nColumns = sizeof...(tp_pMembers);

		constexpr TCSqlMemberIndex() = default;
		constexpr TCSqlMemberIndex(NStr::CStr const *_pName, ESqlIndexFlag _Flags);

		constexpr NStr::CStr const &f_Name() const;
		constexpr bool f_IsUnique() const;

		template <typename tf_CColumns>
		CSqlIndexDescription f_Describe(tf_CColumns const &_Columns) const;
		template <typename tf_CColumns, typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumnName(tf_CColumns const &_Columns, tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_pName = nullptr;
		ESqlIndexFlag m_Flags = ESqlIndexFlag::mc_None;
	};

	template <typename ...tp_CIndexes>
	struct TCSqlIndexes
	{
		using CIndexes = NStorage::TCTuple<tp_CIndexes...>;

		static constexpr umint mc_nIndexes = sizeof...(tp_CIndexes);

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachIndex(tf_FFunctor &&_fFunctor) const;

		CIndexes m_Indexes;
	};

	template <umint t_nColumns>
	struct TCSqlColumnNames
	{
		static constexpr umint mc_nColumns = t_nColumns;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumnName(tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_ColumnNames[t_nColumns] = {};
	};

	template <umint t_nColumns>
	struct TCSqlForeignKeyReference
	{
		static constexpr umint mc_nColumns = t_nColumns;

		constexpr NStr::CStr const &f_TableName() const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumnName(tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_pTableName = nullptr;
		NStr::CStr const *m_ColumnNames[t_nColumns] = {};
	};

	template <ESqlConstraintType t_Type, umint t_nColumns>
	struct TCSqlColumnConstraint : public ICSqlConstraint
	{
		static constexpr ESqlConstraintType mc_Type = t_Type;
		static constexpr umint mc_nColumns = t_nColumns;

		constexpr TCSqlColumnConstraint() = default;
		constexpr TCSqlColumnConstraint(NStr::CStr const *_pName, NStr::CStr const *const (&_ColumnNames)[t_nColumns]);

		constexpr NStr::CStr const &f_Name() const;

		CSqlConstraintDescription f_Describe() const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumnName(tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_pName = nullptr;
		NStr::CStr const *m_ColumnNames[t_nColumns] = {};
	};

	template <ESqlConstraintType t_Type, auto ...tp_pMembers>
	struct TCSqlMemberColumnConstraint
	{
		static constexpr ESqlConstraintType mc_Type = t_Type;
		static constexpr umint mc_nColumns = sizeof...(tp_pMembers);

		constexpr TCSqlMemberColumnConstraint() = default;
		constexpr TCSqlMemberColumnConstraint(NStr::CStr const *_pName);

		constexpr NStr::CStr const &f_Name() const;

		template <typename tf_CColumns>
		CSqlConstraintDescription f_Describe(tf_CColumns const &_Columns) const;
		template <typename tf_CColumns, typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumnName(tf_CColumns const &_Columns, tf_FFunctor &&_fFunctor) const;
		template <typename tf_CMember>
		constexpr bool f_MemberIsInConstraint(tf_CMember _pMember) const;

		NStr::CStr const *m_pName = nullptr;
	};

	struct CSqlCheckConstraint : public ICSqlConstraint
	{
		static constexpr ESqlConstraintType mc_Type = ESqlConstraintType::mc_Check;
		static constexpr umint mc_nColumns = 0;

		constexpr CSqlCheckConstraint() = default;
		constexpr CSqlCheckConstraint(NStr::CStr const *_pName, NStr::CStr const *_pCheckSql);

		constexpr NStr::CStr const &f_Name() const;
		constexpr NStr::CStr const &f_CheckSql() const;

		CSqlConstraintDescription f_Describe() const;

		NStr::CStr const *m_pName = nullptr;
		NStr::CStr const *m_pCheckSql = nullptr;
	};

	template <umint t_nColumns, umint t_nReferencedColumns>
	struct TCSqlForeignKeyConstraint : public ICSqlConstraint
	{
		static constexpr ESqlConstraintType mc_Type = ESqlConstraintType::mc_ForeignKey;
		static constexpr umint mc_nColumns = t_nColumns;
		static constexpr umint mc_nReferencedColumns = t_nReferencedColumns;

		constexpr TCSqlForeignKeyConstraint() = default;
		constexpr TCSqlForeignKeyConstraint
			(
				NStr::CStr const *_pName
				, TCSqlColumnNames<t_nColumns> _Columns
				, TCSqlForeignKeyReference<t_nReferencedColumns> _Reference
				, ESqlForeignKeyAction _OnDelete
				, ESqlForeignKeyAction _OnUpdate
			)
		;

		constexpr NStr::CStr const &f_Name() const;
		constexpr NStr::CStr const &f_ReferencedTable() const;

		CSqlConstraintDescription f_Describe() const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumnName(tf_FFunctor &&_fFunctor) const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachReferencedColumnName(tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_pName = nullptr;
		TCSqlColumnNames<t_nColumns> m_Columns;
		TCSqlForeignKeyReference<t_nReferencedColumns> m_Reference;
		ESqlForeignKeyAction m_OnDelete = ESqlForeignKeyAction::mc_Default;
		ESqlForeignKeyAction m_OnUpdate = ESqlForeignKeyAction::mc_Default;
	};

	template <umint t_nReferencedColumns, auto ...tp_pMembers>
	struct TCSqlMemberForeignKeyConstraint
	{
		static constexpr ESqlConstraintType mc_Type = ESqlConstraintType::mc_ForeignKey;
		static constexpr umint mc_nColumns = sizeof...(tp_pMembers);
		static constexpr umint mc_nReferencedColumns = t_nReferencedColumns;

		constexpr TCSqlMemberForeignKeyConstraint() = default;
		constexpr TCSqlMemberForeignKeyConstraint
			(
				NStr::CStr const *_pName
				, TCSqlForeignKeyReference<t_nReferencedColumns> _Reference
				, ESqlForeignKeyAction _OnDelete
				, ESqlForeignKeyAction _OnUpdate
			)
		;

		constexpr NStr::CStr const &f_Name() const;
		constexpr NStr::CStr const &f_ReferencedTable() const;

		template <typename tf_CColumns>
		CSqlConstraintDescription f_Describe(tf_CColumns const &_Columns) const;
		template <typename tf_CColumns, typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumnName(tf_CColumns const &_Columns, tf_FFunctor &&_fFunctor) const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachReferencedColumnName(tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_pName = nullptr;
		TCSqlForeignKeyReference<t_nReferencedColumns> m_Reference;
		ESqlForeignKeyAction m_OnDelete = ESqlForeignKeyAction::mc_Default;
		ESqlForeignKeyAction m_OnUpdate = ESqlForeignKeyAction::mc_Default;
	};

	template <typename ...tp_CConstraints>
	struct TCSqlConstraints
	{
		using CConstraints = NStorage::TCTuple<tp_CConstraints...>;

		static constexpr umint mc_nConstraints = sizeof...(tp_CConstraints);

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachConstraint(tf_FFunctor &&_fFunctor) const;

		CConstraints m_Constraints;
	};

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	struct TCSqlTable : public ICSqlTable
	{
		using CRow = t_CRow;
		using CColumns = t_CColumns;
		using CIndexes = t_CIndexes;
		using CConstraints = t_CConstraints;

		static constexpr umint mc_nColumns = CColumns::mc_nColumns;
		static constexpr umint mc_nIndexes = CIndexes::mc_nIndexes;
		static constexpr umint mc_nConstraints = CConstraints::mc_nConstraints;

		constexpr TCSqlTable() = default;
		constexpr TCSqlTable(NStr::CStr const *_pName, CColumns _Columns, CIndexes _Indexes, CConstraints _Constraints);

		constexpr NStr::CStr const &f_Name() const;
		CSqlTableDescription f_Describe() const override;

		NContainer::TCVector<CSqlColumnDescription> f_DescribeColumns() const;
		NContainer::TCVector<CSqlIndexDescription> f_DescribeIndexes() const;
		NContainer::TCVector<CSqlConstraintDescription> f_DescribeConstraints() const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachColumn(tf_FFunctor &&_fFunctor) const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachIndex(tf_FFunctor &&_fFunctor) const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachConstraint(tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_pName = nullptr;
		CColumns m_Columns;
		CIndexes m_Indexes;
		CConstraints m_Constraints;
		// Content hash of the table name, computed at consteval time in fg_SqlTable. See TCSqlColumn::m_NameHash.
		uint64 m_NameHash = 0;
	};

	template <typename t_CTable, auto t_pIDMember, auto t_pVersionMember, auto ...tp_pSaveMembers>
	struct TCSqlRepository
	{
		using CTable = t_CTable;
		using CRow = typename CTable::CRow;

		static constexpr auto mc_pIDMember = t_pIDMember;
		static constexpr auto mc_pVersionMember = t_pVersionMember;

		constexpr TCSqlRepository(CTable const &_Table);

		CTable const &m_Table;
	};

	constexpr ESqlDialect fg_SqlColumnTypeSupportedDialects(ESqlColumnType _Type);
	constexpr bool fg_SqlDialectsSupportColumnType(ESqlDialect _Dialects, ESqlColumnType _Type);
	constexpr ESqlDialect fg_SqlValueTypeSupportedDialects(ESqlValueType _Type);
	constexpr bool fg_SqlDialectsSupportValueType(ESqlDialect _Dialects, ESqlValueType _Type);
	constexpr bool fg_SqlColumnTypeIsInteger(ESqlColumnType _Type);

	template <ESqlDialect t_Dialects, typename t_CType>
	concept cSqlDialectSupportsType = fg_SqlDialectsSupportValueType(t_Dialects, TCSqlTypeTraits<t_CType>::mc_ValueType);
	CSqlSchemaMigrationPlan fg_SqlPlanSchemaMigration(ICSqlSchemaVersions const &_SchemaVersions, NStr::CStr const *_pCurrentVersionID = nullptr);

	template <typename tf_CColumn>
	constexpr ch8 const *fg_SqlValidateColumn(tf_CColumn const &_Column);
	template <typename tf_CColumns>
	constexpr bool fg_SqlColumnsContainName(tf_CColumns const &_Columns, NStr::CStr const *_pName);
	template <typename tf_CColumns>
	constexpr ch8 const *fg_SqlValidateColumns(tf_CColumns const &_Columns);
	template <typename tf_CColumns, typename tf_CIndexes>
	constexpr ch8 const *fg_SqlValidateIndexes(tf_CColumns const &_Columns, tf_CIndexes const &_Indexes);
	template <typename tf_CColumns, typename tf_CConstraints>
	constexpr ch8 const *fg_SqlValidateLocalConstraints(tf_CColumns const &_Columns, tf_CConstraints const &_Constraints);
	template <typename tf_CTable>
	constexpr ch8 const *fg_SqlValidateTable(tf_CTable const &_Table);
	template <typename tf_CDatabase>
	constexpr ch8 const *fg_SqlValidateReferencedColumn(tf_CDatabase const &_Database, NStr::CStr const *_pTableName, NStr::CStr const *_pColumnName);
	template <typename tf_CDatabase>
	constexpr ch8 const *fg_SqlValidateDatabase(tf_CDatabase const &_Database);

	template <NStr::TCStrConstData t_Prefix, NStr::TCStrConstData ...tp_Parts>
	consteval auto fg_SqlGeneratedNameData();

	template <NStr::TCStrConstData t_TableName, NStr::TCStrConstData ...tp_ColumnNames>
	constexpr auto gc_SqlIndexName = NStr::gc_Str<fg_SqlGeneratedNameData<"idx", t_TableName, tp_ColumnNames...>()>;
	template <NStr::TCStrConstData t_TableName, NStr::TCStrConstData ...tp_ColumnNames>
	constexpr auto gc_SqlUniqueName = NStr::gc_Str<fg_SqlGeneratedNameData<t_TableName, tp_ColumnNames..., "unique">()>;
	template <NStr::TCStrConstData t_TableName, NStr::TCStrConstData ...tp_ColumnNames>
	constexpr auto gc_SqlPrimaryKeyName = NStr::gc_Str<fg_SqlGeneratedNameData<t_TableName, tp_ColumnNames..., "pk">()>;
	template <NStr::TCStrConstData t_TableName, NStr::TCStrConstData t_ReferencedTableName, NStr::TCStrConstData ...tp_ColumnNames>
	constexpr auto gc_SqlForeignKeyName = NStr::gc_Str<fg_SqlGeneratedNameData<t_TableName, tp_ColumnNames..., t_ReferencedTableName, "fk">()>;

	consteval CSqlColumnOptions fg_SqlColumnOptions
		(
			ESqlColumnFlag _Flags = ESqlColumnFlag::mc_None
			, NStr::TCStrConst<NStr::CStr> _Comment = NStr::gc_Str<"">
		)
	;
	template <umint tf_nNonPortableOptions>
	consteval CSqlColumnOptions fg_SqlColumnOptions
		(
			ESqlColumnFlag _Flags
			, NStr::TCStrConst<NStr::CStr> _Comment
			, CNonPortableColumnOptions const (&_NonPortableOptions)[tf_nNonPortableOptions]
		)
	;

	template <ESqlDialect t_Dialects, typename t_CColumn>
	constexpr bool fg_SqlColumnMatchesDialect();

	template <ESqlDialect t_Dialects, typename t_CColumns>
	struct TCSqlColumnsMatchDialect;

	template <ESqlDialect t_Dialects, typename t_CTable>
	constexpr bool fg_SqlTableMatchesDialect();

	template <ESqlDialect t_Dialects, typename ...tp_CTables>
	struct TCSqlDatabase : public ICSqlDatabase
	{
		using CTables = NStorage::TCTuple<tp_CTables...>;

		static constexpr ESqlDialect mc_Dialects = t_Dialects;
		static constexpr umint mc_nTables = sizeof...(tp_CTables);
		static_assert((fg_SqlTableMatchesDialect<t_Dialects, tp_CTables>() && ...), "SQL database schema uses column types not supported by the selected dialects");

		constexpr TCSqlDatabase() = default;
		constexpr TCSqlDatabase(NStr::CStr const *_pName, CTables _Tables);

		constexpr NStr::CStr const &f_Name() const;

		CSqlDatabaseDescription f_Describe() const override;
		NContainer::TCVector<CSqlTableDescription> f_DescribeTables() const;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachTable(tf_FFunctor &&_fFunctor) const;

		NStr::CStr const *m_pName = nullptr;
		CTables m_Tables;
	};

	template <typename t_CDatabase>
	struct TCSqlSchemaVersion : public ICSqlSchema
	{
		using CDatabase = t_CDatabase;

		constexpr TCSqlSchemaVersion() = default;
		constexpr TCSqlSchemaVersion(NStr::CStr const *_pID, CDatabase _Database);

		constexpr NStr::CStr const &f_ID() const;

		constexpr CDatabase const &f_Database() const;

		CSqlSchemaVersionDescription f_Describe() const override;

		NStr::CStr const *m_pID = nullptr;
		CDatabase m_Database;
	};

	template <typename t_CSchemaMigrations>
	struct TCSqlSchemaVersions : public ICSqlSchemaVersions
	{
		using CSchemaMigrations = t_CSchemaMigrations;

		static constexpr umint mc_nVersions = CSchemaMigrations::mc_nMigrations;
		static_assert(mc_nVersions > 0);

		constexpr TCSqlSchemaVersions() = default;
		constexpr TCSqlSchemaVersions(CSchemaMigrations _Migrations);

		CSqlSchemaVersionDescription f_Describe() const override;
		NContainer::TCVector<CSqlSchemaVersionDescription> f_DescribeVersions() const override;
		NContainer::TCVector<CSqlSchemaMigrationDescription> f_DescribeMigrations() const override;

		template <typename tf_FFunctor>
		constexpr decltype(auto) f_ForEachVersion(tf_FFunctor &&_fFunctor) const;

		CSchemaMigrations m_Migrations;
	};

	consteval auto fg_SqlRenameTable(NStr::TCStrConst<NStr::CStr> _OldName, NStr::TCStrConst<NStr::CStr> _NewName);
	consteval auto fg_SqlRenameColumn(NStr::TCStrConst<NStr::CStr> _TableName, NStr::TCStrConst<NStr::CStr> _OldName, NStr::TCStrConst<NStr::CStr> _NewName);
	consteval auto fg_SqlRebuildTable(NStr::TCStrConst<NStr::CStr> _TableName);
	consteval auto fg_SqlDropTable(NStr::TCStrConst<NStr::CStr> _TableName);
	consteval auto fg_SqlDropColumn(NStr::TCStrConst<NStr::CStr> _TableName, NStr::TCStrConst<NStr::CStr> _ColumnName);
	consteval auto fg_SqlUpdateColumnSql(NStr::TCStrConst<NStr::CStr> _TableName, NStr::TCStrConst<NStr::CStr> _ColumnName, NStr::TCStrConst<NStr::CStr> _Sql);

	template <typename tf_CSchemaVersion, typename ...tfp_COperations>
	consteval auto fg_SqlSchemaMigration(tf_CSchemaVersion &&_SchemaVersion, tfp_COperations &&...p_Operations);

	template <typename ...tfp_CMigrations>
	consteval auto fg_SqlSchemaMigrations(tfp_CMigrations &&...p_Migrations);

	template <typename tf_CRow, typename tf_CMember>
	consteval auto fg_SqlColumn
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, tf_CMember tf_CRow::*_pMember
			, ESqlColumnFlag _Flags = ESqlColumnFlag::mc_None
			, NStr::TCStrConst<NStr::CStr> _DefaultSql = NStr::gc_Str<"">
		)
		requires (TCSqlTypeTraits<NStorage::TCOptionalType<tf_CMember>>::mc_bSupported)
	;
	template <typename tf_CRow, typename tf_CMember>
	consteval auto fg_SqlColumn
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, tf_CMember tf_CRow::*_pMember
			, CSqlColumnOptions _Options
		)
		requires (TCSqlTypeTraits<NStorage::TCOptionalType<tf_CMember>>::mc_bSupported)
	;

	template <typename tf_CRow, typename tf_CMember>
	consteval auto fg_SqlColumn
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, tf_CMember tf_CRow::*_pMember
			, ESqlColumnType _Type
			, ESqlColumnFlag _Flags = ESqlColumnFlag::mc_None
			, NStr::TCStrConst<NStr::CStr> _DefaultSql = NStr::gc_Str<"">
		)
		requires (TCSqlTypeTraits<NStorage::TCOptionalType<tf_CMember>>::mc_bSupported)
	;
	template <typename tf_CRow, typename tf_CMember>
	consteval auto fg_SqlColumn
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, tf_CMember tf_CRow::*_pMember
			, ESqlColumnType _Type
			, CSqlColumnOptions _Options
		)
		requires (TCSqlTypeTraits<NStorage::TCOptionalType<tf_CMember>>::mc_bSupported)
	;

	template <typename ...tfp_CColumns>
	consteval auto fg_SqlColumns(tfp_CColumns &&...p_Columns);

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlIndex
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, ESqlIndexFlag _Flags
			, tfp_CColumnNames &&...p_ColumnNames
		)
	;

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlIndex(NStr::TCStrConst<NStr::CStr> _Name, tfp_CColumnNames &&...p_ColumnNames);
	template <auto ...tfp_pMembers>
	consteval auto fg_SqlIndex(NStr::TCStrConst<NStr::CStr> _Name, ESqlIndexFlag _Flags)
		requires (sizeof...(tfp_pMembers) > 0)
	;
	template <auto ...tfp_pMembers>
	consteval auto fg_SqlIndex(NStr::TCStrConst<NStr::CStr> _Name)
		requires (sizeof...(tfp_pMembers) > 0)
	;

	template <typename ...tfp_CIndexes>
	consteval auto fg_SqlIndexes(tfp_CIndexes &&...p_Indexes);

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlColumnNames(tfp_CColumnNames &&...p_ColumnNames);
	template <auto ...tfp_pMembers, typename tf_CTable>
	consteval auto fg_SqlColumnNames(tf_CTable const &_Table)
		requires (sizeof...(tfp_pMembers) > 0)
	;

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlPrimaryKey(NStr::TCStrConst<NStr::CStr> _Name, tfp_CColumnNames &&...p_ColumnNames);
	template <auto ...tfp_pMembers>
	consteval auto fg_SqlPrimaryKey(NStr::TCStrConst<NStr::CStr> _Name)
		requires (sizeof...(tfp_pMembers) > 0)
	;

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlUnique(NStr::TCStrConst<NStr::CStr> _Name, tfp_CColumnNames &&...p_ColumnNames);
	template <auto ...tfp_pMembers>
	consteval auto fg_SqlUnique(NStr::TCStrConst<NStr::CStr> _Name)
		requires (sizeof...(tfp_pMembers) > 0)
	;

	consteval auto fg_SqlCheck(NStr::TCStrConst<NStr::CStr> _Name, NStr::TCStrConst<NStr::CStr> _CheckSql);

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlReferences(NStr::TCStrConst<NStr::CStr> _TableName, tfp_CColumnNames &&...p_ColumnNames);
	template <auto ...tfp_pMembers, typename tf_CTable>
	consteval auto fg_SqlReferences(tf_CTable const &_Table)
		requires (sizeof...(tfp_pMembers) > 0)
	;

	template <umint tf_nColumns, umint tf_nReferencedColumns>
	consteval auto fg_SqlForeignKey
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, TCSqlColumnNames<tf_nColumns> _Columns
			, TCSqlForeignKeyReference<tf_nReferencedColumns> _Reference
			, ESqlForeignKeyAction _OnDelete = ESqlForeignKeyAction::mc_Default
			, ESqlForeignKeyAction _OnUpdate = ESqlForeignKeyAction::mc_Default
		)
	;
	template <auto ...tfp_pMembers, umint tf_nReferencedColumns>
	consteval auto fg_SqlForeignKey
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, TCSqlForeignKeyReference<tf_nReferencedColumns> _Reference
			, ESqlForeignKeyAction _OnDelete = ESqlForeignKeyAction::mc_Default
			, ESqlForeignKeyAction _OnUpdate = ESqlForeignKeyAction::mc_Default
		)
		requires (sizeof...(tfp_pMembers) > 0)
	;

	template <typename ...tfp_CConstraints>
	consteval auto fg_SqlConstraints(tfp_CConstraints &&...p_Constraints);

	template <typename tf_CRow, typename ...tfp_CColumns>
	consteval auto fg_SqlTable(NStr::TCStrConst<NStr::CStr> _Name, TCSqlColumns<tfp_CColumns...> _Columns);

	template <typename tf_CRow, typename ...tfp_CColumns, typename ...tfp_CIndexes>
	consteval auto fg_SqlTable(NStr::TCStrConst<NStr::CStr> _Name, TCSqlColumns<tfp_CColumns...> _Columns, TCSqlIndexes<tfp_CIndexes...> _Indexes);

	template <typename tf_CRow, typename ...tfp_CColumns, typename ...tfp_CIndexes, typename ...tfp_CConstraints>
	consteval auto fg_SqlTable
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, TCSqlColumns<tfp_CColumns...> _Columns
			, TCSqlIndexes<tfp_CIndexes...> _Indexes
			, TCSqlConstraints<tfp_CConstraints...> _Constraints
		)
	;

	template <auto tf_pIDMember, auto tf_pVersionMember, auto ...tfp_pSaveMembers, typename tf_CTable>
	consteval auto fg_SqlRepository(tf_CTable const &_Table);

	template <typename ...tfp_CTables>
	consteval auto fg_SqlDatabase(NStr::TCStrConst<NStr::CStr> _Name, tfp_CTables &&...p_Tables);

	template <typename tf_CDatabase>
	consteval auto fg_SqlSchemaVersion(NStr::TCStrConst<NStr::CStr> _ID, tf_CDatabase &&_Database);

	template <typename ...tfp_CMigrations>
	consteval auto fg_SqlSchemaVersions(tfp_CMigrations &&...p_Migrations);
}

#include "Malterlib_SQL_DatabaseSchema_Types.hpp"
#include "Malterlib_SQL_DatabaseSchema.hpp"

#ifndef DMibPNoShortCuts
using namespace NMib::NSQL;
#endif
