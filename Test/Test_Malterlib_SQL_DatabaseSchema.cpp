// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseSchemaMigrations.h"
#include "Test_Malterlib_SQL_DatabaseBackendShared.h"

#include "../Source/PostgresDatabase/Malterlib_SQL_PostgresDatabase_Internal.h"
#include "../Source/SQLiteDatabase/Malterlib_SQL_SQLiteDatabase_Internal.h"

#include <Mib/SQL/PostgresDatabase>
#include <Mib/SQL/SQLiteDatabase>

using namespace NMib;
using namespace NMib::NSQL;
using namespace NMib::NSQL::NTest;
using namespace NMib::NStr;
using namespace NMib::NStorage;

namespace NMib::NSQL::NTest
{
	struct CInvalidMappedWrapper
	{
		void *m_pValue = nullptr;
	};
}

namespace NMib::NSQL
{
	template <>
	struct TCSqlTypeTraits<NTest::CInvalidMappedWrapper> : public TCSqlMappedTypeTraits<NTest::CInvalidMappedWrapper, void *>
	{
	};
}

namespace
{
	NConcurrency::TCFuture<CSqlSchemaMigrationPlan> fg_PlanSqliteMigration(NStr::CStr _CurrentVersionID)
	{
		co_return co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_RebuildThenRenameSchemaVersions, &_CurrentVersionID);
	}

	NConcurrency::TCFuture<CSqlSchemaMigrationPlan> fg_PlanSqliteCaseCollision()
	{
		co_return co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_CaseCollisionSchemaVersions);
	}

	NConcurrency::TCFuture<CSqlSchemaMigrationPlan> fg_PlanPostgresLongIdentifier()
	{
		co_return co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_LongIdentifierSchemaVersions);
	}

	NConcurrency::TCFuture<CSqlSchemaMigrationPlan> fg_PlanPostgresDropCheck(NStr::CStr _CurrentVersionID)
	{
		co_return co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_DropCheckSchemaVersions, &_CurrentVersionID);
	}

	NConcurrency::TCFuture<CSqlSchemaMigrationPlan> fg_PlanPostgresMigration(NStr::CStr _CurrentVersionID)
	{
		co_return co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_RebuildThenRenameSchemaVersions, &_CurrentVersionID);
	}

	constexpr auto gc_pDatabaseValidationError = fg_SqlValidateDatabase(gc_Database);
	static_assert(gc_pDatabaseValidationError == nullptr, CConstExprSubStr(gc_pDatabaseValidationError));

	struct CUnsupportedColumnRow
	{
		void *m_pUnsupported = nullptr;
		TCOptional<void *> m_OptionalUnsupported;
	};

	struct CUUIDSchemaRow
	{
		NCryptography::CUniversallyUniqueIdentifier m_UUID;
	};

	struct CPostgresSpecificSchemaRow
	{
		CSqlDate m_Date;
		CSqlTimeOfDay m_Time;
		CSqlTimestamp m_Timestamp;
		CSqlTimestampTz m_TimestampTz;
		CSqlInterval m_Interval;
		NEncoding::CJsonOrdered m_Json;
		NEncoding::CJsonSorted m_Jsonb;
		TCSqlArray<int32> m_IntArray;
	};

	struct CSchemaValidationRow
	{
		uint64 m_ID = 0;
		NStr::CStr m_Value;
	};

	struct CNullablePrimaryKeyRow
	{
		uint64 m_ID = 0;
		NStorage::TCOptional<NStr::CStr> m_Value;
	};

	consteval ch8 const *fg_TestValidateNullableTablePrimaryKey()
	{
		constexpr auto c_Columns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &CNullablePrimaryKeyRow::m_ID), fg_SqlColumn(gc_Str<"value">, &CNullablePrimaryKeyRow::m_Value));
		constexpr auto c_Indexes = fg_SqlIndexes();
		constexpr auto c_Constraints = fg_SqlConstraints(fg_SqlPrimaryKey(gc_Str<"nullable_pk">, gc_Str<"id">, gc_Str<"value">));
		constexpr auto c_Table = fg_SqlTable<CNullablePrimaryKeyRow>(gc_Str<"nullable_primary_key">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"nullable_primary_key_database">, c_Table);

		return fg_SqlValidateDatabase(c_Database);
	}

	struct CQuotedIdentifierRow
	{
		int64 m_Value = 0;
	};

	template <typename tf_CMember>
	constexpr bool fg_TestCanDeclareSqlColumn()
	{
		return requires
			{
				fg_SqlColumn(gc_Str<"unsupported">, (tf_CMember CUnsupportedColumnRow:: *)nullptr);
			}
		;
	}

	template <typename tf_CMember>
	constexpr bool fg_TestCanDeclareSqlColumnWithExplicitType()
	{
		return requires
			{
				fg_SqlColumn(gc_Str<"unsupported">, (tf_CMember CUnsupportedColumnRow:: *)nullptr, ESqlColumnType::mc_Text);
			}
		;
	}

	template <typename tf_CRow>
	consteval ch8 const *fg_TestValidateDuplicateColumnTable()
	{
		constexpr auto c_Columns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &tf_CRow::m_ID), fg_SqlColumn(gc_Str<"id">, &tf_CRow::m_Value));
		constexpr auto c_Indexes = fg_SqlIndexes();
		constexpr auto c_Constraints = fg_SqlConstraints();
		constexpr auto c_Table = fg_SqlTable<tf_CRow>(gc_Str<"duplicate_columns">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"duplicate_column_database">, c_Table);

		return fg_SqlValidateDatabase(c_Database);
	}

	template <typename tf_CRow>
	consteval ch8 const *fg_TestValidateInvalidIndexTable()
	{
		constexpr auto c_Columns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &tf_CRow::m_ID), fg_SqlColumn(gc_Str<"value">, &tf_CRow::m_Value));
		constexpr auto c_Indexes = fg_SqlIndexes(fg_SqlIndex(gc_Str<"invalid_index_missing_column">, gc_Str<"missing_column">));
		constexpr auto c_Constraints = fg_SqlConstraints();
		constexpr auto c_Table = fg_SqlTable<tf_CRow>(gc_Str<"invalid_index">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"invalid_index_database">, c_Table);

		return fg_SqlValidateDatabase(c_Database);
	}

	template <typename tf_CRow>
	consteval ch8 const *fg_TestValidateInvalidAutoIncrementTable()
	{
		constexpr auto c_Columns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &tf_CRow::m_ID, ESqlColumnFlag::mc_AutoIncrement));
		constexpr auto c_Indexes = fg_SqlIndexes();
		constexpr auto c_Constraints = fg_SqlConstraints();
		constexpr auto c_Table = fg_SqlTable<tf_CRow>(gc_Str<"invalid_auto_increment">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"invalid_auto_increment_database">, c_Table);

		return fg_SqlValidateDatabase(c_Database);
	}

	template <typename tf_CRow>
	consteval ch8 const *fg_TestValidateInvalidGeneratedDefaultTable()
	{
		constexpr auto c_Columns = fg_SqlColumns
			(
				fg_SqlColumn
				(
					gc_Str<"value">
					, &tf_CRow::m_Value
					, fg_SqlColumnOptions
					(
						ESqlColumnFlag::mc_None
						, gc_Str<"">
						,
						{
							{
								.m_pBackendID = &gc_Str<"sqlite">.m_Str
								, .m_pDefaultSql = &gc_Str<"'value'">.m_Str
								, .m_pGeneratedSql = &gc_Str<"lower(value)">.m_Str
							}
						}
					)
				)
			)
		;
		constexpr auto c_Indexes = fg_SqlIndexes();
		constexpr auto c_Constraints = fg_SqlConstraints();
		constexpr auto c_Table = fg_SqlTable<tf_CRow>(gc_Str<"invalid_generated_default">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"invalid_generated_default_database">, c_Table);

		return fg_SqlValidateDatabase(c_Database);
	}

	template <typename tf_CRow>
	consteval ch8 const *fg_TestValidateInvalidForeignKeyDatabase()
	{
		constexpr auto c_Columns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &tf_CRow::m_ID), fg_SqlColumn(gc_Str<"value">, &tf_CRow::m_Value));
		constexpr auto c_Indexes = fg_SqlIndexes();
		constexpr auto c_Constraints = fg_SqlConstraints
			(
				fg_SqlForeignKey(gc_Str<"invalid_fk_missing_table">, fg_SqlColumnNames(gc_Str<"id">), fg_SqlReferences(gc_Str<"missing_table">, gc_Str<"id">))
			)
		;
		constexpr auto c_Table = fg_SqlTable<tf_CRow>(gc_Str<"invalid_fk">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"invalid_fk_database">, c_Table);

		return fg_SqlValidateDatabase(c_Database);
	}

	template <typename tf_CRow>
	consteval ch8 const *fg_TestValidateDuplicateTableDatabase()
	{
		constexpr auto c_Columns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &tf_CRow::m_ID), fg_SqlColumn(gc_Str<"value">, &tf_CRow::m_Value));
		constexpr auto c_Indexes = fg_SqlIndexes();
		constexpr auto c_Constraints = fg_SqlConstraints();
		constexpr auto c_Table = fg_SqlTable<tf_CRow>(gc_Str<"validation">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"duplicate_tables">, c_Table, c_Table);

		return fg_SqlValidateDatabase(c_Database);
	}

	template <typename tf_CRow>
	consteval ch8 const *fg_TestValidateDuplicateIndexDatabase()
	{
		constexpr auto c_Columns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &tf_CRow::m_ID), fg_SqlColumn(gc_Str<"value">, &tf_CRow::m_Value));
		// Index names are schema-wide, not table-local
		constexpr auto c_Indexes = fg_SqlIndexes(fg_SqlIndex(gc_Str<"shared_index">, gc_Str<"value">));
		constexpr auto c_Constraints = fg_SqlConstraints();
		constexpr auto c_TableA = fg_SqlTable<tf_CRow>(gc_Str<"duplicate_index_a">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_TableB = fg_SqlTable<tf_CRow>(gc_Str<"duplicate_index_b">, c_Columns, c_Indexes, c_Constraints);
		constexpr auto c_Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"duplicate_index_database">, c_TableA, c_TableB);

		return fg_SqlValidateDatabase(c_Database);
	}

	constexpr auto gc_PostgresSpecificTable = fg_SqlTable<CPostgresSpecificSchemaRow>
		(
			gc_Str<"postgres_specific">
			, fg_SqlColumns(fg_SqlColumn(gc_Str<"pg_date">, &CPostgresSpecificSchemaRow::m_Date), fg_SqlColumn(gc_Str<"pg_int_array">, &CPostgresSpecificSchemaRow::m_IntArray))
		)
	;
	constexpr auto gc_UserIDColumn = fg_SqlColumn(gc_Str<"id">, &CUserRow::m_ID);
	constexpr auto gc_EmailTextColumn = fg_SqlColumn(gc_Str<"email">, &CUserRow::m_Email, ESqlColumnType::mc_Text);
	constexpr auto gc_UUIDColumn = fg_SqlColumn(gc_Str<"uuid">, &CUUIDSchemaRow::m_UUID);
	constexpr auto gc_PostgresDateColumn = fg_SqlColumn(gc_Str<"pg_date">, &CPostgresSpecificSchemaRow::m_Date);
	constexpr auto gc_PostgresTimeColumn = fg_SqlColumn(gc_Str<"pg_time">, &CPostgresSpecificSchemaRow::m_Time);
	constexpr auto gc_PostgresTimestampColumn = fg_SqlColumn(gc_Str<"pg_timestamp">, &CPostgresSpecificSchemaRow::m_Timestamp);
	constexpr auto gc_PostgresTimestampTzColumn = fg_SqlColumn(gc_Str<"pg_timestamptz">, &CPostgresSpecificSchemaRow::m_TimestampTz);
	constexpr auto gc_PostgresIntervalColumn = fg_SqlColumn(gc_Str<"pg_interval">, &CPostgresSpecificSchemaRow::m_Interval);
	constexpr auto gc_PostgresJsonColumn = fg_SqlColumn(gc_Str<"pg_json">, &CPostgresSpecificSchemaRow::m_Json);
	constexpr auto gc_PostgresJsonbColumn = fg_SqlColumn(gc_Str<"pg_jsonb">, &CPostgresSpecificSchemaRow::m_Jsonb);
	constexpr auto gc_PostgresIntArrayColumn = fg_SqlColumn(gc_Str<"pg_int_array">, &CPostgresSpecificSchemaRow::m_IntArray);
	constexpr auto gc_ValidationColumns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &CSchemaValidationRow::m_ID), fg_SqlColumn(gc_Str<"value">, &CSchemaValidationRow::m_Value));
	constexpr auto gc_ColumnOptionsTable = fg_SqlTable<CSchemaValidationRow>
		(
			gc_Str<"column_options">
			, fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"id">, &CSchemaValidationRow::m_ID)
				, fg_SqlColumn
				(
					gc_Str<"value">
					, &CSchemaValidationRow::m_Value
					, fg_SqlColumnOptions
					(
						ESqlColumnFlag::mc_None
						, gc_Str<"Value display text">
						,
						{
							{
								.m_pBackendID = &gc_Str<"sqlite">.m_Str
								, .m_pDefaultSql = &gc_Str<"'unknown'">.m_Str
								, .m_pCollationSql = &gc_Str<"NOCASE">.m_Str
								, .m_pCustomSql = &gc_Str<"CHECK (length(value) > 0)">.m_Str
							}
						}
					)
				)
			)
		)
	;
	constexpr auto gc_QuotedIdentifierTable = fg_SqlTable<CQuotedIdentifierRow>
		(
			gc_Str<"quoted\"table">
			, fg_SqlColumns(fg_SqlColumn(gc_Str<"quoted\"column">, &CQuotedIdentifierRow::m_Value))
		)
	;
	constexpr auto gc_QuotedIdentifierDatabase = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"quoted_identifier_database">, gc_QuotedIdentifierTable);
	constexpr auto gc_QuotedIdentifierSchemaVersion = fg_SqlSchemaVersion(gc_Str<"0001">, gc_QuotedIdentifierDatabase);
	constexpr auto gc_ChecksumBaseTable = fg_SqlTable<CSchemaValidationRow>
		(
			gc_Str<"checksum_table">
			, gc_ValidationColumns
			, fg_SqlIndexes(fg_SqlIndex(gc_Str<"checksum_index">, gc_Str<"id">))
			, fg_SqlConstraints(fg_SqlCheck(gc_Str<"checksum_check">, gc_Str<"id >= 0">))
		)
	;
	constexpr auto gc_ChecksumChangedIndexTable = fg_SqlTable<CSchemaValidationRow>
		(
			gc_Str<"checksum_table">
			, gc_ValidationColumns
			, fg_SqlIndexes(fg_SqlIndex(gc_Str<"checksum_index">, ESqlIndexFlag::mc_Unique, gc_Str<"id">))
			, fg_SqlConstraints(fg_SqlCheck(gc_Str<"checksum_check">, gc_Str<"id >= 0">))
		)
	;
	constexpr auto gc_ChecksumChangedConstraintTable = fg_SqlTable<CSchemaValidationRow>
		(
			gc_Str<"checksum_table">
			, gc_ValidationColumns
			, fg_SqlIndexes(fg_SqlIndex(gc_Str<"checksum_index">, gc_Str<"id">))
			, fg_SqlConstraints(fg_SqlCheck(gc_Str<"checksum_check">, gc_Str<"id > 0">))
		)
	;
	constexpr auto gc_ChecksumBaseSchemaVersion = fg_SqlSchemaVersion(gc_Str<"0001">, fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"checksum_database">, gc_ChecksumBaseTable));
	constexpr auto gc_ChecksumChangedIndexSchemaVersion = fg_SqlSchemaVersion
		(
			gc_Str<"0001">
			, fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"checksum_database">, gc_ChecksumChangedIndexTable)
		)
	;
	constexpr auto gc_ChecksumChangedConstraintSchemaVersion = fg_SqlSchemaVersion
		(
			gc_Str<"0001">
			, fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"checksum_database">, gc_ChecksumChangedConstraintTable)
		)
	;
	static_assert(!TCSqlTypeTraits<void *>::mc_bSupported);
	static_assert(!TCSqlTypeTraits<CInvalidMappedWrapper>::mc_bSupported);
	static_assert(!TCSqlTypeTraits<CSqlUnrecognizedBackendValue>::mc_bSupported);
	static_assert(!fg_TestCanDeclareSqlColumn<void *>());
	static_assert(!fg_TestCanDeclareSqlColumnWithExplicitType<void *>());
	static_assert(!fg_TestCanDeclareSqlColumn<TCOptional<void *>>());
	static_assert(!fg_TestCanDeclareSqlColumn<CInvalidMappedWrapper>());
	static_assert(!fg_TestCanDeclareSqlColumn<CSqlUnrecognizedBackendValue>());
	static_assert
		(
			fg_StrCmp(fg_TestValidateDuplicateColumnTable<CSchemaValidationRow>(), "SQL table has duplicate column names") == 0
			, CConstExprSubStr(fg_TestValidateDuplicateColumnTable<CSchemaValidationRow>())
		)
	;
	static_assert(fg_StrCmp(fg_TestValidateNullableTablePrimaryKey(), "SQL primary-key column cannot be nullable") == 0, CConstExprSubStr(fg_TestValidateNullableTablePrimaryKey()));
	static_assert
		(
			fg_StrCmp(fg_TestValidateInvalidIndexTable<CSchemaValidationRow>(), "SQL index references a missing column") == 0
			, CConstExprSubStr(fg_TestValidateInvalidIndexTable<CSchemaValidationRow>())
		)
	;
	static_assert
		(
			fg_StrCmp(fg_TestValidateInvalidAutoIncrementTable<CSchemaValidationRow>(), "SQL autoincrement column must be a primary key") == 0
			, CConstExprSubStr(fg_TestValidateInvalidAutoIncrementTable<CSchemaValidationRow>())
		)
	;
	static_assert
		(
			fg_StrCmp(fg_TestValidateInvalidGeneratedDefaultTable<CSchemaValidationRow>(), "SQL generated column cannot have a default expression") == 0
			, CConstExprSubStr(fg_TestValidateInvalidGeneratedDefaultTable<CSchemaValidationRow>())
		)
	;
	static_assert
		(
			fg_StrCmp(fg_TestValidateInvalidForeignKeyDatabase<CSchemaValidationRow>(), "SQL foreign key references a missing table") == 0
			, CConstExprSubStr(fg_TestValidateInvalidForeignKeyDatabase<CSchemaValidationRow>())
		)
	;
	static_assert
		(
			fg_StrCmp(fg_TestValidateDuplicateTableDatabase<CSchemaValidationRow>(), "SQL database has duplicate table names") == 0
			, CConstExprSubStr(fg_TestValidateDuplicateTableDatabase<CSchemaValidationRow>())
		)
	;
	static_assert
		(
			fg_StrCmp(fg_TestValidateDuplicateIndexDatabase<CSchemaValidationRow>(), "SQL database has duplicate index names") == 0
			, CConstExprSubStr(fg_TestValidateDuplicateIndexDatabase<CSchemaValidationRow>())
		)
	;
	static_assert(fg_SqlDialectsSupportColumnType(ESqlDialect::mc_SQL2023, ESqlColumnType::mc_Integer64));
	static_assert(!fg_SqlDialectsSupportColumnType(ESqlDialect::mc_SQL1999, ESqlColumnType::mc_Date));
	static_assert(fg_SqlDialectsSupportColumnType(ESqlDialect::mc_SQL2011, ESqlColumnType::mc_Date));
	static_assert(!fg_SqlDialectsSupportColumnType(ESqlDialect::mc_SQL2011, ESqlColumnType::mc_Json));
	static_assert(fg_SqlDialectsSupportColumnType(ESqlDialect::mc_SQL2016, ESqlColumnType::mc_Json));
	static_assert(fg_SqlDialectsSupportColumnType(ESqlDialect::mc_SQL2023, ESqlColumnType::mc_Json));
	static_assert(!fg_SqlDialectsSupportColumnType(ESqlDialect::mc_SQL2023, ESqlColumnType::mc_Jsonb));
	static_assert(!fg_SqlDialectsSupportColumnType(ESqlDialect::mc_SQL2023, ESqlColumnType::mc_UUID));
	static_assert(!fg_SqlTableMatchesDialect<ESqlDialect::mc_SQL1999, decltype(gc_PostgresSpecificTable)>());
	static_assert(fg_SqlTableMatchesDialect<ESqlDialect::mc_SQL2011, decltype(gc_PostgresSpecificTable)>());
	static_assert(fg_SqlTableMatchesDialect<ESqlDialect::mc_SQL2016, decltype(gc_PostgresSpecificTable)>());
	static_assert(fg_SqlTableMatchesDialect<ESqlDialect::mc_SQL2023, decltype(gc_PostgresSpecificTable)>());
	static_assert(!fg_SqlTableMatchesDialect<ESqlDialect::mc_SQLite, decltype(gc_PostgresSpecificTable)>());
	static_assert(fg_SqlTableMatchesDialect<ESqlDialect::mc_SQLite | ESqlDialect::mc_Postgres, decltype(gc_PostgresSpecificTable)>());
	static_assert(fg_SqlTableMatchesDialect<ESqlDialect::mc_Postgres, decltype(gc_PostgresSpecificTable)>());
	static_assert(fg_SqlTableMatchesDialect<ESqlDialect::mc_Dynamic, decltype(gc_PostgresSpecificTable)>());

	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQL2023, ESqlValueType::mc_Integer64));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQLite, ESqlValueType::mc_Text));
	static_assert(!fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQLite, ESqlValueType::mc_UUID));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_Postgres, ESqlValueType::mc_UUID));
	static_assert(!fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQL1999, ESqlValueType::mc_Date));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQL2011, ESqlValueType::mc_Date));
	static_assert(!fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQL2011, ESqlValueType::mc_Json));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQL2016, ESqlValueType::mc_Json));
	static_assert(!fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQL2023, ESqlValueType::mc_Jsonb));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_Postgres, ESqlValueType::mc_Jsonb));
	static_assert(!fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQLite, ESqlValueType::mc_Array_Integer32));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_Postgres, ESqlValueType::mc_Array_Integer32));
	static_assert(!fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQLite, ESqlValueType::mc_UnrecognizedBackend));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_Postgres, ESqlValueType::mc_UnrecognizedBackend));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_SQLite, ESqlValueType::mc_Null));
	static_assert(fg_SqlDialectsSupportValueType(ESqlDialect::mc_Postgres, ESqlValueType::mc_Null));

	static_assert(cSqlDialectSupportsType<ESqlDialect::mc_SQLite, int64>);
	static_assert(cSqlDialectSupportsType<ESqlDialect::mc_SQLite, NStr::CStr>);
	static_assert(!cSqlDialectSupportsType<ESqlDialect::mc_SQLite, NCryptography::CUniversallyUniqueIdentifier>);
	static_assert(!cSqlDialectSupportsType<ESqlDialect::mc_SQLite, NEncoding::CJsonOrdered>);
	static_assert(cSqlDialectSupportsType<ESqlDialect::mc_Postgres, NCryptography::CUniversallyUniqueIdentifier>);
	static_assert(cSqlDialectSupportsType<ESqlDialect::mc_Postgres, NEncoding::CJsonOrdered>);
	static_assert(cSqlDialectSupportsType<ESqlDialect::mc_Postgres, NEncoding::CJsonSorted>);
	static_assert(cSqlDialectSupportsType<ESqlDialect::mc_Postgres, CSqlInterval>);

	struct CDatabaseSchema_Tests : public NMib::NTest::CTest
	{
		void f_DoTests();
	};

	void CDatabaseSchema_Tests::f_DoTests()
	{
		DMibTestSuite("Describe")
		{
			auto const &Table = gc_UserTable;

			DMibExpect(Table.f_Name(), ==, CStr("users"));
			DMibExpect(Table.mc_nColumns, ==, umint(4));

			auto Descriptions = Table.f_DescribeColumns();
			DMibExpect(Descriptions.f_GetLen(), ==, umint(4));

			DMibExpect(Descriptions[0].f_Name(), ==, CStr("id"));
			DMibExpect(Descriptions[0].m_Type, ==, ESqlColumnType::mc_UnsignedInteger64);
			DMibExpect(Descriptions[0].f_IsPrimaryKey(), ==, true);
			DMibExpect(Descriptions[0].f_IsNullable(), ==, false);

			DMibExpect(Descriptions[1].f_Name(), ==, CStr("email"));
			DMibExpect(Descriptions[1].m_Type, ==, ESqlColumnType::mc_Text);
			DMibExpect(fg_IsSet(Descriptions[1].m_Flags, ESqlColumnFlag::mc_Unique), ==, false);

			DMibExpect(Descriptions[2].f_Name(), ==, CStr("display_name"));
			DMibExpect(Descriptions[2].m_Type, ==, ESqlColumnType::mc_Text);
			DMibExpect(Descriptions[2].f_IsNullable(), ==, true);

			DMibExpect(Descriptions[3].f_Name(), ==, CStr("admin"));
			DMibExpect(Descriptions[3].m_Type, ==, ESqlColumnType::mc_Boolean);
			DMibExpect(Descriptions[3].f_DefaultSql(), ==, CStr("0"));

			DMibExpect(gc_UUIDColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_UUID);
			DMibExpect(gc_PostgresDateColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_Date);
			DMibExpect(gc_PostgresTimeColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_TimeOfDay);
			DMibExpect(gc_PostgresTimestampColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_Timestamp);
			DMibExpect(gc_PostgresTimestampTzColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_TimestampTz);
			DMibExpect(gc_PostgresIntervalColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_Interval);
			DMibExpect(gc_PostgresJsonColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_Json);
			DMibExpect(gc_PostgresJsonbColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_Jsonb);
			DMibExpect(gc_PostgresIntArrayColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_Array_Integer32);

			auto IndexDescriptions = Table.f_DescribeIndexes();
			DMibExpect(IndexDescriptions.f_GetLen(), ==, umint(1));
			DMibExpect(IndexDescriptions[0].f_Name(), ==, CStr("idx_users_email"));
			DMibExpect(IndexDescriptions[0].f_IsUnique(), ==, false);
			DMibExpect(IndexDescriptions[0].m_Columns.f_GetLen(), ==, umint(1));
			DMibExpect(*IndexDescriptions[0].m_Columns[0], ==, CStr("email"));

			auto ConstraintDescriptions = Table.f_DescribeConstraints();
			DMibExpect(ConstraintDescriptions.f_GetLen(), ==, umint(2));
			DMibExpect(ConstraintDescriptions[0].f_Name(), ==, CStr("users_email_unique"));
			DMibExpect(ConstraintDescriptions[0].m_Type, ==, ESqlConstraintType::mc_Unique);
			DMibExpect(ConstraintDescriptions[0].m_Columns.f_GetLen(), ==, umint(1));
			DMibExpect(*ConstraintDescriptions[0].m_Columns[0], ==, CStr("email"));
			DMibExpect(ConstraintDescriptions[1].f_Name(), ==, CStr("users_admin_bool"));
			DMibExpect(ConstraintDescriptions[1].m_Type, ==, ESqlConstraintType::mc_Check);
			DMibExpect(ConstraintDescriptions[1].f_CheckSql(), ==, CStr("admin IN (FALSE, TRUE)"));
		};

		DMibTestSuite("Member access")
		{
			CUserRow Row;

			gc_UserIDColumn.f_Value(Row) = 42;
			DMibExpect(Row.m_ID, ==, uint64(42));
			DMibExpect(gc_UserIDColumn.f_Value(Row), ==, uint64(42));
		};

		DMibTestSuite("Column type overrides")
		{
			DMibExpect(gc_EmailTextColumn.f_Describe().m_Type, ==, ESqlColumnType::mc_Text);
		};

		DMibTestSuite("Column options")
		{
			auto ColumnDescriptions = gc_ColumnOptionsTable.f_DescribeColumns();
			DMibExpect(ColumnDescriptions[1].f_Comment(), ==, CStr("Value display text"));

			CNonPortableColumnOptions const *pSqliteOptions = ColumnDescriptions[1].f_NonPortableOptions(CStr("sqlite"));
			DMibExpect(pSqliteOptions != nullptr, ==, true);
			DMibExpect(*pSqliteOptions->m_pDefaultSql, ==, CStr("'unknown'"));
			DMibExpect(*pSqliteOptions->m_pCollationSql, ==, CStr("NOCASE"));
			DMibExpect(*pSqliteOptions->m_pCustomSql, ==, CStr("CHECK (length(value) > 0)"));
		};

		DMibTestSuite("Database")
		{
			auto const &Database = gc_Database;

			DMibExpect(Database.f_Name(), ==, CStr("build_coordinator"));
			DMibExpect(Database.mc_nTables, ==, umint(3));

			auto TableDescriptions = Database.f_DescribeTables();
			DMibExpect(TableDescriptions.f_GetLen(), ==, umint(3));
			DMibExpect(TableDescriptions[0].f_Name(), ==, CStr("users"));
			DMibExpect(TableDescriptions[0].m_nColumns, ==, umint(4));
			DMibExpect(TableDescriptions[0].m_nIndexes, ==, umint(1));
			DMibExpect(TableDescriptions[0].m_nConstraints, ==, umint(2));
			DMibExpect(TableDescriptions[1].f_Name(), ==, CStr("sessions"));
			DMibExpect(TableDescriptions[1].m_nColumns, ==, umint(3));
			DMibExpect(TableDescriptions[1].m_nIndexes, ==, umint(2));
			DMibExpect(TableDescriptions[1].m_nConstraints, ==, umint(1));
			DMibExpect(TableDescriptions[2].f_Name(), ==, CStr("user_roles"));
			DMibExpect(TableDescriptions[2].m_nColumns, ==, umint(2));
			DMibExpect(TableDescriptions[2].m_nIndexes, ==, umint(1));
			DMibExpect(TableDescriptions[2].m_nConstraints, ==, umint(2));
		};

		DMibTestSuite("Schema versions")
		{
			DMibExpect(gc_SchemaVersion.f_ID(), ==, CStr("0002"));
			DMibExpect(gc_SchemaVersion.f_Database().f_Name(), ==, CStr("build_coordinator"));
			DMibExpect(gc_ChecksumBaseSchemaVersion.f_Describe().m_Checksum, !=, gc_ChecksumChangedIndexSchemaVersion.f_Describe().m_Checksum);
			DMibExpect(gc_ChecksumBaseSchemaVersion.f_Describe().m_Checksum, !=, gc_ChecksumChangedConstraintSchemaVersion.f_Describe().m_Checksum);

			auto VersionDescriptions = NMigrations::gc_SchemaVersions.f_DescribeVersions();
			DMibExpect(VersionDescriptions.f_GetLen(), ==, umint(2));
			DMibExpect(VersionDescriptions[0].f_ID(), ==, CStr("0001"));
			DMibExpect(VersionDescriptions[0].f_DatabaseName(), ==, CStr("build_coordinator"));
			DMibExpect(VersionDescriptions[0].m_nTables, ==, umint(2));
			DMibExpect(VersionDescriptions[1].f_ID(), ==, CStr("0002"));
			DMibExpect(VersionDescriptions[1].f_DatabaseName(), ==, CStr("build_coordinator"));
			DMibExpect(VersionDescriptions[1].m_nTables, ==, umint(3));

			auto LatestVersionDescription = NMigrations::gc_SchemaVersions.f_Describe();
			DMibExpect(LatestVersionDescription.f_ID(), ==, CStr("0002"));
			DMibExpect(LatestVersionDescription.m_nTables, ==, umint(3));

			auto MigrationDescriptions = NMigrations::gc_SchemaVersions.f_DescribeMigrations();
			DMibExpect(MigrationDescriptions.f_GetLen(), ==, umint(1));
			DMibExpect(*MigrationDescriptions[0].m_pFromVersionID, ==, CStr("0001"));
			DMibExpect(*MigrationDescriptions[0].m_pToVersionID, ==, CStr("0002"));

			auto RenameMigration = fg_SqlSchemaMigration
				(
					gc_SchemaVersion
					, fg_SqlRenameTable(NStr::gc_Str<"old_table">, NStr::gc_Str<"new_table">)
					, fg_SqlRenameColumn(NStr::gc_Str<"new_table">, NStr::gc_Str<"old_column">, NStr::gc_Str<"new_column">)
				)
			;
			auto RenameDescription = RenameMigration.f_DescribeMigration(&NVersion1::gc_SchemaVersion.f_ID());
			DMibExpect(*RenameDescription.m_pFromVersionID, ==, CStr("0001"));
			DMibExpect(*RenameDescription.m_pToVersionID, ==, CStr("0002"));
			DMibExpect(RenameDescription.m_Operations.f_GetLen(), ==, umint(2));
			DMibExpect(RenameDescription.m_Operations[0].m_Type, ==, ESqlSchemaMigrationOperationType::mc_RenameTable);
			DMibExpect(*RenameDescription.m_Operations[0].m_pOldName, ==, CStr("old_table"));
			DMibExpect(*RenameDescription.m_Operations[0].m_pNewName, ==, CStr("new_table"));
			DMibExpect(RenameDescription.m_Operations[1].m_Type, ==, ESqlSchemaMigrationOperationType::mc_RenameColumn);
			DMibExpect(*RenameDescription.m_Operations[1].m_pTableName, ==, CStr("new_table"));
			DMibExpect(*RenameDescription.m_Operations[1].m_pOldName, ==, CStr("old_column"));
			DMibExpect(*RenameDescription.m_Operations[1].m_pNewName, ==, CStr("new_column"));
		};

		DMibTestSuite("Migration plan")
		{
			CSqlSchemaMigrationPlan EmptyPlan = fg_SqlPlanSchemaMigration(NMigrations::gc_SchemaVersions);
			DMibExpect(EmptyPlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(EmptyPlan.m_Steps.f_GetLen(), ==, umint(3));
			if (EmptyPlan.m_Steps.f_GetLen() == 3)
			{
				DMibExpect(EmptyPlan.m_Steps[0].m_Type, ==, ESqlSchemaMigrationPlanStepType::mc_CreateInitialSchema);
				DMibExpect(EmptyPlan.m_Steps[0].m_ToVersionID, ==, CStr("0002"));
				DMibExpect(EmptyPlan.m_Steps[1].m_Type, ==, ESqlSchemaMigrationPlanStepType::mc_MarkSchemaVersionApplied);
				DMibExpect(EmptyPlan.m_Steps[1].m_ToVersionID, ==, CStr("0001"));
				DMibExpect(EmptyPlan.m_Steps[2].m_Type, ==, ESqlSchemaMigrationPlanStepType::mc_MarkSchemaVersionApplied);
				DMibExpect(EmptyPlan.m_Steps[2].m_ToVersionID, ==, CStr("0002"));
			}

			CStr Version1ID("0001");
			CSqlSchemaMigrationPlan UpgradePlan = fg_SqlPlanSchemaMigration(NMigrations::gc_SchemaVersions, &Version1ID);
			DMibExpect(UpgradePlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(UpgradePlan.m_Steps.f_GetLen(), ==, umint(3));
			if (UpgradePlan.m_Steps.f_GetLen() == 3)
			{
				DMibExpect(UpgradePlan.m_Steps[0].m_Type, ==, ESqlSchemaMigrationPlanStepType::mc_ApplyMigrationOperations);
				DMibExpect(UpgradePlan.m_Steps[0].m_FromVersionID, ==, CStr("0001"));
				DMibExpect(UpgradePlan.m_Steps[0].m_ToVersionID, ==, CStr("0002"));
				DMibExpect(UpgradePlan.m_Steps[0].m_nOperations, ==, umint(0));
				DMibExpect(UpgradePlan.m_Steps[1].m_Type, ==, ESqlSchemaMigrationPlanStepType::mc_SyncAdditiveSchema);
				DMibExpect(UpgradePlan.m_Steps[2].m_Type, ==, ESqlSchemaMigrationPlanStepType::mc_MarkSchemaVersionApplied);
			}

			CStr LatestID("0002");
			CSqlSchemaMigrationPlan CurrentPlan = fg_SqlPlanSchemaMigration(NMigrations::gc_SchemaVersions, &LatestID);
			DMibExpect(CurrentPlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(CurrentPlan.m_Steps.f_GetLen(), ==, umint(1));
			if (CurrentPlan.m_Steps.f_GetLen() == 1)
				DMibExpect(CurrentPlan.m_Steps[0].m_Type, ==, ESqlSchemaMigrationPlanStepType::mc_AlreadyCurrent);

			CStr UnknownID("unknown");
			CSqlSchemaMigrationPlan UnknownPlan = fg_SqlPlanSchemaMigration(NMigrations::gc_SchemaVersions, &UnknownID);
			DMibExpect(UnknownPlan.m_Steps.f_GetLen(), ==, umint(0));
			DMibExpect(UnknownPlan.m_Warnings.f_GetLen(), ==, umint(1));
		};

		DMibTestSuite("SQLite rejects identifiers that differ only in case") -> NConcurrency::TCFuture<void>
		{
			auto Plan = co_await fg_PlanSqliteCaseCollision().f_Wrap();
			DMibExpect(bool(Plan), ==, false);

			co_return {};
		};

		DMibTestSuite("PostgreSQL rejects identifiers it would truncate") -> NConcurrency::TCFuture<void>
		{
			auto Plan = co_await fg_PlanPostgresLongIdentifier().f_Wrap();
			DMibExpect(bool(Plan), ==, false);

			co_return {};
		};

		DMibTestSuite("Migration rejects a rename after rebuilding its table") -> NConcurrency::TCFuture<void>
		{
			auto SqlitePlan = co_await fg_PlanSqliteMigration("rename_0001").f_Wrap();
			DMibExpect(bool(SqlitePlan), ==, false);

			auto PostgresPlan = co_await fg_PlanPostgresMigration("rename_0001").f_Wrap();
			DMibExpect(bool(PostgresPlan), ==, false);

			co_return {};
		};

		DMibTestSuite("PostgreSQL plan re-adds a CHECK dropped with its column") -> NConcurrency::TCFuture<void>
		{
			CSqlSchemaMigrationPlan Plan = co_await fg_PlanPostgresDropCheck("drop_check_0001");

			bool bAddsCheck = false;
			for (auto const &Statement : Plan.m_Statements)
			{
				if (Statement.f_Find("\"drop_check_test_check\" CHECK (length(name) > 0)") >= 0)
					bAddsCheck = true;
			}
			DMibExpect(bAddsCheck, ==, true);

			co_return {};
		};

		DMibTestSuite("PostgreSQL plan restores a dropped child foreign key as later operations left its table") -> NConcurrency::TCFuture<void>
		{
			auto fFindAddChildForeignKey = [](CSqlSchemaMigrationPlan const &_Plan) -> CStr
				{
					for (auto const &Statement : _Plan.m_Statements)
					{
						if (Statement.f_Find("ADD CONSTRAINT \"fk_child_parent_fk\"") >= 0)
							return Statement;
					}

					return {};
				}
			;

			CStr VersionID("fk_0001");
			{
				DMibTestPath("Child column renamed");
				CSqlSchemaMigrationPlan Plan = co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_FkChildRenameAfterParentRebuildSchemaVersions, &VersionID);
				DMibExpect
					(
						fFindAddChildForeignKey(Plan)
						, ==
						, CStr("ALTER TABLE \"fk_child\" ADD CONSTRAINT \"fk_child_parent_fk\" FOREIGN KEY (\"owner_id\") REFERENCES \"fk_parent\" (\"id\") ON DELETE CASCADE")
					)
				;
			}
			{
				DMibTestPath("Child column dropped");
				CSqlSchemaMigrationPlan Plan = co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_FkChildDropColumnAfterParentRebuildSchemaVersions, &VersionID);
				DMibExpect(fFindAddChildForeignKey(Plan), ==, CStr());
			}
			{
				DMibTestPath("Child table dropped");
				CSqlSchemaMigrationPlan Plan = co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_FkChildDropTableAfterParentRebuildSchemaVersions, &VersionID);
				DMibExpect(fFindAddChildForeignKey(Plan), ==, CStr());
			}
			{
				DMibTestPath("Child table renamed keeping the declared key");
				CSqlSchemaMigrationPlan Plan = co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_FkChildTableRenameAfterParentRebuildSchemaVersions, &VersionID);

				umint nAddChildForeignKeys = 0;
				for (auto const &Statement : Plan.m_Statements)
				{
					if (Statement.f_Find("ADD CONSTRAINT \"fk_child_parent_fk\"") >= 0)
						++nAddChildForeignKeys;
				}

				DMibExpect(nAddChildForeignKeys, ==, umint(1));
				DMibExpect(fFindAddChildForeignKey(Plan).f_Find("ALTER TABLE \"fk_child_moved\"") >= 0, ==, true);
			}

			co_return {};
		};

		DMibTestSuite("PostgreSQL column rename rewrites expressions as the server does")
		{
			using NMib::NSQL::NPrivate::fg_SqlTextRenameIdentifier;

			DMibExpect(fg_SqlTextRenameIdentifier("value * 2", "value", "NewValue"), ==, CStr("\"NewValue\" * 2"));
			DMibExpect(fg_SqlTextRenameIdentifier("lower(lower)", "lower", "value"), ==, CStr("lower(value)"));
			DMibExpect(fg_SqlTextRenameIdentifier("code || 'code'", "code", "label"), ==, CStr("label || 'code'"));
			DMibExpect(fg_SqlTextRenameIdentifier("code || $$code$$", "code", "label"), ==, CStr("label || $$code$$"));
			DMibExpect(fg_SqlTextRenameIdentifier("arr[code]", "code", "label"), ==, CStr("arr[label]"));
			DMibExpect(fg_SqlTextRenameIdentifier("code::text", "text", "label"), ==, CStr("code::text"));
			DMibExpect(fg_SqlTextRenameIdentifier("\"Code\" + code", "code", "label"), ==, CStr("\"Code\" + label"));
			DMibExpect(fg_SqlTextRenameIdentifier("value + 1", "value", "order"), ==, CStr("\"order\" + 1"));
			DMibExpect(fg_SqlTextRenameIdentifier("CAST(text AS text)", "text", "body"), ==, CStr("CAST(body AS text)"));
			DMibExpect(fg_SqlTextRenameIdentifier("precision::double precision", "precision", "amount"), ==, CStr("amount::double precision"));
			DMibExpect(fg_SqlTextRenameIdentifier("amount /* outer /* inner */ amount */ + 1", "amount", "total"), ==, CStr("total /* outer /* inner */ amount */ + 1"));
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("CREATE UNIQUE INDEX i ON t(name COLLATE NOCASE)", "nocase", true), ==, false);
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase("Name", "nAME"), ==, true);
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase("\xC3\x84", "\xC3\xA4"), ==, false);
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("\xC3\x84 > 0", "\xC3\xA4"), ==, false);
			DMibExpect(fg_SqlTextRenameIdentifier("\"lower\"(\"lower\")", "lower", "value"), ==, CStr("\"lower\"(\"value\")"));
			DMibExpect(fg_SqlTextRenameIdentifier("\"date\" - DATE '2000-01-01'", "date", "created_on"), ==, CStr("\"created_on\" - DATE '2000-01-01'"));
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("CREATE INDEX i ON t(lower(email))", "lower", true), ==, false);
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("CREATE UNIQUE INDEX i ON t(value DESC)", "desc", true), ==, false);
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT NEW.last; END", "last"), ==, true);
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("CREATE TRIGGER t AFTER UPDATE OF ignore ON x BEGIN SELECT 1; END", "ignore"), ==, true);
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("CREATE TRIGGER t AFTER UPDATE OF a, \"b\"\"c\", [d] , match ON x BEGIN SELECT 1; END", "match"), ==, true);
			DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("CREATE TRIGGER t BEFORE INSERT ON x BEGIN SELECT RAISE(IGNORE); END", "ignore"), ==, false);
			DMibExpect(fg_SqlTextRenameIdentifier("CASE WHEN \"end\" THEN 1 END", "end", "finish"), ==, CStr("CASE WHEN \"finish\" THEN 1 END"));
			DMibExpect(fg_SqlTextRenameIdentifier("code || E'\\'code'", "code", "label"), ==, CStr("label || E'\\'code'"));
			DMibExpect(fg_SqlTextRenameIdentifier("amount -- note\ramount + 1", "amount", "total"), ==, CStr("total -- note\rtotal + 1"));

			DMibTestCategory("Foreign key definitions")
			{
				using NMib::NSQL::NPrivate::fg_PostgresForeignKeyRenameColumn;

				DMibExpect
					(
						fg_PostgresForeignKeyRenameColumn("FOREIGN KEY (parent_id, tenant_id) REFERENCES parents(parent_id, tenant_id)", "parent_id", "owner_id")
						, ==
						, CStr("FOREIGN KEY (owner_id, tenant_id) REFERENCES parents(parent_id, tenant_id)")
					)
				;
				DMibExpect
					(
						fg_PostgresForeignKeyRenameColumn
						(
							"FOREIGN KEY (parent_id, tenant_id) REFERENCES parents(parent_id, tenant_id) ON UPDATE CASCADE ON DELETE SET NULL (parent_id) DEFERRABLE"
							, "parent_id"
							, "owner_id"
						)
						, ==
						, CStr("FOREIGN KEY (owner_id, tenant_id) REFERENCES parents(parent_id, tenant_id) ON UPDATE CASCADE ON DELETE SET NULL (owner_id) DEFERRABLE")
					)
				;
				DMibExpect
					(
						fg_PostgresForeignKeyRenameColumn
						(
							"FOREIGN KEY (parent_id, \"Tenant\") REFERENCES parents(parent_id, \"Tenant\") ON DELETE SET DEFAULT (\"Tenant\")"
							, "Tenant"
							, "Organization"
						)
						, ==
						, CStr("FOREIGN KEY (parent_id, \"Organization\") REFERENCES parents(parent_id, \"Tenant\") ON DELETE SET DEFAULT (\"Organization\")")
					)
				;
				DMibExpect
					(
						fg_PostgresForeignKeyRenameColumn("FOREIGN KEY (\"a REFERENCES b\", parent_id) REFERENCES parents(x, parent_id)", "parent_id", "owner_id")
						, ==
						, CStr("FOREIGN KEY (\"a REFERENCES b\", owner_id) REFERENCES parents(x, parent_id)")
					)
				;
				DMibExpect(NMib::NSQL::NPrivate::fg_PostgresForeignKeyFindReferences("FOREIGN KEY (\"a \"\" REFERENCES b\") REFERENCES parents(x)"), ==, aint(33));
				DMibExpect(fg_PostgresForeignKeyRenameColumn("FOREIGN KEY (glob) REFERENCES parents(id)", "glob", "pattern"), ==, CStr("FOREIGN KEY (pattern) REFERENCES parents(id)"));

				CStr RenamedToKeyword = fg_PostgresForeignKeyRenameColumn("FOREIGN KEY (customer_id) REFERENCES parents(id)", "customer_id", "between");
				DMibExpect(RenamedToKeyword, ==, CStr("FOREIGN KEY (\"between\") REFERENCES parents(id)"));
				DMibExpect(fg_PostgresForeignKeyRenameColumn(RenamedToKeyword, "between", "buyer_id"), ==, CStr("FOREIGN KEY (\"buyer_id\") REFERENCES parents(id)"));
				DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsPostgresColumn("FOREIGN KEY (match)", "match"), ==, true);
				DMibExpect(NMib::NSQL::NPrivate::fg_SqlTextMentionsSqliteColumn("name GLOB 'a*'", "glob"), ==, false);
			};
		};

		DMibTestSuite("SQLite column rename rewrites expressions as the server does")
		{
			using NMib::NSQL::NPrivate::fg_SqlTextRenameSqliteIdentifier;

			DMibExpect(fg_SqlTextRenameSqliteIdentifier("value > 0", "value", "amount"), ==, CStr("amount > 0"));
			DMibExpect(fg_SqlTextRenameSqliteIdentifier("value > 0", "value", "autoincrement"), ==, CStr("\"autoincrement\" > 0"));
			DMibExpect(fg_SqlTextRenameSqliteIdentifier("value > 0", "value", "Index"), ==, CStr("\"Index\" > 0"));
			DMibExpect(fg_SqlTextRenameSqliteIdentifier("value > 0", "value", "indexed"), ==, CStr("\"indexed\" > 0"));
			DMibExpect(fg_SqlTextRenameSqliteIdentifier("value > 0", "value", "order"), ==, CStr("\"order\" > 0"));
			DMibExpect(fg_SqlTextRenameSqliteIdentifier("amount -- note\ramount + 1", "amount", "total"), ==, CStr("total -- note\ramount + 1"));
		};

		DMibTestSuite("Migration SQL dry run") -> NConcurrency::TCFuture<void>
		{
			CSqlSchemaMigrationPlan EmptySqlitePlan = co_await fg_SqlitePlanSchemaMigration(NMigrations::gc_SchemaVersions);
			DMibExpect(EmptySqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(EmptySqlitePlan.m_Statements.f_GetLen(), ==, umint(10));
			if (EmptySqlitePlan.m_Statements.f_GetLen() == 10)
			{
				DMibExpect
					(
						EmptySqlitePlan.m_Statements[0]
						, ==
						, "CREATE TABLE IF NOT EXISTS \"schema_migrations\" (\"id\" TEXT PRIMARY KEY, \"name\" TEXT NOT NULL, \"checksum\" "
						"TEXT NOT NULL, \"applied_at\" TEXT NOT NULL, \"applied_by_version\" TEXT NOT NULL)"
					)
				;
				DMibExpect
					(
						EmptySqlitePlan.m_Statements[1]
						, ==
						, "CREATE TABLE IF NOT EXISTS \"users\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"email\" TEXT NOT NULL, \"display_name\" TEXT, "
						"\"admin\" INTEGER NOT NULL DEFAULT 0, CONSTRAINT \"users_email_unique\" UNIQUE (\"email\"), CONSTRAINT \"users_admin_bool\" CHECK (admin IN (FALSE, TRUE)))"
					)
				;
				DMibExpect
					(
						EmptySqlitePlan.m_Statements[8]
						, ==
						, "INSERT INTO \"schema_migrations\" (\"id\", \"name\", \"checksum\", \"applied_at\", \"applied_by_version\") VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)"
					)
				;
			}

			CSqlSchemaMigrationPlan EmptyPostgresPlan = co_await fg_PostgresPlanSchemaMigration(NMigrations::gc_SchemaVersions);
			DMibExpect(EmptyPostgresPlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(EmptyPostgresPlan.m_Statements.f_GetLen(), ==, umint(12));
			if (EmptyPostgresPlan.m_Statements.f_GetLen() == 12)
			{
				DMibExpect
					(
						EmptyPostgresPlan.m_Statements[0]
						, ==
						, "CREATE TABLE IF NOT EXISTS \"schema_migrations\" (\"id\" TEXT PRIMARY KEY, \"name\" TEXT NOT NULL, \"checksum\" TEXT NOT NULL, "
						"\"applied_at\" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, \"applied_by_version\" TEXT NOT NULL)"
					)
				;
				DMibExpect
					(
						EmptyPostgresPlan.m_Statements[1]
						, ==
						, "CREATE TABLE IF NOT EXISTS \"users\" (\"id\" BIGSERIAL PRIMARY KEY, \"email\" TEXT NOT NULL, \"display_name\" TEXT, \"admin\" BOOLEAN NOT NULL "
						"DEFAULT FALSE, CONSTRAINT \"users_email_unique\" UNIQUE (\"email\"), CONSTRAINT \"users_admin_bool\" CHECK (admin IN (FALSE, TRUE)))"
					)
				;
				DMibExpect
					(
						EmptyPostgresPlan.m_Statements[8]
						, ==
						, "ALTER TABLE \"sessions\" ADD CONSTRAINT \"sessions_user_id_users_fk\" FOREIGN KEY (\"user_id\") REFERENCES \"users\" (\"id\") ON DELETE CASCADE"
					)
				;
				DMibExpect
					(
						EmptyPostgresPlan.m_Statements[10]
						, ==
						, "INSERT INTO \"schema_migrations\" (\"id\", \"name\", \"checksum\", \"applied_by_version\") VALUES ($1, $2, $3, $4) ON CONFLICT (\"id\") DO NOTHING"
					)
				;
			}

			CStr Version1ID("0001");
			CSqlSchemaMigrationPlan UpgradeSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NMigrations::gc_SchemaVersions, &Version1ID);
			DMibExpect(UpgradeSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(UpgradeSqlitePlan.m_Statements.f_GetLen(), ==, umint(23));
			if (UpgradeSqlitePlan.m_Statements.f_GetLen() == 23)
			{
				// No pre-drop of the scratch name: an existing table there must stay untouched
				DMibExpect(UpgradeSqlitePlan.m_Statements[1], ==, CStr("PRAGMA legacy_alter_table=ON"));
				DMibExpect(UpgradeSqlitePlan.m_Statements[3], ==, CStr("PRAGMA legacy_alter_table=OFF"));
				DMibExpect
					(
						UpgradeSqlitePlan.m_Statements[5]
						, ==
						, "INSERT OR ABORT INTO \"users\" (\"id\", \"email\", \"display_name\") SELECT \"id\", \"email\", \"display_name\" FROM \"__mib_rebuild_old_users\""
					)
				;
				DMibExpect
					(
						UpgradeSqlitePlan.m_Statements[6]
						, ==
						, "UPDATE sqlite_sequence SET seq = MAX(seq, COALESCE((SELECT seq FROM sqlite_sequence WHERE name = '__mib_rebuild_old_users'), 0)) WHERE name = 'users'"
					)
				;
				DMibExpect
					(
						UpgradeSqlitePlan.m_Statements[22]
						, ==
						, "INSERT INTO \"schema_migrations\" (\"id\", \"name\", \"checksum\", \"applied_at\", \"applied_by_version\") VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)"
					)
				;
			}

			CStr RenameVersion1ID("rename_0001");
			CSqlSchemaMigrationPlan RenameRebuildSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_RenameRebuildSchemaVersions, &RenameVersion1ID);
			DMibExpect(RenameRebuildSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(RenameRebuildSqlitePlan.m_Statements.f_GetLen(), ==, umint(12));
			if (RenameRebuildSqlitePlan.m_Statements.f_GetLen() == 12)
			{
				DMibExpect(RenameRebuildSqlitePlan.m_Statements[1], ==, CStr("ALTER TABLE \"legacy_sessions\" RENAME TO \"sessions_renamed\""));
				DMibExpect(RenameRebuildSqlitePlan.m_Statements[2], ==, CStr("ALTER TABLE \"sessions_renamed\" RENAME COLUMN \"old_token\" TO \"token\""));
				DMibExpect
					(
						RenameRebuildSqlitePlan.m_Statements[7]
						, ==
						, "INSERT OR ABORT INTO \"sessions_renamed\" (\"id\", \"token\") SELECT \"id\", \"token\" FROM \"__mib_rebuild_old_sessions_renamed\""
					)
				;
			}

			CStr RenamedCheckVersion1ID("renamed_check_0001");
			CSqlSchemaMigrationPlan RenamedCheckSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_RenamedCheckSchemaVersions, &RenamedCheckVersion1ID);
			DMibExpect(RenamedCheckSqlitePlan.m_Statements.f_GetLen(), ==, umint(3));
			if (RenamedCheckSqlitePlan.m_Statements.f_GetLen() == 3)
				DMibExpect(RenamedCheckSqlitePlan.m_Statements[1], ==, CStr("ALTER TABLE \"renamed_check_test\" RENAME COLUMN \"old_name\" TO \"new_name\""));

			DMibExpect
				(
					NMib::NSQL::NPrivate::fg_SqlTextRenameSqliteIdentifier("length(OLD_NAME) > 3 AND \"Old_Name\" <> ''", "old_name", "New_Name")
					, ==
					, CStr("length(New_Name) > 3 AND \"New_Name\" <> ''")
				)
			;

			CStr BackendDefaultVersion1ID("backend_default_0001");
			CSqlSchemaMigrationPlan BackendDefaultSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_BackendDefaultSchemaVersions, &BackendDefaultVersion1ID);
			DMibExpect(BackendDefaultSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(BackendDefaultSqlitePlan.m_Statements.f_GetLen(), ==, umint(3));
			if (BackendDefaultSqlitePlan.m_Statements.f_GetLen() == 3)
				DMibExpect(BackendDefaultSqlitePlan.m_Statements[1], ==, CStr("ALTER TABLE \"backend_default_test\" ADD COLUMN \"backend_value\" TEXT NOT NULL DEFAULT 'sqlite-default'"));

			CStr RebuildAddColumnVersion1ID("rebuild_add_0001");
			CSqlSchemaMigrationPlan RebuildAddColumnSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_RebuildAddColumnSchemaVersions, &RebuildAddColumnVersion1ID);
			DMibExpect(RebuildAddColumnSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));

			bool bRebuildAddColumnHasAddNote = false;
			bool bRebuildAddColumnRecreatesWithNote = false;
			for (auto const &Statement : RebuildAddColumnSqlitePlan.m_Statements)
			{
				if (Statement.f_Find("ADD COLUMN \"note\"") >= aint(0))
					bRebuildAddColumnHasAddNote = true;

				if (Statement.f_Find("CREATE TABLE \"rebuild_add_column\"") >= aint(0) && Statement.f_Find("\"note\"") >= aint(0))
					bRebuildAddColumnRecreatesWithNote = true;
			}
			DMibExpect(bRebuildAddColumnRecreatesWithNote, ==, true);
			DMibExpect(bRebuildAddColumnHasAddNote, ==, false);

			CStr CaseRenameVersion1ID("case_rename_0001");
			CSqlSchemaMigrationPlan CaseRenameSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_CaseRenameSchemaVersions, &CaseRenameVersion1ID);
			DMibExpect(CaseRenameSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));

			// The column renamed only in case is the same column to SQLite, so the rebuild copies it
			bool bCaseRenameCopiesToken = false;
			for (auto const &Statement : CaseRenameSqlitePlan.m_Statements)
			{
				if (Statement.f_StartsWith("INSERT OR ABORT INTO \"case_rename\"") && Statement.f_Find("\"token\"") >= aint(0))
					bCaseRenameCopiesToken = true;
			}
			DMibExpect(bCaseRenameCopiesToken, ==, true);

			CStr DropAddVersion1ID("drop_add_0001");
			CSqlSchemaMigrationPlan DropAddSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_DropAddV2SchemaVersions, &DropAddVersion1ID);
			DMibExpect(DropAddSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));

			bool bDropAddHasAddNote = false;
			bool bDropAddRecreatesWithNote = false;
			for (auto const &Statement : DropAddSqlitePlan.m_Statements)
			{
				if (Statement.f_Find("ADD COLUMN \"note\"") >= aint(0))
					bDropAddHasAddNote = true;

				if (Statement.f_Find("CREATE TABLE \"drop_add_test\"") >= aint(0) && Statement.f_Find("\"note\"") >= aint(0))
					bDropAddRecreatesWithNote = true;
			}
			DMibExpect(bDropAddRecreatesWithNote, ==, true);
			DMibExpect(bDropAddHasAddNote, ==, false);

			// SQLite updates the child's reference when the parent is renamed
			CStr RenameRefVersion1ID("rename_ref_0001");
			CSqlSchemaMigrationPlan RenameRefSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_RenameRefSchemaVersions, &RenameRefVersion1ID);
			DMibExpect(RenameRefSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));

			bool bRenameRefRenamesParent = false;
			bool bRenameRefRebuildsChild = false;
			for (auto const &Statement : RenameRefSqlitePlan.m_Statements)
			{
				if (Statement.f_Find("ALTER TABLE \"rename_ref_parent\" RENAME TO \"rename_ref_parent_renamed\"") >= aint(0))
					bRenameRefRenamesParent = true;
				if (Statement.f_Find("__mib_rebuild_old_rename_ref_child") >= aint(0))
					bRenameRefRebuildsChild = true;
			}
			DMibExpect(bRenameRefRenamesParent, ==, true);
			DMibExpect(bRenameRefRebuildsChild, ==, false);

			CSqlSchemaMigrationPlan BackendDefaultPostgresPlan = co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_BackendDefaultSchemaVersions, &BackendDefaultVersion1ID);
			DMibExpect(BackendDefaultPostgresPlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(BackendDefaultPostgresPlan.m_Statements.f_GetLen(), ==, umint(3));
			if (BackendDefaultPostgresPlan.m_Statements.f_GetLen() == 3)
			{
				DMibExpect
					(
						BackendDefaultPostgresPlan.m_Statements[1]
						, ==
						, CStr("ALTER TABLE \"backend_default_test\" ADD COLUMN \"backend_value\" TEXT NOT NULL DEFAULT 'postgres-default'")
					)
				;
			}

			// Child foreign keys follow the renamed table and would block dropping it
			CStr FkParentRebuildVersion1ID("fk_0001");
			CSqlSchemaMigrationPlan FkParentRebuildPostgresPlan = co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_FkParentRebuildSchemaVersions, &FkParentRebuildVersion1ID);
			DMibExpect(FkParentRebuildPostgresPlan.m_Warnings.f_GetLen(), ==, umint(0));

			aint iDropChildFk = -1;
			aint iRename = -1;
			aint iSequenceReset = -1;
			aint iAddChildFk = -1;
			for (umint iStatement = 0; iStatement < FkParentRebuildPostgresPlan.m_Statements.f_GetLen(); ++iStatement)
			{
				NStr::CStr const &Statement = FkParentRebuildPostgresPlan.m_Statements[iStatement];
				if (Statement.f_Find("DROP CONSTRAINT \"fk_child_parent_fk\"") >= aint(0))
					iDropChildFk = aint(iStatement);
				if (Statement.f_Find("ALTER TABLE \"fk_parent\" RENAME TO \"__mib_rebuild_old_fk_parent\"") >= aint(0))
					iRename = aint(iStatement);
				if (Statement.f_Find("setval(pg_get_serial_sequence") >= aint(0))
					iSequenceReset = aint(iStatement);
				if (Statement.f_Find("ADD CONSTRAINT \"fk_child_parent_fk\"") >= aint(0))
					iAddChildFk = aint(iStatement);
			}
			DMibExpect(iDropChildFk, >=, aint(0));
			DMibExpect(iRename, >=, aint(0));
			DMibExpect(iSequenceReset, >=, aint(0));
			DMibExpect(iAddChildFk, >=, aint(0));
			if (iDropChildFk >= 0 && iRename >= 0 && iAddChildFk >= 0)
			{
				DMibExpect(iDropChildFk < iRename, ==, true);
				DMibExpect(iAddChildFk > iRename, ==, true);
			}

			CSqlSchemaMigrationPlan RenamePostgresPlan = co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_RenameSchemaVersions, &RenameVersion1ID);
			DMibExpect(RenamePostgresPlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(RenamePostgresPlan.m_Statements.f_GetLen(), ==, umint(4));
			if (RenamePostgresPlan.m_Statements.f_GetLen() == 4)
			{
				DMibExpect(RenamePostgresPlan.m_Statements[1], ==, CStr("ALTER TABLE \"legacy_sessions\" RENAME TO \"sessions_renamed\""));
				DMibExpect(RenamePostgresPlan.m_Statements[2], ==, CStr("ALTER TABLE \"sessions_renamed\" RENAME COLUMN \"old_token\" TO \"token\""));
			}

			CSqlSchemaMigrationPlan UpgradePostgresPlan = co_await fg_PostgresPlanSchemaMigration(NMigrations::gc_SchemaVersions, &Version1ID);
			DMibExpect(UpgradePostgresPlan.m_Warnings.f_GetLen(), ==, umint(0));
			DMibExpect(UpgradePostgresPlan.m_Statements.f_GetLen(), ==, umint(12));
			if (UpgradePostgresPlan.m_Statements.f_GetLen() == 12)
			{
				DMibExpect(UpgradePostgresPlan.m_Statements[1], ==, CStr("ALTER TABLE \"users\" ADD COLUMN \"admin\" BOOLEAN NOT NULL DEFAULT FALSE"));
				DMibExpect(UpgradePostgresPlan.m_Statements[3], ==, CStr("ALTER TABLE \"users\" DROP CONSTRAINT IF EXISTS \"users_admin_bool\""));
				DMibExpect(UpgradePostgresPlan.m_Statements[4], ==, CStr("ALTER TABLE \"users\" ADD CONSTRAINT \"users_admin_bool\" CHECK (admin IN (FALSE, TRUE))"));
				DMibExpect
					(
						UpgradePostgresPlan.m_Statements[11]
						, ==
						, "INSERT INTO \"schema_migrations\" (\"id\", \"name\", \"checksum\", \"applied_by_version\") VALUES ($1, $2, $3, $4) ON CONFLICT (\"id\") DO NOTHING"
					)
				;
			}

			co_return {};
		};

		DMibTestSuite("Foreign keys")
		{
			auto ConstraintDescriptions = gc_SessionTable.f_DescribeConstraints();
			DMibExpect(ConstraintDescriptions.f_GetLen(), ==, umint(1));
			DMibExpect(ConstraintDescriptions[0].f_Name(), ==, CStr("sessions_user_id_users_fk"));
			DMibExpect(ConstraintDescriptions[0].m_Type, ==, ESqlConstraintType::mc_ForeignKey);
			DMibExpect(ConstraintDescriptions[0].m_Columns.f_GetLen(), ==, umint(1));
			DMibExpect(*ConstraintDescriptions[0].m_Columns[0], ==, CStr("user_id"));
			DMibExpect(ConstraintDescriptions[0].f_ReferencedTable(), ==, CStr("users"));
			DMibExpect(ConstraintDescriptions[0].m_ReferencedColumns.f_GetLen(), ==, umint(1));
			DMibExpect(*ConstraintDescriptions[0].m_ReferencedColumns[0], ==, CStr("id"));
			DMibExpect(ConstraintDescriptions[0].m_OnDelete, ==, ESqlForeignKeyAction::mc_Cascade);
		};

		DMibTestSuite("Composite primary key")
		{
			auto ConstraintDescriptions = gc_UserRoleTable.f_DescribeConstraints();
			DMibExpect(ConstraintDescriptions.f_GetLen(), ==, umint(2));
			DMibExpect(ConstraintDescriptions[0].f_Name(), ==, CStr("user_roles_user_id_role_pk"));
			DMibExpect(ConstraintDescriptions[0].m_Type, ==, ESqlConstraintType::mc_PrimaryKey);
			DMibExpect(ConstraintDescriptions[0].m_Columns.f_GetLen(), ==, umint(2));
			DMibExpect(*ConstraintDescriptions[0].m_Columns[0], ==, CStr("user_id"));
			DMibExpect(*ConstraintDescriptions[0].m_Columns[1], ==, CStr("role"));
		};

		DMibTestSuite("PostgreSQL subquery limit offset parameters")
		{
			auto Description = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPeopleByEmailLikeWithLimitedProfileDisplayNameSubquery.f_Describe();
			CStr Sql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(Description, Description.m_Predicate.m_nParameters);

			DMibExpect(Sql.f_Find("\"email\" LIKE $1"), >=, aint(0));
			DMibExpect(Sql.f_Find("$2 LIMIT $3 OFFSET $4"), >=, aint(0));

			auto HavingDescription = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPeopleByEmailLikeWithHavingLimitedProfileEmailSubquery.f_Describe();
			CStr HavingSql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(HavingDescription, HavingDescription.m_Predicate.m_nParameters);

			DMibExpect(HavingSql.f_Find("\"email\" LIKE $1"), >=, aint(0));
			DMibExpect(HavingSql.f_Find("\"email\" LIKE $2"), >=, aint(0));
			DMibExpect(HavingSql.f_Find("HAVING COUNT(*) > $3 LIMIT $4 OFFSET $5"), >=, aint(0));
		};

		DMibTestSuite("PostgreSQL subquery parameter offsets in compound predicates and having")
		{
			auto LimitedDescription = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPeopleInProfileSubqueryAndEmailLikeLimited.f_Describe();
			CStr LimitedSql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(LimitedDescription, LimitedDescription.m_Predicate.m_nParameters);

			DMibExpect(LimitedSql.f_Find("\"display_name\" = $1"), >=, aint(0));
			DMibExpect(LimitedSql.f_Find("\"email\" LIKE $2"), >=, aint(0));
			DMibExpect(LimitedSql.f_Find("LIMIT $3 OFFSET $4"), >=, aint(0));

			auto HavingDescription = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPeopleInProfileSubqueryGroupedHaving.f_Describe();
			CStr HavingSql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(HavingDescription, HavingDescription.m_Predicate.m_nParameters);

			DMibExpect(HavingSql.f_Find("\"display_name\" = $1"), >=, aint(0));
			DMibExpect(HavingSql.f_Find("HAVING COUNT(*) > $2"), >=, aint(0));

			// An IS NULL predicate binds no value, so it must report zero parameters; a following HAVING parameter
			// must then be $1, not $2.
			auto NullDescription = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectNullableTypesWithNullInteger.f_Describe();
			DMibExpect(NullDescription.m_Predicate.m_nParameters, ==, umint(0));

			auto NullHavingDescription = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectNullableNullIntegerGroupedHaving.f_Describe();
			CStr NullHavingSql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(NullHavingDescription, NullHavingDescription.m_Predicate.m_nParameters);
			DMibExpect(NullHavingSql.f_Find("HAVING COUNT(*) > $1"), >=, aint(0));
		};

		DMibTestSuite("PostgreSQL set operation parameters")
		{
			auto Description = NMib::NSQL::NTest::NDatabaseBackend::gc_UnionThreeParameterizedEmailSelects.f_Describe();
			CStr Sql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(Description, Description.m_Predicate.m_nParameters);

			DMibExpect(Sql.f_Find("\"email\" LIKE $1"), >=, aint(0));
			DMibExpect(Sql.f_Find("\"email\" LIKE $2"), >=, aint(0));
			DMibExpect(Sql.f_Find("\"display_name\" = $3"), >=, aint(0));
		};

		DMibTestSuite("PostgreSQL REAL sum accumulates in double precision")
		{
			auto Description = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectValueTypeFloat32Sum.f_Describe();
			CStr Sql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(Description, Description.m_Predicate.m_nParameters);
			DMibExpect(Sql.f_Find("SUM(\"float32_value\"::double precision)"), >=, aint(0));
		};

		DMibTestSuite("PostgreSQL distinct expression count exists projection")
		{
			auto Description = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectDistinctValueTypeExpressionsByKeyLike.f_Describe();
			CStr CountSql = NMib::NSQL::NPrivate::fg_PostgresSelectCountSql(Description);
			CStr ExistsSql = NMib::NSQL::NPrivate::fg_PostgresSelectExistsSql(Description);

			DMibExpect(CountSql.f_Find("SELECT DISTINCT (\"int32_value\"::bigint + \"int16_value\"), UPPER(\"key\") FROM"), >=, aint(0));
			DMibExpect(ExistsSql.f_Find("SELECT DISTINCT (\"int32_value\"::bigint + \"int16_value\"), UPPER(\"key\") FROM"), >=, aint(0));
			DMibExpect(CountSql.f_Find("SELECT DISTINCT  FROM"), ==, aint(-1));
			DMibExpect(ExistsSql.f_Find("SELECT DISTINCT  FROM"), ==, aint(-1));
		};

		DMibTestSuite("PostgreSQL raw command affected rows")
		{
			auto fParse = [](ch8 const *_pTag, umint &_nRows) -> NException::CExceptionPointer
				{
					CPostgresQueryResult Result;
					Result.m_CommandComplete = CPostgresCommandComplete{.m_Tag = CStr(_pTag)};
					return NMib::NSQL::NPrivate::fg_PostgresParseAffectedRows(Result, _nRows);
				}
			;

			// DDL CommandComplete tags can omit the row count entirely
			auto fExpectZero = [&](ch8 const *_pTag)
				{
					DMibTestPath(CStr(_pTag));
					umint nRows = 123;
					DMibExpect(bool(fParse(_pTag, nRows)), ==, false);
					DMibExpect(nRows, ==, umint(0));
				}
			;

			fExpectZero("CREATE TABLE");
			fExpectZero("CREATE INDEX");
			fExpectZero("DROP TABLE");
			fExpectZero("ALTER TABLE");
			fExpectZero("BEGIN");

			// Row-affecting commands still parse their trailing count; INSERT reports "INSERT oid rows" so the
			// affected count is the last token.
			auto fExpectCount = [&](ch8 const *_pTag, umint _nExpected)
				{
					DMibTestPath(CStr(_pTag));
					umint nRows = 0;
					DMibExpect(bool(fParse(_pTag, nRows)), ==, false);
					DMibExpect(nRows, ==, _nExpected);
				}
			;

			fExpectCount("INSERT 0 5", umint(5));
			fExpectCount("UPDATE 3", umint(3));
			fExpectCount("DELETE 7", umint(7));
			fExpectCount("DELETE 0", umint(0));
		};

		DMibTestSuite("PostgreSQL SUM bigint cast")
		{
			auto Int64Description = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectValueTypeInt64SumByKeyLike.f_Describe();
			CStr Int64Sql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(Int64Description, Int64Description.m_Predicate.m_nParameters);

			// SUM over a BIGINT column must be cast back to BIGINT so the declared int64 projection decodes;
			// PostgreSQL would otherwise widen the result to NUMERIC, which the integer decoder rejects.
			DMibExpect(Int64Sql.f_Find("CAST(SUM(\"int64_value\") AS BIGINT)"), >=, aint(0));

			auto Float64Description = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectValueTypeFloat64SumByKeyLike.f_Describe();
			CStr Float64Sql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(Float64Description, Float64Description.m_Predicate.m_nParameters);

			// SUM(double precision) already returns double precision
			DMibExpect(Float64Sql.f_Find("SUM(\"float64_value\"::double precision)"), >=, aint(0));
			DMibExpect(Float64Sql.f_Find("CAST(SUM(\"float64_value\""), ==, aint(-1));
		};

		DMibTestSuite("Insert default values for autoincrement-only table")
		{
			auto Operation = NMib::NSQL::NPrivate::fg_SqlInsertOperation(NDatabaseBackend::gc_IdOnlyTable, NDatabaseBackend::CIdOnlyRow{});

			CStr SqliteSql = NMib::NSQL::NPrivate::fg_SqliteInsertSql(Operation);
			DMibExpect(SqliteSql, ==, CStr("INSERT INTO \"id_only_default\" DEFAULT VALUES"));

			CStr PostgresSql = NMib::NSQL::NPrivate::fg_PostgresInsertSql(Operation);
			DMibExpect(PostgresSql, ==, CStr("INSERT INTO \"id_only_default\" DEFAULT VALUES"));
		};

		DMibTestSuite("Unique column flag emits UNIQUE constraint") -> NConcurrency::TCFuture<void>
		{
			auto Description = NDatabaseBackend::gc_UniqueColumnTable.f_Describe();

			// The fixture has no other UNIQUE declaration, so the token can only come from the code column
			CStr PostgresSql = NMib::NSQL::NPrivate::fg_PostgresCreateTable(Description, false);
			DMibExpect(PostgresSql.f_Find("UNIQUE"), >=, aint(0));

			CStr SqliteSql = co_await NMib::NSQL::NPrivate::fg_SqliteCreateTable(Description, false);
			DMibExpect(SqliteSql.f_Find("UNIQUE"), >=, aint(0));

			co_return {};
		};

		DMibTestSuite("Quoted identifiers escape embedded quotes") -> NConcurrency::TCFuture<void>
		{
			auto Description = gc_QuotedIdentifierSchemaVersion.f_Describe();

			auto PostgresStatements = NMib::NSQL::NPrivate::fg_PostgresCreateSchemaStatements(Description);
			DMibExpect(PostgresStatements.f_GetLen(), ==, umint(1));
			if (PostgresStatements.f_GetLen() == 1)
			{
				DMibExpect(PostgresStatements[0].f_Find("\"quoted\"\"table\""), >=, aint(0));
				DMibExpect(PostgresStatements[0].f_Find("\"quoted\"\"column\""), >=, aint(0));
			}

			auto SqliteStatements = co_await NMib::NSQL::NPrivate::fg_SqliteCreateSchemaStatements(Description);
			DMibExpect(SqliteStatements.f_GetLen(), ==, umint(1));
			if (SqliteStatements.f_GetLen() == 1)
			{
				DMibExpect(SqliteStatements[0].f_Find("\"quoted\"\"table\""), >=, aint(0));
				DMibExpect(SqliteStatements[0].f_Find("\"quoted\"\"column\""), >=, aint(0));
			}

			co_return {};
		};
	}

	DMibTestRegister(CDatabaseSchema_Tests, Malterlib::SQL);
}
