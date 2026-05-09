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

	struct CQuotedIdentifierRow
	{
		int64 m_Value = 0;
	};

	template <typename t_CMember>
	constexpr bool fg_TestCanDeclareSqlColumn()
	{
		return requires { fg_SqlColumn(gc_Str<"unsupported">, (t_CMember CUnsupportedColumnRow::*)nullptr); };
	}

	template <typename t_CMember>
	constexpr bool fg_TestCanDeclareSqlColumnWithExplicitType()
	{
		return requires { fg_SqlColumn(gc_Str<"unsupported">, (t_CMember CUnsupportedColumnRow::*)nullptr, ESqlColumnType::mc_Text); };
	}

	template <typename t_CRow>
	consteval ch8 const *fg_TestValidateDuplicateColumnTable()
	{
		constexpr auto Columns = fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"id">, &t_CRow::m_ID)
				, fg_SqlColumn(gc_Str<"id">, &t_CRow::m_Value)
			)
		;
		constexpr auto Indexes = fg_SqlIndexes();
		constexpr auto Constraints = fg_SqlConstraints();
		constexpr auto Table = fg_SqlTable<t_CRow>(gc_Str<"duplicate_columns">, Columns, Indexes, Constraints);
		constexpr auto Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"duplicate_column_database">, Table);

		return fg_SqlValidateDatabase(Database);
	}

	template <typename t_CRow>
	consteval ch8 const *fg_TestValidateInvalidIndexTable()
	{
		constexpr auto Columns = fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"id">, &t_CRow::m_ID)
				, fg_SqlColumn(gc_Str<"value">, &t_CRow::m_Value)
			)
		;
		constexpr auto Indexes = fg_SqlIndexes(fg_SqlIndex(gc_Str<"invalid_index_missing_column">, gc_Str<"missing_column">));
		constexpr auto Constraints = fg_SqlConstraints();
		constexpr auto Table = fg_SqlTable<t_CRow>(gc_Str<"invalid_index">, Columns, Indexes, Constraints);
		constexpr auto Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"invalid_index_database">, Table);

		return fg_SqlValidateDatabase(Database);
	}

	template <typename t_CRow>
	consteval ch8 const *fg_TestValidateInvalidAutoIncrementTable()
	{
		constexpr auto Columns = fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &t_CRow::m_ID, ESqlColumnFlag::mc_AutoIncrement));
		constexpr auto Indexes = fg_SqlIndexes();
		constexpr auto Constraints = fg_SqlConstraints();
		constexpr auto Table = fg_SqlTable<t_CRow>(gc_Str<"invalid_auto_increment">, Columns, Indexes, Constraints);
		constexpr auto Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"invalid_auto_increment_database">, Table);

		return fg_SqlValidateDatabase(Database);
	}

	template <typename t_CRow>
	consteval ch8 const *fg_TestValidateInvalidGeneratedDefaultTable()
	{
		constexpr auto Columns = fg_SqlColumns
			(
				fg_SqlColumn
				(
					gc_Str<"value">
					, &t_CRow::m_Value
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
		constexpr auto Indexes = fg_SqlIndexes();
		constexpr auto Constraints = fg_SqlConstraints();
		constexpr auto Table = fg_SqlTable<t_CRow>(gc_Str<"invalid_generated_default">, Columns, Indexes, Constraints);
		constexpr auto Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"invalid_generated_default_database">, Table);

		return fg_SqlValidateDatabase(Database);
	}

	template <typename t_CRow>
	consteval ch8 const *fg_TestValidateInvalidForeignKeyDatabase()
	{
		constexpr auto Columns = fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"id">, &t_CRow::m_ID)
				, fg_SqlColumn(gc_Str<"value">, &t_CRow::m_Value)
			)
		;
		constexpr auto Indexes = fg_SqlIndexes();
		constexpr auto Constraints = fg_SqlConstraints
			(
				fg_SqlForeignKey
				(
					gc_Str<"invalid_fk_missing_table">
					, fg_SqlColumnNames(gc_Str<"id">)
					, fg_SqlReferences(gc_Str<"missing_table">, gc_Str<"id">)
				)
			)
		;
		constexpr auto Table = fg_SqlTable<t_CRow>(gc_Str<"invalid_fk">, Columns, Indexes, Constraints);
		constexpr auto Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"invalid_fk_database">, Table);

		return fg_SqlValidateDatabase(Database);
	}

	template <typename t_CRow>
	consteval ch8 const *fg_TestValidateDuplicateTableDatabase()
	{
		constexpr auto Columns = fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"id">, &t_CRow::m_ID)
				, fg_SqlColumn(gc_Str<"value">, &t_CRow::m_Value)
			)
		;
		constexpr auto Indexes = fg_SqlIndexes();
		constexpr auto Constraints = fg_SqlConstraints();
		constexpr auto Table = fg_SqlTable<t_CRow>(gc_Str<"validation">, Columns, Indexes, Constraints);
		constexpr auto Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"duplicate_tables">, Table, Table);

		return fg_SqlValidateDatabase(Database);
	}

	template <typename t_CRow>
	consteval ch8 const *fg_TestValidateDuplicateIndexDatabase()
	{
		constexpr auto Columns = fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"id">, &t_CRow::m_ID)
				, fg_SqlColumn(gc_Str<"value">, &t_CRow::m_Value)
			)
		;
		// Two differently named tables that both declare an index named shared_index: distinct table names, but the
		// schema-wide index name collides.
		constexpr auto Indexes = fg_SqlIndexes(fg_SqlIndex(gc_Str<"shared_index">, gc_Str<"value">));
		constexpr auto Constraints = fg_SqlConstraints();
		constexpr auto TableA = fg_SqlTable<t_CRow>(gc_Str<"duplicate_index_a">, Columns, Indexes, Constraints);
		constexpr auto TableB = fg_SqlTable<t_CRow>(gc_Str<"duplicate_index_b">, Columns, Indexes, Constraints);
		constexpr auto Database = fg_SqlDatabase<ESqlDialect::mc_SQL1999>(gc_Str<"duplicate_index_database">, TableA, TableB);

		return fg_SqlValidateDatabase(Database);
	}

	constexpr auto gc_PostgresSpecificTable = fg_SqlTable<CPostgresSpecificSchemaRow>
		(
			gc_Str<"postgres_specific">
			, fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"pg_date">, &CPostgresSpecificSchemaRow::m_Date)
				, fg_SqlColumn(gc_Str<"pg_int_array">, &CPostgresSpecificSchemaRow::m_IntArray)
			)
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
	constexpr auto gc_ValidationColumns = fg_SqlColumns
		(
			fg_SqlColumn(gc_Str<"id">, &CSchemaValidationRow::m_ID)
			, fg_SqlColumn(gc_Str<"value">, &CSchemaValidationRow::m_Value)
		)
	;
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
			, fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"quoted\"column">, &CQuotedIdentifierRow::m_Value)
			)
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

	// Value-type dialect support mirrors the column-type tables.
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

	// cSqlDialectSupportsType lets call sites compile-time gate parameter types per dialect.
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
		void f_DoTests()
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
							// Initial adoption uses the additive generator (matching f_ApplySchema), so the table is created
							// with IF NOT EXISTS rather than a plain CREATE that would fail against an existing table.
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
				DMibExpect(UpgradeSqlitePlan.m_Statements.f_GetLen(), ==, umint(19));
				if (UpgradeSqlitePlan.m_Statements.f_GetLen() == 19)
				{
					// The rebuild no longer pre-drops the scratch name, so the rename/copy sequence starts one statement
					// earlier than before.
					DMibExpect(UpgradeSqlitePlan.m_Statements[1], ==, CStr("PRAGMA legacy_alter_table=ON"));
					DMibExpect(UpgradeSqlitePlan.m_Statements[3], ==, CStr("PRAGMA legacy_alter_table=OFF"));
					DMibExpect
						(
							UpgradeSqlitePlan.m_Statements[5]
							, ==
							, "INSERT INTO \"users\" (\"id\", \"email\", \"display_name\") SELECT \"id\", \"email\", \"display_name\" FROM \"__mib_rebuild_old_users\""
						)
					;
					DMibExpect
						(
							UpgradeSqlitePlan.m_Statements[18]
							, ==
							, "INSERT INTO \"schema_migrations\" (\"id\", \"name\", \"checksum\", \"applied_at\", \"applied_by_version\") VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)"
						)
					;
				}

				CStr RenameVersion1ID("rename_0001");
				CSqlSchemaMigrationPlan RenameRebuildSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_RenameRebuildSchemaVersions, &RenameVersion1ID);
				DMibExpect(RenameRebuildSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));
				DMibExpect(RenameRebuildSqlitePlan.m_Statements.f_GetLen(), ==, umint(10));
				if (RenameRebuildSqlitePlan.m_Statements.f_GetLen() == 10)
				{
					DMibExpect(RenameRebuildSqlitePlan.m_Statements[1], ==, CStr("ALTER TABLE \"legacy_sessions\" RENAME TO \"sessions_renamed\""));
					DMibExpect(RenameRebuildSqlitePlan.m_Statements[2], ==, CStr("ALTER TABLE \"sessions_renamed\" RENAME COLUMN \"old_token\" TO \"token\""));
					DMibExpect
						(
							RenameRebuildSqlitePlan.m_Statements[7]
							, ==
							, "INSERT INTO \"sessions_renamed\" (\"id\", \"token\") SELECT \"id\", \"token\" FROM \"__mib_rebuild_old_sessions_renamed\""
						)
					;
				}

				CStr BackendDefaultVersion1ID("backend_default_0001");
				CSqlSchemaMigrationPlan BackendDefaultSqlitePlan = co_await fg_SqlitePlanSchemaMigration(NDatabaseBackend::gc_BackendDefaultSchemaVersions, &BackendDefaultVersion1ID);
				DMibExpect(BackendDefaultSqlitePlan.m_Warnings.f_GetLen(), ==, umint(0));
				DMibExpect(BackendDefaultSqlitePlan.m_Statements.f_GetLen(), ==, umint(3));
				if (BackendDefaultSqlitePlan.m_Statements.f_GetLen() == 3)
				{
					DMibExpect
						(
							BackendDefaultSqlitePlan.m_Statements[1]
							, ==
							, CStr("ALTER TABLE \"backend_default_test\" ADD COLUMN \"backend_value\" TEXT NOT NULL DEFAULT 'sqlite-default'")
						)
					;
				}

				// A rebuild whose target adds a plain column must not make the SQLite migration plan emit a duplicate
				// ALTER TABLE ADD COLUMN: the rebuild already recreates rebuild_add_column with the note column, so the
				// additive planning pass must treat it as present. Applying a plan that re-added the column would fail
				// with a duplicate-column error.
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

				// A drop-column migration whose target also adds a plain column must not make the SQLite plan emit a
				// duplicate ALTER TABLE ADD COLUMN: SQLite drops a column by rebuilding drop_add_test from the target,
				// which already recreates it with the note column, so the additive planning pass must treat note as
				// present. Applying a plan that re-added the column would fail with a duplicate-column error.
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

				// Renaming a table referenced by a foreign key must produce a minimal plan: the parent is renamed and
				// the referencing child table is not rebuilt. The planner rewrites the child foreign key's referenced
				// table to the new name in its planned schema (mirroring the PostgreSQL planner) so the additive
				// planning pass keeps comparing against a consistent schema.
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

				// Rebuilding a referenced auto-increment table must, in the plan as at runtime, drop the child foreign
				// keys before the rename and recreate them afterwards (otherwise the DROP of the renamed copy fails
				// because child constraints still depend on it) and reset the rebuilt table's owned sequence.
				CStr FkParentRebuildVersion1ID("fk_0001");
				CSqlSchemaMigrationPlan FkParentRebuildPostgresPlan = co_await fg_PostgresPlanSchemaMigration(NDatabaseBackend::gc_FkParentRebuildSchemaVersions, &FkParentRebuildVersion1ID);
				DMibExpect(FkParentRebuildPostgresPlan.m_Warnings.f_GetLen(), ==, umint(0));

				aint iDropChildFk = -1;
				aint iRename = -1;
				aint iSequenceReset = -1;
				aint iAddChildFk = -1;
				for (umint i = 0; i < FkParentRebuildPostgresPlan.m_Statements.f_GetLen(); ++i)
				{
					NStr::CStr const &Statement = FkParentRebuildPostgresPlan.m_Statements[i];
					if (Statement.f_Find("DROP CONSTRAINT \"fk_child_parent_fk\"") >= aint(0))
						iDropChildFk = aint(i);
					if (Statement.f_Find("ALTER TABLE \"fk_parent\" RENAME TO \"__mib_rebuild_old_fk_parent\"") >= aint(0))
						iRename = aint(i);
					if (Statement.f_Find("setval(pg_get_serial_sequence") >= aint(0))
						iSequenceReset = aint(i);
					if (Statement.f_Find("ADD CONSTRAINT \"fk_child_parent_fk\"") >= aint(0))
						iAddChildFk = aint(i);
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
				DMibExpect(UpgradePostgresPlan.m_Statements.f_GetLen(), ==, umint(11));
				if (UpgradePostgresPlan.m_Statements.f_GetLen() == 11)
				{
					DMibExpect(UpgradePostgresPlan.m_Statements[1], ==, CStr("ALTER TABLE \"users\" ADD COLUMN \"admin\" BOOLEAN NOT NULL DEFAULT FALSE"));
					DMibExpect(UpgradePostgresPlan.m_Statements[3], ==, CStr("ALTER TABLE \"users\" ADD CONSTRAINT \"users_admin_bool\" CHECK (admin IN (FALSE, TRUE))"));
					DMibExpect
						(
							UpgradePostgresPlan.m_Statements[10]
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

				// The subquery uses $1 and the outer email LIKE uses $2, so the outer LIMIT/OFFSET must be $3/$4.
				// Undercounting the subquery parameter would make LIMIT reuse $2.
				DMibExpect(LimitedSql.f_Find("\"display_name\" = $1"), >=, aint(0));
				DMibExpect(LimitedSql.f_Find("\"email\" LIKE $2"), >=, aint(0));
				DMibExpect(LimitedSql.f_Find("LIMIT $3 OFFSET $4"), >=, aint(0));

				auto HavingDescription = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPeopleInProfileSubqueryGroupedHaving.f_Describe();
				CStr HavingSql = NMib::NSQL::NPrivate::fg_PostgresSelectSql(HavingDescription, HavingDescription.m_Predicate.m_nParameters);

				// The subquery uses $1, so the HAVING placeholder must be $2 rather than reusing $1.
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

			DMibTestSuite("PostgreSQL distinct expression count exists projection")
			{
				auto Description = NMib::NSQL::NTest::NDatabaseBackend::gc_SelectDistinctValueTypeExpressionsByKeyLike.f_Describe();
				CStr CountSql = NMib::NSQL::NPrivate::fg_PostgresSelectCountSql(Description);
				CStr ExistsSql = NMib::NSQL::NPrivate::fg_PostgresSelectExistsSql(Description);

				DMibExpect(CountSql.f_Find("SELECT DISTINCT (\"int32_value\" + \"int16_value\"), UPPER(\"key\") FROM"), >=, aint(0));
				DMibExpect(ExistsSql.f_Find("SELECT DISTINCT (\"int32_value\" + \"int16_value\"), UPPER(\"key\") FROM"), >=, aint(0));
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
				};

				// DDL and other utility statements report only a command name with no numeric suffix. A successful
				// raw CREATE TABLE through f_ExecuteRaw must report zero affected rows, not parse "TABLE" as a count
				// and fail an already-committed statement.
				auto fExpectZero = [&](ch8 const *_pTag)
				{
					DMibTestPath(CStr(_pTag));
					umint nRows = 123;
					DMibExpect(bool(fParse(_pTag, nRows)), ==, false);
					DMibExpect(nRows, ==, umint(0));
				};

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
				};

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

				// SUM over a floating-point column keeps its double-precision result and must not be cast.
				DMibExpect(Float64Sql.f_Find("SUM(\"float64_value\")"), >=, aint(0));
				DMibExpect(Float64Sql.f_Find("CAST(SUM(\"float64_value\")"), ==, aint(-1));
			};

			DMibTestSuite("Insert default values for autoincrement-only table")
			{
				auto Operation = NMib::NSQL::NPrivate::fg_SqlInsertOperation(NDatabaseBackend::gc_IdOnlyTable, NDatabaseBackend::CIdOnlyRow{});

				// The only column is an autoincrement primary key, so there is nothing to insert; both backends
				// must emit DEFAULT VALUES instead of the invalid "() VALUES ()".
				CStr SqliteSql = NMib::NSQL::NPrivate::fg_SqliteInsertSql(Operation);
				DMibExpect(SqliteSql, ==, CStr("INSERT INTO \"id_only_default\" DEFAULT VALUES"));

				CStr PostgresSql = NMib::NSQL::NPrivate::fg_PostgresInsertSql(Operation);
				DMibExpect(PostgresSql, ==, CStr("INSERT INTO \"id_only_default\" DEFAULT VALUES"));
			};

			DMibTestSuite("Unique column flag emits UNIQUE constraint") -> NConcurrency::TCFuture<void>
			{
				auto Description = NDatabaseBackend::gc_UniqueColumnTable.f_Describe();

				// A column-level mc_Unique flag must surface as a UNIQUE constraint in the generated DDL so the
				// database enforces the uniqueness the schema checksum records. The id column is a primary key,
				// so the only UNIQUE in the statement comes from the flagged code column.
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
	};

	DMibTestRegister(CDatabaseSchema_Tests, Malterlib::SQL);
}
