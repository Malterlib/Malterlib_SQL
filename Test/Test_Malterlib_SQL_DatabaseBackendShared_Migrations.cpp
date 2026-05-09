// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_Version1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			auto Version1UserID = co_await Version1Database.template f_InsertReturning<&NMib::NSQL::NTest::NVersion1::CUserRow::m_ID, gc_InsertVersion1User>
				(
					NStr::CStr("migration-user@example.com")
					, NStorage::TCOptional<NStr::CStr>()
				)
			;

			NMib::NSQL::NTest::NVersion1::CSessionRow Version1Session;
			Version1Session.m_UserID = Version1UserID;
			Version1Session.m_Token = "migration-token";
			co_await Version1Database.f_Insert(NMib::NSQL::NTest::NVersion1::gc_SessionTable, fg_Move(Version1Session));

			DMibTestCategory("Before migration data check") -> NConcurrency::TCFuture<void>
			{
				auto Rows = Version1Database.template f_Query<gc_SelectVersion1SessionByToken>(NStr::CStr("migration-token"));
				umint nRows = 0;

				for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				{
					for (auto const &pRow : *iBatch)
					{
						++nRows;
						DMibExpect(pRow->m_Data.m_UserID, ==, Version1UserID)(ETestFlag_Aggregated);
						DMibExpect(pRow->m_Data.m_Token, ==, NStr::CStr("migration-token"))(ETestFlag_Aggregated);
					}
				}

				DMibExpect(nRows, ==, umint(1));

				co_return {};
			};
		}

		CSqlDatabaseClient LatestDatabase(_fCreateBackend(&NMib::NSQL::NTest::NMigrations::gc_SchemaVersions));
		co_await LatestDatabase.f_Open();
		co_await LatestDatabase.f_ApplySchema();

		DMibTestCategory("After migration data check") -> NConcurrency::TCFuture<void>
		{
			auto Rows = LatestDatabase.template f_Query<gc_SelectLatestSessionByToken>(NStr::CStr("migration-token"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_UserID, ==, uint64(1))(ETestFlag_Aggregated);
					DMibExpect(pRow->m_Data.m_Token, ==, NStr::CStr("migration-token"))(ETestFlag_Aggregated);
				}
			}

			DMibExpect(nRows, ==, umint(1));

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRenameMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RenameVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CRenameVersion1Row Row;
			Row.m_OldToken = "rename-token";
			co_await Version1Database.f_Insert(gc_RenameVersion1Table, fg_Move(Row));

			DMibTestCategory("Before rename migration data check") -> NConcurrency::TCFuture<void>
			{
				auto Rows = Version1Database.template f_Query<gc_SelectRenameVersion1ByToken>(NStr::CStr("rename-token"));
				umint nRows = 0;

				for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				{
					for (auto const &pRow : *iBatch)
					{
						++nRows;
						DMibExpect(pRow->m_Data.m_OldToken, ==, NStr::CStr("rename-token"))(ETestFlag_Aggregated);
					}
				}

				DMibExpect(nRows, ==, umint(1));

				co_return {};
			};
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RenameSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("After rename migration data check") -> NConcurrency::TCFuture<void>
		{
			auto Rows = Version2Database.template f_Query<gc_SelectRenameVersion2ByToken>(NStr::CStr("rename-token"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_Token, ==, NStr::CStr("rename-token"))(ETestFlag_Aggregated);
				}
			}

			DMibExpect(nRows, ==, umint(1));

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRenameConstrainedMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RenameConstrainedVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CRenameConstrainedVersion1Row Row;
			Row.m_Token = "rename-constrained-token";
			co_await Version1Database.f_Insert(gc_RenameConstrainedVersion1Table, fg_Move(Row));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RenameConstrainedSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("After constrained rename migration data check") -> NConcurrency::TCFuture<void>
		{
			auto Rows = Version2Database.template f_Query<gc_SelectRenameConstrainedVersion2ByToken>(NStr::CStr("rename-constrained-token"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_Token, ==, NStr::CStr("rename-constrained-token"))(ETestFlag_Aggregated);
				}
			}

			DMibExpect(nRows, ==, umint(1));

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseUnknownVersionValidation(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RenameVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();
		}

		CSqlDatabaseClient UnknownVersionDatabase(_fCreateBackend(&gc_SchemaVersions));
		co_await UnknownVersionDatabase.f_Open();

		auto ApplySchemaResult = co_await UnknownVersionDatabase.f_ApplySchema().f_Wrap();

		DMibExpect(bool(ApplySchemaResult), ==, false);

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseChecksumValidation(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Database(_fCreateBackend(&gc_SchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();
		}

		CSqlDatabaseClient ChecksumMismatchDatabase(_fCreateBackend(&gc_ChecksumMismatchSchemaVersions));
		co_await ChecksumMismatchDatabase.f_Open();

		auto ApplySchemaResult = co_await ChecksumMismatchDatabase.f_ApplySchema().f_Wrap();

		DMibExpect(bool(ApplySchemaResult), ==, false);

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRequiredColumnValidation(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RequiredColumnVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RequiredColumnSchemaVersions));
		co_await Version2Database.f_Open();

		auto ApplySchemaResult = co_await Version2Database.f_ApplySchema().f_Wrap();

		DMibExpect(bool(ApplySchemaResult), ==, false);

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RebuildVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CRebuildVersion1Row Row;
			Row.m_Token = "rebuild-token";
			Row.m_LegacyValue = "legacy-value";
			co_await Version1Database.f_Insert(gc_RebuildVersion1Table, fg_Move(Row));

			DMibTestCategory("Before rebuild migration data check") -> NConcurrency::TCFuture<void>
			{
				auto Rows = Version1Database.template f_Query<gc_SelectRebuildVersion1ByToken>(NStr::CStr("rebuild-token"));
				umint nRows = 0;

				for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				{
					for (auto const &pRow : *iBatch)
					{
						++nRows;
						DMibExpect(pRow->m_Data.m_Token, ==, NStr::CStr("rebuild-token"))(ETestFlag_Aggregated);
						DMibExpect(pRow->m_Data.m_LegacyValue, ==, NStr::CStr("legacy-value"))(ETestFlag_Aggregated);
					}
				}

				DMibExpect(nRows, ==, umint(1));

				co_return {};
			};
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RebuildSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("After rebuild migration data check") -> NConcurrency::TCFuture<void>
		{
			auto Rows = Version2Database.template f_Query<gc_SelectRebuildVersion2ByToken>(NStr::CStr("rebuild-token"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_Token, ==, NStr::CStr("rebuild-token"))(ETestFlag_Aggregated);
				}
			}

			DMibExpect(nRows, ==, umint(1));

			co_return {};
		};

		DMibTestCategory("Insert after rebuild does not reuse copied ids") -> NConcurrency::TCFuture<void>
		{
			// PostgreSQL does not advance a BIGSERIAL sequence when the rebuild copies rows with explicit IDs
			CRebuildVersion2Row NewRow;
			NewRow.m_Token = "post-rebuild-token";
			co_await Version2Database.f_Insert(gc_RebuildVersion2Table, fg_Move(NewRow));

			umint nRows = co_await Version2Database.template f_Count<gc_SelectRebuildVersion2ByToken>(NStr::CStr("post-rebuild-token"));
			DMibExpect(nRows, ==, umint(1));

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildScratchNameCollision(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RebuildVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CRebuildVersion1Row Row;
			Row.m_Token = "scratch-collision-token";
			Row.m_LegacyValue = "legacy-value";
			co_await Version1Database.f_Insert(gc_RebuildVersion1Table, fg_Move(Row));

			// A legitimate user table happens to share the deterministic rebuild scratch name for rebuild_test.
			co_await Version1Database.f_QueryRaw(NStr::CStr("CREATE TABLE \"__mib_rebuild_old_rebuild_test\" (id INTEGER)"));
			co_await Version1Database.f_QueryRaw(NStr::CStr("INSERT INTO \"__mib_rebuild_old_rebuild_test\" (id) VALUES (4242)"));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RebuildSchemaVersions));
		co_await Version2Database.f_Open();

		auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
		DMibExpect(bool(ApplyResult), ==, false)(ETestFlag_Aggregated);

		DMibTestCategory("Colliding user table survives the aborted rebuild") -> NConcurrency::TCFuture<void>
		{
			auto DecoyResult = co_await Version2Database.f_QueryRaw(NStr::CStr("SELECT id FROM \"__mib_rebuild_old_rebuild_test\"")).f_Wrap();
			DMibExpect(bool(DecoyResult), ==, true)(ETestFlag_Aggregated);
			if (DecoyResult)
				DMibExpect(DecoyResult->m_Rows.f_GetLen(), ==, umint(1))(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildAddsForeignKeyToNewTable(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RebuildFkV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertRebuildFkItemV1>(NStr::CStr("rebuild-fk-token"));
		}

		// The new foreign key points to a table that does not exist yet when the rebuild starts
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RebuildFkSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Rebuilt row preserved and deferred foreign key enforced") -> NConcurrency::TCFuture<void>
		{
			umint nItems = co_await Version2Database.template f_Count<gc_SelectRebuildFkItemByToken>(NStr::CStr("rebuild-fk-token"));
			DMibExpect(nItems, ==, umint(1))(ETestFlag_Aggregated);

			co_await Version2Database.template f_Insert<gc_InsertRebuildFkCategory>(NStr::CStr("category-1"));

			auto ValidResult = co_await Version2Database.template f_Insert<gc_InsertRebuildFkItemV2>(NStr::CStr("valid-item"), NStorage::TCOptional<int64>(int64(1))).f_Wrap();
			DMibExpect(bool(ValidResult), ==, true)(ETestFlag_Aggregated);

			auto OrphanResult = co_await Version2Database.template f_Insert<gc_InsertRebuildFkItemV2>(NStr::CStr("orphan-item"), NStorage::TCOptional<int64>(int64(999999))).f_Wrap();
			DMibExpect(bool(OrphanResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAddUniqueColumnMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_UniqueAddV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			// Two existing rows give the new UNIQUE column two NULLs
			co_await Version1Database.template f_Insert<gc_InsertUniqueAddV1>(NStr::CStr("first"));
			co_await Version1Database.template f_Insert<gc_InsertUniqueAddV1>(NStr::CStr("second"));
		}

		// v2 adds a nullable UNIQUE column. SQLite cannot ALTER TABLE ADD COLUMN a UNIQUE column, so the additive
		// migration must rebuild the table; otherwise applying the schema fails.
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_UniqueAddSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Unique column added and enforced") -> NConcurrency::TCFuture<void>
		{
			umint nFirst = co_await Version2Database.template f_Count<gc_SelectUniqueAddByName>(NStr::CStr("first"));
			DMibExpect(nFirst, ==, umint(1))(ETestFlag_Aggregated);

			co_await Version2Database.template f_Insert<gc_InsertUniqueAddV2>(NStr::CStr("third"), NStorage::TCOptional<NStr::CStr>(NStr::CStr("CODE")));

			auto DuplicateResult = co_await Version2Database.template f_Insert<gc_InsertUniqueAddV2>(NStr::CStr("fourth"), NStorage::TCOptional<NStr::CStr>(NStr::CStr("CODE"))).f_Wrap();
			DMibExpect(bool(DuplicateResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseGeneratedColumn(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_GenColV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertGenCol>(NStr::CStr("HELLO"));

			DMibTestCategory("Generated column computed on insert") -> NConcurrency::TCFuture<void>
			{
				auto Row = co_await Version1Database.template f_QueryOne<gc_SelectGenColV1ByValue>(NStr::CStr("HELLO"));
				DMibExpect(bool(Row.m_ValueLower), ==, true)(ETestFlag_Aggregated);
				if (Row.m_ValueLower)
					DMibExpect(*Row.m_ValueLower, ==, NStr::CStr("hello"))(ETestFlag_Aggregated);

				co_return {};
			};
		}

		// v2 adds a plain column. The additive sync must detect the existing generated column (table_xinfo on SQLite)
		// and not try to re-add it, which would fail.
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_GenColSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Generated column survives additive migration") -> NConcurrency::TCFuture<void>
		{
			auto Row = co_await Version2Database.template f_QueryOne<gc_SelectGenColV2ByValue>(NStr::CStr("HELLO"));
			DMibExpect(bool(Row.m_ValueLower), ==, true)(ETestFlag_Aggregated);
			if (Row.m_ValueLower)
				DMibExpect(*Row.m_ValueLower, ==, NStr::CStr("hello"))(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Column generated only on another backend is rejected") -> NConcurrency::TCFuture<void>
		{
			CSqlDatabaseClient OtherBackendDatabase(_fCreateBackend(&gc_GenColOtherBackendSchemaVersions));
			co_await OtherBackendDatabase.f_Open();
			auto ApplySchemaResult = co_await OtherBackendDatabase.f_ApplySchema().f_Wrap();
			DMibExpect(bool(ApplySchemaResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseGeneratedColumnInsertPaths(FCreateBackend _fCreateBackend)
	{
		CSqlDatabaseClient Database(_fCreateBackend(&gc_GenColUpsertSchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		DMibTestCategory("Whole-row insert omits the generated column") -> NConcurrency::TCFuture<void>
		{
			// A stale generated value tells recomputation apart from copying
			CGenColUpsertRow Row;
			Row.m_Value = NStr::CStr("WHOLEROW");
			Row.m_ValueLower = NStr::CStr("ignored-stale-value");
			Row.m_Note = NStr::CStr("first");
			co_await Database.f_Insert(gc_GenColUpsertTable, fg_Move(Row));

			auto Stored = co_await Database.template f_QueryOne<gc_SelectGenColUpsertByValue>(NStr::CStr("WHOLEROW"));
			DMibExpect(bool(Stored.m_ValueLower), ==, true)(ETestFlag_Aggregated);
			if (Stored.m_ValueLower)
				DMibExpect(*Stored.m_ValueLower, ==, NStr::CStr("wholerow"))(ETestFlag_Aggregated);
			DMibExpect(Stored.m_Note, ==, NStr::CStr("first"))(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Upsert binds only the implicit insert columns") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Upsert<gc_UpsertGenColNoteByValue>(NStr::CStr("UPSERT"), NStr::CStr("inserted"));

			auto Inserted = co_await Database.template f_QueryOne<gc_SelectGenColUpsertByValue>(NStr::CStr("UPSERT"));
			DMibExpect(Inserted.m_Note, ==, NStr::CStr("inserted"))(ETestFlag_Aggregated);
			DMibExpect(bool(Inserted.m_ValueLower), ==, true)(ETestFlag_Aggregated);
			if (Inserted.m_ValueLower)
				DMibExpect(*Inserted.m_ValueLower, ==, NStr::CStr("upsert"))(ETestFlag_Aggregated);

			co_await Database.template f_Upsert<gc_UpsertGenColNoteByValue>(NStr::CStr("UPSERT"), NStr::CStr("updated"));

			auto Updated = co_await Database.template f_QueryOne<gc_SelectGenColUpsertByValue>(NStr::CStr("UPSERT"));
			DMibExpect(Updated.m_Note, ==, NStr::CStr("updated"))(ETestFlag_Aggregated);
			DMibExpect(bool(Updated.m_ValueLower), ==, true)(ETestFlag_Aggregated);
			if (Updated.m_ValueLower)
				DMibExpect(*Updated.m_ValueLower, ==, NStr::CStr("upsert"))(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseGeneratedColumnRebuildMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_GenColMigrateV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertGenColMigrateV1>(NStr::CStr("MIGRATE"));
		}

		// SQLite must rebuild this table, which has a row, to add a STORED generated column
		{
			CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_GenColMigrateV2SchemaVersions));
			co_await Version2Database.f_Open();
			co_await Version2Database.f_ApplySchema();

			DMibTestCategory("Migration adds a generated column") -> NConcurrency::TCFuture<void>
			{
				auto Row = co_await Version2Database.template f_QueryOne<gc_SelectGenColMigrateV2ByValue>(NStr::CStr("MIGRATE"));
				DMibExpect(bool(Row.m_ValueLower), ==, true)(ETestFlag_Aggregated);
				if (Row.m_ValueLower)
					DMibExpect(*Row.m_ValueLower, ==, NStr::CStr("migrate"))(ETestFlag_Aggregated);

				co_return {};
			};
		}

		// The generated column exists before this rebuild, so copying it instead of recomputing it would fail
		CSqlDatabaseClient Version3Database(_fCreateBackend(&gc_GenColMigrateV3SchemaVersions));
		co_await Version3Database.f_Open();
		co_await Version3Database.f_ApplySchema();

		DMibTestCategory("Rebuild of a table with a generated column preserves data") -> NConcurrency::TCFuture<void>
		{
			auto Row = co_await Version3Database.template f_QueryOne<gc_SelectGenColMigrateV2ByValue>(NStr::CStr("MIGRATE"));
			DMibExpect(Row.m_Value, ==, NStr::CStr("MIGRATE"))(ETestFlag_Aggregated);
			DMibExpect(bool(Row.m_ValueLower), ==, true)(ETestFlag_Aggregated);
			if (Row.m_ValueLower)
				DMibExpect(*Row.m_ValueLower, ==, NStr::CStr("migrate"))(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildKeepsTableGuarantees(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RebuildKeepV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertRebuildKeepV1>(NStr::CStr("A"));
			co_await Version1Database.template f_Insert<gc_InsertRebuildKeepV1>(NStr::CStr("B"));
			int64 HighestID = co_await Version1Database.template f_InsertReturning<&CRebuildKeepV1Row::m_ID, gc_InsertRebuildKeepV1>(NStr::CStr("C"));
			DMibExpect(HighestID, ==, int64(3));
			// Deleting the highest id tells keeping the sequence from recomputing MAX(id)
			co_await Version1Database.f_ExecuteRaw(NStr::CStr("DELETE FROM rebuild_keep WHERE code = 'C'"));

			co_await Version1Database.f_ExecuteRaw(NStr::CStr("INSERT INTO rebuild_rename_all (old_id) VALUES (10), (11)"));

			co_await Version1Database.template f_Insert<gc_InsertRebuildKeepParent>(NStr::CStr("parent"));
			co_await Version1Database.template f_Insert<gc_InsertRebuildKeepChild>(NStr::CStr("parent"));
			co_await Version1Database.template f_Insert<gc_InsertRebuildKeepLongName>(NStr::CStr("long-name-value"));
		}

		{
			CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RebuildKeepV2SchemaVersions));
			co_await Version2Database.f_Open();

			DMibTestCategory("Migration adds a required generated column") -> NConcurrency::TCFuture<void>
			{
				co_await Version2Database.f_ApplySchema();

				auto Row = co_await Version2Database.template f_QueryOne<gc_SelectRebuildKeepV2ByCode>(NStr::CStr("A"));
				DMibExpect(Row.m_CodeLower, ==, NStr::CStr("a"));

				co_return {};
			};
		}

		CSqlDatabaseClient Version3Database(_fCreateBackend(&gc_RebuildKeepV3SchemaVersions));
		co_await Version3Database.f_Open();
		co_await Version3Database.f_ApplySchema();

		DMibTestCategory("Rebuild keeps the autoincrement high-water mark") -> NConcurrency::TCFuture<void>
		{
			int64 NextID = co_await Version3Database.template f_InsertReturning<&CRebuildKeepV2Row::m_ID, gc_InsertRebuildKeepV2>(NStr::CStr("D"));
			DMibExpect(NextID, ==, int64(4));

			co_return {};
		};

		DMibTestCategory("Rebuild keeps a named unique constraint") -> NConcurrency::TCFuture<void>
		{
			auto DuplicateResult = co_await Version3Database.template f_Insert<gc_InsertRebuildKeepV2>(NStr::CStr("A")).f_Wrap();
			DMibExpect(bool(DuplicateResult), ==, false);

			co_return {};
		};

		DMibTestCategory("Rebuild keeps foreign keys to a column unique through an index") -> NConcurrency::TCFuture<void>
		{
			auto MissingParentResult = co_await Version3Database.template f_Insert<gc_InsertRebuildKeepChild>(NStr::CStr("missing-parent")).f_Wrap();
			DMibExpect(bool(MissingParentResult), ==, false);

			CSqlRawResult Count = co_await Version3Database.f_QueryRaw(NStr::CStr("SELECT COUNT(*) FROM rebuild_keep_child"));
			DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

			co_return {};
		};

		DMibTestCategory("Rebuild of a table with a long name keeps its values") -> NConcurrency::TCFuture<void>
		{
			DMibExpect(co_await Version3Database.template f_Exists<gc_SelectRebuildKeepLongNameByValue>(NStr::CStr("long-name-value")), ==, true);

			co_return {};
		};

		DMibTestCategory("Rebuild without shared columns keeps the rows") -> NConcurrency::TCFuture<void>
		{
			CSqlRawResult Count = co_await Version3Database.f_QueryRaw(NStr::CStr("SELECT COUNT(*) FROM rebuild_rename_all"));
			DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseNarrowAutoIncrementKey(FCreateBackend _fCreateBackend)
	{
		CSqlDatabaseClient Database(_fCreateBackend(&gc_NarrowKeySchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		int32 ID = co_await Database.template f_InsertReturning<&CNarrowKeyRow::m_ID, gc_InsertNarrowKey>(NStr::CStr("narrow"));
		DMibExpect(ID, >, int32(0));

		auto Row = co_await Database.template f_GetByID<gc_NarrowKeyTable, &CNarrowKeyRow::m_ID>(ID);
		DMibExpect(bool(Row), ==, true);

		DMibExpect((co_await Database.template f_UpdateByID<gc_NarrowKeyTable, &CNarrowKeyRow::m_ID, &CNarrowKeyRow::m_Value>(NStr::CStr("narrow-updated"), ID)), ==, umint(1));
		DMibExpect((co_await Database.template f_DeleteByID<gc_NarrowKeyTable, &CNarrowKeyRow::m_ID>(ID)), ==, umint(1));

		{
			DMibTestPath("out-of-range ID");

			// 2^32 + ID would wrap to ID in the int32 key; building the operation rejects it
			NStorage::TCOptional<NConcurrency::TCFuture<umint>> OutOfRangeDelete;
			bool bRejected = false;
			try
			{
				OutOfRangeDelete = Database.template f_DeleteByID<gc_NarrowKeyTable, &CNarrowKeyRow::m_ID>(int64(4294967296) + ID);
			}
			catch (NException::CException const &)
			{
				bRejected = true;
			}

			if (OutOfRangeDelete)
			{
				auto DeleteResult = co_await fg_Move(*OutOfRangeDelete).f_Wrap();
				DMibExpect(bool(DeleteResult), ==, false);
			}
			else
				DMibExpect(bRejected, ==, true);
		}

		{
			auto Transaction = co_await Database.f_BeginTransaction();
			int32 TransactionID = co_await Transaction.template f_InsertReturning<&CNarrowKeyRow::m_ID, gc_InsertNarrowKey>(NStr::CStr("narrow-transaction"));
			umint nUpdated = co_await Transaction.template f_UpdateByID<gc_NarrowKeyTable, &CNarrowKeyRow::m_ID, &CNarrowKeyRow::m_Value>
				(
					NStr::CStr("narrow-transaction-updated")
					, TransactionID
				)
			;
			DMibExpect(nUpdated, ==, umint(1));
			DMibExpect((co_await Transaction.template f_DeleteByID<gc_NarrowKeyTable, &CNarrowKeyRow::m_ID>(TransactionID)), ==, umint(1));
			co_await Transaction.f_Commit();
		}

		DMibTestCategory("Prepared insert without values") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertKeyOnly>();
			int64 KeyOnlyID = co_await Database.template f_InsertReturning<&CKeyOnlyRow::m_ID, gc_InsertKeyOnly>();
			DMibExpect(KeyOnlyID, ==, int64(2));

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptExistingColumnConstraint(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_AdoptConstraintV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertAdoptConstraintV1>(NStr::CStr("dup"));

			co_await Version1Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
		}

		CSqlDatabaseClient AdoptingDatabase(_fCreateBackend(&gc_AdoptColumnConstraintSchemaVersions));
		co_await AdoptingDatabase.f_Open();
		co_await AdoptingDatabase.f_ApplySchema();

		DMibTestCategory("Adopted table enforces a column constraint") -> NConcurrency::TCFuture<void>
		{
			co_await AdoptingDatabase.template f_Insert<gc_InsertAdoptColumnConstraint>(NStr::CStr("unique-column-code"));

			auto DuplicateResult = co_await AdoptingDatabase.template f_Insert<gc_InsertAdoptColumnConstraint>(NStr::CStr("dup")).f_Wrap();
			DMibExpect(bool(DuplicateResult), ==, false);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseBackfillOfAddedColumn(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_BackfillV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertBackfillV1>(NStr::CStr("backfill"));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_BackfillV2SchemaVersions));
		co_await Version2Database.f_Open();

		DMibTestCategory("Backfill of a column the same version adds is rejected") -> NConcurrency::TCFuture<void>
		{
			auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
			DMibExpect(bool(ApplyResult), ==, false);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseIndexDefinitionChange(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_IndexChangeV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertIndexChangeV1>(NStr::CStr("code"));

			// A partial unique index under the declared name must not pass for the declared full unique index
			co_await Version1Database.f_ExecuteRaw(NStr::CStr("DROP INDEX idx_index_change_code"));
			co_await Version1Database.f_ExecuteRaw(NStr::CStr("CREATE UNIQUE INDEX idx_index_change_code ON index_change (code) WHERE code = 'other'"));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_IndexChangeV2SchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Changed index definition takes effect") -> NConcurrency::TCFuture<void>
		{
			auto DuplicateResult = co_await Version2Database.template f_Insert<gc_InsertIndexChangeV2>(NStr::CStr("code")).f_Wrap();
			DMibExpect(bool(DuplicateResult), ==, false);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptExistingTableConstraint(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_AdoptConstraintV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertAdoptConstraintV1>(NStr::CStr("dup"));

			// Removing only the version record forces adoption of the existing table
			co_await Version1Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_AdoptConstraintV2SchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Adopted table enforces the declared constraint") -> NConcurrency::TCFuture<void>
		{
			co_await Version2Database.template f_Insert<gc_InsertAdoptConstraintV2>(NStr::CStr("unique-code"));

			auto DuplicateResult = co_await Version2Database.template f_Insert<gc_InsertAdoptConstraintV2>(NStr::CStr("dup")).f_Wrap();
			DMibExpect(bool(DuplicateResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAddColumnWithExpressionDefault(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_AddDefaultV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertAddDefaultV1>(NStr::CStr("row-one"));
		}

		// SQLite cannot add a column with a nonconstant default to this table, which has a row, without a rebuild
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_AddDefaultV2SchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Column with expression default added to existing rows") -> NConcurrency::TCFuture<void>
		{
			auto Row = co_await Version2Database.template f_QueryOne<gc_SelectAddDefaultV2ByName>(NStr::CStr("row-one"));
			DMibExpect(Row.m_Name, ==, NStr::CStr("row-one"))(ETestFlag_Aggregated);
			DMibExpect(bool(Row.m_CreatedAt), ==, true)(ETestFlag_Aggregated);
			if (Row.m_CreatedAt)
				DMibExpect(Row.m_CreatedAt->f_IsEmpty(), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseSameShapedTablePreparedCache(FCreateBackend _fCreateBackend)
	{
		CSqlDatabaseClient Database(_fCreateBackend(&gc_SameShapeSchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		co_await Database.template f_Insert<gc_InsertSameShapeA>(NStr::CStr("a-value"));
		co_await Database.template f_Insert<gc_InsertSameShapeB>(NStr::CStr("b-value"));

		DMibTestCategory("Same-shaped tables keep separate prepared SQL") -> NConcurrency::TCFuture<void>
		{
			// Raw SQL bypasses the prepared cache under test
			auto CountA = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM same_shape_a"));
			DMibExpect(CountA.m_Rows.f_GetLen(), ==, umint(1))(ETestFlag_Aggregated);
			DMibExpect(CountA.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1))(ETestFlag_Aggregated);

			auto CountB = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM same_shape_b"));
			DMibExpect(CountB.m_Rows.f_GetLen(), ==, umint(1))(ETestFlag_Aggregated);
			DMibExpect(CountB.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1))(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnReusedConstraint(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_DropConstraintV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertDropConstraintV1>(NStr::CStr("code-one"), NStr::CStr("name-one"));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_DropConstraintV2SchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Reused constraint name applies to its new column") -> NConcurrency::TCFuture<void>
		{
			co_await Version2Database.template f_Insert<gc_InsertDropConstraintV2>(NStr::CStr("name-two"));

			auto DuplicateResult = co_await Version2Database.template f_Insert<gc_InsertDropConstraintV2>(NStr::CStr("name-one")).f_Wrap();
			DMibExpect(bool(DuplicateResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptUntrackedExistingTable(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptSchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			co_await Database.template f_Insert<gc_InsertAdopt>(NStr::CStr("seed"));

			co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
		}

		CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptSchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		DMibTestCategory("Existing untracked table is adopted, not recreated") -> NConcurrency::TCFuture<void>
		{
			auto AdoptedCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_existing"));
			DMibExpect(AdoptedCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1))(ETestFlag_Aggregated);

			co_await Database.template f_Insert<gc_InsertAdopt>(NStr::CStr("after-adoption"));

			auto FinalCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_existing"));
			DMibExpect(FinalCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2))(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Adoption rejects a column the schema does not declare") -> NConcurrency::TCFuture<void>
		{
			co_await Database.f_ExecuteRaw(NStr::CStr("ALTER TABLE adopt_existing ADD COLUMN extra_data TEXT"));
			co_await Database.f_ExecuteRaw(NStr::CStr("UPDATE adopt_existing SET extra_data = 'kept'"));
			co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));

			CSqlDatabaseClient ReadoptingDatabase(_fCreateBackend(&gc_AdoptSchemaVersions));
			co_await ReadoptingDatabase.f_Open();
			auto ApplySchemaResult = co_await ReadoptingDatabase.f_ApplySchema().f_Wrap();
			DMibExpect(bool(ApplySchemaResult), ==, false)(ETestFlag_Aggregated);

			auto KeptCount = co_await ReadoptingDatabase.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_existing WHERE extra_data = 'kept'"));
			DMibExpect(KeptCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2))(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptRejectsUndeclaredConstraints(FCreateBackend _fCreateBackend)
	{
		struct CAdoptionCase
		{
			ch8 const *m_pName;
			ch8 const *m_pCreateSql;
			ch8 const *m_pExtraSql;        // Run after the table is created, or nullptr
			ch8 const *m_pExpectedInError; // A part of the error naming the constraint, or nullptr when the adoption succeeds
		};

		static constexpr CAdoptionCase c_Cases[] =
			{
				{
					"Unique constraint"
					, "CREATE TABLE adopt_existing (id INTEGER PRIMARY KEY, value TEXT NOT NULL, CONSTRAINT adopt_extra_unique UNIQUE (value))"
					, nullptr
					, "UNIQUE (value)"
				}
				, {"Column check constraint", "CREATE TABLE adopt_existing (id INTEGER PRIMARY KEY, value TEXT NOT NULL CHECK (value <> 'forbidden'))", nullptr, "forbidden"}
				,
				{
					"Named table check constraint"
					, "CREATE TABLE adopt_existing (id INTEGER PRIMARY KEY, value TEXT NOT NULL, CONSTRAINT adopt_extra_check CHECK (length(value) > 0))"
					, nullptr
					, "adopt_extra_check"
				}
				,
				{
					"Foreign key"
					, "CREATE TABLE adopt_existing (id INTEGER PRIMARY KEY, value TEXT NOT NULL REFERENCES adopt_parent_codes (code))"
					, nullptr
					, "adopt_parent_codes"
				}
				, {"Primary key on another column", "CREATE TABLE adopt_existing (id INTEGER NOT NULL, value TEXT NOT NULL PRIMARY KEY)", nullptr, "PRIMARY KEY"}
				,
				{
					"Separately created unique index survives adoption"
					, "CREATE TABLE adopt_existing (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
					, "CREATE UNIQUE INDEX adopt_value_unique ON adopt_existing (value)"
					, nullptr
				}
			}
		;

		// A table is adopted when no version is recorded, and when a later version first declares it
		for (bool bLaterVersion : {false, true})
		{
			DMibTestPath(bLaterVersion ? "Later version" : "Initial version");

			auto const *pSetupVersions = bLaterVersion ? static_cast<ICSqlSchemaVersions const *>(&gc_AdoptLaterV1SchemaVersions) : &gc_AdoptSchemaVersions;
			auto const *pAdoptingVersions = bLaterVersion ? static_cast<ICSqlSchemaVersions const *>(&gc_AdoptLaterSchemaVersions) : &gc_AdoptSchemaVersions;

			for (auto const &Case : c_Cases)
			{
				DMibTestPath(NStr::CStr(Case.m_pName));

				{
					CSqlDatabaseClient Database(_fCreateBackend(pSetupVersions));
					co_await Database.f_Open();
					co_await Database.f_ApplySchema();

					co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE IF EXISTS adopt_existing"));
					co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE IF EXISTS adopt_parent_codes"));
					co_await Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE adopt_parent_codes (code TEXT PRIMARY KEY)"));
					co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_parent_codes (code) VALUES ('seed')"));
					co_await Database.f_ExecuteRaw(NStr::CStr(Case.m_pCreateSql));
					if (Case.m_pExtraSql)
						co_await Database.f_ExecuteRaw(NStr::CStr(Case.m_pExtraSql));
					co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_existing (id, value) VALUES (1, 'seed')"));
					if (!bLaterVersion)
						co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
				}

				CSqlDatabaseClient Database(_fCreateBackend(pAdoptingVersions));
				co_await Database.f_Open();

				auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
				if (Case.m_pExpectedInError)
				{
					DMibExpect(bool(ApplyResult), ==, false);
					if (!ApplyResult)
					{
						NStr::CStr Error = ApplyResult.f_GetExceptionStr();
						DMibExpect(Error.f_Find("does not declare") >= aint(0), ==, true);
						DMibExpect(Error.f_Find(Case.m_pExpectedInError) >= aint(0), ==, true);
					}
				}
				else
				{
					DMibExpect(bool(ApplyResult), ==, true);

					auto DuplicateResult = co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_existing (id, value) VALUES (2, 'seed')")).f_Wrap();
					DMibExpect(bool(DuplicateResult), ==, false);
				}

				auto Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_existing"));
				DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

				co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_existing"));
				co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE IF EXISTS adopt_later_marker"));
				co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
			}
		}

		DMibTestCategory("NOT NULL on a column the schema declares nullable") -> NConcurrency::TCFuture<void>
		{
			{
				CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptNullableSchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_nullable"));
				co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_nullable_parent (code) VALUES ('seed')"));
				co_await Database.f_ExecuteRaw
					(
						NStr::CStr("CREATE TABLE adopt_nullable (id INTEGER PRIMARY KEY, code TEXT NOT NULL, parent_code TEXT NOT NULL REFERENCES adopt_nullable_parent (code))")
					)
				;
				co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_nullable (id, code, parent_code) VALUES (1, 'live-code', 'seed')"));
				co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
			}

			CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptNullableSchemaVersions));
			co_await Database.f_Open();

			auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
			DMibExpect(bool(ApplyResult), ==, false);
			if (!ApplyResult)
			{
				NStr::CStr Error = ApplyResult.f_GetExceptionStr();
				DMibExpect(Error.f_Find("NOT NULL") >= aint(0), ==, true);
				DMibExpect(Error.f_Find("'code'") >= aint(0), ==, true);
			}

			auto NullResult = co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_nullable (id, code, parent_code) VALUES (2, NULL, 'seed')")).f_Wrap();
			DMibExpect(bool(NullResult), ==, false);

			co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_nullable"));
			co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_nullable_parent"));
			co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));

			co_return {};
		};

		DMibTestCategory("Foreign key whose actions differ from the declared one") -> NConcurrency::TCFuture<void>
		{
			// The declared key has the default actions, so a parent delete that cascades would fail after the adoption
			{
				CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptNullableSchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_nullable"));
				co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_nullable_parent (code) VALUES ('seed')"));
				co_await Database.f_ExecuteRaw
					(
						NStr::CStr
						(
							"CREATE TABLE adopt_nullable (id INTEGER PRIMARY KEY, code TEXT, parent_code TEXT NOT NULL, "
							"CONSTRAINT adopt_nullable_parent_fk FOREIGN KEY (parent_code) REFERENCES adopt_nullable_parent (code) ON DELETE CASCADE)"
						)
					)
				;
				co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_nullable (id, code, parent_code) VALUES (1, 'live-code', 'seed')"));
				co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
			}

			CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptNullableSchemaVersions));
			co_await Database.f_Open();

			auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
			DMibExpect(bool(ApplyResult), ==, false);
			if (!ApplyResult)
			{
				NStr::CStr Error = ApplyResult.f_GetExceptionStr();
				DMibExpect(Error.f_Find("does not declare") >= aint(0), ==, true);
				DMibExpect(Error.f_Find("CASCADE") >= aint(0), ==, true);
			}

			co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM adopt_nullable_parent WHERE code = 'seed'"));
			auto Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_nullable"));
			DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

			co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_nullable"));
			co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_nullable_parent"));
			co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildKeepsLaterDroppedColumn(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_DropLaterV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertDropLaterV1>
				(
					NStorage::TCOptional<NStr::CStr>(NStr::CStr("first-value"))
					, NStorage::TCOptional<NStr::CStr>(NStr::CStr("second-value"))
					, NStorage::TCOptional<NStr::CStr>()
				)
			;
		}

		// The backfill after the rebuild reads the column the drop after it removes
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RebuildDropLaterSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		CSqlRawResult Rows = co_await Version2Database.f_QueryRaw(NStr::CStr("SELECT kept FROM drop_later"));
		DMibAssert(Rows.m_Rows.f_GetLen(), ==, umint(1));
		DMibExpect(Rows.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("second-value"));

		auto SecondResult = co_await Version2Database.f_QueryRaw(NStr::CStr("SELECT second FROM drop_later")).f_Wrap();
		DMibExpect(bool(SecondResult), ==, false);

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRenameColumnWithCheck(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RenamedCheckV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RenamedCheckSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Renamed column keeps its CHECK") -> NConcurrency::TCFuture<void>
		{
			auto ShortResult = co_await Version2Database.template f_Insert<gc_InsertRenamedCheckV2>(NStr::CStr("abc")).f_Wrap();
			DMibExpect(bool(ShortResult), ==, false)(ETestFlag_Aggregated);

			co_await Version2Database.template f_Insert<gc_InsertRenamedCheckV2>(NStr::CStr("abcd"));

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnWithAddedColumn(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_DropAddV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertDropAddV1>(NStr::CStr("code-one"), NStr::CStr("name-one"));
		}

		// The SQLite drop rebuilds from the v2 target, which already creates note; the additive pass must not add it again
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_DropAddV2SchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Drop column alongside an added column preserves rows") -> NConcurrency::TCFuture<void>
		{
			auto ExistingRow = co_await Version2Database.template f_QueryOne<gc_SelectDropAddV2ByName>(NStr::CStr("name-one"));
			DMibExpect(ExistingRow.m_Name, ==, NStr::CStr("name-one"))(ETestFlag_Aggregated);
			DMibExpect(bool(ExistingRow.m_Note), ==, false)(ETestFlag_Aggregated);

			co_await Version2Database.template f_Insert<gc_InsertDropAddV2>(NStr::CStr("name-two"), NStorage::TCOptional<NStr::CStr>(NStr::CStr("note-two")));

			auto AddedRow = co_await Version2Database.template f_QueryOne<gc_SelectDropAddV2ByName>(NStr::CStr("name-two"));
			DMibExpect(bool(AddedRow.m_Note), ==, true)(ETestFlag_Aggregated);
			if (AddedRow.m_Note)
				DMibExpect(*AddedRow.m_Note, ==, NStr::CStr("note-two"))(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseForeignKeyRebuildMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_FkVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertFkParent>(NStr::CStr("parent"));
			co_await Version1Database.template f_Insert<gc_InsertFkChild>(int64(1));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_FkParentRebuildSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Child foreign key bound to rebuilt parent") -> NConcurrency::TCFuture<void>
		{
			umint nChildren = co_await Version2Database.template f_Count<gc_SelectFkChildByParent>(int64(1));
			DMibExpect(nChildren, ==, umint(1))(ETestFlag_Aggregated);

			co_await Version2Database.template f_Insert<gc_InsertFkChild>(int64(1));

			auto OrphanResult = co_await Version2Database.template f_Insert<gc_InsertFkChild>(int64(999999)).f_Wrap();
			DMibExpect(bool(OrphanResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	// An upgrade recreates a recorded table that was dropped outside the migrations, with the foreign keys that the drop removed
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRecreatedTableForeignKeys(FCreateBackend _fCreateBackend, NStr::CStr _DropParentSql)
	{
		struct CDropCase
		{
			ch8 const *m_pName;
			NStr::CStr m_DropSql;
			bool m_bInsertChild;
		};

		CDropCase const Cases[] =
			{
				{"Dropped child", NStr::CStr("DROP TABLE fk_child"), true}
				, {"Dropped parent", _DropParentSql, false}
			}
		;

		for (auto const &Case : Cases)
		{
			DMibTestPath(NStr::CStr(Case.m_pName));

			{
				CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_FkVersion1SchemaVersions));
				co_await Version1Database.f_Open();
				co_await Version1Database.f_ApplySchema();

				if (Case.m_bInsertChild)
				{
					co_await Version1Database.template f_Insert<gc_InsertFkParent>(NStr::CStr("parent"));
					co_await Version1Database.template f_Insert<gc_InsertFkChild>(int64(1));
				}

				co_await Version1Database.f_ExecuteRaw(fg_TempCopy(Case.m_DropSql));
			}

			CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_FkUnchangedUpgradeSchemaVersions));
			co_await Version2Database.f_Open();
			co_await Version2Database.f_ApplySchema();

			if (!Case.m_bInsertChild)
				co_await Version2Database.template f_Insert<gc_InsertFkParent>(NStr::CStr("parent"));

			co_await Version2Database.template f_Insert<gc_InsertFkChild>(int64(1));

			auto OrphanResult = co_await Version2Database.template f_Insert<gc_InsertFkChild>(int64(999999)).f_Wrap();
			DMibExpect(bool(OrphanResult), ==, false);

			co_await Version2Database.f_ExecuteRaw(NStr::CStr("DROP TABLE fk_child"));
			co_await Version2Database.f_ExecuteRaw(NStr::CStr("DROP TABLE fk_parent"));
			co_await Version2Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptKeepsForeignKeyBehavior(FCreateBackend _fCreateBackend, bool _bDeleteSetColumns)
	{
		struct CAdoptionCase
		{
			ch8 const *m_pName;
			ch8 const *m_pClauses;         // After ON DELETE SET NULL
			ch8 const *m_pExpectedInError; // A part of the error naming the constraint, or nullptr when the adoption succeeds
			bool m_bDeleteSetColumns;      // Lists the columns that ON DELETE SET NULL sets, which only PostgreSQL can
		};

		static constexpr CAdoptionCase c_Cases[] =
			{
				{"Declared foreign key", "", nullptr, false}
				, {"Deferred foreign key", " DEFERRABLE INITIALLY DEFERRED", "DEFERRABLE INITIALLY DEFERRED", false}
				, {"Delete sets some key columns", " (parent_id)", "SET NULL (parent_id)", true}
			}
		;

		for (auto const &Case : c_Cases)
		{
			if (Case.m_bDeleteSetColumns && !_bDeleteSetColumns)
				continue;

			DMibTestPath(NStr::CStr(Case.m_pName));

			{
				CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptForeignKeySchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_fk_child"));
				co_await Database.f_ExecuteRaw
					(
						NStr::CStr::CFormat
						(
							"CREATE TABLE adopt_fk_child (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, parent_id INTEGER, "
							"FOREIGN KEY (tenant_id, parent_id) REFERENCES adopt_fk_parent (tenant_id, id) ON DELETE SET NULL{})"
						)
						<< Case.m_pClauses
					)
				;
				co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_fk_parent (tenant_id, id) VALUES (1, 1)"));
				co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_fk_child (id, tenant_id, parent_id) VALUES (1, 1, 1)"));
				co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
			}

			CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptForeignKeySchemaVersions));
			co_await Database.f_Open();

			auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
			if (Case.m_pExpectedInError)
			{
				DMibExpect(bool(ApplyResult), ==, false);
				if (!ApplyResult)
				{
					NStr::CStr Error = ApplyResult.f_GetExceptionStr();
					DMibExpect(Error.f_Find("does not declare") >= aint(0), ==, true);
					DMibExpect(Error.f_Find(Case.m_pExpectedInError) >= aint(0), ==, true);
				}
			}
			else
				DMibExpect(bool(ApplyResult), ==, true);

			auto Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_fk_child"));
			DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

			co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_fk_child"));
			co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_fk_parent"));
			co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRejectsViewNamedLikeTable(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptSchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ExecuteRaw(NStr::CStr("CREATE VIEW adopt_existing AS SELECT 1 AS id, 'seed' AS value"));
		}

		CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptSchemaVersions));
		co_await Database.f_Open();

		auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
		DMibExpect(bool(ApplyResult), ==, false);

		auto Rows = co_await Database.f_QueryRaw(NStr::CStr("SELECT value FROM adopt_existing"));
		DMibAssert(Rows.m_Rows.f_GetLen(), ==, umint(1));
		DMibExpect(Rows.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("seed"));

		co_await Database.f_ExecuteRaw(NStr::CStr("DROP VIEW adopt_existing"));

		co_return {};
	}

	// The child has no rows that its foreign key could find without a parent
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropReferencedTable(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_FkVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_FkParentDropSchemaVersions));
		co_await Version2Database.f_Open();

		auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
		DMibExpect(bool(ApplyResult), ==, false);
		if (!ApplyResult)
			DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("fk_parent") >= aint(0), ==, true);

		co_await Version2Database.f_ExecuteRaw(NStr::CStr("INSERT INTO fk_parent (name) VALUES ('parent')"));
		co_await Version2Database.f_ExecuteRaw(NStr::CStr("INSERT INTO fk_child (parent_id) VALUES (1)"));

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseForeignKeyOrderingSchema(FCreateBackend _fCreateBackend)
	{
		// The schema declares the child before its parent, and PostgreSQL resolves the referenced table when it creates the foreign key
		CSqlDatabaseClient Database(_fCreateBackend(&gc_FkForwardReferenceSchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		DMibTestCategory("Forward foreign-key reference schema applies and enforces") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertFkParent>(NStr::CStr("parent"));
			co_await Database.template f_Insert<gc_InsertFkChild>(int64(1));

			umint nChildren = co_await Database.template f_Count<gc_SelectFkChildByParent>(int64(1));
			DMibExpect(nChildren, ==, umint(1))(ETestFlag_Aggregated);

			auto OrphanResult = co_await Database.template f_Insert<gc_InsertFkChild>(int64(999999)).f_Wrap();
			DMibExpect(bool(OrphanResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Schema tracking records the database name") -> NConcurrency::TCFuture<void>
		{
			CSqlRawResult Result = co_await Database.f_QueryRaw(NStr::CStr("SELECT name FROM schema_migrations WHERE id = 'fk_forward_0001'"));
			DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1))(ETestFlag_Aggregated);
			if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values.f_GetLen() == 1)
				DMibExpect(Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("fk_forward_database"))(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseSelfReferentialForeignKeyRebuildMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_SelfRefVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertSelfRef>(NStorage::TCOptional<int64>());
			co_await Version1Database.template f_Insert<gc_InsertSelfRef>(NStorage::TCOptional<int64>(int64(1)));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_SelfRefRebuildSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Self foreign key preserved across rebuild") -> NConcurrency::TCFuture<void>
		{
			umint nChildren = co_await Version2Database.template f_Count<gc_SelectSelfRefByParent>(int64(1));
			DMibExpect(nChildren, ==, umint(1))(ETestFlag_Aggregated);

			co_await Version2Database.template f_Insert<gc_InsertSelfRef>(NStorage::TCOptional<int64>(int64(1)));

			auto OrphanResult = co_await Version2Database.template f_Insert<gc_InsertSelfRef>(NStorage::TCOptional<int64>(int64(999999))).f_Wrap();
			DMibExpect(bool(OrphanResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RebuildVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CRebuildVersion1Row Row;
			Row.m_Token = "drop-column-token";
			Row.m_LegacyValue = "legacy-value";
			co_await Version1Database.f_Insert(gc_RebuildVersion1Table, fg_Move(Row));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_DropColumnSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		auto Rows = Version2Database.template f_Query<gc_SelectRebuildVersion2ByToken>(NStr::CStr("drop-column-token"));
		umint nRows = 0;

		for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
		{
			for (auto const &pRow : *iBatch)
			{
				++nRows;
				DMibExpect(pRow->m_Data.m_Token, ==, NStr::CStr("drop-column-token"))(ETestFlag_Aggregated);
			}
		}

		DMibExpect(nRows, ==, umint(1));

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnKeepsUndeclaredColumn(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RebuildVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CRebuildVersion1Row Row;
			Row.m_Token = "drop-column-undeclared-token";
			Row.m_LegacyValue = "legacy-value";
			co_await Version1Database.f_Insert(gc_RebuildVersion1Table, fg_Move(Row));

			co_await Version1Database.f_QueryRaw(NStr::CStr("ALTER TABLE rebuild_test ADD COLUMN undeclared_value TEXT"));
			co_await Version1Database.f_QueryRaw(NStr::CStr("UPDATE rebuild_test SET undeclared_value = 'undeclared-kept'"));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_DropColumnSchemaVersions));
		co_await Version2Database.f_Open();

		// SQLite drops a column by rebuilding the table, which would remove the undeclared column too, so it refuses the migration
		auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
		if (!ApplyResult)
			DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("undeclared_value") >= aint(0), ==, true)(ETestFlag_Aggregated);

		auto UndeclaredResult = co_await Version2Database.f_QueryRaw(NStr::CStr("SELECT undeclared_value FROM rebuild_test")).f_Wrap();
		DMibExpect(bool(UndeclaredResult), ==, true);
		if (UndeclaredResult)
		{
			DMibExpect(UndeclaredResult->m_Rows.f_GetLen(), ==, umint(1));
			if (UndeclaredResult->m_Rows.f_GetLen() == 1)
				DMibExpect(UndeclaredResult->m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("undeclared-kept"));
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRebuildKeepsUndeclaredColumn(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_RebuildVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CRebuildVersion1Row Row;
			Row.m_Token = "rebuild-undeclared-token";
			Row.m_LegacyValue = "legacy-value";
			co_await Version1Database.f_Insert(gc_RebuildVersion1Table, fg_Move(Row));

			co_await Version1Database.f_QueryRaw(NStr::CStr("ALTER TABLE rebuild_test ADD COLUMN undeclared_value TEXT"));
			co_await Version1Database.f_QueryRaw(NStr::CStr("UPDATE rebuild_test SET undeclared_value = 'undeclared-kept'"));
		}

		// The rebuild drops legacy_value, which the previous version declares, but no version knows undeclared_value
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RebuildSchemaVersions));
		co_await Version2Database.f_Open();

		auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
		DMibExpect(bool(ApplyResult), ==, false);
		if (!ApplyResult)
			DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("undeclared_value") >= aint(0), ==, true);

		auto UndeclaredResult = co_await Version2Database.f_QueryRaw(NStr::CStr("SELECT undeclared_value FROM rebuild_test")).f_Wrap();
		DMibExpect(bool(UndeclaredResult), ==, true);
		if (UndeclaredResult)
		{
			DMibAssert(UndeclaredResult->m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(UndeclaredResult->m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("undeclared-kept"));
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseDropColumnKeepsLaterDroppedColumn(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_DropLaterV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertDropLaterV1>
				(
					NStorage::TCOptional<NStr::CStr>(NStr::CStr("first-value"))
					, NStorage::TCOptional<NStr::CStr>(NStr::CStr("second-value"))
					, NStorage::TCOptional<NStr::CStr>()
				)
			;
		}

		// The backfill between the drops reads the column the second drop removes
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_DropLaterSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		CSqlRawResult Rows = co_await Version2Database.f_QueryRaw(NStr::CStr("SELECT kept FROM drop_later"));
		DMibAssert(Rows.m_Rows.f_GetLen(), ==, umint(1));
		DMibExpect(Rows.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("second-value"));

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseHistoryGapValidation(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient AppliedDatabase(_fCreateBackend(&gc_HistoryGapAppliedSchemaVersions));
			co_await AppliedDatabase.f_Open();
			co_await AppliedDatabase.f_ApplySchema();

			CRebuildVersion2Row Row;
			Row.m_Token = "history-gap-original";
			co_await AppliedDatabase.f_Insert(gc_RebuildVersion2Table, fg_Move(Row));
		}

		CSqlDatabaseClient Database(_fCreateBackend(&gc_HistoryGapSchemaVersions));
		co_await Database.f_Open();

		auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
		DMibExpect(bool(ApplyResult), ==, false);
		if (!ApplyResult)
			DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("history_gap_0002") >= aint(0), ==, true);

		umint nOriginal = co_await Database.template f_Count<gc_SelectRebuildVersion2ByToken>(NStr::CStr("history-gap-original"));
		DMibExpect(nOriginal, ==, umint(1));

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTransformMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_TransformVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CRebuildVersion2Row Row;
			Row.m_Token = "original-token";
			co_await Version1Database.f_Insert(gc_RebuildVersion2Table, fg_Move(Row));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_TransformSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		auto Rows = Version2Database.template f_Query<gc_SelectRebuildVersion2ByToken>(NStr::CStr("transformed-token"));
		umint nRows = 0;

		for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
		{
			for (auto const &pRow : *iBatch)
			{
				++nRows;
				DMibExpect(pRow->m_Data.m_Token, ==, NStr::CStr("transformed-token"))(ETestFlag_Aggregated);
			}
		}

		DMibExpect(nRows, ==, umint(1));

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseIndexMigration(FCreateBackend _fCreateBackend)
	{
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_IndexVersion1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			CIndexedRow Row;
			Row.m_Email = "unique@example.com";
			co_await Version1Database.f_Insert(gc_IndexVersion1Table, fg_Move(Row));
		}

		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_IndexSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		CIndexedRow DuplicateRow;
		DuplicateRow.m_Email = "unique@example.com";

		auto InsertDuplicateResult = co_await Version2Database.f_Insert(gc_IndexVersion2Table, fg_Move(DuplicateRow)).f_Wrap();

		DMibExpect(bool(InsertDuplicateResult), ==, false);

		co_return {};
	}
}
