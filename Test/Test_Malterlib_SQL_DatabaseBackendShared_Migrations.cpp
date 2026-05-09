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
			// The rebuild copied the existing row's id explicitly. On PostgreSQL the rebuilt table's BIGSERIAL
			// sequence must have been advanced past that id, otherwise this default insert would reuse it and fail
			// with a duplicate primary key. (SQLite advances its AUTOINCREMENT sequence on explicit insert, so this
			// always holds there.)
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

		// Rebuilding rebuild_test renames it to the scratch name, which is already taken by the user table. The
		// migration must fail on the rename rather than first dropping the user table to make room: the rebuild no
		// longer pre-drops the scratch name, so the colliding user table and its row survive the aborted migration.
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

		// v2 rebuilds rebuild_fk_item to add category_id and adds the new rebuild_fk_category table that category_id
		// references. PostgreSQL must defer the new foreign key until rebuild_fk_category exists; creating it inline
		// during the rebuild would fail with "relation rebuild_fk_category does not exist". A foreign key carried over
		// from the previous version would otherwise also be added a second time by the additive pass.
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_RebuildFkSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Rebuilt row preserved and deferred foreign key enforced") -> NConcurrency::TCFuture<void>
		{
			umint nItems = co_await Version2Database.template f_Count<gc_SelectRebuildFkItemByToken>(NStr::CStr("rebuild-fk-token"));
			DMibExpect(nItems, ==, umint(1))(ETestFlag_Aggregated);

			// The new referenced table exists and the deferred foreign key is present and satisfiable: an item pointing
			// at an existing category inserts, while one pointing at a missing category is rejected.
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

			// Two rows so the column add copies multiple rows; their code is NULL, which a UNIQUE column allows.
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
			// Existing rows survive the column add, with a NULL code.
			umint nFirst = co_await Version2Database.template f_Count<gc_SelectUniqueAddByName>(NStr::CStr("first"));
			DMibExpect(nFirst, ==, umint(1))(ETestFlag_Aggregated);

			// The added column's UNIQUE constraint is enforced: a second row reusing a code is rejected.
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

			// The implicit insert must omit the generated column (and the autoincrement primary key); the database
			// computes value_lower. Including value_lower would make this a two-value insert that the backend rejects.
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

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseGeneratedColumnInsertPaths(FCreateBackend _fCreateBackend)
	{
		CSqlDatabaseClient Database(_fCreateBackend(&gc_GenColUpsertSchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		DMibTestCategory("Whole-row insert omits the generated column") -> NConcurrency::TCFuture<void>
		{
			// The whole-row insert path (fg_SqlAppendInsertValue) must skip value_lower even though the row carries a
			// value for it. A stale value here must be ignored - the database recomputes lower(value).
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
			// The prepared upsert (fg_SqlUpsertOperation) inserts (value, note) and lets the database fill value_lower.
			// Binding value_lower as well would map one value too many onto the generated SQL placeholders.
			co_await Database.template f_Upsert<gc_UpsertGenColNoteByValue>(NStr::CStr("UPSERT"), NStr::CStr("inserted"));

			auto Inserted = co_await Database.template f_QueryOne<gc_SelectGenColUpsertByValue>(NStr::CStr("UPSERT"));
			DMibExpect(Inserted.m_Note, ==, NStr::CStr("inserted"))(ETestFlag_Aggregated);
			DMibExpect(bool(Inserted.m_ValueLower), ==, true)(ETestFlag_Aggregated);
			if (Inserted.m_ValueLower)
				DMibExpect(*Inserted.m_ValueLower, ==, NStr::CStr("upsert"))(ETestFlag_Aggregated);

			// Conflicting on value updates note while value_lower stays computed.
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

		// v2 adds a STORED generated column. ALTER TABLE ADD COLUMN cannot add it on SQLite, so the additive sync
		// must rebuild the table; on PostgreSQL it is added directly. Either way value_lower is computed for the
		// existing row.
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

		// v3 explicitly rebuilds the table while a generated column is present. The copy list of the rebuild must omit
		// value_lower on both backends (fg_SqliteCommonColumns / fg_PostgresCommonColumns); otherwise INSERT ... SELECT
		// names a column the database fills itself and the rebuild fails.
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

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseAdoptExistingTableConstraint(FCreateBackend _fCreateBackend)
	{
		// Create the table through the v1 schema (no UNIQUE constraint), seed one row, then wipe the recorded schema
		// versions so the table looks like one created outside version tracking.
		{
			CSqlDatabaseClient Version1Database(_fCreateBackend(&gc_AdoptConstraintV1SchemaVersions));
			co_await Version1Database.f_Open();
			co_await Version1Database.f_ApplySchema();

			co_await Version1Database.template f_Insert<gc_InsertAdoptConstraintV1>(NStr::CStr("dup"));

			co_await Version1Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
		}

		// Applying the v2 schema (which declares the UNIQUE constraint) finds the table already present but no recorded
		// version, so it must add the declared constraint while adopting the table rather than recording the schema as
		// current with the constraint unenforced.
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_AdoptConstraintV2SchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Adopted table enforces the declared constraint") -> NConcurrency::TCFuture<void>
		{
			co_await Version2Database.template f_Insert<gc_InsertAdoptConstraintV2>(NStr::CStr("unique-code"));

			// A second 'dup' row duplicates the one seeded before adoption, so the adopted UNIQUE constraint must reject it.
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

		// v2 adds created_at DEFAULT CURRENT_TIMESTAMP. SQLite's ALTER TABLE ADD COLUMN rejects a non-constant default,
		// so the additive sync must add the column through a rebuild; the pre-existing row then receives the default.
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

		// Inserting into same_shape_a caches the prepared INSERT under its type-derived QueryID; same_shape_b shares
		// that QueryID, so without a statement check the second insert would run against same_shape_a.
		co_await Database.template f_Insert<gc_InsertSameShapeA>(NStr::CStr("a-value"));
		co_await Database.template f_Insert<gc_InsertSameShapeB>(NStr::CStr("b-value"));

		DMibTestCategory("Same-shaped tables keep separate prepared SQL") -> NConcurrency::TCFuture<void>
		{
			// Verify through raw SQL (which bypasses the typed prepared cache) that each row landed in its own table.
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

		// The migration drops the code column - dropping the dct_unique constraint that depended on it - and v2 reuses
		// the dct_unique name on the name column. The planned previous schema must drop the column's constraint so the
		// additive sync recreates dct_unique on name.
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_DropConstraintV2SchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Reused constraint name applies to its new column") -> NConcurrency::TCFuture<void>
		{
			co_await Version2Database.template f_Insert<gc_InsertDropConstraintV2>(NStr::CStr("name-two"));

			// name-one already exists, so the recreated UNIQUE constraint on name must reject the duplicate.
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

			// Simulate a database whose tables exist but were created outside version tracking: the table and its row
			// remain, but schema_migrations is cleared.
			co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
		}

		// schema_migrations is now empty while adopt_existing already exists. Applying the schema must adopt the table
		// through the additive sync, not run a plain CREATE TABLE that aborts with "table already exists".
		CSqlDatabaseClient Database(_fCreateBackend(&gc_AdoptSchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		DMibTestCategory("Existing untracked table is adopted, not recreated") -> NConcurrency::TCFuture<void>
		{
			// The pre-existing row survived, proving the table was adopted rather than dropped and recreated.
			auto AdoptedCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_existing"));
			DMibExpect(AdoptedCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1))(ETestFlag_Aggregated);

			// The version is recorded now, so a further insert against the adopted table works.
			co_await Database.template f_Insert<gc_InsertAdopt>(NStr::CStr("after-adoption"));

			auto FinalCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_existing"));
			DMibExpect(FinalCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2))(ETestFlag_Aggregated);

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

		// The v2 migration drops code and adds the nullable note column. SQLite drops a column by rebuilding the table
		// from the v2 target, which already creates note, so f_ApplySchema must not then emit a second ALTER TABLE ADD
		// COLUMN note - applying that plan fails with a duplicate column. Reaching the assertions proves it did not.
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_DropAddV2SchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Drop column alongside an added column preserves rows") -> NConcurrency::TCFuture<void>
		{
			auto ExistingRow = co_await Version2Database.template f_QueryOne<gc_SelectDropAddV2ByName>(NStr::CStr("name-one"));
			DMibExpect(ExistingRow.m_Name, ==, NStr::CStr("name-one"))(ETestFlag_Aggregated);
			DMibExpect(bool(ExistingRow.m_Note), ==, false)(ETestFlag_Aggregated);

			// The added note column is usable on the migrated table.
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

		// Rebuilding fk_parent, which fk_child references, must not orphan the child foreign key. On PostgreSQL the
		// rename leaves the child constraint depending on the renamed object so the drop fails; on SQLite the rename
		// rewrites the child clause to the temporary name. The migration must drop and recreate (PostgreSQL) or
		// preserve (SQLite) the child foreign key so it ends up bound to the rebuilt parent.
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_FkParentRebuildSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Child foreign key bound to rebuilt parent") -> NConcurrency::TCFuture<void>
		{
			umint nChildren = co_await Version2Database.template f_Count<gc_SelectFkChildByParent>(int64(1));
			DMibExpect(nChildren, ==, umint(1))(ETestFlag_Aggregated);

			// A new child against the surviving parent still inserts, and an orphan is rejected, proving the foreign
			// key is intact and enforced against the rebuilt parent.
			co_await Version2Database.template f_Insert<gc_InsertFkChild>(int64(1));

			auto OrphanResult = co_await Version2Database.template f_Insert<gc_InsertFkChild>(int64(999999)).f_Wrap();
			DMibExpect(bool(OrphanResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseForeignKeyOrderingSchema(FCreateBackend _fCreateBackend)
	{
		// The schema declares the referencing child table before the parent it references. Applying it must
		// succeed: PostgreSQL has to create both tables before adding the foreign key, otherwise CREATE TABLE
		// fk_child fails because fk_parent does not exist yet. SQLite resolves references lazily and is unaffected.
		CSqlDatabaseClient Database(_fCreateBackend(&gc_FkForwardReferenceSchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		DMibTestCategory("Forward foreign-key reference schema applies and enforces") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertFkParent>(NStr::CStr("parent"));
			co_await Database.template f_Insert<gc_InsertFkChild>(int64(1));

			umint nChildren = co_await Database.template f_Count<gc_SelectFkChildByParent>(int64(1));
			DMibExpect(nChildren, ==, umint(1))(ETestFlag_Aggregated);

			// The foreign key is present and enforced even though it was added after both tables existed.
			auto OrphanResult = co_await Database.template f_Insert<gc_InsertFkChild>(int64(999999)).f_Wrap();
			DMibExpect(bool(OrphanResult), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Schema tracking records the database name") -> NConcurrency::TCFuture<void>
		{
			// The schema_migrations.name column must hold the database name, not a duplicate of the version id.
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

			// A root row references no parent, then a child row references the root by its generated id, exercising the
			// self foreign key with real data before the rebuild.
			co_await Version1Database.template f_Insert<gc_InsertSelfRef>(NStorage::TCOptional<int64>());
			co_await Version1Database.template f_Insert<gc_InsertSelfRef>(NStorage::TCOptional<int64>(int64(1)));
		}

		// Rebuilding self_ref, whose foreign key references its own primary key, must not recreate that self foreign key
		// twice. PostgreSQL creates the rebuilt table with the inline self foreign key and then re-adds the referencing
		// foreign keys it had to drop; the self foreign key has to be excluded from that re-add set or the rebuild aborts
		// with a duplicate-constraint error. SQLite preserves the self reference through the rename.
		CSqlDatabaseClient Version2Database(_fCreateBackend(&gc_SelfRefRebuildSchemaVersions));
		co_await Version2Database.f_Open();
		co_await Version2Database.f_ApplySchema();

		DMibTestCategory("Self foreign key preserved across rebuild") -> NConcurrency::TCFuture<void>
		{
			umint nChildren = co_await Version2Database.template f_Count<gc_SelectSelfRefByParent>(int64(1));
			DMibExpect(nChildren, ==, umint(1))(ETestFlag_Aggregated);

			// A new child against an existing row still inserts, and a row referencing a missing id is rejected, proving
			// the self foreign key survived the rebuild and remains enforced against the rebuilt table.
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
