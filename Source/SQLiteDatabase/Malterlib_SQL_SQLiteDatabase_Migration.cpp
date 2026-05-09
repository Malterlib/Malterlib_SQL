// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLiteDatabase_Internal.h"

namespace NMib::NSQL::NPrivate
{
	CSqlTableDescription const *fg_SqliteFindTable(CSqlSchemaVersionDescription const &_Schema, NStr::CStr const &_TableName)
	{
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			if (Table.f_Name() == _TableName)
				return &Table;
		}

		return nullptr;
	}

	CSqlTableDescription *fg_SqliteFindTable(CSqlSchemaVersionDescription &_Schema, NStr::CStr const &_TableName)
	{
		for (auto &Table : _Schema.m_Database.m_Tables)
		{
			if (Table.f_Name() == _TableName)
				return &Table;
		}

		return nullptr;
	}

	void fg_SqliteApplyMigrationOperationToPlannedSchema
		(
			CSqlSchemaVersionDescription &_Schema
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
		)
	{
		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable:
			if (CSqlTableDescription *pTable = fg_SqliteFindTable(_Schema, *_Operation.m_pOldName))
				pTable->m_pName = _Operation.m_pNewName;

			// SQLite rewrites foreign keys in other tables that reference the renamed table to the new name. Mirror
			// that in the planned schema (as the PostgreSQL planner does) so the additive planning pass compares
			// against a schema whose foreign-key references match the renamed table, not the stale old name.
			for (auto &Table : _Schema.m_Database.m_Tables)
			{
				for (auto &Constraint : Table.m_Constraints)
				{
					if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey && Constraint.f_ReferencedTable() == *_Operation.m_pOldName)
						Constraint.m_pReferencedTable = _Operation.m_pNewName;
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn:
			if (CSqlTableDescription *pTable = fg_SqliteFindTable(_Schema, *_Operation.m_pTableName))
			{
				for (auto &Column : pTable->m_Columns)
				{
					if (Column.f_Name() == *_Operation.m_pOldName)
					{
						Column.m_pName = _Operation.m_pNewName;
						break;
					}
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_DropTable:
			for (umint iTable = 0; iTable < _Schema.m_Database.m_Tables.f_GetLen(); ++iTable)
			{
				if (_Schema.m_Database.m_Tables[iTable].f_Name() == *_Operation.m_pTableName)
				{
					_Schema.m_Database.m_Tables.f_Remove(iTable);
					break;
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_DropColumn:
			if (CSqlTableDescription *pPlanned = fg_SqliteFindTable(_Schema, *_Operation.m_pTableName))
			{
				bool bHasColumn = false;
				for (auto const &Column : pPlanned->m_Columns)
				{
					if (Column.f_Name() == *_Operation.m_pOldName)
					{
						bHasColumn = true;
						break;
					}
				}

				// The executor drops a column by rebuilding the whole table from the target schema (SQLite cannot drop
				// a column without recreating the table), so the physical table ends up matching the target exactly -
				// including any column the same migration adds. Adopt the full target table here, exactly as the rebuild
				// case does; only removing the dropped column would leave the additive planning pass treating a newly
				// added target column as still missing and emitting a duplicate ALTER TABLE ADD COLUMN that fails.
				if (bHasColumn)
				{
					if (CSqlTableDescription const *pTarget = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName))
						*pPlanned = *pTarget;
					else
					{
						for (umint iColumn = 0; iColumn < pPlanned->m_Columns.f_GetLen(); ++iColumn)
						{
							if (pPlanned->m_Columns[iColumn].f_Name() == *_Operation.m_pOldName)
							{
								pPlanned->m_Columns.f_Remove(iColumn);
								break;
							}
						}
					}
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			// The rebuild recreates the table from the target schema with every column and constraint inline, so the
			// physical table now matches the target exactly. Adopt the target table definition in the planned previous
			// schema; otherwise the additive planning pass treats columns or constraints the rebuild already
			// materialized as still missing and emits a duplicate ALTER TABLE ADD COLUMN (or rejects a required
			// no-default column) that the runtime never runs.
			if (CSqlTableDescription const *pTarget = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName))
			{
				if (CSqlTableDescription *pPlanned = fg_SqliteFindTable(_Schema, *_Operation.m_pTableName))
					*pPlanned = *pTarget;
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql:
			break;
		}
	}

	bool fg_SqliteTableHasConstraint(CSqlTableDescription const *_pTable, NStr::CStr const &_ConstraintName)
	{
		if (!_pTable)
			return false;

		for (auto const &Constraint : _pTable->m_Constraints)
		{
			if (Constraint.f_Name() == _ConstraintName)
				return true;
		}

		return false;
	}

	bool fg_SqliteTableHasNewConstraints(CSqlTableDescription const &_Table, CSqlSchemaVersionDescription const *_pPreviousSchema)
	{
		if (!_pPreviousSchema)
			return false;

		CSqlTableDescription const *pPreviousTable = fg_SqliteFindTable(*_pPreviousSchema, _Table.f_Name());
		for (auto const &Constraint : _Table.m_Constraints)
		{
			if (!fg_SqliteTableHasConstraint(pPreviousTable, Constraint.f_Name()))
				return true;
		}

		return false;
	}

	bool fg_SqliteColumnIsGenerated(CSqlColumnDescription const &_Column)
	{
		CNonPortableColumnOptions const *pNonPortableOptions = _Column.f_NonPortableOptions(NStr::gc_Str<"sqlite">.m_Str);
		return pNonPortableOptions && !pNonPortableOptions->m_pGeneratedSql->f_IsEmpty();
	}

	NContainer::TCVector<NStr::CStr const *> fg_SqliteCommonColumns(CSqlTableDescription const &_Table, CSqlTableDescription const *_pPreviousTable)
	{
		NContainer::TCVector<NStr::CStr const *> ExistingColumns;
		if (!_pPreviousTable)
			return ExistingColumns;

		for (auto const &Column : _Table.m_Columns)
		{
			// A generated column is recomputed by the database and cannot be named in the rebuild's INSERT ... SELECT,
			// so it must not be part of the copy list even though it exists in both tables.
			if (fg_SqliteColumnIsGenerated(Column))
				continue;

			for (auto const &PreviousColumn : _pPreviousTable->m_Columns)
			{
				if (Column.f_Name() == PreviousColumn.f_Name())
				{
					ExistingColumns.f_InsertLast(&Column.f_Name());
					break;
				}
			}
		}

		return ExistingColumns;
	}

	NStr::CStr const &fg_SqliteColumnDefaultSql(CSqlColumnDescription const &_Column)
	{
		CNonPortableColumnOptions const *pNonPortableOptions = _Column.f_NonPortableOptions(NStr::gc_Str<"sqlite">.m_Str);
		if (pNonPortableOptions && !pNonPortableOptions->m_pDefaultSql->f_IsEmpty())
			return *pNonPortableOptions->m_pDefaultSql;

		return _Column.f_DefaultSql();
	}

	bool fg_SqliteColumnNeedsRebuildToAdd(CSqlColumnDescription const &_Column)
	{
		// SQLite's ALTER TABLE ADD COLUMN rejects a UNIQUE or PRIMARY KEY column constraint and a STORED generated
		// column, so a new column carrying any of those must be materialized through a table rebuild (which recreates
		// the table with the column inline, as on initial creation) instead of an ADD COLUMN.
		if (fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_Unique) || _Column.f_IsPrimaryKey() || fg_SqliteColumnIsGenerated(_Column))
			return true;

		// ALTER TABLE ADD COLUMN also rejects a default that is not a constant - the CURRENT_TIME/CURRENT_DATE/
		// CURRENT_TIMESTAMP keywords or a parenthesized expression - even though CREATE TABLE accepts it. Such a column
		// must likewise be added through a rebuild.
		NStr::CStr const &DefaultSql = fg_SqliteColumnDefaultSql(_Column);
		if (!DefaultSql.f_IsEmpty() && (DefaultSql[0] == '(' || DefaultSql.f_FindNoCase("CURRENT_") == 0))
			return true;

		return false;
	}

	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_SqliteMigrationOperationStatements
		(
			CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable:
			Statements.f_InsertLast(fg_SqliteRenameTable(_Operation));

			break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn:
			Statements.f_InsertLast(fg_SqliteRenameColumn(_Operation));

			break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			{
				auto pTable = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance("Cannot rebuild SQLite table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_SqliteFindTable(*_pPreviousSchema, *_Operation.m_pTableName) : nullptr;
				Statements = co_await fg_SqliteRebuildTableStatements(*pTable, fg_SqliteCommonColumns(*pTable, pPreviousTable));

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_DropTable:
			Statements.f_InsertLast(fg_SqliteDropTableSql(*_Operation.m_pTableName));

			break;
		case ESqlSchemaMigrationOperationType::mc_DropColumn:
			{
				auto pTable = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance("Cannot drop SQLite column from table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_SqliteFindTable(*_pPreviousSchema, *_Operation.m_pTableName) : nullptr;
				Statements = co_await fg_SqliteRebuildTableStatements(*pTable, fg_SqliteCommonColumns(*pTable, pPreviousTable));

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql:
			Statements.f_InsertLast(fg_SqliteUpdateColumnSql(_Operation));

			break;
		}

		co_return fg_Move(Statements);
	}

	auto fg_SqliteSyncAdditiveSchemaStatements(CSqlSchemaVersionDescription const &_Schema, CSqlSchemaVersionDescription const *_pPreviousSchema)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_SqliteFindTable(*_pPreviousSchema, Table.f_Name()) : nullptr;
			if (!pPreviousTable)
			{
				Statements.f_InsertLast(co_await fg_SqliteCreateTable(Table, true));
				for (auto const &Index : Table.m_Indexes)
					Statements.f_InsertLast(fg_SqliteCreateIndex(Table, Index, true));

				continue;
			}

			bool bRebuild = fg_SqliteTableHasNewConstraints(Table, _pPreviousSchema);
			NContainer::TCVector<CSqlColumnDescription const *> NewColumns;
			for (auto const &Column : Table.m_Columns)
			{
				bool bColumnExists = false;
				for (auto const &PreviousColumn : pPreviousTable->m_Columns)
				{
					if (PreviousColumn.f_Name() == Column.f_Name())
					{
						bColumnExists = true;
						break;
					}
				}

				if (bColumnExists)
					continue;

				if (!Column.f_IsNullable() && fg_SqliteColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey())
					co_return DMibErrorDatabaseInstance("Cannot add required SQLite column '{}.{}' without a default value"_f << Table.f_Name() << Column.f_Name());

				// A new column whose constraint ALTER TABLE ADD COLUMN cannot add (UNIQUE/PRIMARY KEY) forces a rebuild,
				// matching the executor so the previewed plan applies cleanly.
				if (fg_SqliteColumnNeedsRebuildToAdd(Column))
					bRebuild = true;

				NewColumns.f_InsertLast(&Column);
			}

			if (bRebuild)
			{
				for (auto &Statement : co_await fg_SqliteRebuildTableStatements(Table, fg_SqliteCommonColumns(Table, pPreviousTable)))
					Statements.f_InsertLast(fg_Move(Statement));
			}
			else
			{
				for (auto const *pColumn : NewColumns)
					Statements.f_InsertLast(co_await fg_SqliteAlterTableAddColumn(Table, *pColumn));
			}

			for (auto const &Index : Table.m_Indexes)
				Statements.f_InsertLast(fg_SqliteCreateIndex(Table, Index, true));
		}

		co_return fg_Move(Statements);
	}

	auto fg_SqliteRebuildTableStatements(CSqlTableDescription const &_Table, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		NStr::CStr OldTableName = "__mib_rebuild_old_{}"_f << _Table.f_Name();
		// The scratch name is only ever a transient rename target within this migration's transaction. Migrations run
		// inside a transaction that rolls back on failure (and SQLite discards an incomplete transaction's journal on
		// the next open), so a committed leftover from an aborted rebuild cannot exist. Do not pre-drop the scratch
		// name: if a legitimate user table happens to share it, the rename below fails and the migration aborts safely
		// instead of silently destroying that table's data.
		// By default (legacy_alter_table=OFF) renaming the table makes SQLite rewrite foreign-key clauses in other
		// tables that reference it to point at the temporary name; dropping that temporary table at the end would
		// then leave those child foreign keys dangling. Scope legacy_alter_table=ON to just this rename so child
		// foreign keys keep pointing at the original name, which the recreated table satisfies, while leaving an
		// intentional RenameTable migration free to update references as usual.
		Statements.f_InsertLast("PRAGMA legacy_alter_table=ON");
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "ALTER TABLE ";
				fg_SqliteAppendQuotedIdentifier(Appender, _Table.f_Name());
				Appender += " RENAME TO ";
				fg_SqliteAppendQuotedIdentifier(Appender, OldTableName);
			}
			Statements.f_InsertLast(fg_Move(Sql));
		}
		Statements.f_InsertLast("PRAGMA legacy_alter_table=OFF");
		Statements.f_InsertLast(co_await fg_SqliteCreateTable(_Table, false));

		if (!_ExistingColumns.f_IsEmpty())
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "INSERT INTO ";
				fg_SqliteAppendQuotedIdentifier(Appender, _Table.f_Name());
				Appender += " (";
				for (umint i = 0; i < _ExistingColumns.f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					fg_SqliteAppendQuotedIdentifier(Appender, *_ExistingColumns[i]);
				}
				Appender += ") SELECT ";
				for (umint i = 0; i < _ExistingColumns.f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					fg_SqliteAppendQuotedIdentifier(Appender, *_ExistingColumns[i]);
				}
				Appender += " FROM ";
				fg_SqliteAppendQuotedIdentifier(Appender, OldTableName);
			}
			Statements.f_InsertLast(fg_Move(Sql));
		}

		Statements.f_InsertLast(fg_SqliteDropTableSql(OldTableName));

		co_return fg_Move(Statements);
	}

	NConcurrency::TCWrapped<void> fg_SqliteRebuildTable(CSQLiteDatabaseHandle &_Database, CSqlTableDescription const &_Table)
	{
		if (!co_await _Database.f_HasTable(_Table.f_Name()))
			co_return {};

		NContainer::TCVector<NStr::CStr const *> ExistingColumns;
		for (auto const &Column : _Table.m_Columns)
		{
			// A generated column is computed by SQLite, so it cannot appear in the INSERT ... SELECT copy list even
			// when it already exists in the live table - the same exclusion fg_SqliteCommonColumns applies on the plan path.
			if (fg_SqliteColumnIsGenerated(Column))
				continue;

			if (co_await _Database.f_HasColumn(_Table.f_Name(), Column.f_Name()))
				ExistingColumns.f_InsertLast(&Column.f_Name());
		}
		auto Statements = co_await fg_SqliteRebuildTableStatements(_Table, ExistingColumns);

		for (auto &Sql : Statements)
			co_await _Database.f_Execute(Sql);

		co_return {};
	}

	NConcurrency::TCWrapped<void> fg_SqliteDropTable(CSQLiteDatabaseHandle &_Database, NStr::CStr const &_TableName)
	{
		if (co_await _Database.f_HasTable(_TableName))
			co_return _Database.f_Execute(fg_SqliteDropTableSql(_TableName));

		co_return {};
	}

	NConcurrency::TCWrapped<void> fg_SqliteUpdateColumnSql(CSQLiteDatabaseHandle &_Database, CSqlSchemaMigrationOperationDescription const &_Operation)
	{
		if
		(
			co_await _Database.f_HasTable(*_Operation.m_pTableName)
			&& co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pNewName)
		)
		{
			co_return _Database.f_Execute(fg_SqliteUpdateColumnSql(_Operation));
		}

		co_return {};
	}

	auto fg_SqliteApplyMigrationOperation
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
		)
		-> NConcurrency::TCWrapped<void>
	{
		using namespace NStr;

		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable:
			{
				if
				(
					co_await _Database.f_HasTable(*_Operation.m_pOldName)
					&& !co_await _Database.f_HasTable(*_Operation.m_pNewName)
				)
				{
					co_return _Database.f_Execute(fg_SqliteRenameTable(_Operation));
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn:
			{
				if
				(
					co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pOldName)
					&& !co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pNewName)
				)
				{
					co_return _Database.f_Execute(fg_SqliteRenameColumn(_Operation));
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			{
				auto pTable = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance("Cannot rebuild SQLite table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				co_return fg_SqliteRebuildTable(_Database, *pTable);
			}
		case ESqlSchemaMigrationOperationType::mc_DropTable:
			co_return fg_SqliteDropTable(_Database, *_Operation.m_pTableName);
		case ESqlSchemaMigrationOperationType::mc_DropColumn:
			{
				auto pTable = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance("Cannot drop SQLite column from table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				if (co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pOldName))
					co_return fg_SqliteRebuildTable(_Database, *pTable);

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql:
			co_return fg_SqliteUpdateColumnSql(_Database, _Operation);
		}

		co_return {};
	}

	auto fg_SqliteApplyMigrationOperations
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaVersionDescription const &_TargetSchema
		)
		-> NConcurrency::TCWrapped<void>
	{
		for (auto const &Operation : _Migration.m_Operations)
			co_await fg_SqliteApplyMigrationOperation(_Database, Operation, _TargetSchema);

		co_return {};
	}

	auto fg_SqliteSyncAdditiveSchema
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlSchemaVersionDescription const &_Schema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
		-> NConcurrency::TCWrapped<void>
	{
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			if (!co_await _Database.f_HasTable(Table.f_Name()))
			{
				co_await _Database.f_Execute(co_await fg_SqliteCreateTable(Table, true));

				for (auto const &Index : Table.m_Indexes)
					co_await _Database.f_Execute(fg_SqliteCreateIndex(Table, Index, true));

				continue;
			}

			bool bRebuild = fg_SqliteTableHasNewConstraints(Table, _pPreviousSchema);

			// Adopting a table that already exists with no tracked previous schema (schema_migrations was empty): it may
			// have been created outside version tracking and be missing declared table-level constraints
			// (UNIQUE/CHECK/foreign key). SQLite cannot add such a constraint in place - the PostgreSQL backend uses
			// ALTER TABLE ADD CONSTRAINT, but SQLite must recreate the table - so rebuild it to materialize them. The
			// rebuild copies forward every column shared with the live table, so existing data is preserved; if those
			// rows violate a now-enforced constraint the rebuild (or the trailing PRAGMA foreign_key_check) fails the
			// apply, which is the correct outcome for data inconsistent with the declared schema.
			if (!_pPreviousSchema && !Table.m_Constraints.f_IsEmpty())
				bRebuild = true;

			NContainer::TCVector<CSqlColumnDescription const *> NewColumns;
			for (auto const &Column : Table.m_Columns)
			{
				if (co_await _Database.f_HasColumn(Table.f_Name(), Column.f_Name()))
					continue;

				if (!Column.f_IsNullable() && fg_SqliteColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey())
				{
					using namespace NStr;

					co_return DMibErrorDatabaseInstance("Cannot add required SQLite column '{}.{}' without a default value"_f << Table.f_Name() << Column.f_Name());
				}

				// A new column whose constraint ALTER TABLE ADD COLUMN cannot add (UNIQUE/PRIMARY KEY) forces a rebuild
				// of the whole table, exactly as initial creation would produce it.
				if (fg_SqliteColumnNeedsRebuildToAdd(Column))
					bRebuild = true;

				NewColumns.f_InsertLast(&Column);
			}

			if (bRebuild)
				co_await fg_SqliteRebuildTable(_Database, Table);
			else
			{
				for (auto const *pColumn : NewColumns)
					co_await _Database.f_Execute(co_await fg_SqliteAlterTableAddColumn(Table, *pColumn));
			}

			for (auto const &Index : Table.m_Indexes)
				co_await _Database.f_Execute(fg_SqliteCreateIndex(Table, Index, true));
		}

		co_return {};
	}

	auto fg_SqliteFindMigration(NContainer::TCVector<CSqlSchemaMigrationDescription> const &_Migrations, NStr::CStr const &_FromVersionID, NStr::CStr const &_ToVersionID)
		-> CSqlSchemaMigrationDescription const *
	{
		for (auto const &Migration : _Migrations)
		{
			if (*Migration.m_pFromVersionID == _FromVersionID && *Migration.m_pToVersionID == _ToVersionID)
				return &Migration;
		}

		return nullptr;
	}
}

namespace NMib::NSQL
{
	NConcurrency::TCWrapped<CSqlSchemaMigrationPlan> fg_SqlitePlanSchemaMigration(ICSqlSchemaVersions const &_SchemaVersions, NStr::CStr const *_pCurrentVersionID)
	{
		using namespace NStr;
		using namespace NPrivate;

		CSqlSchemaMigrationPlan Plan;
		NContainer::TCVector<CSqlSchemaVersionDescription> Versions = _SchemaVersions.f_DescribeVersions();
		NContainer::TCVector<CSqlSchemaMigrationDescription> Migrations = _SchemaVersions.f_DescribeMigrations();
		if (Versions.f_IsEmpty())
			co_return fg_Move(Plan);

		NStr::CStr CurrentVersionID = _pCurrentVersionID ? *_pCurrentVersionID : NStr::CStr();
		umint iCurrentVersion = umint(-1);
		if (!CurrentVersionID.f_IsEmpty())
		{
			for (umint iVersion = 0; iVersion < Versions.f_GetLen(); ++iVersion)
			{
				if (Versions[iVersion].f_ID() == CurrentVersionID)
				{
					iCurrentVersion = iVersion;
					break;
				}
			}

			if (iCurrentVersion == umint(-1))
			{
				Plan.m_Warnings.f_InsertLast("Current schema version '{}' is not present in the compiled schema versions"_f << CurrentVersionID);
				co_return fg_Move(Plan);
			}
		}

		Plan.m_Statements.f_InsertLast(fg_SqliteCreateSchemaVersionTableSql());
		if (CurrentVersionID.f_IsEmpty())
		{
			// Initial adoption: f_ApplySchema syncs the target schema additively (fg_SqliteSyncAdditiveSchema with no
			// previous schema), so an existing untracked table is adopted rather than recreated. The plan must reflect
			// that same behavior - plain CREATE TABLE/INDEX would fail or mislead when those objects already exist - so
			// generate the additive statements with no previous schema here too.
			Plan.m_Statements.f_InsertLast(co_await fg_SqliteSyncAdditiveSchemaStatements(Versions[Versions.f_GetLen() - 1], nullptr));
			for (umint iVersion = 0; iVersion < Versions.f_GetLen(); ++iVersion)
				Plan.m_Statements.f_InsertLast(fg_SqliteInsertSchemaVersionSql());

			co_return fg_Move(Plan);
		}

		if (iCurrentVersion == Versions.f_GetLen() - 1)
			co_return fg_Move(Plan);

		for (umint iVersion = iCurrentVersion + 1; iVersion < Versions.f_GetLen(); ++iVersion)
		{
			auto const &PreviousVersion = Versions[iVersion - 1];
			auto const &NextVersion = Versions[iVersion];
			CSqlSchemaVersionDescription PlannedPreviousVersion = PreviousVersion;
			if (auto const *pMigration = fg_SqliteFindMigration(Migrations, PreviousVersion.f_ID(), NextVersion.f_ID()))
			{
				for (auto const &Operation : pMigration->m_Operations)
				{
					Plan.m_Statements.f_InsertLast(co_await fg_SqliteMigrationOperationStatements(Operation, NextVersion, &PlannedPreviousVersion));
					fg_SqliteApplyMigrationOperationToPlannedSchema(PlannedPreviousVersion, Operation, NextVersion);
				}
			}

			Plan.m_Statements.f_InsertLast(co_await fg_SqliteSyncAdditiveSchemaStatements(NextVersion, &PlannedPreviousVersion));
			Plan.m_Statements.f_InsertLast(fg_SqliteInsertSchemaVersionSql());
		}

		co_return fg_Move(Plan);
	}
}
