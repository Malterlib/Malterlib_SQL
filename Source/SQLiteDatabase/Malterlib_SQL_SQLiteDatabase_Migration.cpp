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

	// A rebuild creates the target table before the migration's later operations run, so a column that a later operation
	// renames does not exist yet under its old name and its data would not be copied
	NException::CExceptionPointer fg_SqliteValidateRenameAfterRebuild(CSqlSchemaMigrationDescription const &_Migration)
	{
		using namespace NStr;

		for (umint iRebuild = 0; iRebuild < _Migration.m_Operations.f_GetLen(); ++iRebuild)
		{
			auto const &Rebuild = _Migration.m_Operations[iRebuild];
			if (Rebuild.m_Type != ESqlSchemaMigrationOperationType::mc_RebuildTable && Rebuild.m_Type != ESqlSchemaMigrationOperationType::mc_DropColumn)
				continue;

			for (umint iRename = iRebuild + 1; iRename < _Migration.m_Operations.f_GetLen(); ++iRename)
			{
				auto const &Rename = _Migration.m_Operations[iRename];
				// SQLite matches table names case-insensitively
				if (Rename.m_Type == ESqlSchemaMigrationOperationType::mc_RenameColumn && NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*Rename.m_pTableName, *Rebuild.m_pTableName))
				{
					return DMibErrorDatabaseInstance
						(
							"Migration renames column '{}.{}' after rebuilding (or dropping a column from) its table; order the rename first"_f
							<< *Rename.m_pTableName
							<< *Rename.m_pOldName
						)
					;
				}
			}
		}

		return {};
	}

	void fg_SqliteRenameColumnReference(NContainer::TCVector<NStr::CStr const *> &_Columns, NStr::CStr const *_pOldName, NStr::CStr const *_pNewName)
	{
		for (auto &pColumn : _Columns)
		{
			if (*pColumn == *_pOldName)
				pColumn = _pNewName;
		}
	}

	void fg_SqliteApplyMigrationOperationToPlannedSchema
		(
			CSqlSchemaVersionDescription &_Schema
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlitePlannedSchemaStorage &o_Storage
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

				for (auto &Index : pTable->m_Indexes)
					fg_SqliteRenameColumnReference(Index.m_Columns, _Operation.m_pOldName, _Operation.m_pNewName);

				for (auto &Constraint : pTable->m_Constraints)
					fg_SqliteRenameColumnReference(Constraint.m_Columns, _Operation.m_pOldName, _Operation.m_pNewName);

				// SQLite also rewrites the CHECK and generated expressions that use the column
				for (auto &Constraint : pTable->m_Constraints)
				{
					if (Constraint.m_Type != ESqlConstraintType::mc_Check || !Constraint.m_pCheckSql)
						continue;

					NStr::CStr Renamed = NPrivate::fg_SqlTextRenameSqliteIdentifier(*Constraint.m_pCheckSql, *_Operation.m_pOldName, *_Operation.m_pNewName);
					if (Renamed == *Constraint.m_pCheckSql)
						continue;

					Constraint.m_pCheckSql = o_Storage.m_Strings.f_InsertLast(NStorage::TCUniquePointer<NStr::CStr>(fg_Construct(fg_Move(Renamed)))).f_Get();
				}

				for (auto &Column : pTable->m_Columns)
				{
					CNonPortableColumnOptions const *pOptions = Column.f_NonPortableOptions(NStr::gc_Str<"sqlite">.m_Str);
					if (!pOptions || pOptions->m_pGeneratedSql->f_IsEmpty())
						continue;

					NStr::CStr Renamed = NPrivate::fg_SqlTextRenameSqliteIdentifier(*pOptions->m_pGeneratedSql, *_Operation.m_pOldName, *_Operation.m_pNewName);
					if (Renamed == *pOptions->m_pGeneratedSql)
						continue;

					auto &pGeneratedSql = o_Storage.m_Strings.f_InsertLast(NStorage::TCUniquePointer<NStr::CStr>(fg_Construct(fg_Move(Renamed))));
					auto &pOptionsCopy = o_Storage.m_Options.f_InsertLast
						(
							NStorage::TCUniquePointer<NContainer::TCVector<CNonPortableColumnOptions>>(fg_Construct())
						)
					;
					for (umint iOption = 0; iOption < Column.m_nNonPortableOptions; ++iOption)
					{
						CNonPortableColumnOptions Option = Column.m_pNonPortableOptions[iOption];
						if (&Column.m_pNonPortableOptions[iOption] == pOptions)
							Option.m_pGeneratedSql = pGeneratedSql.f_Get();
						pOptionsCopy->f_InsertLast(Option);
					}

					Column.m_pNonPortableOptions = pOptionsCopy->f_GetArray();
				}
			}

			// SQLite also rewrites the foreign keys that reference the renamed column
			for (auto &Table : _Schema.m_Database.m_Tables)
			{
				for (auto &Constraint : Table.m_Constraints)
				{
					if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey && Constraint.f_ReferencedTable() == *_Operation.m_pTableName)
						fg_SqliteRenameColumnReference(Constraint.m_ReferencedColumns, _Operation.m_pOldName, _Operation.m_pNewName);
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

	// SQLite cannot add a table constraint or change an existing column or constraint in place
	auto fg_SqliteTableNeedsRebuild(CSqlTableDescription const &_Table, CSqlSchemaVersionDescription const *_pPreviousSchema)
		-> NConcurrency::TCWrapped<bool>
	{
		if (!_pPreviousSchema)
			co_return false;

		// A live table that a later version first declares is adopted like one without any recorded version
		CSqlTableDescription const *pPreviousTable = fg_SqliteFindTable(*_pPreviousSchema, _Table.f_Name());
		if (!pPreviousTable)
			co_return true;

		for (auto const &Constraint : _Table.m_Constraints)
		{
			bool bUnchanged = false;
			for (auto const &PreviousConstraint : pPreviousTable->m_Constraints)
			{
				if (PreviousConstraint.f_Name() == Constraint.f_Name())
					bUnchanged = fg_SqliteConstraintDefinitionSql(PreviousConstraint) == fg_SqliteConstraintDefinitionSql(Constraint);
			}

			if (!bUnchanged)
				co_return true;
		}

		for (auto const &Column : _Table.m_Columns)
		{
			for (auto const &PreviousColumn : pPreviousTable->m_Columns)
			{
				if (PreviousColumn.f_Name() == Column.f_Name() && co_await fg_SqliteColumnDefinitionSql(PreviousColumn) != co_await fg_SqliteColumnDefinitionSql(Column))
					co_return true;
			}
		}

		co_return false;
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
		NStr::CStr TrimmedDefaultSql = DefaultSql.f_Trim();
		if (!TrimmedDefaultSql.f_IsEmpty() && (TrimmedDefaultSql[0] == '(' || TrimmedDefaultSql.f_FindNoCase("CURRENT_") == 0))
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

	// The definition of an index that a later schema version changed while keeping its name
	bool fg_SqliteIndexDefinitionChanged(CSqlTableDescription const *_pPreviousTable, CSqlIndexDescription const &_Index)
	{
		if (!_pPreviousTable)
			return false;

		for (auto const &PreviousIndex : _pPreviousTable->m_Indexes)
		{
			if (PreviousIndex.f_Name() != _Index.f_Name())
				continue;

			if (PreviousIndex.f_IsUnique() != _Index.f_IsUnique() || PreviousIndex.m_Columns.f_GetLen() != _Index.m_Columns.f_GetLen())
				return true;

			for (umint i = 0; i < _Index.m_Columns.f_GetLen(); ++i)
			{
				if (*PreviousIndex.m_Columns[i] != *_Index.m_Columns[i])
					return true;
			}

			return false;
		}

		return false;
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

			bool bRebuild = co_await fg_SqliteTableNeedsRebuild(Table, _pPreviousSchema);
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

				if (!Column.f_IsNullable() && fg_SqliteColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey() && !fg_SqliteColumnIsGenerated(Column))
					co_return DMibErrorDatabaseInstance("Cannot add required SQLite column '{}.{}' without a default value"_f << Table.f_Name() << Column.f_Name());

				// A new column whose constraint ALTER TABLE ADD COLUMN cannot add (UNIQUE/PRIMARY KEY) forces a rebuild,
				// matching the executor so the previewed plan applies cleanly.
				if (fg_SqliteColumnNeedsRebuildToAdd(Column))
					bRebuild = true;

				NewColumns.f_InsertLast(&Column);
			}

			if (bRebuild)
			{
				// As the executor, refuse to drop a column that only the previous version declares
				for (auto const &PreviousColumn : pPreviousTable->m_Columns)
				{
					bool bKept = false;
					for (auto const &Column : Table.m_Columns)
					{
						if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), PreviousColumn.f_Name()))
							bKept = true;
					}

					if (!bKept)
					{
						co_return DMibErrorDatabaseInstance
							(
								"SQLite table '{}' has column '{}' that the schema does not declare; rebuilding the table would drop it, so drop it with a migration"_f
								<< Table.f_Name()
								<< PreviousColumn.f_Name()
							)
						;
					}
				}

				for (auto &Statement : co_await fg_SqliteRebuildTableStatements(Table, fg_SqliteCommonColumns(Table, pPreviousTable)))
					Statements.f_InsertLast(fg_Move(Statement));
			}
			else
			{
				for (auto const *pColumn : NewColumns)
					Statements.f_InsertLast(co_await fg_SqliteAlterTableAddColumn(Table, *pColumn));
			}

			for (auto const &Index : Table.m_Indexes)
			{
				if (!bRebuild && fg_SqliteIndexDefinitionChanged(pPreviousTable, Index))
				{
					NStr::CStr DropSql;
					{
						NStr::CStr::CAppender Appender(DropSql);
						// DROP COLUMN can already have dropped the index
						Appender += "DROP INDEX IF EXISTS ";
						fg_SqliteAppendQuotedIdentifier(Appender, Index.f_Name());
					}
					Statements.f_InsertLast(fg_Move(DropSql));
					Statements.f_InsertLast(fg_SqliteCreateIndex(Table, Index, false));
				}
				else
					Statements.f_InsertLast(fg_SqliteCreateIndex(Table, Index, true));
			}
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

		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "INSERT INTO ";
				fg_SqliteAppendQuotedIdentifier(Appender, _Table.f_Name());
				if (_ExistingColumns.f_IsEmpty())
				{
					// Without shared columns the rows are still kept, every column taking its default. A declared column
					// can shadow any rowid alias, so the old table's rowid is not read and the new table's alias is
					// one that no column uses.
					ch8 const *pRowidAlias = nullptr;
					for (ch8 const *pCandidate : {"rowid", "_rowid_", "oid"})
					{
						bool bShadowed = false;
						for (auto const &Column : _Table.m_Columns)
						{
							if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), pCandidate))
								bShadowed = true;
						}

						if (!bShadowed)
						{
							pRowidAlias = pCandidate;
							break;
						}
					}

					if (!pRowidAlias)
						co_return DMibErrorDatabaseInstance("Cannot rebuild SQLite table '{}': its columns shadow every rowid alias"_f << _Table.f_Name());

					Appender += " (";
					Appender += pRowidAlias;
					Appender += ") SELECT NULL";
				}
				else
				{
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
				}
				Appender += " FROM ";
				fg_SqliteAppendQuotedIdentifier(Appender, OldTableName);
			}
			Statements.f_InsertLast(fg_Move(Sql));
		}

		bool bAutoIncrement = false;
		for (auto const &Column : _Table.m_Columns)
		{
			if (fg_IsSet(Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
				bAutoIncrement = true;
		}

		if (bAutoIncrement)
		{
			// The copy only advances the new sequence to the highest surviving ID, and dropping the old table deletes the
			// old sequence, so carry the old high-water mark over to keep AUTOINCREMENT from reusing deleted IDs.
			// sqlite_sequence has no unique key on name, which rules out an upsert.
			auto fAppendSequenceOfTable = [&](NStr::CStr::CAppender &_Appender, NStr::CStr const &_TableName)
				{
					_Appender += "(SELECT seq FROM sqlite_sequence WHERE name = ";
					fg_SqliteAppendStringLiteral(_Appender, _TableName);
					_Appender += ")";
				}
			;

			NStr::CStr UpdateSql;
			{
				NStr::CStr::CAppender Appender(UpdateSql);
				Appender += "UPDATE sqlite_sequence SET seq = MAX(seq, COALESCE(";
				fAppendSequenceOfTable(Appender, OldTableName);
				Appender += ", 0)) WHERE name = ";
				fg_SqliteAppendStringLiteral(Appender, _Table.f_Name());
			}
			Statements.f_InsertLast(fg_Move(UpdateSql));

			NStr::CStr InsertSql;
			{
				NStr::CStr::CAppender Appender(InsertSql);
				Appender += "INSERT INTO sqlite_sequence (name, seq) SELECT ";
				fg_SqliteAppendStringLiteral(Appender, _Table.f_Name());
				Appender += ", seq FROM sqlite_sequence WHERE name = ";
				fg_SqliteAppendStringLiteral(Appender, OldTableName);
				Appender += " AND NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = ";
				fg_SqliteAppendStringLiteral(Appender, _Table.f_Name());
				Appender += ")";
			}
			Statements.f_InsertLast(fg_Move(InsertSql));
		}

		Statements.f_InsertLast(fg_SqliteDropTableSql(OldTableName));

		co_return fg_Move(Statements);
	}

	// _Columns lists names as "<byte length>:<name>" separated by commas, so a name containing a comma stays intact
	bool fg_SqliteTableHasAllColumns(CSqlTableDescription const &_Table, NStr::CStr const &_Columns)
	{
		ch8 const *pChar = _Columns.f_GetStr();
		while (*pChar)
		{
			umint Length = 0;
			while (*pChar >= '0' && *pChar <= '9')
				Length = Length * 10 + umint(*pChar++ - '0');
			if (*pChar != ':')
				return false;
			++pChar;

			NStr::CStr Name(pChar, aint(Length));
			pChar += Length;
			if (*pChar == ',')
				++pChar;

			bool bFound = false;
			for (auto const &Column : _Table.m_Columns)
			{
				if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), Name))
					bFound = true;
			}

			if (!bFound)
				return false;
		}

		return true;
	}

	NConcurrency::TCWrapped<void> fg_SqliteRebuildTable(CSQLiteDatabaseHandle &_Database, CSqlTableDescription const &_Table)
	{
		using namespace NStr;

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
		// Triggers and indexes move with the renamed table and are dropped with it, so they are recreated from their
		// definitions. The schema's own indexes are created again by the caller.
		NContainer::TCVector<CSqlValue> TableValues;
		TableValues.f_InsertLast(_Table.f_Name());
		auto Dependents = co_await _Database.f_SelectRaw
			(
				NStr::CStr
				(
					"SELECT m.type, m.name, m.sql, "
					"(SELECT group_concat(length(CAST(i.name AS BLOB)) || ':' || i.name, ',') FROM pragma_index_info(m.name) AS i WHERE i.name IS NOT NULL) "
					"FROM sqlite_master AS m WHERE m.type IN ('index', 'trigger') AND m.tbl_name = ? COLLATE NOCASE AND m.sql IS NOT NULL "
					"ORDER BY m.type = 'trigger', m.rowid"
				)
				, TableValues
			)
		;
		// SQLite accepts a trigger whose body names a missing column and fails the writes that fire it. Any identifier
		// in the trigger counts, so one merely named like a dropped column is refused too.
		NContainer::TCVector<NStr::CStr> DroppedColumns;
		for (auto const &LiveColumn : co_await _Database.f_ColumnNames(_Table.f_Name()))
		{
			bool bKept = false;
			for (auto const &Column : _Table.m_Columns)
			{
				if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), LiveColumn))
					bKept = true;
			}

			if (!bKept)
				DroppedColumns.f_InsertLast(LiveColumn);
		}

		// Triggers on other tables and views are kept as text too, and fail once run against the dropped column
		if (!DroppedColumns.f_IsEmpty())
		{
			auto Others = co_await _Database.f_SelectRaw
				(
					NStr::CStr("SELECT name, sql FROM sqlite_master WHERE type IN ('trigger', 'view') AND tbl_name <> ? COLLATE NOCASE AND sql IS NOT NULL")
					, TableValues
				)
			;
			for (auto const &Other : Others.m_Rows)
			{
				NStr::CStr const &OtherSql = Other.m_Values[1].f_GetAsType<NStr::CStr>();
				if (!NPrivate::fg_SqlTextHasIdentifierToken(OtherSql, _Table.f_Name()))
					continue;

				for (auto const &DroppedColumn : DroppedColumns)
				{
					if (NPrivate::fg_SqlTextMentionsIdentifier(OtherSql, DroppedColumn))
					{
						co_return DMibErrorDatabaseInstance
							(
								"SQLite '{}' may use column '{}.{}' that the rebuild removes"_f
								<< Other.m_Values[0].f_GetAsType<NStr::CStr>()
								<< _Table.f_Name()
								<< DroppedColumn
							)
						;
					}
				}
			}
		}

		NContainer::TCVector<NStr::CStr> DependentSql;
		for (auto const &Dependent : Dependents.m_Rows)
		{
			if (Dependent.m_Values[0].f_GetAsType<NStr::CStr>() == "trigger")
			{
				for (auto const &DroppedColumn : DroppedColumns)
				{
					if (NPrivate::fg_SqlTextMentionsIdentifier(Dependent.m_Values[2].f_GetAsType<NStr::CStr>(), DroppedColumn))
					{
						co_return DMibErrorDatabaseInstance
							(
								"SQLite trigger '{}' uses column '{}.{}' that the rebuild removes"_f
								<< Dependent.m_Values[1].f_GetAsType<NStr::CStr>()
								<< _Table.f_Name()
								<< DroppedColumn
							)
						;
					}
				}
			}

			bool bSkip = false;
			if (Dependent.m_Values[0].f_GetAsType<NStr::CStr>() == "index")
			{
				for (auto const &Index : _Table.m_Indexes)
				{
					if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Index.f_Name(), Dependent.m_Values[1].f_GetAsType<NStr::CStr>()))
						bSkip = true;
				}

				// An index on a column the target drops goes with the column, also when only its expression or WHERE uses it
				if (Dependent.m_Values[3].f_GetTypeID() == ESqlValueType::mc_Text)
					bSkip = bSkip || !fg_SqliteTableHasAllColumns(_Table, Dependent.m_Values[3].f_GetAsType<NStr::CStr>());

				for (auto const &DroppedColumn : DroppedColumns)
					bSkip = bSkip || NPrivate::fg_SqlTextMentionsIdentifier(Dependent.m_Values[2].f_GetAsType<NStr::CStr>(), DroppedColumn, true);
			}

			if (!bSkip)
				DependentSql.f_InsertLast(Dependent.m_Values[2].f_GetAsType<NStr::CStr>());
		}

		auto Statements = co_await fg_SqliteRebuildTableStatements(_Table, ExistingColumns);

		for (auto &Sql : Statements)
			co_await _Database.f_Execute(Sql);

		for (auto const &Sql : DependentSql)
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
		using namespace NStr;

		if (!co_await _Database.f_HasTable(*_Operation.m_pTableName))
			co_return {};

		// Columns a version adds appear only after its operations ran, so a backfill of one would silently do nothing
		if (!co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pNewName))
		{
			co_return DMibErrorDatabaseInstance
				(
					"Cannot update column '{}.{}' before it exists; add the column in an earlier schema version"_f
					<< *_Operation.m_pTableName
					<< *_Operation.m_pNewName
				)
			;
		}

		co_return _Database.f_Execute(fg_SqliteUpdateColumnSql(_Operation));
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
			if (co_await _Database.f_HasTable(*_Operation.m_pOldName))
			{
				// SQLite matches names case-insensitively, so a rename that only changes case finds the table itself
				if (!NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*_Operation.m_pOldName, *_Operation.m_pNewName) && co_await _Database.f_HasTable(*_Operation.m_pNewName))
				{
					co_return DMibErrorDatabaseInstance
						(
							"Cannot rename SQLite table '{}' to '{}': both tables exist"_f << *_Operation.m_pOldName << *_Operation.m_pNewName
						)
					;
				}

				for (auto const &Sql : fg_SqliteRenameTable(_Operation))
					co_await _Database.f_Execute(Sql);

				co_return {};
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn:
			if (co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pOldName))
			{
				if
				(
					!NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*_Operation.m_pOldName, *_Operation.m_pNewName)
					&& co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pNewName)
				)
				{
					co_return DMibErrorDatabaseInstance
						(
							"Cannot rename SQLite column '{}.{}' to '{}': both columns exist"_f
							<< *_Operation.m_pTableName
							<< *_Operation.m_pOldName
							<< *_Operation.m_pNewName
						)
					;
				}

				co_return _Database.f_Execute(fg_SqliteRenameColumn(_Operation));
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
		if (auto pException = fg_SqliteValidateRenameAfterRebuild(_Migration))
			co_return fg_Move(pException);

		for (auto const &Operation : _Migration.m_Operations)
			co_await fg_SqliteApplyMigrationOperation(_Database, Operation, _TargetSchema);

		co_return {};
	}

	// IF NOT EXISTS keeps an existing index of the same name, compared case-insensitively, even when it is on another
	// table or no longer matches the declaration, which would leave the declared index missing without an error
	NConcurrency::TCWrapped<void> fg_SqliteEnsureIndex(CSQLiteDatabaseHandle &_Database, CSqlTableDescription const &_Table, CSqlIndexDescription const &_Index)
	{
		using namespace NStr;

		co_await _Database.f_Execute(fg_SqliteCreateIndex(_Table, _Index, true));

		NContainer::TCVector<CSqlValue> Values;
		Values.f_InsertLast(_Index.f_Name());
		auto Existing = co_await _Database.f_SelectRaw
			(
				NStr::CStr
				(
					"SELECT m.name, m.tbl_name, il.\"unique\", il.origin, "
					"(SELECT group_concat(length(CAST(name AS BLOB)) || ':' || name || ':' || upper(coll), ',') FROM "
					"(SELECT name, coll FROM pragma_index_xinfo(m.name) WHERE key ORDER BY seqno)), "
					"il.partial = 0 AND NOT EXISTS (SELECT 1 FROM pragma_index_info(m.name) WHERE name IS NULL) "
					"FROM sqlite_master m JOIN pragma_index_list(m.tbl_name) il ON il.name = m.name "
					"WHERE m.type = 'index' AND m.name = ? COLLATE NOCASE"
				)
				, Values
			)
		;
		if (Existing.m_Rows.f_GetLen() != 1)
			co_return DMibErrorDatabaseInstance("SQLite index '{}' of table '{}' could not be found after creating it"_f << _Index.f_Name() << _Table.f_Name());

		// Length-prefixed, so a column name containing the separator cannot match another column list. The created index
		// takes each column's collation, which an existing index of the same name must match.
		NStr::CStr Columns;
		for (auto const *pColumn : _Index.m_Columns)
		{
			NStr::CStr Collation = "BINARY";
			for (auto const &Column : _Table.m_Columns)
			{
				if (Column.f_Name() != *pColumn)
					continue;

				CNonPortableColumnOptions const *pNonPortableOptions = Column.f_NonPortableOptions(NStr::gc_Str<"sqlite">.m_Str);
				if (pNonPortableOptions && !pNonPortableOptions->m_pCollationSql->f_IsEmpty())
					Collation = pNonPortableOptions->m_pCollationSql->f_UpperCase();
			}

			if (!Columns.f_IsEmpty())
				Columns += ",";
			Columns += "{}:{}:{}"_f << pColumn->f_GetLen() << *pColumn << Collation;
		}

		auto const &Row = Existing.m_Rows[0].m_Values;
		bool bSameTable = Row[0].f_GetAsType<NStr::CStr>() == _Index.f_Name() && Row[1].f_GetAsType<NStr::CStr>() == _Table.f_Name();
		bool bStandalone = Row[3].f_GetAsType<NStr::CStr>() == "c";
		bool bMatches = (Row[2].f_GetAsType<int64>() != 0) == _Index.f_IsUnique()
			&& Row[4].f_GetTypeID() == ESqlValueType::mc_Text
			&& Row[4].f_GetAsType<NStr::CStr>() == Columns
			&& Row[5].f_GetAsType<int64>() != 0
		;

		if (bMatches && bSameTable && bStandalone)
			co_return {};

		if (!bSameTable || !bStandalone)
			co_return DMibErrorDatabaseInstance("SQLite index name '{}' of table '{}' is already used by another index"_f << _Index.f_Name() << _Table.f_Name());

		// A later schema version changed the definition of this table's index
		NStr::CStr DropSql;
		{
			NStr::CStr::CAppender Appender(DropSql);
			Appender += "DROP INDEX ";
			fg_SqliteAppendQuotedIdentifier(Appender, _Index.f_Name());
		}
		co_await _Database.f_Execute(DropSql);
		co_await _Database.f_Execute(fg_SqliteCreateIndex(_Table, _Index, false));

		co_return {};
	}

	// A rebuild copies only the target's columns, so a live column the target does not declare would be dropped with its
	// data: one added outside version tracking, or one an earlier version declared that no migration drops explicitly
	auto fg_SqliteCheckNoUndeclaredColumns(CSQLiteDatabaseHandle &_Database, CSqlTableDescription const &_Table)
		-> NConcurrency::TCWrapped<NException::CExceptionPointer>
	{
		using namespace NStr;

		auto fDeclares = [](CSqlTableDescription const *_pTable, NStr::CStr const &_ColumnName)
			{
				for (auto const &Column : _pTable->m_Columns)
				{
					if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), _ColumnName))
						return true;
				}

				return false;
			}
		;

		auto LiveColumns = co_await _Database.f_ColumnNames(_Table.f_Name());
		for (auto const &LiveColumn : LiveColumns)
		{
			if (!fDeclares(&_Table, LiveColumn))
			{
				co_return DMibErrorDatabaseInstance
					(
						"SQLite table '{}' has column '{}' that the schema does not declare; rebuilding the table would drop it, so drop it with a migration"_f
						<< _Table.f_Name()
						<< LiveColumn
					)
				;
			}
		}

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
					co_await fg_SqliteEnsureIndex(_Database, Table, Index);

				continue;
			}

			bool bRebuild = co_await fg_SqliteTableNeedsRebuild(Table, _pPreviousSchema);

			// Adopting a table that already exists with no tracked previous schema (schema_migrations was empty): it may
			// have been created outside version tracking and be missing declared constraints, both table-level
			// (UNIQUE/CHECK/foreign key) and column-level (PRIMARY KEY/UNIQUE/NOT NULL). SQLite cannot add them in
			// place, so rebuild the table to materialize the declared definition. The rebuild copies forward every column
			// shared with the live table, so existing data is preserved; if those rows violate a now-enforced constraint
			// the rebuild (or the trailing PRAGMA foreign_key_check) fails the apply, which is the correct outcome for
			// data inconsistent with the declared schema.
			if (!_pPreviousSchema)
				bRebuild = true;

			NContainer::TCVector<CSqlColumnDescription const *> NewColumns;
			for (auto const &Column : Table.m_Columns)
			{
				if (co_await _Database.f_HasColumn(Table.f_Name(), Column.f_Name()))
					continue;

				if (!Column.f_IsNullable() && fg_SqliteColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey() && !fg_SqliteColumnIsGenerated(Column))
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
			{
				if (auto pException = co_await fg_SqliteCheckNoUndeclaredColumns(_Database, Table))
					co_return fg_Move(pException);

				co_await fg_SqliteRebuildTable(_Database, Table);
			}
			else
			{
				for (auto const *pColumn : NewColumns)
					co_await _Database.f_Execute(co_await fg_SqliteAlterTableAddColumn(Table, *pColumn));
			}

			for (auto const &Index : Table.m_Indexes)
				co_await fg_SqliteEnsureIndex(_Database, Table, Index);
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
	// SQLite matches identifiers case-insensitively for ASCII letters, so names the schema keeps apart would refer to one
	// object: IF NOT EXISTS would then silently skip the second table or index. Tables and indexes share one namespace.
	NException::CExceptionPointer NPrivate::fg_SqliteValidateSchemaVersions(NContainer::TCVector<CSqlSchemaVersionDescription> const &_Versions)
	{
		using namespace NStr;

		auto fInsertUnique = [](NContainer::TCSet<NStr::CStr> &o_Names, NStr::CStr const &_Name)
			{
				NStr::CStr Key = _Name;
				ch8 *pKey = Key.f_GetStrUniqueWritable();
				for (aint i = 0; i < Key.f_GetLen(); ++i)
				{
					if (pKey[i] >= 'A' && pKey[i] <= 'Z')
						pKey[i] = ch8(pKey[i] - 'A' + 'a');
				}
				if (o_Names.f_FindEqual(Key))
					return false;

				o_Names[fg_Move(Key)];
				return true;
			}
		;

		for (umint iVersion = 0; iVersion < _Versions.f_GetLen(); ++iVersion)
		{
			auto const &Version = _Versions[iVersion];

			// An empty ID reads as no applied version, which would skip creating the schema
			if (Version.f_ID().f_IsEmpty())
				return DMibErrorDatabaseInstance("SQL schema version ID cannot be empty");

			// Applied versions are looked up by ID, so a repeated ID would hide the later version
			for (umint iOther = 0; iOther < iVersion; ++iOther)
			{
				if (_Versions[iOther].f_ID() == Version.f_ID())
					return DMibErrorDatabaseInstance("SQL schema version ID '{}' is used more than once"_f << Version.f_ID());
			}

			NContainer::TCSet<NStr::CStr> RelationNames;
			for (auto const &Table : Version.m_Database.m_Tables)
			{
				if (!fInsertUnique(RelationNames, Table.f_Name()))
					return DMibErrorDatabaseInstance("SQLite table name '{}' collides with another table or index name"_f << Table.f_Name());

				NContainer::TCSet<NStr::CStr> ColumnNames;
				for (auto const &Column : Table.m_Columns)
				{
					if (!fInsertUnique(ColumnNames, Column.f_Name()))
						return DMibErrorDatabaseInstance("SQLite column name '{}.{}' collides with another column name"_f << Table.f_Name() << Column.f_Name());

					if (Column.f_IsGeneratedOnAnyBackend() && !fg_SqliteColumnIsGenerated(Column))
					{
						return DMibErrorDatabaseInstance
							(
								"SQL column '{}.{}' is generated on another backend but has no SQLite generated expression"_f << Table.f_Name() << Column.f_Name()
							)
						;
					}
				}
			}

			for (auto const &Table : Version.m_Database.m_Tables)
			{
				// Constraints are matched across versions by name, so a repeated name would hide the second one
				NContainer::TCSet<NStr::CStr> ConstraintNames;
				for (auto const &Constraint : Table.m_Constraints)
				{
					if (!fInsertUnique(ConstraintNames, Constraint.f_Name()))
						return DMibErrorDatabaseInstance("SQLite table '{}' declares constraint '{}' more than once"_f << Table.f_Name() << Constraint.f_Name());
				}

				for (auto const &Index : Table.m_Indexes)
				{
					if (!fInsertUnique(RelationNames, Index.f_Name()))
						return DMibErrorDatabaseInstance("SQLite index name '{}' collides with another table or index name"_f << Index.f_Name());
				}
			}
		}

		return {};
	}

	NConcurrency::TCWrapped<CSqlSchemaMigrationPlan> fg_SqlitePlanSchemaMigration(ICSqlSchemaVersions const &_SchemaVersions, NStr::CStr const *_pCurrentVersionID)
	{
		using namespace NStr;
		using namespace NPrivate;

		CSqlSchemaMigrationPlan Plan;
		NContainer::TCVector<CSqlSchemaVersionDescription> Versions = _SchemaVersions.f_DescribeVersions();
		NContainer::TCVector<CSqlSchemaMigrationDescription> Migrations = _SchemaVersions.f_DescribeMigrations();
		if (Versions.f_IsEmpty())
			co_return fg_Move(Plan);

		if (auto pException = fg_SqliteValidateSchemaVersions(Versions))
			co_return fg_Move(pException);

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
			CSqlitePlannedSchemaStorage PlannedSchemaStorage;
			CSqlSchemaVersionDescription PlannedPreviousVersion = PreviousVersion;
			if (auto const *pMigration = fg_SqliteFindMigration(Migrations, PreviousVersion.f_ID(), NextVersion.f_ID()))
			{
				if (auto pException = fg_SqliteValidateRenameAfterRebuild(*pMigration))
					co_return fg_Move(pException);

				for (auto const &Operation : pMigration->m_Operations)
				{
					Plan.m_Statements.f_InsertLast(co_await fg_SqliteMigrationOperationStatements(Operation, NextVersion, &PlannedPreviousVersion));
					fg_SqliteApplyMigrationOperationToPlannedSchema(PlannedPreviousVersion, Operation, NextVersion, PlannedSchemaStorage);
				}
			}

			Plan.m_Statements.f_InsertLast(co_await fg_SqliteSyncAdditiveSchemaStatements(NextVersion, &PlannedPreviousVersion));
			Plan.m_Statements.f_InsertLast(fg_SqliteInsertSchemaVersionSql());
		}

		co_return fg_Move(Plan);
	}
}
