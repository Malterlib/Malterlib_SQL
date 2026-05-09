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
	// renames can be missing under its old name and lose its data
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
				if (Rename.m_Type == ESqlSchemaMigrationOperationType::mc_RenameColumn && NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*Rename.m_pTableName, *Rebuild.m_pTableName))
				{
					return DMibErrorDatabaseInstance
						(
							"Migration renames column '{}.{}' after rebuilding (or dropping a column from) its table; order the rename first"_f << *Rename.m_pTableName << *Rename.m_pOldName
						)
					;
				}
			}
		}

		return {};
	}

	bool fg_SqliteMigrationDropsColumn(CSqlSchemaMigrationDescription const *_pMigration, NStr::CStr const &_TableName, NStr::CStr const &_ColumnName)
	{
		if (!_pMigration)
			return false;

		for (auto const &Operation : _pMigration->m_Operations)
		{
			if
			(
				Operation.m_Type == ESqlSchemaMigrationOperationType::mc_DropColumn
				&& NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*Operation.m_pTableName, _TableName)
				&& NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*Operation.m_pOldName, _ColumnName)
			)
			{
				return true;
			}
		}

		return false;
	}

	// A rebuild creates only the columns of _TargetTable, which would remove a column that a later operation of the migration drops before
	// the operations in between can use it, so such a column is kept until that drop. _Operation is one of _Migration's operations, and
	// _pPlannedTable describes the table just before _Operation.
	auto fg_SqliteRebuildTableKeepingLaterDrops
		(
			CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlTableDescription const &_TargetTable
			, CSqlTableDescription const *_pPlannedTable
		)
		-> CSqlTableDescription
	{
		CSqlTableDescription Table = _TargetTable;
		if (!_pPlannedTable)
			return Table;

		umint iOperation = umint(&_Operation - _Migration.m_Operations.f_GetArray());
		for (umint iLater = iOperation + 1; iLater < _Migration.m_Operations.f_GetLen(); ++iLater)
		{
			auto const &Later = _Migration.m_Operations[iLater];
			if (Later.m_Type != ESqlSchemaMigrationOperationType::mc_DropColumn || !NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*Later.m_pTableName, _TargetTable.f_Name()))
				continue;

			if (_Operation.m_Type == ESqlSchemaMigrationOperationType::mc_DropColumn && NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*Later.m_pOldName, *_Operation.m_pOldName))
				continue;

			bool bInTable = false;
			for (auto const &Column : Table.m_Columns)
				bInTable = bInTable || NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), *Later.m_pOldName);

			if (bInTable)
				continue;

			for (auto const &Column : _pPlannedTable->m_Columns)
			{
				if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), *Later.m_pOldName))
				{
					Table.m_Columns.f_InsertLast(Column);
					break;
				}
			}
		}

		return Table;
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
			, CSqlSchemaMigrationDescription const &_Migration
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

			// SQLite retargets the child foreign keys of a renamed table, and the additive diff must see the same references
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
					auto &pOptionsCopy = o_Storage.m_Options.f_InsertLast(NStorage::TCUniquePointer<NContainer::TCVector<CNonPortableColumnOptions>>(fg_Construct()));
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

				// The executor drops a column by rebuilding from the target schema, so the additive pass must not add columns the rebuild created.
				if (bHasColumn)
				{
					if (CSqlTableDescription const *pTarget = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName))
						*pPlanned = fg_SqliteRebuildTableKeepingLaterDrops(_Migration, _Operation, *pTarget, pPlanned);
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
			// Advance the planned schema to the rebuilt shape so the additive pass does not add its columns or constraints again.
			if (CSqlTableDescription const *pTarget = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName))
			{
				if (CSqlTableDescription *pPlanned = fg_SqliteFindTable(_Schema, *_Operation.m_pTableName))
					*pPlanned = fg_SqliteRebuildTableKeepingLaterDrops(_Migration, _Operation, *pTarget, pPlanned);
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql: break;
		}
	}

	// SQLite cannot alter most constraint and column definitions in place, so a new or changed constraint, or a changed definition of a kept column,
	// rebuilds the table
	auto fg_SqliteTableNeedsRebuild(CSqlTableDescription const &_Table, CSqlSchemaVersionDescription const *_pPreviousSchema) -> NConcurrency::TCWrapped<bool>
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
				if (!NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(PreviousColumn.f_Name(), Column.f_Name()))
					continue;

				// SQLite matches column names ignoring ASCII case, so a column whose name changes only in case keeps its definition
				CSqlColumnDescription NamedPreviousColumn = PreviousColumn;
				NamedPreviousColumn.m_pName = Column.m_pName;
				if (co_await fg_SqliteColumnDefinitionSql(NamedPreviousColumn) != co_await fg_SqliteColumnDefinitionSql(Column))
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

			// SQLite matches column names ignoring ASCII case, as the copy's INSERT ... SELECT reads them
			for (auto const &PreviousColumn : _pPreviousTable->m_Columns)
			{
				if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), PreviousColumn.f_Name()))
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
		// ADD COLUMN rejects UNIQUE and PRIMARY KEY columns, and STORED generated columns on a table that has rows
		if (fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_Unique) || _Column.f_IsPrimaryKey() || fg_SqliteColumnIsGenerated(_Column))
			return true;

		// ADD COLUMN also rejects a nonconstant default, such as CURRENT_TIMESTAMP or an expression, on a table that has rows
		NStr::CStr const &DefaultSql = fg_SqliteColumnDefaultSql(_Column);
		NStr::CStr TrimmedDefaultSql = DefaultSql.f_Trim();
		if (!TrimmedDefaultSql.f_IsEmpty() && (TrimmedDefaultSql[0] == '(' || TrimmedDefaultSql.f_FindNoCase("CURRENT_") == 0))
			return true;

		return false;
	}

	auto fg_SqliteRebuildCopiedRowid
		(
			CSqlTableDescription const &_Table
			, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns
			, NContainer::TCVector<NStr::CStr> const &_OldColumns
		)
		-> ch8 const *
	{
		// A copied INTEGER PRIMARY KEY column is the rowid of the new table, which a copied rowid would override
		NStr::CStr const *pRowidAliasColumn = nullptr;
		NContainer::TCVector<NStr::CStr const *> PrimaryKeyColumns;
		for (auto const &Column : _Table.m_Columns)
		{
			if (fg_IsSet(Column.m_Flags, ESqlColumnFlag::mc_PrimaryKey))
				PrimaryKeyColumns.f_InsertLast(&Column.f_Name());
		}
		for (auto const &Constraint : _Table.m_Constraints)
		{
			if (Constraint.m_Type == ESqlConstraintType::mc_PrimaryKey)
				PrimaryKeyColumns.f_Insert(Constraint.m_Columns);
		}
		if (PrimaryKeyColumns.f_GetLen() == 1)
		{
			for (auto const &Column : _Table.m_Columns)
			{
				NException::CExceptionPointer pTypeException;
				if (Column.f_Name() == *PrimaryKeyColumns[0] && fg_SqliteColumnType(Column.m_Type, pTypeException) == "INTEGER")
					pRowidAliasColumn = &Column.f_Name();
			}
		}

		if (pRowidAliasColumn)
		{
			for (auto const *pColumn : _ExistingColumns)
			{
				if (*pColumn == *pRowidAliasColumn)
					return nullptr;
			}
		}

		// A column of either table can shadow a rowid name
		for (ch8 const *pCandidate : {"rowid", "_rowid_", "oid"})
		{
			bool bShadowed = false;
			for (auto const &Column : _Table.m_Columns)
				bShadowed = bShadowed || NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), pCandidate);
			for (auto const &OldColumn : _OldColumns)
				bShadowed = bShadowed || NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(OldColumn, pCandidate);

			if (!bShadowed)
				return pCandidate;
		}

		return nullptr;
	}

	auto fg_SqliteRebuildTableStatementsFromPrevious(CSqlTableDescription const &_Table, CSqlTableDescription const *_pPreviousTable)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	{
		auto ExistingColumns = fg_SqliteCommonColumns(_Table, _pPreviousTable);

		// Tables created from a schema have rowids
		ch8 const *pCopiedRowid = nullptr;
		if (_pPreviousTable)
		{
			NContainer::TCVector<NStr::CStr> OldColumns;
			for (auto const &Column : _pPreviousTable->m_Columns)
				OldColumns.f_InsertLast(Column.f_Name());

			pCopiedRowid = fg_SqliteRebuildCopiedRowid(_Table, ExistingColumns, OldColumns);
		}

		co_return co_await fg_SqliteRebuildTableStatements(_Table, ExistingColumns, pCopiedRowid, ESqliteTableOption::mc_None);
	}

	auto fg_SqliteMigrationOperationStatements
		(
			CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable: Statements.f_InsertLast(fg_SqliteRenameTable(_Operation)); break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn: Statements.f_InsertLast(fg_SqliteRenameColumn(_Operation)); break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			{
				auto pTable = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance("Cannot rebuild SQLite table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_SqliteFindTable(*_pPreviousSchema, *_Operation.m_pTableName) : nullptr;
				Statements = co_await fg_SqliteRebuildTableStatementsFromPrevious(fg_SqliteRebuildTableKeepingLaterDrops(_Migration, _Operation, *pTable, pPreviousTable), pPreviousTable);

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_DropTable: Statements.f_InsertLast(fg_SqliteDropTableSql(*_Operation.m_pTableName)); break;
		case ESqlSchemaMigrationOperationType::mc_DropColumn:
			{
				auto pTable = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance("Cannot drop SQLite column from table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_SqliteFindTable(*_pPreviousSchema, *_Operation.m_pTableName) : nullptr;
				if (pPreviousTable)
				{
					// The rebuild removes every column the target does not declare, not only the dropped one
					for (auto const &PreviousColumn : pPreviousTable->m_Columns)
					{
						bool bKept = fg_SqliteMigrationDropsColumn(&_Migration, pTable->f_Name(), PreviousColumn.f_Name());
						for (auto const &Column : pTable->m_Columns)
						{
							if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), PreviousColumn.f_Name()))
								bKept = true;
						}

						if (!bKept)
						{
							co_return DMibErrorDatabaseInstance
								(
									"SQLite table '{}' has column '{}' that the schema does not declare; rebuilding the table would drop it, so drop it with a migration"_f
									<< pTable->f_Name()
									<< PreviousColumn.f_Name()
								)
							;
						}
					}
				}

				Statements = co_await fg_SqliteRebuildTableStatementsFromPrevious(fg_SqliteRebuildTableKeepingLaterDrops(_Migration, _Operation, *pTable, pPreviousTable), pPreviousTable);

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql: Statements.f_InsertLast(fg_SqliteUpdateColumnSql(_Operation)); break;
		}

		co_return fg_Move(Statements);
	}

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

			for (umint iColumn = 0; iColumn < _Index.m_Columns.f_GetLen(); ++iColumn)
			{
				if (*PreviousIndex.m_Columns[iColumn] != *_Index.m_Columns[iColumn])
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
					if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(PreviousColumn.f_Name(), Column.f_Name()))
					{
						bColumnExists = true;
						break;
					}
				}

				if (bColumnExists)
					continue;

				if (!Column.f_IsNullable() && fg_SqliteColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey() && !fg_SqliteColumnIsGenerated(Column))
					co_return DMibErrorDatabaseInstance("Cannot add required SQLite column '{}.{}' without a default value"_f << Table.f_Name() << Column.f_Name());

				if (fg_SqliteColumnNeedsRebuildToAdd(Column))
					bRebuild = true;

				NewColumns.f_InsertLast(&Column);
			}

			if (bRebuild)
			{
				// An implicit rebuild must not discard a column of the previous schema
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

				for (auto &Statement : co_await fg_SqliteRebuildTableStatementsFromPrevious(Table, pPreviousTable))
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
						// IF EXISTS: a column drop that the planned schema does not reflect can have rebuilt the table without its indexes
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

	// _pCopiedRowid names a rowid name that neither table shadows, to copy the rowids through, or is nullptr to copy none separately.
	// _Options are the table options of the old table, which the new one keeps.
	auto fg_SqliteRebuildTableStatements
		(
			CSqlTableDescription const &_Table
			, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns
			, ch8 const *_pCopiedRowid
			, ESqliteTableOption _Options
		)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		NStr::CStr OldTableName = "__mib_rebuild_old_{}"_f << _Table.f_Name();
		// Run in one transaction, with foreign_keys=OFF set before it, so an abort cannot leave this scratch table committed and legacy_alter_table=ON
		// for just this rename keeps child foreign keys on the original table name. Never pre-drop the name: a user table that has it must fail the rename.
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
		{
			NStr::CStr CreateSql = co_await fg_SqliteCreateTable(_Table, false);
			if (fg_IsSet(_Options, ESqliteTableOption::mc_Strict))
				CreateSql += " STRICT";
			// SQLite refuses it for a table without a primary key or with AUTOINCREMENT, which then cannot be rebuilt without
			// changing how it stores and generates keys
			if (fg_IsSet(_Options, ESqliteTableOption::mc_WithoutRowid))
				CreateSql += fg_IsSet(_Options, ESqliteTableOption::mc_Strict) ? ", WITHOUT ROWID" : " WITHOUT ROWID";
			Statements.f_InsertLast(fg_Move(CreateSql));
		}

		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				// OR ABORT overrides the conflict clauses of the new table, as ON CONFLICT IGNORE or REPLACE in a column's custom SQL,
				// so a row that conflicts fails the rebuild instead of being dropped
				Appender += "INSERT OR ABORT INTO ";
				fg_SqliteAppendQuotedIdentifier(Appender, _Table.f_Name());
				if (_pCopiedRowid)
				{
					Appender += " (";
					Appender += _pCopiedRowid;
					for (auto const *pExistingColumn : _ExistingColumns)
					{
						Appender += ", ";
						fg_SqliteAppendQuotedIdentifier(Appender, *pExistingColumn);
					}
					Appender += ") SELECT ";
					Appender += _pCopiedRowid;
					for (auto const *pExistingColumn : _ExistingColumns)
					{
						Appender += ", ";
						fg_SqliteAppendQuotedIdentifier(Appender, *pExistingColumn);
					}
				}
				else if (_ExistingColumns.f_IsEmpty() && fg_IsSet(_Options, ESqliteTableOption::mc_WithoutRowid))
				{
					// A WITHOUT ROWID table has no rowid to insert NULL into, so each row is kept through one column given its
					// default, which SQLite evaluates per row as for an omitted column. Without a default the column is given
					// NULL, which only an empty table copies, since the primary key of a WITHOUT ROWID table is NOT NULL.
					CSqlColumnDescription const *pCopiedColumn = nullptr;
					for (auto const &Column : _Table.m_Columns)
					{
						if (fg_SqliteColumnIsGenerated(Column))
							continue;

						if (!pCopiedColumn || (fg_SqliteColumnDefaultSql(*pCopiedColumn).f_IsEmpty() && !fg_SqliteColumnDefaultSql(Column).f_IsEmpty()))
							pCopiedColumn = &Column;
					}

					if (!pCopiedColumn)
						co_return DMibErrorDatabaseInstance("Cannot rebuild SQLite table '{}': it has no column that is not generated"_f << _Table.f_Name());

					NStr::CStr const &DefaultSql = fg_SqliteColumnDefaultSql(*pCopiedColumn);
					Appender += " (";
					fg_SqliteAppendQuotedIdentifier(Appender, pCopiedColumn->f_Name());
					Appender += ") SELECT ";
					if (DefaultSql.f_IsEmpty())
						Appender += "NULL";
					else
					{
						Appender += "(";
						Appender += DefaultSql;
						Appender += ")";
					}
				}
				else if (_ExistingColumns.f_IsEmpty())
				{
					// Without shared columns each row is kept with a new rowid, the other columns taking their defaults. A declared column can shadow any rowid
					// alias, so the old table's rowid is not read and the new table's alias is one that no column uses.
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
					for (umint iExistingColumn = 0; iExistingColumn < _ExistingColumns.f_GetLen(); ++iExistingColumn)
					{
						if (iExistingColumn != 0)
							Appender += ", ";

						fg_SqliteAppendQuotedIdentifier(Appender, *_ExistingColumns[iExistingColumn]);
					}
					Appender += ") SELECT ";
					for (umint iExistingColumn = 0; iExistingColumn < _ExistingColumns.f_GetLen(); ++iExistingColumn)
					{
						if (iExistingColumn != 0)
							Appender += ", ";

						fg_SqliteAppendQuotedIdentifier(Appender, *_ExistingColumns[iExistingColumn]);
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
			// SQLite computes generated columns; the rebuild's INSERT cannot supply their values.
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
		// SQLite keeps a trigger that names a missing column, which fails statements that can fire it, and an UPDATE OF entry
		// for it never matches, so refuse likely references to a dropped column without resolving names
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

		// The views that use the table, also through other views, where a SELECT * of an inner view passes the columns on
		auto Views = co_await _Database.f_SelectRaw(NStr::CStr("SELECT name, sql FROM sqlite_master WHERE type = 'view' AND sql IS NOT NULL"), {});
		NContainer::TCVector<NStr::CStr> DependencyNames;
		DependencyNames.f_InsertLast(_Table.f_Name());
		NContainer::TCVector<bool> ViewDepends;
		ViewDepends.f_SetLen(Views.m_Rows.f_GetLen());
		for (auto &bDepends : ViewDepends)
			bDepends = false;

		for (bool bAdded = true; bAdded;)
		{
			bAdded = false;
			for (umint iView = 0; iView < Views.m_Rows.f_GetLen(); ++iView)
			{
				if (ViewDepends[iView])
					continue;

				NStr::CStr const &ViewSql = Views.m_Rows[iView].m_Values[1].f_GetAsType<NStr::CStr>();
				for (auto const &DependencyName : DependencyNames)
				{
					if (NPrivate::fg_SqlTextHasIdentifierToken(ViewSql, DependencyName))
					{
						ViewDepends[iView] = true;
						break;
					}
				}

				if (ViewDepends[iView])
				{
					DependencyNames.f_InsertLast(Views.m_Rows[iView].m_Values[0].f_GetAsType<NStr::CStr>());
					bAdded = true;
				}
			}
		}

		// Triggers on other tables and views are kept as text too, and fail once run against the dropped column, also when they
		// read it through a view of the table
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
				bool bUsesTable = false;
				for (auto const &DependencyName : DependencyNames)
					bUsesTable = bUsesTable || NPrivate::fg_SqlTextHasIdentifierToken(OtherSql, DependencyName);

				if (!bUsesTable)
					continue;

				for (auto const &DroppedColumn : DroppedColumns)
				{
					if (NPrivate::fg_SqlTextMentionsSqliteColumn(OtherSql, DroppedColumn))
					{
						co_return DMibErrorDatabaseInstance
							(
								"SQLite '{}' may use column '{}.{}' that the rebuild removes"_f << Other.m_Values[0].f_GetAsType<NStr::CStr>() << _Table.f_Name() << DroppedColumn
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
					if (NPrivate::fg_SqlTextMentionsSqliteColumn(Dependent.m_Values[2].f_GetAsType<NStr::CStr>(), DroppedColumn))
					{
						co_return DMibErrorDatabaseInstance
							(
								"SQLite trigger '{}' uses column '{}.{}' that the rebuild removes"_f << Dependent.m_Values[1].f_GetAsType<NStr::CStr>() << _Table.f_Name() << DroppedColumn
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

				// An index on a column the target drops goes with the column, also when only its expression or WHERE appears to use it
				if (Dependent.m_Values[3].f_GetTypeID() == ESqlValueType::mc_Text)
					bSkip = bSkip || !fg_SqliteTableHasAllColumns(_Table, Dependent.m_Values[3].f_GetAsType<NStr::CStr>());

				for (auto const &DroppedColumn : DroppedColumns)
					bSkip = bSkip || NPrivate::fg_SqlTextMentionsSqliteColumn(Dependent.m_Values[2].f_GetAsType<NStr::CStr>(), DroppedColumn, true);
			}

			if (!bSkip)
				DependentSql.f_InsertLast(Dependent.m_Values[2].f_GetAsType<NStr::CStr>());
		}

		// Rows keep their rowids unless no copied INTEGER PRIMARY KEY carries them and the columns of the tables shadow every rowid name.
		// A WITHOUT ROWID table has none, and stays WITHOUT ROWID, where an INTEGER PRIMARY KEY is no rowid alias that generates keys.
		// A STRICT table stays STRICT, which keeps rejecting values of the wrong type.
		ch8 const *pCopiedRowid = nullptr;
		ESqliteTableOption TableOptions = ESqliteTableOption::mc_None;
		{
			auto TableList = co_await _Database.f_SelectRaw
				(
					NStr::CStr("SELECT wr, strict FROM pragma_table_list WHERE schema = 'main' AND type = 'table' AND name = ? COLLATE NOCASE")
					, TableValues
				)
			;
			if (TableList.m_Rows.f_GetLen() == 1)
			{
				if (TableList.m_Rows[0].m_Values[0].f_GetAsType<int64>() == 0)
					pCopiedRowid = fg_SqliteRebuildCopiedRowid(_Table, ExistingColumns, co_await _Database.f_ColumnNames(_Table.f_Name()));
				else
					TableOptions |= ESqliteTableOption::mc_WithoutRowid;

				if (TableList.m_Rows[0].m_Values[1].f_GetAsType<int64>() != 0)
					TableOptions |= ESqliteTableOption::mc_Strict;
			}
		}

		auto Statements = co_await fg_SqliteRebuildTableStatements(_Table, ExistingColumns, pCopiedRowid, TableOptions);

		for (auto &Sql : Statements)
			co_await _Database.f_Execute(Sql);

		for (auto const &Sql : DependentSql)
			co_await _Database.f_Execute(Sql);

		// A view can depend on the number of columns without naming them, as SELECT * under a column list does, which SQLite reports
		// only when the view is used, so every view naming the table must still prepare
		for (umint iView = 0; iView < Views.m_Rows.f_GetLen(); ++iView)
		{
			if (!ViewDepends[iView])
				continue;

			NStr::CStr const &ViewName = Views.m_Rows[iView].m_Values[0].f_GetAsType<NStr::CStr>();

			NStr::CStr CheckSql;
			{
				NStr::CStr::CAppender Appender(CheckSql);
				Appender += "SELECT * FROM ";
				fg_SqliteAppendQuotedIdentifier(Appender, ViewName);
				Appender += " LIMIT 0";
			}

			auto CheckResult = _Database.f_SelectRaw(CheckSql, {});
			if (!CheckResult)
				co_return DMibErrorDatabaseInstance("SQLite view '{}' fails after rebuilding table '{}': {}"_f << ViewName << _Table.f_Name() << CheckResult.f_GetExceptionStr());
		}

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

		// The additive pass adds columns after the migration operations, so a backfill needs a column that already exists
		if (!co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pNewName))
		{
			co_return DMibErrorDatabaseInstance
				(
					"Cannot update column '{}.{}' before it exists; add the column in an earlier schema version"_f << *_Operation.m_pTableName << *_Operation.m_pNewName
				)
			;
		}

		co_return _Database.f_Execute(fg_SqliteUpdateColumnSql(_Operation));
	}

	// A rebuild copies only the target's columns, so a live column the target does not declare would be dropped with its
	// data: one added outside version tracking, or one an earlier version declared that no migration drops explicitly.
	// _pMigration gives the columns its drop operations remove from the table, and the columns of _pRemovableTable, the
	// previous version's table that an explicit rebuild turns into the target's, can be removed too.
	auto fg_SqliteCheckNoUndeclaredColumns
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlTableDescription const &_Table
			, CSqlSchemaMigrationDescription const *_pMigration
			, CSqlTableDescription const *_pRemovableTable
		)
		-> NConcurrency::TCWrapped<NException::CExceptionPointer>
	{
		using namespace NStr;

		auto fDeclares = [&](NStr::CStr const &_ColumnName)
			{
				for (auto const &Column : _Table.m_Columns)
				{
					if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), _ColumnName))
						return true;
				}

				if (_pRemovableTable)
				{
					for (auto const &Column : _pRemovableTable->m_Columns)
					{
						if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Column.f_Name(), _ColumnName))
							return true;
					}
				}

				return fg_SqliteMigrationDropsColumn(_pMigration, _Table.f_Name(), _ColumnName);
			}
		;

		auto LiveColumns = co_await _Database.f_ColumnNames(_Table.f_Name());
		for (auto const &LiveColumn : LiveColumns)
		{
			if (!fDeclares(LiveColumn))
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

	// _PlannedSchema describes the tables before _Operation
	auto fg_SqliteApplyMigrationOperation
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription const &_PlannedSchema
		)
		-> NConcurrency::TCWrapped<void>
	{
		using namespace NStr;

		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable:
			if (co_await _Database.f_HasTable(*_Operation.m_pOldName))
			{
				// A rename differing only in ASCII case finds the source table under the target name
				if (!NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*_Operation.m_pOldName, *_Operation.m_pNewName) && co_await _Database.f_HasTable(*_Operation.m_pNewName))
					co_return DMibErrorDatabaseInstance("Cannot rename SQLite table '{}' to '{}': both tables exist"_f << *_Operation.m_pOldName << *_Operation.m_pNewName);

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
							"Cannot rename SQLite column '{}.{}' to '{}': both columns exist"_f << *_Operation.m_pTableName << *_Operation.m_pOldName << *_Operation.m_pNewName
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

				if (!co_await _Database.f_HasTable(*_Operation.m_pTableName))
					co_return {};

				CSqlTableDescription const *pPlannedTable = fg_SqliteFindTable(_PlannedSchema, *_Operation.m_pTableName);
				CSqlTableDescription Table = fg_SqliteRebuildTableKeepingLaterDrops(_Migration, _Operation, *pTable, pPlannedTable);

				// The rebuild removes the columns of the previous version that the target leaves out, but not one added outside version tracking
				if (auto pException = co_await fg_SqliteCheckNoUndeclaredColumns(_Database, Table, &_Migration, pPlannedTable))
					co_return fg_Move(pException);

				co_return fg_SqliteRebuildTable(_Database, Table);
			}
		case ESqlSchemaMigrationOperationType::mc_DropTable: co_return fg_SqliteDropTable(_Database, *_Operation.m_pTableName);
		case ESqlSchemaMigrationOperationType::mc_DropColumn:
			{
				auto pTable = fg_SqliteFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance("Cannot drop SQLite column from table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				if (co_await _Database.f_HasColumn(*_Operation.m_pTableName, *_Operation.m_pOldName))
				{
					CSqlTableDescription Table = fg_SqliteRebuildTableKeepingLaterDrops(_Migration, _Operation, *pTable, fg_SqliteFindTable(_PlannedSchema, *_Operation.m_pTableName));

					// The rebuild removes every live column the target does not declare, not only the dropped one
					if (auto pException = co_await fg_SqliteCheckNoUndeclaredColumns(_Database, Table, &_Migration, nullptr))
						co_return fg_Move(pException);

					co_return fg_SqliteRebuildTable(_Database, Table);
				}

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql: co_return fg_SqliteUpdateColumnSql(_Database, _Operation);
		}

		co_return {};
	}

	// o_PlannedSchema starts as the previous version and follows each operation, which the additive sync then compares against
	auto fg_SqliteApplyMigrationOperations
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription &o_PlannedSchema
			, CSqlitePlannedSchemaStorage &o_PlannedSchemaStorage
		)
		-> NConcurrency::TCWrapped<void>
	{
		if (auto pException = fg_SqliteValidateRenameAfterRebuild(_Migration))
			co_return fg_Move(pException);

		for (auto const &Operation : _Migration.m_Operations)
		{
			co_await fg_SqliteApplyMigrationOperation(_Database, _Migration, Operation, _TargetSchema, o_PlannedSchema);
			fg_SqliteApplyMigrationOperationToPlannedSchema(o_PlannedSchema, _Migration, Operation, _TargetSchema, o_PlannedSchemaStorage);
		}

		co_return {};
	}

	// Schema application turns foreign key enforcement off, so SQLite drops a table that another table references. The foreign key
	// check finds no violation in a child without rows, and later writes that check the child's foreign key fail for the missing parent.
	// SQLite also drops a table that a view reads and keeps the view, which fails once it is used.
	auto fg_SqliteCheckNoReferencesToDroppedTables(CSQLiteDatabaseHandle &_Database, NContainer::TCVector<NStr::CStr> const &_DroppedTables) -> NConcurrency::TCWrapped<void>
	{
		using namespace NStr;

		for (auto const &TableName : _DroppedTables)
		{
			// A later version can create the table again
			if (co_await _Database.f_HasTable(TableName))
				continue;

			auto Views = co_await _Database.f_SelectRaw(NStr::CStr("SELECT name, sql FROM sqlite_master WHERE type = 'view' AND sql IS NOT NULL"), {});
			for (auto const &View : Views.m_Rows)
			{
				if (!NPrivate::fg_SqlTextHasIdentifierToken(View.m_Values[1].f_GetAsType<NStr::CStr>(), TableName))
					continue;

				NStr::CStr const &ViewName = View.m_Values[0].f_GetAsType<NStr::CStr>();

				NStr::CStr CheckSql;
				{
					NStr::CStr::CAppender Appender(CheckSql);
					Appender += "SELECT * FROM ";
					fg_SqliteAppendQuotedIdentifier(Appender, ViewName);
					Appender += " LIMIT 0";
				}

				auto CheckResult = _Database.f_SelectRaw(CheckSql, {});
				if (!CheckResult)
					co_return DMibErrorDatabaseInstance("SQLite view '{}' fails after the migration drops table '{}': {}"_f << ViewName << TableName << CheckResult.f_GetExceptionStr());
			}

			NContainer::TCVector<CSqlValue> Values;
			Values.f_InsertLast(TableName);
			auto Referencing = co_await _Database.f_SelectRaw
				(
					NStr::CStr
					(
						"SELECT m.name FROM sqlite_master AS m JOIN pragma_foreign_key_list(m.name) AS fk "
						"WHERE m.type = 'table' AND fk.\"table\" = ? COLLATE NOCASE LIMIT 1"
					)
					, Values
				)
			;
			if (!Referencing.m_Rows.f_IsEmpty())
			{
				co_return DMibErrorDatabaseInstance
					(
						"SQLite table '{}' references table '{}' that the migration drops"_f << Referencing.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() << TableName
					)
				;
			}
		}

		co_return {};
	}

	// Upper case, as the index check reads it from the catalog: a quoted declaration names the collation inside the quotes
	NStr::CStr fg_SqliteCollationName(NStr::CStr const &_CollationSql)
	{
		ch8 const *pSql = _CollationSql.f_GetStr();
		aint Length = _CollationSql.f_GetLen();
		if (Length < 2)
			return _CollationSql.f_UpperCase();

		ch8 Open = pSql[0];
		ch8 Close = Open == '[' ? ']' : Open;
		if ((Open != '"' && Open != '\'' && Open != '`' && Open != '[') || pSql[Length - 1] != Close)
			return _CollationSql.f_UpperCase();

		NStr::CStr Name(pSql + 1, Length - 2);
		if (Open != '[')
			Name = Name.f_Replace(NStr::CStr(pSql, 1) + NStr::CStr(pSql, 1), NStr::CStr(pSql, 1));

		return Name.f_UpperCase();
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
					Collation = fg_SqliteCollationName(*pNonPortableOptions->m_pCollationSql);
			}

			if (!Columns.f_IsEmpty())
				Columns += ",";
			Columns += "{}:{}:{}"_f << pColumn->f_GetLen() << *pColumn << Collation;
		}

		// SQLite treats index names that differ only in ASCII case as the same name, so the query finds the index under either spelling
		// and a matching index keeps the spelling it was created with
		auto const &Row = Existing.m_Rows[0].m_Values;
		bool bSameTable = NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Row[1].f_GetAsType<NStr::CStr>(), _Table.f_Name());
		bool bStandalone = Row[3].f_GetAsType<NStr::CStr>() == "c";
		bool bMatches
			= (Row[2].f_GetAsType<int64>() != 0) == _Index.f_IsUnique()
			&& Row[4].f_GetTypeID() == ESqlValueType::mc_Text
			&& Row[4].f_GetAsType<NStr::CStr>() == Columns
			&& Row[5].f_GetAsType<int64>() != 0
		;

		if (bMatches && bSameTable && bStandalone)
			co_return {};

		if (!bSameTable || !bStandalone)
			co_return DMibErrorDatabaseInstance("SQLite index name '{}' of table '{}' is already used by another index"_f << _Index.f_Name() << _Table.f_Name());

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

	struct CSqliteSqlToken
	{
		enum class EKind : uint8
		{
			mc_Word
			, mc_QuotedIdentifier
			, mc_String
			, mc_Symbol
		};

		NStr::CStr m_Text;
		aint m_iStart = 0;
		aint m_iEnd = 0;
		EKind m_Kind = EKind::mc_Symbol;
	};

	// Identifiers and keywords compare case-insensitively for ASCII letters, as SQLite matches them, whether they are quoted or not
	NContainer::TCVector<CSqliteSqlToken> fg_SqliteNormalizedTokens(NStr::CStr const &_Sql)
	{
		NContainer::TCVector<CSqliteSqlToken> Tokens;
		ch8 const *pSql = _Sql.f_GetStr();
		aint Length = _Sql.f_GetLen();
		aint iChar = 0;

		auto fLowerAscii = [](NStr::CStr &&_Text)
			{
				ch8 *pText = _Text.f_GetStrUniqueWritable();
				for (aint iText = 0; iText < _Text.f_GetLen(); ++iText)
				{
					if (pText[iText] >= 'A' && pText[iText] <= 'Z')
						pText[iText] = ch8(pText[iText] - 'A' + 'a');
				}

				return fg_Move(_Text);
			}
		;

		while (iChar < Length)
		{
			ch8 Char = pSql[iChar];
			if (Char == ' ' || Char == '\t' || Char == '\n' || Char == '\r' || Char == '\f' || Char == '\v')
			{
				++iChar;
				continue;
			}

			if (Char == '-' && iChar + 1 < Length && pSql[iChar + 1] == '-')
			{
				while (iChar < Length && pSql[iChar] != '\n')
					++iChar;

				continue;
			}

			if (Char == '/' && iChar + 1 < Length && pSql[iChar + 1] == '*')
			{
				iChar += 2;
				while (iChar < Length && !(pSql[iChar] == '*' && iChar + 1 < Length && pSql[iChar + 1] == '/'))
					++iChar;

				iChar = fg_Min(iChar + 2, Length);
				continue;
			}

			if (Char == '\'' || Char == '"' || Char == '`' || Char == '[')
			{
				ch8 Close = Char == '[' ? ']' : Char;
				aint iStart = iChar++;
				NStr::CStr Text;
				while (iChar < Length)
				{
					if (pSql[iChar] == Close)
					{
						if (Close != ']' && iChar + 1 < Length && pSql[iChar + 1] == Close)
						{
							Text += NStr::CStr(pSql + iChar, 1);
							iChar += 2;
							continue;
						}

						++iChar;
						break;
					}

					Text += NStr::CStr(pSql + iChar, 1);
					++iChar;
				}

				if (Char == '\'')
				{
					Tokens.f_InsertLast
						(
							CSqliteSqlToken{.m_Text = NStr::CStr(pSql + iStart, iChar - iStart), .m_iStart = iStart, .m_iEnd = iChar, .m_Kind = CSqliteSqlToken::EKind::mc_String}
						)
					;
				}
				else
					Tokens.f_InsertLast(CSqliteSqlToken{.m_Text = fLowerAscii(fg_Move(Text)), .m_iStart = iStart, .m_iEnd = iChar, .m_Kind = CSqliteSqlToken::EKind::mc_QuotedIdentifier});

				continue;
			}

			auto fIsWordChar = [](ch8 _Char)
				{
					return (_Char >= 'a' && _Char <= 'z') || (_Char >= 'A' && _Char <= 'Z') || (_Char >= '0' && _Char <= '9') || _Char == '_' || _Char == '$' || uint8(_Char) >= 0x80;
				}
			;

			if (fIsWordChar(Char))
			{
				aint iStart = iChar;
				while (iChar < Length && fIsWordChar(pSql[iChar]))
					++iChar;

				Tokens.f_InsertLast
					(
						CSqliteSqlToken{.m_Text = fLowerAscii(NStr::CStr(pSql + iStart, iChar - iStart)), .m_iStart = iStart, .m_iEnd = iChar, .m_Kind = CSqliteSqlToken::EKind::mc_Word}
					)
				;
				continue;
			}

			Tokens.f_InsertLast(CSqliteSqlToken{.m_Text = NStr::CStr(pSql + iChar, 1), .m_iStart = iChar, .m_iEnd = iChar + 1, .m_Kind = CSqliteSqlToken::EKind::mc_Symbol});
			++iChar;
		}

		return Tokens;
	}

	// The CHECK constraints of a CREATE TABLE statement, column and table constraints alike, with their expressions encoded for comparison
	NContainer::TCVector<CSqliteCheckConstraint> fg_SqliteTableChecks(NStr::CStr const &_CreateTableSql)
	{
		auto Tokens = fg_SqliteNormalizedTokens(_CreateTableSql);
		NContainer::TCVector<CSqliteCheckConstraint> Checks;
		for (umint iToken = 0; iToken + 1 < Tokens.f_GetLen(); ++iToken)
		{
			if (Tokens[iToken].m_Kind != CSqliteSqlToken::EKind::mc_Word || Tokens[iToken].m_Text != "check")
				continue;

			if (Tokens[iToken + 1].m_Kind != CSqliteSqlToken::EKind::mc_Symbol || Tokens[iToken + 1].m_Text != "(")
				continue;

			CSqliteCheckConstraint Check;
			if (iToken >= 2 && Tokens[iToken - 2].m_Kind == CSqliteSqlToken::EKind::mc_Word && Tokens[iToken - 2].m_Text == "constraint")
				Check.m_Name = Tokens[iToken - 1].m_Text;

			umint Depth = 0;
			umint iExpression = iToken + 1;
			aint iSqlStart = -1;
			aint iSqlEnd = -1;
			for (; iExpression < Tokens.f_GetLen(); ++iExpression)
			{
				auto const &Token = Tokens[iExpression];
				if (Token.m_Kind == CSqliteSqlToken::EKind::mc_Symbol && Token.m_Text == "(")
				{
					if (Depth++ == 0)
						continue;
				}
				else if (Token.m_Kind == CSqliteSqlToken::EKind::mc_Symbol && Token.m_Text == ")")
				{
					if (--Depth == 0)
						break;
				}

				// The kind and length of each token keep its bounds, so a quoted identifier holding spaces or operators does not read as the
				// tokens it spells. A quoted identifier that is a plain name compares like the name unquoted, which SQLite reads the same,
				// except for a keyword or TRUE and FALSE: quoted it names a column, such as "null", and unquoted it can be the keyword or value.
				ch8 Kind = 'p';
				if (Token.m_Kind == CSqliteSqlToken::EKind::mc_Word)
					Kind = 'w';
				else if (Token.m_Kind == CSqliteSqlToken::EKind::mc_String)
					Kind = 's';
				else if (Token.m_Kind == CSqliteSqlToken::EKind::mc_QuotedIdentifier)
				{
					bool bPlainName = !Token.m_Text.f_IsEmpty() && !(Token.m_Text[0] >= '0' && Token.m_Text[0] <= '9');
					for (aint iChar = 0; iChar < Token.m_Text.f_GetLen() && bPlainName; ++iChar)
					{
						ch8 Char = Token.m_Text[iChar];
						bPlainName = (Char >= 'a' && Char <= 'z') || (Char >= '0' && Char <= '9') || Char == '_' || Char == '$' || uint8(Char) >= 0x80;
					}

					if (bPlainName && (sqlite3_keyword_check(Token.m_Text.f_GetStr(), int(Token.m_Text.f_GetLen())) != 0 || Token.m_Text == "true" || Token.m_Text == "false"))
						bPlainName = false;

					Kind = bPlainName ? 'w' : 'q';
				}

				{
					using namespace NStr;

					Check.m_Expression += "{}{}:{};"_f << Kind << Token.m_Text.f_GetLen() << Token.m_Text;
				}

				if (iSqlStart < 0)
					iSqlStart = Token.m_iStart;
				iSqlEnd = Token.m_iEnd;
			}

			if (iSqlStart >= 0)
				Check.m_Sql = NStr::CStr(_CreateTableSql.f_GetStr() + iSqlStart, iSqlEnd - iSqlStart);

			Checks.f_InsertLast(fg_Move(Check));
			iToken = iExpression;
		}

		return Checks;
	}

	// The foreign key clauses of a CREATE TABLE statement that are DEFERRABLE INITIALLY DEFERRED, as written from REFERENCES to the
	// deferral; pragma_foreign_key_list does not report deferral
	NContainer::TCVector<NStr::CStr> fg_SqliteDeferredForeignKeys(NStr::CStr const &_CreateTableSql)
	{
		auto Tokens = fg_SqliteNormalizedTokens(_CreateTableSql);
		auto fIsWord = [&](umint _iToken, ch8 const *_pWord)
			{
				return _iToken < Tokens.f_GetLen() && Tokens[_iToken].m_Kind == CSqliteSqlToken::EKind::mc_Word && Tokens[_iToken].m_Text == _pWord;
			}
		;

		NContainer::TCVector<NStr::CStr> DeferredForeignKeys;
		aint iReferencesStart = -1;
		umint Depth = 0;
		for (umint iToken = 0; iToken < Tokens.f_GetLen(); ++iToken)
		{
			auto const &Token = Tokens[iToken];
			if (Token.m_Kind == CSqliteSqlToken::EKind::mc_Symbol && Token.m_Text == "(")
				++Depth;
			else if (Token.m_Kind == CSqliteSqlToken::EKind::mc_Symbol && Token.m_Text == ")" && Depth > 0)
				--Depth;

			// The clauses of the column and table definitions, outside the column lists and expressions they contain
			if (Depth != 1)
				continue;

			if (fIsWord(iToken, "references"))
				iReferencesStart = Token.m_iStart;
			else if (iReferencesStart >= 0 && fIsWord(iToken, "deferrable") && fIsWord(iToken + 1, "initially") && fIsWord(iToken + 2, "deferred") && !fIsWord(iToken - 1, "not"))
			{
				DeferredForeignKeys.f_InsertLast(NStr::CStr(_CreateTableSql.f_GetStr() + iReferencesStart, Tokens[iToken + 2].m_iEnd - iReferencesStart));
				iToken += 2;
			}
		}

		return DeferredForeignKeys;
	}

	// Whether a CREATE TABLE statement declares its INTEGER PRIMARY KEY AUTOINCREMENT, which no pragma reports
	bool fg_SqliteHasAutoIncrement(NStr::CStr const &_CreateTableSql)
	{
		auto Tokens = fg_SqliteNormalizedTokens(_CreateTableSql);
		auto fIsWord = [&](umint _iToken, ch8 const *_pWord)
			{
				return _iToken < Tokens.f_GetLen() && Tokens[_iToken].m_Kind == CSqliteSqlToken::EKind::mc_Word && Tokens[_iToken].m_Text == _pWord;
			}
		;

		umint Depth = 0;
		bool bPrimaryKey = false;
		for (umint iToken = 0; iToken < Tokens.f_GetLen(); ++iToken)
		{
			auto const &Token = Tokens[iToken];
			if (Token.m_Kind == CSqliteSqlToken::EKind::mc_Symbol && Token.m_Text == "(")
				++Depth;
			else if (Token.m_Kind == CSqliteSqlToken::EKind::mc_Symbol && Token.m_Text == ")" && Depth > 0)
				--Depth;

			if (Depth != 1)
				continue;

			// AUTOINCREMENT follows PRIMARY KEY in the same column definition
			if (Token.m_Kind == CSqliteSqlToken::EKind::mc_Symbol && Token.m_Text == ",")
				bPrimaryKey = false;
			else if (fIsWord(iToken, "primary") && fIsWord(iToken + 1, "key"))
				bPrimaryKey = true;
			else if (bPrimaryKey && fIsWord(iToken, "autoincrement"))
				return true;
		}

		return false;
	}

	// The conflict clauses of a CREATE TABLE statement that resolve a conflict other than as ABORT, as written from the start of their column or
	// table definition; no pragma reports them
	NContainer::TCVector<NStr::CStr> fg_SqliteConflictClauses(NStr::CStr const &_CreateTableSql)
	{
		auto Tokens = fg_SqliteNormalizedTokens(_CreateTableSql);
		auto fIsWord = [&](umint _iToken, ch8 const *_pWord)
			{
				return _iToken < Tokens.f_GetLen() && Tokens[_iToken].m_Kind == CSqliteSqlToken::EKind::mc_Word && Tokens[_iToken].m_Text == _pWord;
			}
		;

		NContainer::TCVector<NStr::CStr> ConflictClauses;
		umint Depth = 0;
		aint iDefinitionStart = -1;
		for (umint iToken = 0; iToken < Tokens.f_GetLen(); ++iToken)
		{
			auto const &Token = Tokens[iToken];
			bool bSymbol = Token.m_Kind == CSqliteSqlToken::EKind::mc_Symbol;
			if (bSymbol && Token.m_Text == "(")
			{
				if (++Depth == 1)
					iDefinitionStart = -1;

				continue;
			}

			if (bSymbol && Token.m_Text == ")")
			{
				if (Depth > 0)
					--Depth;

				continue;
			}

			// The clauses of the column and table definitions, outside the column lists and expressions they contain
			if (Depth != 1)
				continue;

			if (bSymbol && Token.m_Text == ",")
			{
				iDefinitionStart = -1;
				continue;
			}

			if (iDefinitionStart < 0)
				iDefinitionStart = Token.m_iStart;

			if (fIsWord(iToken, "on") && fIsWord(iToken + 1, "conflict") && iToken + 2 < Tokens.f_GetLen())
			{
				if (!fIsWord(iToken + 2, "abort"))
					ConflictClauses.f_InsertLast(NStr::CStr(_CreateTableSql.f_GetStr() + iDefinitionStart, Tokens[iToken + 2].m_iEnd - iDefinitionStart));

				iToken += 2;
			}
		}

		return ConflictClauses;
	}

	struct CSqliteForeignKeyShape
	{
		NStr::CStr m_Table;
		NStr::CStr m_From;
		NStr::CStr m_To;
		NStr::CStr m_OnUpdate; // As pragma_foreign_key_list reports it, such as NO ACTION
		NStr::CStr m_OnDelete;
		NStr::CStr m_Display;
	};

	struct CSqliteKeyColumn
	{
		NStr::CStr m_Name;      // Lower case, as SQLite compares column names; empty for an expression
		NStr::CStr m_Collation; // Upper case
	};

	using CSqliteKey = NContainer::TCVector<CSqliteKeyColumn>;

	struct CSqliteConstraintShape
	{
		CSqliteKey m_PrimaryKey;
		NStr::CStr m_PrimaryKeyDisplay;
		NContainer::TCVector<CSqliteKey> m_UniqueKeys;
		NContainer::TCVector<NStr::CStr> m_UniqueKeyDisplays;
		NContainer::TCVector<CSqliteForeignKeyShape> m_ForeignKeys;
		NContainer::TCVector<NStr::CStr> m_DeferredForeignKeys;
		NContainer::TCVector<CSqliteCheckConstraint> m_Checks;
		NContainer::TCVector<NStr::CStr> m_ConflictClauses;
		bool m_bAutoIncrement = false;
	};

	NStr::CStr fg_SqliteLowerAscii(NStr::CStr const &_Name)
	{
		NStr::CStr Name = _Name;
		ch8 *pName = Name.f_GetStrUniqueWritable();
		for (aint iChar = 0; iChar < Name.f_GetLen(); ++iChar)
		{
			if (pName[iChar] >= 'A' && pName[iChar] <= 'Z')
				pName[iChar] = ch8(pName[iChar] - 'A' + 'a');
		}

		return Name;
	}

	NStr::CStr fg_SqliteKeyDisplay(NContainer::TCVector<NStr::CStr> const &_Names, CSqliteKey const &_Key)
	{
		NStr::CStr Display;
		for (umint iColumn = 0; iColumn < _Key.f_GetLen(); ++iColumn)
		{
			if (!Display.f_IsEmpty())
				Display += ", ";
			Display += _Names[iColumn];
			if (_Key[iColumn].m_Collation != "BINARY")
			{
				Display += " COLLATE ";
				Display += _Key[iColumn].m_Collation;
			}
		}

		return Display;
	}

	// The key columns of an index with their collations, and the names as written for messages
	auto fg_SqliteIndexKey(CSQLiteDatabaseHandle &_Database, NStr::CStr const &_IndexName, NStr::CStr const &_Schema, NContainer::TCVector<NStr::CStr> *o_pNames = nullptr)
		-> NConcurrency::TCWrapped<CSqliteKey>
	{
		NContainer::TCVector<CSqlValue> Values;
		Values.f_InsertLast(_IndexName);
		Values.f_InsertLast(_Schema);

		auto Rows = co_await _Database.f_SelectRaw(NStr::CStr("SELECT name, upper(coll) FROM pragma_index_xinfo(?1, ?2) WHERE key ORDER BY seqno"), Values);

		CSqliteKey Key;
		for (auto const &Row : Rows.m_Rows)
		{
			NStr::CStr Name = Row.m_Values[0].f_GetTypeID() == ESqlValueType::mc_Text ? Row.m_Values[0].f_GetAsType<NStr::CStr>() : NStr::CStr();
			NStr::CStr Collation = Row.m_Values[1].f_GetTypeID() == ESqlValueType::mc_Text ? Row.m_Values[1].f_GetAsType<NStr::CStr>() : NStr::CStr("BINARY");
			Key.f_InsertLast(CSqliteKeyColumn{.m_Name = fg_SqliteLowerAscii(Name), .m_Collation = fg_Move(Collation)});
			if (o_pNames)
				o_pNames->f_InsertLast(fg_Move(Name));
		}

		co_return fg_Move(Key);
	}

	// A declared key enforces a live key when it is on the same columns and each of its collations treats at least the values as
	// equal that the live one does, which every collation does for BINARY
	bool fg_SqliteKeyEnforces(CSqliteKey const &_Declared, CSqliteKey const &_Live)
	{
		if (_Declared.f_GetLen() != _Live.f_GetLen())
			return false;

		for (umint iColumn = 0; iColumn < _Live.f_GetLen(); ++iColumn)
		{
			if (_Live[iColumn].m_Name.f_IsEmpty() || _Declared[iColumn].m_Name != _Live[iColumn].m_Name)
				return false;

			if (_Declared[iColumn].m_Collation != _Live[iColumn].m_Collation && _Live[iColumn].m_Collation != "BINARY")
				return false;
		}

		return true;
	}

	auto fg_SqliteConstraintShape(CSQLiteDatabaseHandle &_Database, NStr::CStr const &_TableName, NStr::CStr const &_Schema) -> NConcurrency::TCWrapped<CSqliteConstraintShape>
	{
		CSqliteConstraintShape Shape;
		NContainer::TCVector<CSqlValue> Values;
		Values.f_InsertLast(_TableName);
		Values.f_InsertLast(_Schema);

		auto fText = [](CSqlValue const &_Value)
			{
				return _Value.f_GetTypeID() == ESqlValueType::mc_Text ? _Value.f_GetAsType<NStr::CStr>() : NStr::CStr();
			}
		;

		auto Indexes = co_await _Database.f_SelectRaw(NStr::CStr("SELECT name, origin FROM pragma_index_list(?1, ?2) WHERE origin IN ('pk', 'u') ORDER BY seq"), Values);

		// A rowid alias primary key has no index, and compares integers
		bool bPrimaryKeyIndex = false;
		for (auto const &Row : Indexes.m_Rows)
		{
			NContainer::TCVector<NStr::CStr> Names;
			CSqliteKey Key = co_await fg_SqliteIndexKey(_Database, fText(Row.m_Values[0]), _Schema, &Names);
			NStr::CStr Display = fg_SqliteKeyDisplay(Names, Key);
			if (fText(Row.m_Values[1]) == "pk")
			{
				bPrimaryKeyIndex = true;
				Shape.m_PrimaryKey = fg_Move(Key);
				Shape.m_PrimaryKeyDisplay = fg_Move(Display);
			}
			else
			{
				Shape.m_UniqueKeys.f_InsertLast(fg_Move(Key));
				Shape.m_UniqueKeyDisplays.f_InsertLast(fg_Move(Display));
			}
		}

		if (!bPrimaryKeyIndex)
		{
			auto PrimaryKey = co_await _Database.f_SelectRaw(NStr::CStr("SELECT name FROM pragma_table_info(?1, ?2) WHERE pk > 0 ORDER BY pk"), Values);
			NContainer::TCVector<NStr::CStr> Names;
			for (auto const &Row : PrimaryKey.m_Rows)
			{
				NStr::CStr Name = fText(Row.m_Values[0]);
				Shape.m_PrimaryKey.f_InsertLast(CSqliteKeyColumn{.m_Name = fg_SqliteLowerAscii(Name), .m_Collation = NStr::CStr("BINARY")});
				Names.f_InsertLast(fg_Move(Name));
			}
			Shape.m_PrimaryKeyDisplay = fg_SqliteKeyDisplay(Names, Shape.m_PrimaryKey);
		}

		auto ForeignKeys = co_await _Database.f_SelectRaw
			(
				NStr::CStr
				(
					"SELECT lower(\"table\"), group_concat(From_, ','), group_concat(To_, ','), sum(\"to\" IS NULL), group_concat(\"from\", ', '), \"table\", "
					"max(on_update), max(on_delete) FROM "
					"(SELECT id, \"table\", \"from\", \"to\", on_update, on_delete, length(CAST(lower(\"from\") AS BLOB)) || ':' || lower(\"from\") AS From_, "
					"CASE WHEN \"to\" IS NULL THEN '' ELSE length(CAST(lower(\"to\") AS BLOB)) || ':' || lower(\"to\") END AS To_ "
					"FROM pragma_foreign_key_list(?1, ?2) ORDER BY id, seq) GROUP BY id"
				)
				, Values
			)
		;
		for (auto const &Row : ForeignKeys.m_Rows)
		{
			NStr::CStr OnUpdate = fText(Row.m_Values[6]);
			NStr::CStr OnDelete = fText(Row.m_Values[7]);

			NStr::CStr Display = "FOREIGN KEY (";
			Display += fText(Row.m_Values[4]);
			Display += ") REFERENCES ";
			Display += fText(Row.m_Values[5]);
			Display += " ON UPDATE ";
			Display += OnUpdate;
			Display += " ON DELETE ";
			Display += OnDelete;

			// A reference without columns names the parent's primary key, in its order, and no other unique key of the parent
			NStr::CStr To = fText(Row.m_Values[2]);
			if (Row.m_Values[3].f_GetTypeID() == ESqlValueType::mc_Integer64 && Row.m_Values[3].f_GetAsType<int64>() != 0)
			{
				NContainer::TCVector<CSqlValue> ParentValues;
				ParentValues.f_InsertLast(fText(Row.m_Values[5]));
				ParentValues.f_InsertLast(_Schema);
				auto ParentKey = co_await _Database.f_SelectRaw
					(
						NStr::CStr
						(
							"SELECT group_concat(Name_, ',') FROM (SELECT length(CAST(lower(name) AS BLOB)) || ':' || lower(name) AS Name_ "
							"FROM pragma_table_info(?1, ?2) WHERE pk > 0 ORDER BY pk)"
						)
						, ParentValues
					)
				;
				To = ParentKey.m_Rows.f_GetLen() == 1 ? fText(ParentKey.m_Rows[0].m_Values[0]) : NStr::CStr();
			}

			Shape.m_ForeignKeys.f_InsertLast
				(
					CSqliteForeignKeyShape
					{
						.m_Table = fText(Row.m_Values[0])
						, .m_From = fText(Row.m_Values[1])
						, .m_To = fg_Move(To)
						, .m_OnUpdate = fg_Move(OnUpdate)
						, .m_OnDelete = fg_Move(OnDelete)
						, .m_Display = fg_Move(Display)
					}
				)
			;
		}

		NContainer::TCVector<CSqlValue> TableValues;
		TableValues.f_InsertLast(_TableName);
		auto CreateSql = co_await _Database.f_SelectRaw
			(
				_Schema == "temp"
				? NStr::CStr("SELECT sql FROM temp.sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE")
				: NStr::CStr("SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE")
				, TableValues
			)
		;
		if (CreateSql.m_Rows.f_GetLen() == 1)
		{
			Shape.m_Checks = fg_SqliteTableChecks(fText(CreateSql.m_Rows[0].m_Values[0]));
			Shape.m_DeferredForeignKeys = fg_SqliteDeferredForeignKeys(fText(CreateSql.m_Rows[0].m_Values[0]));
			Shape.m_bAutoIncrement = fg_SqliteHasAutoIncrement(fText(CreateSql.m_Rows[0].m_Values[0]));
			Shape.m_ConflictClauses = fg_SqliteConflictClauses(fText(CreateSql.m_Rows[0].m_Values[0]));
		}

		co_return fg_Move(Shape);
	}

	// The key of an index created from the declaration, which takes each column's collation
	CSqliteKey fg_SqliteDeclaredIndexKey(CSqlTableDescription const &_Table, NContainer::TCVector<NStr::CStr const *> const &_Columns)
	{
		CSqliteKey Key;
		for (auto const *pColumn : _Columns)
		{
			NStr::CStr Collation = "BINARY";
			for (auto const &Column : _Table.m_Columns)
			{
				if (Column.f_Name() != *pColumn)
					continue;

				CNonPortableColumnOptions const *pNonPortableOptions = Column.f_NonPortableOptions(NStr::gc_Str<"sqlite">.m_Str);
				if (pNonPortableOptions && !pNonPortableOptions->m_pCollationSql->f_IsEmpty())
					Collation = fg_SqliteCollationName(*pNonPortableOptions->m_pCollationSql);
			}

			Key.f_InsertLast(CSqliteKeyColumn{.m_Name = fg_SqliteLowerAscii(*pColumn), .m_Collation = fg_Move(Collation)});
		}

		return Key;
	}

	// Adopting an untracked table rebuilds it from the declaration, which keeps only the declared constraints, so a live
	// constraint the schema does not declare would be dropped. The declared constraints are read back from a temporary
	// table created from the declaration, as SQLite reports them. Indexes created on their own are recreated by the
	// rebuild, except one named like a declared index, which the declared index replaces.
	auto fg_SqliteCheckNoUndeclaredConstraints(CSQLiteDatabaseHandle &_Database, CSqlTableDescription const &_Table) -> NConcurrency::TCWrapped<NException::CExceptionPointer>
	{
		using namespace NStr;

		auto Live = co_await fg_SqliteConstraintShape(_Database, _Table.f_Name(), NStr::CStr("main"));

		NStr::CStr ScratchName = "__mib_adopt_check_" + _Table.f_Name();
		CSqlTableDescription ScratchTable = _Table;
		ScratchTable.m_pName = &ScratchName;
		NStr::CStr CreateScratchSql = co_await fg_SqliteCreateTable(ScratchTable, false);
		co_await _Database.f_Execute("CREATE TEMP TABLE " + CreateScratchSql.f_Extract(NStr::CStr("CREATE TABLE ").f_GetLen()));

		auto Declared = co_await fg_SqliteConstraintShape(_Database, ScratchName, NStr::CStr("temp"));

		// The declared NOT NULL columns, and the INTEGER PRIMARY KEY, which holds no NULL either: inserting NULL assigns a key.
		// table_xinfo also lists the generated columns, which table_info leaves out.
		NContainer::TCVector<NStr::CStr> DeclaredNotNullColumns;
		{
			NContainer::TCVector<CSqlValue> ScratchValues;
			ScratchValues.f_InsertLast(ScratchName);
			auto ScratchColumns = co_await _Database.f_SelectRaw
				(
					NStr::CStr
					(
						"SELECT name FROM pragma_table_xinfo(?1, 'temp') WHERE \"notnull\" OR (pk = 1 AND upper(type) = 'INTEGER' "
						"AND (SELECT count(*) FROM pragma_table_info(?1, 'temp') WHERE pk > 0) = 1)"
					)
					, ScratchValues
				)
			;
			for (auto const &Row : ScratchColumns.m_Rows)
				DeclaredNotNullColumns.f_InsertLast(Row.m_Values[0].f_GetAsType<NStr::CStr>());
		}

		{
			NStr::CStr DropScratchSql;
			{
				NStr::CStr::CAppender Appender(DropScratchSql);
				Appender += "DROP TABLE temp.";
				fg_SqliteAppendQuotedIdentifier(Appender, ScratchName);
			}
			co_await _Database.f_Execute(DropScratchSql);
		}

		auto fUndeclared = [&](NStr::CStr const &_Constraint) -> NException::CExceptionPointer
			{
				return DMibErrorDatabaseInstance
					(
						"SQLite table '{}' has constraint {} that the schema does not declare; adopting the table would drop it"_f << _Table.f_Name() << _Constraint
					)
				;
			}
		;

		if (!Live.m_PrimaryKey.f_IsEmpty() && !fg_SqliteKeyEnforces(Declared.m_PrimaryKey, Live.m_PrimaryKey))
			co_return fUndeclared("PRIMARY KEY ({})"_f << Live.m_PrimaryKeyDisplay);

		// Without AUTOINCREMENT the rebuilt table drops the key's sequence, and new rows can reuse the keys of deleted rows
		if (Live.m_bAutoIncrement && !Declared.m_bAutoIncrement)
			co_return fUndeclared("PRIMARY KEY ({}) AUTOINCREMENT"_f << Live.m_PrimaryKeyDisplay);

		// A declared constraint resolves a conflict as ABORT, unless custom SQL that this does not read says otherwise, so a replacement would fail
		// the statements that another resolution skips or resolves
		if (!Live.m_ConflictClauses.f_IsEmpty())
			co_return fUndeclared(Live.m_ConflictClauses[0]);

		NContainer::TCVector<CSqliteKey> DeclaredUniqueKeys = Declared.m_UniqueKeys;
		if (!Declared.m_PrimaryKey.f_IsEmpty())
			DeclaredUniqueKeys.f_InsertLast(Declared.m_PrimaryKey);

		for (auto const &Index : _Table.m_Indexes)
		{
			if (Index.f_IsUnique())
				DeclaredUniqueKeys.f_InsertLast(fg_SqliteDeclaredIndexKey(_Table, Index.m_Columns));
		}

		for (umint iUnique = 0; iUnique < Live.m_UniqueKeys.f_GetLen(); ++iUnique)
		{
			bool bDeclared = false;
			for (auto const &DeclaredKey : DeclaredUniqueKeys)
				bDeclared = bDeclared || fg_SqliteKeyEnforces(DeclaredKey, Live.m_UniqueKeys[iUnique]);

			if (!bDeclared)
				co_return fUndeclared("UNIQUE ({})"_f << Live.m_UniqueKeyDisplays[iUnique]);
		}

		// A declared foreign key is not deferred, unless custom SQL that this does not read says otherwise, so a replacement would check a
		// deferred one at each statement instead of at commit
		if (!Live.m_DeferredForeignKeys.f_IsEmpty())
			co_return fUndeclared(Live.m_DeferredForeignKeys[0]);

		for (auto const &LiveForeignKey : Live.m_ForeignKeys)
		{
			bool bDeclared = false;
			for (auto const &DeclaredForeignKey : Declared.m_ForeignKeys)
			{
				// A replacement with other actions would change what an update or delete of the parent does to the child rows
				if
				(
					DeclaredForeignKey.m_Table == LiveForeignKey.m_Table
					&& DeclaredForeignKey.m_From == LiveForeignKey.m_From
					&& DeclaredForeignKey.m_To == LiveForeignKey.m_To
					&& DeclaredForeignKey.m_OnUpdate == LiveForeignKey.m_OnUpdate
					&& DeclaredForeignKey.m_OnDelete == LiveForeignKey.m_OnDelete
				)
				{
					bDeclared = true;
				}
			}

			if (!bDeclared)
				co_return fUndeclared(LiveForeignKey.m_Display);
		}

		{
			NContainer::TCVector<CSqlValue> TableValues;
			TableValues.f_InsertLast(_Table.f_Name());
			auto LiveNotNullColumns = co_await _Database.f_SelectRaw(NStr::CStr("SELECT name FROM pragma_table_xinfo(?1, 'main') WHERE \"notnull\""), TableValues);
			for (auto const &Row : LiveNotNullColumns.m_Rows)
			{
				NStr::CStr const &ColumnName = Row.m_Values[0].f_GetAsType<NStr::CStr>();
				bool bDeclared = false;
				for (auto const &DeclaredColumn : DeclaredNotNullColumns)
					bDeclared = bDeclared || NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(DeclaredColumn, ColumnName);

				if (!bDeclared)
				{
					co_return DMibErrorDatabaseInstance
						(
							"SQLite table '{}' has NOT NULL on column '{}' that the schema does not declare; adopting the table would drop it"_f << _Table.f_Name() << ColumnName
						)
					;
				}
			}
		}

		NContainer::TCVector<bool> DeclaredCheckUsed;
		DeclaredCheckUsed.f_SetLen(Declared.m_Checks.f_GetLen());
		for (auto &bUsed : DeclaredCheckUsed)
			bUsed = false;

		for (auto const &LiveCheck : Live.m_Checks)
		{
			bool bDeclared = false;
			for (umint iDeclared = 0; iDeclared < Declared.m_Checks.f_GetLen() && !bDeclared; ++iDeclared)
			{
				if (!DeclaredCheckUsed[iDeclared] && Declared.m_Checks[iDeclared].m_Expression == LiveCheck.m_Expression)
				{
					DeclaredCheckUsed[iDeclared] = true;
					bDeclared = true;
				}
			}

			if (!bDeclared)
			{
				if (LiveCheck.m_Name.f_IsEmpty())
					co_return fUndeclared("CHECK ({})"_f << LiveCheck.m_Sql);

				co_return fUndeclared("'{}' (CHECK ({}))"_f << LiveCheck.m_Name << LiveCheck.m_Sql);
			}
		}

		NContainer::TCVector<CSqlValue> TableValues;
		TableValues.f_InsertLast(_Table.f_Name());
		auto LiveIndexes = co_await _Database.f_SelectRaw
			(
				NStr::CStr
				(
					"SELECT m.name, il.\"unique\", m.sql "
					"FROM sqlite_master AS m JOIN pragma_index_list(m.tbl_name) AS il ON il.name = m.name "
					"WHERE m.type = 'index' AND m.tbl_name = ? COLLATE NOCASE AND m.sql IS NOT NULL"
				)
				, TableValues
			)
		;
		for (auto const &Row : LiveIndexes.m_Rows)
		{
			if (Row.m_Values[1].f_GetAsType<int64>() == 0)
				continue;

			for (auto const &Index : _Table.m_Indexes)
			{
				if (!NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(Index.f_Name(), Row.m_Values[0].f_GetAsType<NStr::CStr>()))
					continue;

				// The declared index replaces the live one, which keeps its uniqueness only when the declared index is unique on the same
				// columns; a declared full index is at least as strict as a live partial one
				NStr::CStr const &IndexName = Row.m_Values[0].f_GetAsType<NStr::CStr>();
				if (!Index.f_IsUnique() || !fg_SqliteKeyEnforces(fg_SqliteDeclaredIndexKey(_Table, Index.m_Columns), co_await fg_SqliteIndexKey(_Database, IndexName, NStr::CStr("main"))))
					co_return fUndeclared("'{}' ({})"_f << IndexName << Row.m_Values[2].f_GetAsType<NStr::CStr>());
			}
		}

		co_return {};
	}

	auto fg_SqliteSyncAdditiveSchema(CSQLiteDatabaseHandle &_Database, CSqlSchemaVersionDescription const &_Schema, CSqlSchemaVersionDescription const *_pPreviousSchema)
		-> NConcurrency::TCWrapped<void>
	{
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			if (!co_await _Database.f_HasTable(Table.f_Name()))
			{
				// f_HasTable finds only tables, and IF NOT EXISTS would silently keep a view of the same name in place of the table
				co_await _Database.f_Execute(co_await fg_SqliteCreateTable(Table, false));

				for (auto const &Index : Table.m_Indexes)
					co_await fg_SqliteEnsureIndex(_Database, Table, Index);

				continue;
			}

			bool bRebuild = co_await fg_SqliteTableNeedsRebuild(Table, _pPreviousSchema);

			// An untracked table can lack declared constraints that SQLite cannot add in place
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

				if (fg_SqliteColumnNeedsRebuildToAdd(Column))
					bRebuild = true;

				NewColumns.f_InsertLast(&Column);
			}

			if (bRebuild)
			{
				if (auto pException = co_await fg_SqliteCheckNoUndeclaredColumns(_Database, Table, nullptr, nullptr))
					co_return fg_Move(pException);

				// A table that no previous version describes is adopted, also when a later version first declares it
				if (!_pPreviousSchema || !fg_SqliteFindTable(*_pPreviousSchema, Table.f_Name()))
				{
					if (auto pException = co_await fg_SqliteCheckNoUndeclaredConstraints(_Database, Table))
						co_return fg_Move(pException);
				}

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

	// Returns umint(-1) when no migration goes from _FromVersionID to _ToVersionID
	umint fg_SqliteFindMigration(NContainer::TCVector<CSqlSchemaMigrationDescription> const &_Migrations, NStr::CStr const &_FromVersionID, NStr::CStr const &_ToVersionID)
	{
		for (auto const &Migration : _Migrations)
		{
			if (*Migration.m_pFromVersionID == _FromVersionID && *Migration.m_pToVersionID == _ToVersionID)
				return umint(&Migration - _Migrations.f_GetArray());
		}

		return umint(-1);
	}
}

namespace NMib::NSQL
{
	// SQLite matches identifiers case-insensitively for ASCII letters, and tables and indexes share one namespace, so names the
	// schema keeps apart can collide: IF NOT EXISTS silently skips a second table or a second index, and fails for a table and an index.
	NException::CExceptionPointer NPrivate::fg_SqliteValidateSchemaVersions(NContainer::TCVector<CSqlSchemaVersionDescription> const &_Versions)
	{
		using namespace NStr;

		auto fInsertUnique = [](NContainer::TCSet<NStr::CStr> &o_Names, NStr::CStr const &_Name)
			{
				NStr::CStr Key = _Name;
				ch8 *pKey = Key.f_GetStrUniqueWritable();
				for (aint iChar = 0; iChar < Key.f_GetLen(); ++iChar)
				{
					if (pKey[iChar] >= 'A' && pKey[iChar] <= 'Z')
						pKey[iChar] = ch8(pKey[iChar] - 'A' + 'a');
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

			// An empty ID means no applied version
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
						return DMibErrorDatabaseInstance("SQL column '{}.{}' is generated on another backend but has no SQLite generated expression"_f << Table.f_Name() << Column.f_Name());
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
			// Without a previous schema to diff against, the plan creates the objects idempotently
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
			if (umint iMigration = fg_SqliteFindMigration(Migrations, PreviousVersion.f_ID(), NextVersion.f_ID()); iMigration != umint(-1))
			{
				auto const &Migration = Migrations[iMigration];
				if (auto pException = fg_SqliteValidateRenameAfterRebuild(Migration))
					co_return fg_Move(pException);

				for (auto const &Operation : Migration.m_Operations)
				{
					Plan.m_Statements.f_InsertLast(co_await fg_SqliteMigrationOperationStatements(Migration, Operation, NextVersion, &PlannedPreviousVersion));
					fg_SqliteApplyMigrationOperationToPlannedSchema(PlannedPreviousVersion, Migration, Operation, NextVersion, PlannedSchemaStorage);
				}
			}

			Plan.m_Statements.f_InsertLast(co_await fg_SqliteSyncAdditiveSchemaStatements(NextVersion, &PlannedPreviousVersion));
			Plan.m_Statements.f_InsertLast(fg_SqliteInsertSchemaVersionSql());
		}

		co_return fg_Move(Plan);
	}
}
