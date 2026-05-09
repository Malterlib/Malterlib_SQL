// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_PostgresDatabase_Internal.h"

namespace NMib::NSQL::NPrivate
{
	void CPostgresConnectionCheckout::f_MarkReusable(bool _bReusable)
	{
		if (m_pState)
			m_pState->m_bReusable = _bReusable;
	}

	NConcurrency::TCFuture<NContainer::TCVector<CPostgresAppliedSchemaVersion>> fg_PostgresReadAppliedSchemaVersions(NConcurrency::TCActor<CPostgresClientActor> _Client)
	{
		NContainer::TCVector<CPostgresAppliedSchemaVersion> Versions;
		auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresReadSchemaVersionsSql());

		for (auto const &Row : Result.m_Rows)
		{
			CPostgresAppliedSchemaVersion Version;
			Version.m_ID = Row.m_Values[0].f_GetAsType<NStr::CStr>();
			Version.m_Checksum = Row.m_Values[1].f_GetAsType<NStr::CStr>();

			Versions.f_InsertLast(fg_Move(Version));
		}

		co_return Versions;
	}

	NException::CExceptionPointer fg_PostgresValidateAppliedSchemaVersions
		(
			NContainer::TCVector<CPostgresAppliedSchemaVersion> const &_AppliedVersions
			, NContainer::TCVector<CSqlSchemaVersionDescription> const &_ExpectedVersions
		)
	{
		for (auto const &AppliedVersion : _AppliedVersions)
		{
			CSqlSchemaVersionDescription const *pExpectedVersion = nullptr;
			for (auto const &ExpectedVersion : _ExpectedVersions)
			{
				if (ExpectedVersion.f_ID() == AppliedVersion.m_ID)
				{
					pExpectedVersion = &ExpectedVersion;

					break;
				}
			}

			if (!pExpectedVersion)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL database has unknown applied schema version '{}'") << AppliedVersion.m_ID);

			if (pExpectedVersion->m_Checksum != AppliedVersion.m_Checksum)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL schema version '{}' checksum mismatch") << AppliedVersion.m_ID);
		}

		return {};
	}

	NConcurrency::TCFuture<bool> fg_PostgresHasRows(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values)
	{
		auto Result = co_await _Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(_Sql), fg_Move(_Values));

		co_return Result.m_Rows.f_GetLen() != 0;
	}

	NConcurrency::TCFuture<CPostgresValue> fg_PostgresExecuteScalar(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values)
	{
		auto Result = co_await _Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(_Sql), fg_Move(_Values));
		if (Result.m_Rows.f_GetLen() != 1 || Result.m_Rows[0].m_Values.f_GetLen() != 1)
			co_return DMibErrorDatabaseInstance("PostgreSQL scalar select did not return exactly one value");

		co_return fg_Move(Result.m_Rows[0].m_Values[0]);
	}

	NConcurrency::TCFuture<umint> fg_PostgresExecuteCount(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values)
	{
		CPostgresValue Value = co_await fg_PostgresExecuteScalar(fg_Move(_Client), fg_Move(_Sql), fg_Move(_Values));
		if (Value.f_GetTypeID() != EPostgresValueType::mc_Integer64)
			co_return DMibErrorDatabaseInstance("PostgreSQL count select did not return a bigint value");

		int64 Count = Value.f_GetAsType<int64>();
		if (Count < 0)
			co_return DMibErrorDatabaseInstance("PostgreSQL count select returned a negative value");

		co_return umint(Count);
	}

	NConcurrency::TCFuture<bool> fg_PostgresExecuteExists(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values)
	{
		CPostgresValue Value = co_await fg_PostgresExecuteScalar(fg_Move(_Client), fg_Move(_Sql), fg_Move(_Values));
		if (Value.f_GetTypeID() != EPostgresValueType::mc_Boolean)
			co_return DMibErrorDatabaseInstance("PostgreSQL exists select did not return a boolean value");

		co_return Value.f_GetAsType<bool>();
	}

	NException::CExceptionPointer fg_PostgresParseAffectedRows(CPostgresQueryResult const &_Result, umint &o_nRows)
	{
		o_nRows = 0;
		if (!_Result.m_CommandComplete)
			return DMibErrorDatabaseInstance("PostgreSQL mutation did not return CommandComplete");

		// Only row-affecting commands (INSERT/UPDATE/DELETE/MERGE/MOVE/FETCH/COPY/SELECT) carry a trailing numeric
		// row count in the CommandComplete tag; for INSERT it is the last of two numbers ("INSERT oid rows").
		// Utility statements - DDL such as CREATE TABLE/DROP INDEX, transaction control such as BEGIN/COMMIT, and
		// SET/VACUUM/etc. - report only a command name with no numeric suffix and affect zero rows. Mirror libpq's
		// PQcmdTuples: take the final whitespace-delimited token when it is numeric, otherwise report zero affected
		// rows. Never fail here on a missing count - the statement has already succeeded on the server, so a raw
		// CREATE TABLE through f_ExecuteRaw must report success, not an error.
		auto const &Tag = _Result.m_CommandComplete->m_Tag;
		umint iCount = Tag.f_GetLen();
		while (iCount != 0 && Tag[iCount - 1] >= '0' && Tag[iCount - 1] <= '9')
			--iCount;

		if (iCount == Tag.f_GetLen() || iCount == 0 || Tag[iCount - 1] != ' ')
			return {};

		for (umint i = iCount; i < Tag.f_GetLen(); ++i)
			o_nRows = o_nRows * 10 + umint(Tag[i] - '0');

		return {};
	}

	NConcurrency::TCFuture<umint> fg_PostgresExecuteAffected(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values)
	{
		auto Result = co_await _Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(_Sql), fg_Move(_Values));
		umint nRows = 0;
		if (NException::CExceptionPointer pException = fg_PostgresParseAffectedRows(Result, nRows))
			co_return pException;

		co_return nRows;
	}

	NConcurrency::TCFuture<umint> fg_PostgresExecutePreparedAffected(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Name, NContainer::TCVector<CPostgresValue> _Values)
	{
		auto Result = co_await _Client(&CPostgresClientActor::f_ExecutePrepared, fg_Move(_Name), fg_Move(_Values));
		umint nRows = 0;
		if (NException::CExceptionPointer pException = fg_PostgresParseAffectedRows(Result, nRows))
			co_return pException;

		co_return nRows;
	}

	auto fg_PostgresParameterBatchGenerator(NConcurrency::TCAsyncGenerator<CSqlBulkInsertRowBatch> _RowBatches, uint32 _nPipelineLength)
		-> NConcurrency::TCAsyncGenerator<NContainer::TCVector<NContainer::TCVector<CPostgresValue>>>
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		for (auto iBatch = co_await fg_Move(_RowBatches).f_GetPipelinedIterator(_nPipelineLength); iBatch; co_await ++iBatch)
		{
			if ((*iBatch).f_IsEmpty())
				continue;

			NContainer::TCVector<NContainer::TCVector<CPostgresValue>> Parameters;
			Parameters.f_Reserve((*iBatch).f_GetLen());
			for (auto &Row : *iBatch)
				Parameters.f_InsertLast(co_await fg_PostgresValues(fg_Move(Row)));

			co_yield fg_Move(Parameters);
		}

		co_return {};
	}

	auto fg_PostgresExecuteReturningValue
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, NStr::CStr _Sql
			, NContainer::TCVector<CPostgresValue> _Values
			, CSqlRowFieldMapping _Field
		)
		-> NConcurrency::TCFuture<CSqlValue>
	{
		CPostgresValue Value = co_await fg_PostgresExecuteScalar(fg_Move(_Client), fg_Move(_Sql), fg_Move(_Values));

		co_return co_await fg_PostgresSqlValue(fg_Move(Value), _Field);
	}

	NContainer::TCVector<EPostgresValueType> fg_PostgresParameterTypes(CSqlParameterTypesDescription _ParameterTypes)
	{
		NContainer::TCVector<EPostgresValueType> ParameterTypes;
		for (umint i = 0; i < _ParameterTypes.m_nTypes; ++i)
			ParameterTypes.f_InsertLast(fg_PostgresValueType(_ParameterTypes.f_GetType(i)));

		return ParameterTypes;
	}

	NContainer::TCVector<EPostgresValueType> fg_PostgresValueTypes(NContainer::TCVector<ESqlValueType> const &_ValueTypes)
	{
		NContainer::TCVector<EPostgresValueType> ValueTypes;
		for (ESqlValueType ValueType : _ValueTypes)
			ValueTypes.f_InsertLast(fg_PostgresValueType(ValueType));

		return ValueTypes;
	}

	NConcurrency::TCFuture<bool> fg_PostgresHasTable(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _TableName)
	{
		NContainer::TCVector<CPostgresValue> Values;
		Values.f_InsertLast(fg_Move(_TableName));

		co_return co_await fg_PostgresHasRows(fg_Move(_Client), fg_PostgresHasTableSql(), fg_Move(Values));
	}

	NConcurrency::TCFuture<bool> fg_PostgresHasColumn(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _TableName, NStr::CStr _ColumnName)
	{
		NContainer::TCVector<CPostgresValue> Values;
		Values.f_InsertLast(fg_Move(_TableName));
		Values.f_InsertLast(fg_Move(_ColumnName));

		co_return co_await fg_PostgresHasRows(fg_Move(_Client), fg_PostgresHasColumnSql(), fg_Move(Values));
	}

	NConcurrency::TCFuture<bool> fg_PostgresHasConstraint(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _TableName, NStr::CStr _ConstraintName)
	{
		NContainer::TCVector<CPostgresValue> Values;
		Values.f_InsertLast(fg_Move(_TableName));
		Values.f_InsertLast(fg_Move(_ConstraintName));

		co_return co_await fg_PostgresHasRows(fg_Move(_Client), fg_PostgresHasConstraintSql(), fg_Move(Values));
	}

	CSqlTableDescription const *fg_PostgresFindTable(CSqlSchemaVersionDescription const &_Schema, NStr::CStr const &_TableName)
	{
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			if (Table.f_Name() == _TableName)
				return &Table;
		}

		return nullptr;
	}

	CSqlTableDescription *fg_PostgresFindTable(CSqlSchemaVersionDescription &_Schema, NStr::CStr const &_TableName)
	{
		for (auto &Table : _Schema.m_Database.m_Tables)
		{
			if (Table.f_Name() == _TableName)
				return &Table;
		}

		return nullptr;
	}

	void fg_PostgresRenameColumnReference(NContainer::TCVector<NStr::CStr const *> &_Columns, NStr::CStr const *_pOldName, NStr::CStr const *_pNewName)
	{
		for (auto &pColumn : _Columns)
		{
			if (*pColumn == *_pOldName)
				pColumn = _pNewName;
		}
	}

	void fg_PostgresRemoveForeignKeyConstraints(CSqlTableDescription &_Table)
	{
		for (umint iConstraint = _Table.m_Constraints.f_GetLen(); iConstraint-- > 0; )
		{
			if (_Table.m_Constraints[iConstraint].m_Type == ESqlConstraintType::mc_ForeignKey)
				_Table.m_Constraints.f_Remove(iConstraint);
		}
	}

	void fg_PostgresRemoveConstraintsForColumn(CSqlTableDescription &_Table, NStr::CStr const &_ColumnName)
	{
		for (umint iConstraint = _Table.m_Constraints.f_GetLen(); iConstraint-- > 0; )
		{
			bool bReferencesColumn = false;
			for (NStr::CStr const *pColumn : _Table.m_Constraints[iConstraint].m_Columns)
			{
				if (pColumn && *pColumn == _ColumnName)
				{
					bReferencesColumn = true;
					break;
				}
			}

			if (bReferencesColumn)
				_Table.m_Constraints.f_Remove(iConstraint);
		}
	}

	void fg_PostgresApplyMigrationOperationToPlannedSchema
		(
			CSqlSchemaVersionDescription &_Schema
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
		)
	{
		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable:
			if (CSqlTableDescription *pTable = fg_PostgresFindTable(_Schema, *_Operation.m_pOldName))
				pTable->m_pName = _Operation.m_pNewName;

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
			if (CSqlTableDescription *pTable = fg_PostgresFindTable(_Schema, *_Operation.m_pTableName))
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
					fg_PostgresRenameColumnReference(Index.m_Columns, _Operation.m_pOldName, _Operation.m_pNewName);

				for (auto &Constraint : pTable->m_Constraints)
					fg_PostgresRenameColumnReference(Constraint.m_Columns, _Operation.m_pOldName, _Operation.m_pNewName);
			}

			for (auto &Table : _Schema.m_Database.m_Tables)
			{
				for (auto &Constraint : Table.m_Constraints)
				{
					if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey && Constraint.f_ReferencedTable() == *_Operation.m_pTableName)
						fg_PostgresRenameColumnReference(Constraint.m_ReferencedColumns, _Operation.m_pOldName, _Operation.m_pNewName);
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
			if (CSqlTableDescription *pTable = fg_PostgresFindTable(_Schema, *_Operation.m_pTableName))
			{
				for (umint iColumn = 0; iColumn < pTable->m_Columns.f_GetLen(); ++iColumn)
				{
					if (pTable->m_Columns[iColumn].f_Name() == *_Operation.m_pOldName)
					{
						pTable->m_Columns.f_Remove(iColumn);
						break;
					}
				}

				// PostgreSQL drops any constraint that depends on the dropped column along with the column, so mirror
				// that here. Otherwise a target schema that reuses the constraint name with a definition no longer
				// covering the dropped column would be skipped by the additive pass, which would see the stale name in
				// the planned previous schema.
				fg_PostgresRemoveConstraintsForColumn(*pTable, *_Operation.m_pOldName);
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			// The rebuild reconstructs the table to its target shape but creates it without its foreign keys (so a
			// foreign key to a table added in the same version cannot fail because that table does not exist yet, and
			// so the additive pass does not try to re-add a foreign key the rebuild already created). Mirror that in
			// the planned previous schema: adopt the target table definition but drop its foreign-key constraints, so
			// the additive pass adds exactly those foreign keys once every table exists and adds nothing else for the
			// rebuilt table.
			if (CSqlTableDescription const *pTarget = fg_PostgresFindTable(_TargetSchema, *_Operation.m_pTableName))
			{
				if (CSqlTableDescription *pPlanned = fg_PostgresFindTable(_Schema, *_Operation.m_pTableName))
				{
					*pPlanned = *pTarget;
					fg_PostgresRemoveForeignKeyConstraints(*pPlanned);
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql:
			break;
		}
	}

	CSqlSchemaMigrationDescription const *fg_PostgresFindMigration
		(
			NContainer::TCVector<CSqlSchemaMigrationDescription> const &_Migrations
			, NStr::CStr const &_FromVersionID
			, NStr::CStr const &_ToVersionID
		)
	{
		for (auto const &Migration : _Migrations)
		{
			if (*Migration.m_pFromVersionID == _FromVersionID && *Migration.m_pToVersionID == _ToVersionID)
				return &Migration;
		}

		return nullptr;
	}

	bool fg_PostgresTableHasConstraint(CSqlTableDescription const *_pTable, NStr::CStr const &_ConstraintName)
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

	NContainer::TCVector<NStr::CStr const *> fg_PostgresCommonColumns(CSqlTableDescription const &_Table, CSqlTableDescription const *_pPreviousTable)
	{
		NContainer::TCVector<NStr::CStr const *> ExistingColumns;
		if (!_pPreviousTable)
			return ExistingColumns;

		for (auto const &Column : _Table.m_Columns)
		{
			// A generated column is recomputed by the database and cannot be named in the rebuild's INSERT ... SELECT,
			// so it must not be part of the copy list even though it exists in both tables.
			if (fg_PostgresColumnIsGenerated(Column))
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

	NStr::CStr fg_PostgresResetSerialSequenceSql(NStr::CStr const &_TableName, NStr::CStr const &_ColumnName);

	NContainer::TCVector<NStr::CStr> fg_PostgresRebuildTableStatements
		(
			CSqlTableDescription const &_Table
			, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		NStr::CStr OldTableName = "__mib_rebuild_old_{}"_f << _Table.f_Name();

		// Mirror the executor's safeguards from the schema description. Foreign keys in other tables that reference this
		// one keep depending on the renamed copy after the rename, so the later DROP of that copy would fail; collect
		// those child foreign keys (self-references are excluded - they are deferred to the additive pass) so they can
		// be dropped first and recreated against the rebuilt table.
		struct CPlannedChildForeignKey
		{
			CSqlTableDescription const *m_pTable = nullptr;
			CSqlConstraintDescription const *m_pConstraint = nullptr;
		};
		NContainer::TCVector<CPlannedChildForeignKey> ChildForeignKeys;
		if (_pPreviousSchema)
		{
			for (auto const &ChildTable : _pPreviousSchema->m_Database.m_Tables)
			{
				if (ChildTable.f_Name() == _Table.f_Name())
					continue;

				for (auto const &Constraint : ChildTable.m_Constraints)
				{
					if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey && Constraint.f_ReferencedTable() == _Table.f_Name())
						ChildForeignKeys.f_InsertLast(CPlannedChildForeignKey{&ChildTable, &Constraint});
				}
			}
		}

		for (auto const &ChildForeignKey : ChildForeignKeys)
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "ALTER TABLE ";
				fg_PostgresAppendQuotedIdentifier(Appender, ChildForeignKey.m_pTable->f_Name());
				Appender += " DROP CONSTRAINT ";
				fg_PostgresAppendQuotedIdentifier(Appender, ChildForeignKey.m_pConstraint->f_Name());
			}
			Statements.f_InsertLast(fg_Move(Sql));
		}

		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "ALTER TABLE ";
				fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
				Appender += " RENAME TO ";
				fg_PostgresAppendQuotedIdentifier(Appender, OldTableName);
			}
			Statements.f_InsertLast(fg_Move(Sql));
		}
		// Mirror the executor: create the rebuilt table without its foreign keys and let the additive pass add them,
		// so the planned statements match the order the backend actually applies.
		Statements.f_InsertLast(fg_PostgresCreateTable(_Table, false, false));

		if (!_ExistingColumns.f_IsEmpty())
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "INSERT INTO ";
				fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
				Appender += " (";
				for (umint i = 0; i < _ExistingColumns.f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					fg_PostgresAppendQuotedIdentifier(Appender, *_ExistingColumns[i]);
				}
				Appender += ") SELECT ";
				for (umint i = 0; i < _ExistingColumns.f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					fg_PostgresAppendQuotedIdentifier(Appender, *_ExistingColumns[i]);
				}
				Appender += " FROM ";
				fg_PostgresAppendQuotedIdentifier(Appender, OldTableName);
			}
			Statements.f_InsertLast(fg_Move(Sql));
		}

		// The copy inserted existing serial ids explicitly without advancing each owned sequence, so realign them
		// before the next default insert would otherwise reuse a copied id, mirroring the executor.
		for (auto const &Column : _Table.m_Columns)
		{
			if (fg_IsSet(Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
				Statements.f_InsertLast(fg_PostgresResetSerialSequenceSql(_Table.f_Name(), Column.f_Name()));
		}

		Statements.f_InsertLast(fg_PostgresDropTable(OldTableName));

		// Recreate the child foreign keys against the rebuilt table; their definitions still name this table.
		for (auto const &ChildForeignKey : ChildForeignKeys)
			Statements.f_InsertLast(fg_PostgresAlterTableAddConstraint(*ChildForeignKey.m_pTable, *ChildForeignKey.m_pConstraint));

		return Statements;
	}

	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_PostgresMigrationOperationStatements
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
			Statements.f_InsertLast(fg_PostgresRenameTable(_Operation));

			break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn:
			Statements.f_InsertLast(fg_PostgresRenameColumn(_Operation));

			break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			{
				auto pTable = fg_PostgresFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					return DMibErrorDatabaseInstance("Cannot rebuild PostgreSQL table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_PostgresFindTable(*_pPreviousSchema, *_Operation.m_pTableName) : nullptr;
				Statements = fg_PostgresRebuildTableStatements(*pTable, fg_PostgresCommonColumns(*pTable, pPreviousTable), _pPreviousSchema);

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_DropTable:
			Statements.f_InsertLast(fg_PostgresDropTable(*_Operation.m_pTableName));

			break;
		case ESqlSchemaMigrationOperationType::mc_DropColumn:
			Statements.f_InsertLast(fg_PostgresDropColumn(_Operation));

			break;
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql:
			Statements.f_InsertLast(fg_PostgresUpdateColumnSql(_Operation));

			break;
		}

		return Statements;
	}

	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_PostgresSyncAdditiveSchemaStatements
		(
			CSqlSchemaVersionDescription const &_Schema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;

		// Pass 1: create new tables without foreign keys, add new columns, indexes and non-foreign-key constraints.
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_PostgresFindTable(*_pPreviousSchema, Table.f_Name()) : nullptr;
			if (!pPreviousTable)
			{
				Statements.f_InsertLast(fg_PostgresCreateTable(Table, true, false));
				for (auto const &Index : Table.m_Indexes)
					Statements.f_InsertLast(fg_PostgresCreateIndex(Table, Index, true));

				continue;
			}

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

				if (!bColumnExists)
				{
					if (!Column.f_IsNullable() && fg_PostgresColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey())
						return DMibErrorDatabaseInstance("Cannot add required PostgreSQL column '{}.{}' without a default value"_f << Table.f_Name() << Column.f_Name());

					Statements.f_InsertLast(fg_PostgresAlterTableAddColumn(Table, Column));
				}
			}

			for (auto const &Index : Table.m_Indexes)
				Statements.f_InsertLast(fg_PostgresCreateIndex(Table, Index, true));

			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.m_Type != ESqlConstraintType::mc_ForeignKey && !fg_PostgresTableHasConstraint(pPreviousTable, Constraint.f_Name()))
					Statements.f_InsertLast(fg_PostgresAlterTableAddConstraint(Table, Constraint));
			}
		}

		// Pass 2: add foreign keys now that every referenced table exists, so creation is independent of table
		// declaration order and supports reference cycles.
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_PostgresFindTable(*_pPreviousSchema, Table.f_Name()) : nullptr;
			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.m_Type != ESqlConstraintType::mc_ForeignKey)
					continue;

				if (!fg_PostgresTableHasConstraint(pPreviousTable, Constraint.f_Name()))
					Statements.f_InsertLast(fg_PostgresAlterTableAddConstraint(Table, Constraint));
			}
		}

		return Statements;
	}

	void fg_PostgresAppendStringLiteral(NStr::CStr::CAppender &_Appender, NStr::CStr const &_Value)
	{
		_Appender += "'";
		for (aint i = 0; i < _Value.f_GetLen(); ++i)
		{
			auto Character = _Value.f_GetAt(i);
			_Appender += Character;
			if (Character == '\'')
				_Appender += "'";
		}
		_Appender += "'";
	}

	NStr::CStr fg_PostgresResetSerialSequenceSql(NStr::CStr const &_TableName, NStr::CStr const &_ColumnName)
	{
		// Set the owned sequence so the next default value is one past the current maximum. is_called=false makes
		// nextval() return the supplied value, so COALESCE(MAX,0)+1 yields 1 for an empty table and MAX+1 otherwise.
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "SELECT setval(pg_get_serial_sequence(quote_ident(";
			fg_PostgresAppendStringLiteral(Appender, _TableName);
			Appender += "), ";
			fg_PostgresAppendStringLiteral(Appender, _ColumnName);
			Appender += "), (SELECT COALESCE(MAX(";
			fg_PostgresAppendQuotedIdentifier(Appender, _ColumnName);
			Appender += "), 0) FROM ";
			fg_PostgresAppendQuotedIdentifier(Appender, _TableName);
			Appender += ") + 1, false)";
		}

		return Sql;
	}

	struct CPostgresChildForeignKey
	{
		NStr::CStr m_ChildTable;
		NStr::CStr m_Name;
		NStr::CStr m_Definition;
	};

	NStr::CStr fg_PostgresReferencingForeignKeysSql(NStr::CStr const &_TableName)
	{
		// Casts to ::text keep the name columns as text (OID 25) so the value decoder accepts them; the constraint
		// definition is reused verbatim to recreate the foreign key against the rebuilt table. con.conrelid !=
		// con.confrelid excludes a self-referential foreign key: that one belongs to the rebuilt table itself and is
		// recreated inline by fg_PostgresCreateTable, so adding it again here would fail with a duplicate constraint.
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender +=
				"SELECT rel.relname::text, con.conname::text, pg_get_constraintdef(con.oid) FROM pg_constraint con JOIN pg_class rel ON rel.oid = con.conrelid "
				"WHERE con.contype = 'f' AND con.conrelid != con.confrelid AND con.confrelid = quote_ident("
			;
			fg_PostgresAppendStringLiteral(Appender, _TableName);
			Appender += ")::regclass";
		}

		return Sql;
	}

	NConcurrency::TCFuture<void> fg_PostgresRebuildTable(NConcurrency::TCActor<CPostgresClientActor> _Client, CSqlTableDescription _Table)
	{
		if (!co_await fg_PostgresHasTable(_Client, _Table.f_Name()))
			co_return {};

		// Foreign-key constraints in other tables depend on this table object. Renaming it to the temporary name
		// leaves those child constraints depending on the renamed object, so the later DROP would fail with
		// "cannot drop ... because other objects depend on it". Drop the child foreign keys first and recreate them
		// after the rebuild; their captured definitions still name this table, so they bind to the recreated one.
		NContainer::TCVector<CPostgresChildForeignKey> ChildForeignKeys;
		{
			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresReferencingForeignKeysSql(_Table.f_Name()));
			for (auto const &Row : Result.m_Rows)
			{
				CPostgresChildForeignKey ChildForeignKey;
				ChildForeignKey.m_ChildTable = Row.m_Values[0].f_GetAsType<NStr::CStr>();
				ChildForeignKey.m_Name = Row.m_Values[1].f_GetAsType<NStr::CStr>();
				ChildForeignKey.m_Definition = Row.m_Values[2].f_GetAsType<NStr::CStr>();
				ChildForeignKeys.f_InsertLast(fg_Move(ChildForeignKey));
			}
		}

		for (auto const &ChildForeignKey : ChildForeignKeys)
		{
			NStr::CStr DropSql;
			{
				NStr::CStr::CAppender Appender(DropSql);
				Appender += "ALTER TABLE ";
				fg_PostgresAppendQuotedIdentifier(Appender, ChildForeignKey.m_ChildTable);
				Appender += " DROP CONSTRAINT ";
				fg_PostgresAppendQuotedIdentifier(Appender, ChildForeignKey.m_Name);
			}

			co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(DropSql));
		}

		// The scratch name is only ever a transient rename target within this schema transaction, which rolls back as a
		// whole on failure, so a committed leftover from an aborted rebuild cannot exist (PostgreSQL DDL is
		// transactional). Do not pre-drop the scratch name: if a legitimate user table happens to share it, the rename
		// below fails and the migration aborts safely instead of silently destroying that table's data.
		NStr::CStr OldTableName = NStr::CStr::CFormat("__mib_rebuild_old_{}") << _Table.f_Name();

		NStr::CStr RenameSql;
		{
			NStr::CStr::CAppender Appender(RenameSql);
			Appender += "ALTER TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " RENAME TO ";
			fg_PostgresAppendQuotedIdentifier(Appender, OldTableName);
		}

		co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(RenameSql));
		// Create the rebuilt table without its own foreign keys. The additive pass that runs after every table exists
		// adds them, so a foreign key to a table created in the same schema version does not fail here, and a foreign
		// key carried over from the previous version is not duplicated. (Self-referential foreign keys are likewise
		// deferred; they are excluded from the referencing-keys recreate set above.)
		co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresCreateTable(_Table, false, false));

		NContainer::TCVector<NStr::CStr const *> ExistingColumns;
		for (auto const &Column : _Table.m_Columns)
		{
			// Generated columns are recomputed by the database, so they must not be named in the copy's INSERT list.
			if (fg_PostgresColumnIsGenerated(Column))
				continue;

			if (co_await fg_PostgresHasColumn(_Client, OldTableName, Column.f_Name()))
				ExistingColumns.f_InsertLast(&Column.f_Name());
		}

		if (!ExistingColumns.f_IsEmpty())
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "INSERT INTO ";
				fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
				Appender += " (";

				for (umint i = 0; i < ExistingColumns.f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					fg_PostgresAppendQuotedIdentifier(Appender, *ExistingColumns[i]);
				}

				Appender += ") SELECT ";

				for (umint i = 0; i < ExistingColumns.f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					fg_PostgresAppendQuotedIdentifier(Appender, *ExistingColumns[i]);
				}

				Appender += " FROM ";
				fg_PostgresAppendQuotedIdentifier(Appender, OldTableName);
			}

			co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(Sql));
		}

		// The copy inserted the existing id values explicitly, which does not advance the BIGSERIAL column's new
		// owned sequence. Without this reset the first later default insert would reuse an already-copied id and
		// fail with a duplicate key, so realign each serial sequence with the copied data.
		for (auto const &Column : _Table.m_Columns)
		{
			if (fg_IsSet(Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresResetSerialSequenceSql(_Table.f_Name(), Column.f_Name()));
		}

		co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresDropTable(OldTableName));

		// Recreate the child foreign keys against the rebuilt table; the recreate validates the existing child rows
		// against the copied parent data.
		for (auto const &ChildForeignKey : ChildForeignKeys)
		{
			NStr::CStr AddSql;
			{
				NStr::CStr::CAppender Appender(AddSql);
				Appender += "ALTER TABLE ";
				fg_PostgresAppendQuotedIdentifier(Appender, ChildForeignKey.m_ChildTable);
				Appender += " ADD CONSTRAINT ";
				fg_PostgresAppendQuotedIdentifier(Appender, ChildForeignKey.m_Name);
				Appender += " ";
				Appender += ChildForeignKey.m_Definition;
			}

			co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(AddSql));
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_PostgresApplyMigrationOperation
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, CSqlSchemaMigrationOperationDescription _Operation
			, CSqlSchemaVersionDescription _TargetSchema
		)
	{
		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable:
			if (co_await fg_PostgresHasTable(_Client, *_Operation.m_pOldName))
			{
				if (co_await fg_PostgresHasTable(_Client, *_Operation.m_pNewName))
					break;

				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresRenameTable(_Operation));
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn:
			if (co_await fg_PostgresHasColumn(_Client, *_Operation.m_pTableName, *_Operation.m_pOldName))
			{
				if (co_await fg_PostgresHasColumn(_Client, *_Operation.m_pTableName, *_Operation.m_pNewName))
					break;

				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresRenameColumn(_Operation));
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			{
				auto pTable = fg_PostgresFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance(NStr::CStr::CFormat("Cannot rebuild PostgreSQL table that is not present in target schema: {}") << *_Operation.m_pTableName);

				co_await fg_PostgresRebuildTable(_Client, *pTable);

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_DropTable:
			if (co_await fg_PostgresHasTable(_Client, *_Operation.m_pTableName))
				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresDropTable(*_Operation.m_pTableName));

			break;
		case ESqlSchemaMigrationOperationType::mc_DropColumn:
			if (co_await fg_PostgresHasColumn(_Client, *_Operation.m_pTableName, *_Operation.m_pOldName))
				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresDropColumn(_Operation));

			break;
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql:
			if (co_await fg_PostgresHasTable(_Client, *_Operation.m_pTableName))
			{
				if (!co_await fg_PostgresHasColumn(_Client, *_Operation.m_pTableName, *_Operation.m_pNewName))
					break;

				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresUpdateColumnSql(_Operation));
			}

			break;
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_PostgresApplyMigrationOperations
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, CSqlSchemaMigrationDescription _Migration
			, CSqlSchemaVersionDescription _TargetSchema
		)
	{
		for (auto const &Operation : _Migration.m_Operations)
			co_await fg_PostgresApplyMigrationOperation(_Client, Operation, _TargetSchema);

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_PostgresMarkSchemaVersionApplied(NConcurrency::TCActor<CPostgresClientActor> _Client, CSqlSchemaVersionDescription _SchemaVersion)
	{
		NContainer::TCVector<CPostgresValue> Values;
		Values.f_InsertLast(_SchemaVersion.f_ID());
		Values.f_InsertLast(_SchemaVersion.f_DatabaseName());
		Values.f_InsertLast(_SchemaVersion.m_Checksum);
		Values.f_InsertLast(NStr::CStr("Malterlib"));

		co_await _Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_PostgresInsertSchemaVersionSql(), fg_Move(Values));

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_PostgresSyncAdditiveSchema
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, CSqlSchemaVersionDescription _Schema
			, CSqlSchemaVersionDescription const *_pPreviousSchema = nullptr
		)
	{
		// Pass 1: create new tables without foreign keys, add new columns, indexes and non-foreign-key constraints.
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_PostgresFindTable(*_pPreviousSchema, Table.f_Name()) : nullptr;
			if (!co_await fg_PostgresHasTable(_Client, Table.f_Name()))
			{
				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresCreateTable(Table, true, false));

				for (auto const &Index : Table.m_Indexes)
					co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresCreateIndex(Table, Index, true));

				continue;
			}

			for (auto const &Column : Table.m_Columns)
			{
				if (!co_await fg_PostgresHasColumn(_Client, Table.f_Name(), Column.f_Name()))
				{
					if (!Column.f_IsNullable() && fg_PostgresColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey())
					{
						co_return DMibErrorDatabaseInstance
							(
								NStr::CStr::CFormat("Cannot add required PostgreSQL column '{}.{}' without a default value")
								<< Table.f_Name()
								<< Column.f_Name()
							)
						;
					}

					co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresAlterTableAddColumn(Table, Column));
				}
			}

			for (auto const &Index : Table.m_Indexes)
				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresCreateIndex(Table, Index, true));

			// Add the declared non-foreign-key constraints this table is missing. During a migration the previous
			// version's tables already carry their constraints, so only add the ones new to this version. During the
			// initial adoption of a pre-existing table there is no previous schema, so consult the live table and add
			// any declared constraint it lacks - otherwise the table would be recorded as up to date with the
			// constraint still unenforced.
			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey)
					continue;

				bool bAlreadyPresent = _pPreviousSchema
					? fg_PostgresTableHasConstraint(pPreviousTable, Constraint.f_Name())
					: co_await fg_PostgresHasConstraint(_Client, Table.f_Name(), Constraint.f_Name())
				;

				if (!bAlreadyPresent)
					co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresAlterTableAddConstraint(Table, Constraint));
			}
		}

		// Pass 2: add foreign keys now that every referenced table exists, so creation is independent of table
		// declaration order and supports reference cycles. New tables (no previous version) get all of their foreign
		// keys; an existing table gets only foreign keys not already present in the previous version.
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_PostgresFindTable(*_pPreviousSchema, Table.f_Name()) : nullptr;
			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.m_Type != ESqlConstraintType::mc_ForeignKey)
					continue;

				// As above: diff against the previous version during a migration, but consult the live table when
				// adopting a pre-existing table with no recorded version so an existing foreign key is not re-added
				// (which would fail) and a missing one is still created.
				bool bAlreadyPresent = _pPreviousSchema
					? fg_PostgresTableHasConstraint(pPreviousTable, Constraint.f_Name())
					: co_await fg_PostgresHasConstraint(_Client, Table.f_Name(), Constraint.f_Name())
				;

				if (!bAlreadyPresent)
					co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresAlterTableAddConstraint(Table, Constraint));
			}
		}

		co_return {};
	}

	CPostgresDatabaseBackendActor::CPostgresDatabaseBackendActor(ICSqlSchemaVersions const *_pSchemaVersions, CPostgresDatabaseBackendSettings _Settings)
		: m_pSchemaVersions(_pSchemaVersions)
		, m_Schema(_pSchemaVersions->f_Describe())
		, m_SchemaVersions(_pSchemaVersions->f_DescribeVersions())
		, m_SchemaMigrations(_pSchemaVersions->f_DescribeMigrations())
		, m_Settings(fg_Move(_Settings))
		, m_pPreparedSelectCache(fg_Construct<CPostgresPreparedSelectCache>())
		, m_pPreparedInsertCache(fg_Construct<CPostgresPreparedInsertCache>())
		, m_pPreparedUpdateCache(fg_Construct<CPostgresPreparedUpdateCache>())
		, m_pPreparedDeleteCache(fg_Construct<CPostgresPreparedDeleteCache>())
		, m_pPreparedUpsertCache(fg_Construct<CPostgresPreparedUpsertCache>())
	{
	}

	CSqlDatabaseBackendCapabilities CPostgresDatabaseBackendActor::f_Capabilities() const
	{
		return
			{
				.m_Dialect = ESqlDialect::mc_SQL1999 | ESqlDialect::mc_SQL2011 | ESqlDialect::mc_SQL2016 | ESqlDialect::mc_SQL2023 | ESqlDialect::mc_Postgres
				, .m_bReadTransactions = true
				, .m_bTransactionalDDL = true
				, .m_bTableRename = true
				, .m_bColumnRename = true
				, .m_bTableRebuild = true
				, .m_bDropColumn = true
				, .m_bForeignKeyEnforcement = true
				, .m_bNumberedPlaceholders = true
				, .m_bMutationReturning = true
				, .m_bUUID = true
				, .m_bDate = true
				, .m_bTimeOfDay = true
				, .m_bTimestamp = true
				, .m_bTimestampTz = true
				, .m_bInterval = true
				, .m_bJSON = true
				, .m_bJSONB = true
				, .m_bArrays = true
				, .m_bUnrecognizedBackend = true
				, .m_bIsolationReadCommitted = true
				, .m_bIsolationRepeatableRead = true
				, .m_bIsolationSerializable = true
			}
		;
	}

	bool fg_PostgresSelectHasModifiers(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		return !_Statement.m_OrderBy.f_IsEmpty() || _Statement.m_LimitOffset.m_bHasLimit || _Statement.m_LimitOffset.m_bHasOffset;
	}

	bool fg_PostgresSelectHasSetOperandModifiers(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		if (_Statement.m_SetOperations.f_IsEmpty())
			return false;

		// The top-level statement IS the left operand of the compound (f_Describe copies the left operand's
		// clauses up), so this check rejects a modified left operand. Do not remove it as redundant - the loop
		// below only inspects the right-hand m_SetOperations entries.
		if (fg_PostgresSelectHasModifiers(_Statement))
			return true;

		for (auto const &SetOperation : _Statement.m_SetOperations)
		{
			CSqlPreparedSelectStatementDescription OperandDescription = SetOperation.m_pStatement->f_Describe();
			if (fg_PostgresSelectHasModifiers(OperandDescription))
				return true;
			if (fg_PostgresSelectHasSetOperandModifiers(OperandDescription))
				return true;
		}

		return false;
	}

	NException::CExceptionPointer fg_PostgresValidateSelectStatement(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		if (fg_PostgresSelectHasSetOperandModifiers(_Statement))
			return DMibErrorDatabaseInstance("PostgreSQL set operation operands cannot have ORDER BY, LIMIT, or OFFSET modifiers");

		return {};
	}

	NConcurrency::TCFuture<void> CPostgresDatabaseBackendActor::f_Open()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();
		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		ConnectionCheckout.f_MarkReusable();

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresDatabaseBackendActor::f_ApplySchema()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();
		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		auto Client = ConnectionCheckout.m_Client;

		co_await Client(&CPostgresClientActor::f_BeginTransaction, false, CSqlTransactionSettings());

		bool bCommitted = false;
		auto RollbackOnExit = co_await NConcurrency::fg_AsyncDestroy
			(
				[Client, pCheckoutState = ConnectionCheckout.m_pState, &bCommitted]() -> NConcurrency::TCFuture<void>
				{
					bool bCommittedLocal = bCommitted;

					if (!bCommittedLocal && Client)
					{
						auto RollbackResult = co_await Client(&CPostgresClientActor::f_RollbackTransaction).f_Wrap();
						if (RollbackResult)
							pCheckoutState->m_bReusable = true;
						else
							NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to roll back schema transaction")(fg_Move(RollbackResult));
					}
					else
						pCheckoutState->m_bReusable = true;

					co_return {};
				}
			)
		;

		co_await Client(&CPostgresClientActor::f_Execute, fg_PostgresCreateSchemaVersionTableSql());

		auto AppliedVersions = co_await fg_PostgresReadAppliedSchemaVersions(Client);

		if (NException::CExceptionPointer pValidationException = fg_PostgresValidateAppliedSchemaVersions(AppliedVersions, m_SchemaVersions))
			co_return pValidationException;

		NStr::CStr CurrentSchemaVersionID;
		umint iCurrentSchemaVersion = umint(-1);
		for (umint i = 0; i < m_SchemaVersions.f_GetLen(); ++i)
		{
			auto const &SchemaVersion = m_SchemaVersions[i];

			for (auto const &AppliedVersion : AppliedVersions)
			{
				if (AppliedVersion.m_ID == SchemaVersion.f_ID())
				{
					CurrentSchemaVersionID = SchemaVersion.f_ID();
					iCurrentSchemaVersion = i;

					break;
				}
			}
		}

		if (CurrentSchemaVersionID != m_Schema.f_ID())
		{
			if (CurrentSchemaVersionID.f_IsEmpty())
			{
				co_await fg_PostgresSyncAdditiveSchema(Client, m_Schema);

				for (auto const &SchemaVersion : m_SchemaVersions)
					co_await fg_PostgresMarkSchemaVersionApplied(Client, SchemaVersion);
			}
			else
			{
				for (umint i = iCurrentSchemaVersion + 1; i < m_SchemaVersions.f_GetLen(); ++i)
				{
					auto const &PreviousSchema = m_SchemaVersions[i - 1];
					auto const &NextSchema = m_SchemaVersions[i];
					CSqlSchemaVersionDescription PlannedPreviousSchema = PreviousSchema;

					if (CSqlSchemaMigrationDescription const *pMigration = fg_PostgresFindMigration(m_SchemaMigrations, PreviousSchema.f_ID(), NextSchema.f_ID()))
					{
						co_await fg_PostgresApplyMigrationOperations(Client, *pMigration, NextSchema);

						for (auto const &Operation : pMigration->m_Operations)
							fg_PostgresApplyMigrationOperationToPlannedSchema(PlannedPreviousSchema, Operation, NextSchema);
					}

					co_await fg_PostgresSyncAdditiveSchema(Client, NextSchema, &PlannedPreviousSchema);
					co_await fg_PostgresMarkSchemaVersionApplied(Client, NextSchema);
				}
			}
		}

		co_await Client(&CPostgresClientActor::f_CommitTransaction);

		bCommitted = true;
		ConnectionCheckout.f_MarkReusable();

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresDatabaseBackendActor::f_Insert(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_EnsurePreparedInsert(&Description);
			auto PostgresValues = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			if (ConnectionCheckout.m_pPreparedInsertCache->f_Find(&Description))
				co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecutePrepared, StatementEntry.m_Name, fg_Move(PostgresValues));
			else
			{
				co_await ConnectionCheckout.m_Client
					(
						&CPostgresClientActor::f_PrepareStatement
						, StatementEntry.m_Name
						, StatementEntry.m_Sql
						, fg_PostgresValueTypes(StatementEntry.m_Description.m_InsertColumnTypes)
					)
				;
				ConnectionCheckout.m_pPreparedInsertCache->f_Insert(&Description, StatementEntry);
				co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecutePrepared, StatementEntry.m_Name, fg_Move(PostgresValues));
			}
		}
		else
		{
			auto Sql = fg_PostgresInsertSql(_Operation);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values));
		}

		ConnectionCheckout.f_MarkReusable();

		co_return {};
	}

	NConcurrency::TCFuture<umint> CPostgresDatabaseBackendActor::f_InsertMany(CSqlBulkInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (!_Operation.m_pDescription)
			co_return DMibErrorDatabaseInstance("PostgreSQL bulk insert requires a prepared insert description");

		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = fp_EnsurePreparedInsert(&Description);

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		if (!ConnectionCheckout.m_pPreparedInsertCache->f_Find(&Description))
		{
			co_await ConnectionCheckout.m_Client
				(
					&CPostgresClientActor::f_PrepareStatement
					, StatementEntry.m_Name
					, StatementEntry.m_Sql
					, fg_PostgresValueTypes(StatementEntry.m_Description.m_InsertColumnTypes)
				)
			;
			ConnectionCheckout.m_pPreparedInsertCache->f_Insert(&Description, StatementEntry);
		}

		umint nTotalAffected = co_await ConnectionCheckout.m_Client
			(
				&CPostgresClientActor::f_ExecutePreparedBulk
				, StatementEntry.m_Name
				, fg_PostgresParameterBatchGenerator(fg_Move(_Operation.m_RowBatches), m_Settings.m_nPipelineLength)
			)
		;

		ConnectionCheckout.f_MarkReusable();

		co_return nTotalAffected;
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresDatabaseBackendActor::f_InsertReturning(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		auto Sql = fg_PostgresInsertSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		CSqlValue Value = co_await fg_PostgresExecuteReturningValue(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));

		ConnectionCheckout.f_MarkReusable();

		co_return Value;
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresDatabaseBackendActor::f_UpsertReturning(CSqlUpsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		auto Sql = fg_PostgresUpsertSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		CSqlValue Value = co_await fg_PostgresExecuteReturningValue(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));

		ConnectionCheckout.f_MarkReusable();

		co_return Value;
	}

	NConcurrency::TCFuture<umint> CPostgresDatabaseBackendActor::f_Upsert(CSqlUpsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		umint nAffected = 0;
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_EnsurePreparedUpsert(&Description);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			if (!ConnectionCheckout.m_pPreparedUpsertCache->f_Find(&Description))
			{
				co_await ConnectionCheckout.m_Client
					(
						&CPostgresClientActor::f_PrepareStatement
						, StatementEntry.m_Name
						, StatementEntry.m_Sql
						, fg_PostgresValueTypes(StatementEntry.m_Description.m_InsertColumnTypes)
					)
				;
				ConnectionCheckout.m_pPreparedUpsertCache->f_Insert(&Description, StatementEntry);
			}

			nAffected = co_await fg_PostgresExecutePreparedAffected(ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
		}
		else
		{
			auto Sql = fg_PostgresUpsertSql(_Operation);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			nAffected = co_await fg_PostgresExecuteAffected(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
		}

		ConnectionCheckout.f_MarkReusable();

		co_return nAffected;
	}

	NConcurrency::TCFuture<umint> CPostgresDatabaseBackendActor::f_Update(CSqlUpdateOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		umint nAffected = 0;
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_EnsurePreparedUpdate(&Description);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			if (!ConnectionCheckout.m_pPreparedUpdateCache->f_Find(&Description))
			{
				co_await ConnectionCheckout.m_Client
					(
						&CPostgresClientActor::f_PrepareStatement
						, StatementEntry.m_Name
						, StatementEntry.m_Sql
						, NContainer::TCVector<EPostgresValueType>()
					)
				;
				ConnectionCheckout.m_pPreparedUpdateCache->f_Insert(&Description, StatementEntry);
			}

			nAffected = co_await fg_PostgresExecutePreparedAffected(ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
		}
		else
		{
			auto Sql = fg_PostgresUpdateSql(_Operation);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			nAffected = co_await fg_PostgresExecuteAffected(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
		}

		ConnectionCheckout.f_MarkReusable();

		co_return nAffected;
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresDatabaseBackendActor::f_UpdateReturning(CSqlUpdateOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		auto Sql = fg_PostgresUpdateSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		CSqlValue Value = co_await fg_PostgresExecuteReturningValue(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));

		ConnectionCheckout.f_MarkReusable();

		co_return Value;
	}

	NConcurrency::TCFuture<umint> CPostgresDatabaseBackendActor::f_Delete(CSqlDeleteOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		umint nAffected = 0;
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_EnsurePreparedDelete(&Description);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			if (!ConnectionCheckout.m_pPreparedDeleteCache->f_Find(&Description))
			{
				co_await ConnectionCheckout.m_Client
					(
						&CPostgresClientActor::f_PrepareStatement
						, StatementEntry.m_Name
						, StatementEntry.m_Sql
						, NContainer::TCVector<EPostgresValueType>()
					)
				;
				ConnectionCheckout.m_pPreparedDeleteCache->f_Insert(&Description, StatementEntry);
			}

			nAffected = co_await fg_PostgresExecutePreparedAffected(ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
		}
		else
		{
			auto Sql = fg_PostgresDeleteSql(_Operation);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			nAffected = co_await fg_PostgresExecuteAffected(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
		}

		ConnectionCheckout.f_MarkReusable();

		co_return nAffected;
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresDatabaseBackendActor::f_DeleteReturning(CSqlDeleteOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		auto Sql = fg_PostgresDeleteSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		CSqlValue Value = co_await fg_PostgresExecuteReturningValue(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));

		ConnectionCheckout.f_MarkReusable();

		co_return Value;
	}

	NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> CPostgresDatabaseBackendActor::fs_Select(CPostgresDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await _pBackend->f_CheckDestroyedOnResume();

		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = _pBackend->fp_GetPreparedSelect(&Description);
		CSqlPreparedSelectStatementDescription const *pStatementDescription = &StatementEntry.m_Description;
		if (auto pException = fg_PostgresValidateSelectStatement(*pStatementDescription))
			co_return pException;

		if (_Operation.m_nResultRowLimit != 0 && !pStatementDescription->m_LimitOffset.m_bHasLimit)
			co_return DMibErrorDatabaseInstance("SELECT specifies a result-row limit but the prepared statement does not declare f_WithLimit()");
		if (_Operation.m_nResultRowOffset != 0 && !pStatementDescription->m_LimitOffset.m_bHasOffset)
			co_return DMibErrorDatabaseInstance("SELECT specifies a result-row offset but the prepared statement does not declare f_WithOffset()");
		auto Sql = StatementEntry.m_Sql;
		auto Mapping = pStatementDescription->m_RowMapping;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		if (pStatementDescription->m_LimitOffset.m_bHasLimit)
			Parameters.f_InsertLast(_Operation.m_nResultRowLimit != 0 ? int64(_Operation.m_nResultRowLimit) : TCLimitsInt<int64>::mc_Max);
		if (pStatementDescription->m_LimitOffset.m_bHasOffset)
			Parameters.f_InsertLast(int64(_Operation.m_nResultRowOffset));
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : _pBackend->m_Settings.m_nSelectRowsPerBatch;
		auto nPipelineLength = _pBackend->m_Settings.m_nPipelineLength;
		auto ConnectionCheckout = co_await _pBackend->fp_CheckoutConnection();

		auto Values = co_await fg_PostgresValues(fg_Move(Parameters));
		auto Rows = co_await ConnectionCheckout.m_Client
			(
				&CPostgresClientActor::f_ExecuteRows
				, fg_Move(Sql)
				, fg_Move(Values)
				, nRowsPerBatch
			)
		;

		for (auto iBatch = co_await fg_Move(Rows).f_GetPipelinedIterator(nPipelineLength); iBatch; co_await ++iBatch)
		{
			CSqlRowDataBatch Batch;
			for (auto &Row : *iBatch)
				Batch.f_InsertLast(co_await fg_PostgresMapRow(fg_Move(Row), Mapping));

			co_yield fg_Move(Batch);
		}

		ConnectionCheckout.f_MarkReusable();

		co_return {};
	}

	NConcurrency::TCFuture<umint> CPostgresDatabaseBackendActor::fs_Count(CPostgresDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await _pBackend->f_CheckDestroyedOnResume();

		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = _pBackend->fp_GetPreparedSelect(&Description);
		if (auto pException = fg_PostgresValidateSelectStatement(StatementEntry.m_Description))
			co_return pException;

		auto Sql = StatementEntry.m_CountSql;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		auto ConnectionCheckout = co_await _pBackend->fp_CheckoutConnection();

		auto Values = co_await fg_PostgresValues(fg_Move(Parameters));
		umint Count = co_await fg_PostgresExecuteCount(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
		ConnectionCheckout.f_MarkReusable();

		co_return Count;
	}

	NConcurrency::TCFuture<bool> CPostgresDatabaseBackendActor::fs_Exists(CPostgresDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await _pBackend->f_CheckDestroyedOnResume();

		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = _pBackend->fp_GetPreparedSelect(&Description);
		if (auto pException = fg_PostgresValidateSelectStatement(StatementEntry.m_Description))
			co_return pException;

		auto Sql = StatementEntry.m_ExistsSql;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		auto ConnectionCheckout = co_await _pBackend->fp_CheckoutConnection();

		auto Values = co_await fg_PostgresValues(fg_Move(Parameters));
		bool bExists = co_await fg_PostgresExecuteExists(ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
		ConnectionCheckout.f_MarkReusable();

		co_return bExists;
	}

	NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> CPostgresDatabaseBackendActor::f_Select(CSqlSelectOperation _Operation)
	{
		return fs_Select(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CPostgresDatabaseBackendActor::f_Count(CSqlSelectOperation _Operation)
	{
		return fs_Count(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<bool> CPostgresDatabaseBackendActor::f_Exists(CSqlSelectOperation _Operation)
	{
		return fs_Exists(this, fg_Move(_Operation));
	}

	CPostgresPreparedSelectCacheEntry CPostgresDatabaseBackendActor::fp_EnsurePreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedSelectCacheEntry const *pEntry = m_pPreparedSelectCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedSelectCacheEntry Entry = fp_GetPreparedSelect(_pDescription);

		if (m_pPreparedSelectCache.f_GetRefCount() != 1)
			m_pPreparedSelectCache = fg_Construct<CPostgresPreparedSelectCache>(*m_pPreparedSelectCache);

		m_pPreparedSelectCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	CPostgresPreparedInsertCacheEntry CPostgresDatabaseBackendActor::fp_EnsurePreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedInsertCacheEntry const *pEntry = m_pPreparedInsertCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedInsertCacheEntry Entry = fp_GetPreparedInsert(_pDescription);

		if (m_pPreparedInsertCache.f_GetRefCount() != 1)
			m_pPreparedInsertCache = fg_Construct<CPostgresPreparedInsertCache>(*m_pPreparedInsertCache);

		m_pPreparedInsertCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	NConcurrency::TCFuture<umint> CPostgresDatabaseBackendActor::f_ExecuteRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_SQLite)
			co_return DMibErrorDatabaseInstance("SQLite-specific raw SQL operation cannot run on the PostgreSQL backend");

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));
		auto Result = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values));
		umint nAffected = 0;
		if (NException::CExceptionPointer pException = fg_PostgresParseAffectedRows(Result, nAffected))
			co_return pException;

		// Raw SQL may include transaction-control statements (BEGIN/SAVEPOINT/...); only return the session to the
		// pool when the server reports it is back at idle. A connection left mid-transaction must not be handed to
		// unrelated work, so leaving it non-reusable makes fp_ReleaseConnection discard and reconnect it.
		if (Result.m_ReadyStatus == EPostgresReadyForQueryStatus::mc_Idle)
			ConnectionCheckout.f_MarkReusable();

		co_return nAffected;
	}

	NConcurrency::TCFuture<CSqlRawResult> CPostgresDatabaseBackendActor::f_QueryRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_SQLite)
			co_return DMibErrorDatabaseInstance("SQLite-specific raw SQL operation cannot run on the PostgreSQL backend");

		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));
		auto QueryResult = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values));

		// See f_ExecuteRaw: only pool the session back when it returned to idle, so raw transaction-control SQL cannot
		// leave an open transaction on a connection handed to unrelated work.
		if (QueryResult.m_ReadyStatus == EPostgresReadyForQueryStatus::mc_Idle)
			ConnectionCheckout.f_MarkReusable();

		co_return fg_PostgresRawResult(fg_Move(QueryResult));
	}

	static NConcurrency::TCAsyncGenerator<CSqlRawRowBatch> fg_PostgresRawRowStreamFromIterator
		(
			NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>::CPipelinedIterator _iItems
			, CPostgresConnectionCheckout _ConnectionCheckout
		)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		for (; _iItems; co_await ++_iItems)
		{
			CPostgresRowStreamItem Item = fg_Move(*_iItems);
			if (Item.m_Description)
				continue;

			if (Item.m_Rows.f_IsEmpty())
				continue;

			CSqlRawRowBatch Batch;
			Batch.f_Reserve(Item.m_Rows.f_GetLen());
			for (auto &Row : Item.m_Rows)
				Batch.f_InsertLast(fg_PostgresRawRow(fg_Move(Row)));

			co_yield fg_Move(Batch);
		}

		_ConnectionCheckout.f_MarkReusable();

		co_return {};
	}

	NConcurrency::TCFuture<CSqlRawStream> CPostgresDatabaseBackendActor::f_QueryRawStream(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_SQLite)
			co_return DMibErrorDatabaseInstance("SQLite-specific raw SQL operation cannot run on the PostgreSQL backend");

		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : m_Settings.m_nSelectRowsPerBatch;
		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));

		auto Items = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteRowsStream, fg_Move(Sql), fg_Move(Values), nRowsPerBatch);
		auto iItems = co_await fg_Move(Items).f_GetPipelinedIterator(m_Settings.m_nPipelineLength);
		if (!iItems)
		{
			// The stream produced no items, which means it drained to ReadyForQuery (for example an UPDATE or DDL, or
			// transaction-control SQL such as BEGIN, routed through the stream API). Only return the connection to the
			// pool when it came back idle; a raw BEGIN leaves it inside a transaction, and pooling that would hand an
			// open transaction to unrelated work - so leave it non-reusable and let fp_ReleaseConnection discard and
			// reconnect it, mirroring f_ExecuteRaw/f_QueryRaw.
			bool bInTransaction = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_IsInTransaction);
			if (!bInTransaction)
				ConnectionCheckout.f_MarkReusable();

			co_return DMibErrorDatabaseInstance("PostgreSQL raw stream completed without producing a row description");
		}

		CPostgresRowStreamItem FirstItem = fg_Move(*iItems);
		if (!FirstItem.m_Description)
			co_return DMibErrorDatabaseInstance("PostgreSQL raw stream did not produce a row description before rows");

		co_await ++iItems;

		CSqlRawStream Stream;
		Stream.m_Columns = fg_PostgresRawColumns(*FirstItem.m_Description);
		Stream.m_Rows = fg_PostgresRawRowStreamFromIterator(fg_Move(iItems), fg_Move(ConnectionCheckout));

		co_return Stream;
	}

	NConcurrency::TCFuture<CSqlTransactionInterface> CPostgresDatabaseBackendActor::f_BeginTransaction(CSqlTransactionSettings _Settings)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();
		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		auto Transaction = NConcurrency::fg_ConstructActor<CPostgresTransactionActor>
			(
				false
				, _Settings
				, fg_Move(ConnectionCheckout)
				, m_Settings.m_nSelectRowsPerBatch
				, m_Settings.m_nPipelineLength
				, NConcurrency::fg_ThisActor(this)
				, m_pPreparedSelectCache
				, m_pPreparedInsertCache
				, m_pPreparedUpdateCache
				, m_pPreparedDeleteCache
				, m_pPreparedUpsertCache
			)
		;
		co_await Transaction(&CPostgresTransactionActor::f_OpenBegin);

		co_return Transaction;
	}

	NConcurrency::TCFuture<CSqlTransactionInterface> CPostgresDatabaseBackendActor::f_BeginReadTransaction(CSqlTransactionSettings _Settings)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();
		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		auto Transaction = NConcurrency::fg_ConstructActor<CPostgresTransactionActor>
			(
				true
				, _Settings
				, fg_Move(ConnectionCheckout)
				, m_Settings.m_nSelectRowsPerBatch
				, m_Settings.m_nPipelineLength
				, NConcurrency::fg_ThisActor(this)
				, m_pPreparedSelectCache
				, m_pPreparedInsertCache
				, m_pPreparedUpdateCache
				, m_pPreparedDeleteCache
				, m_pPreparedUpsertCache
			)
		;
		co_await Transaction(&CPostgresTransactionActor::f_OpenBegin);

		co_return Transaction;
	}

	NConcurrency::TCFuture<void> CPostgresDatabaseBackendActor::fp_Destroy()
	{
		auto pDestroyedException = DMibErrorDatabaseInstance("PostgreSQL database backend destroyed while waiting for connection").f_ExceptionPointer();
		for (NConcurrency::TCPromise<void> &Promise : m_ConnectionPoolWaiters)
			Promise.f_SetException(pDestroyedException);

		m_ConnectionPoolWaiters.f_Clear();

		NConcurrency::TCFutureVector<void> DestroyFutures;
		for (CPostgresConnectionPoolEntry &Entry : m_ConnectionPool)
		{
			if (Entry.m_Client)
				fg_Move(Entry.m_Client).f_Destroy() > DestroyFutures;
		}

		co_await NConcurrency::fg_AllDone(DestroyFutures);

		co_return {};
	}

	NConcurrency::TCFuture<CPostgresConnectionCheckout> CPostgresDatabaseBackendActor::fp_CheckoutConnection()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();
		if (m_Settings.m_ConnectionPoolSize == 0)
			co_return DMibErrorDatabaseInstance("PostgreSQL connection pool size must be at least 1");

		CPostgresConnectionPoolEntry *pEntry = nullptr;

		for (;;)
		{
			pEntry = m_FreeConnectionPoolEntries.f_Pop();
			if (!pEntry)
			{
				if (m_ConnectionPool.f_GetLen() >= m_Settings.m_ConnectionPoolSize)
				{
					co_await m_ConnectionPoolWaiters.f_InsertLast().f_Future();

					continue;
				}

				pEntry = &m_ConnectionPool.f_InsertLast();
			}

			if (!pEntry->m_bConnected)
			{
				if (!pEntry->m_Client)
					pEntry->m_Client = fg_CreatePostgresClient();

				auto ConnectResult = co_await pEntry->m_Client(&CPostgresClientActor::f_Connect, m_Settings.m_ConnectionSettings).f_Wrap();
				if (!ConnectResult)
				{
					pEntry->m_bConnected = false;
					pEntry->m_PreparedSelectCache.f_Clear();
					pEntry->m_PreparedInsertCache.f_Clear();
					pEntry->m_PreparedUpdateCache.f_Clear();
					pEntry->m_PreparedDeleteCache.f_Clear();
					pEntry->m_PreparedUpsertCache.f_Clear();
					co_await fg_Move(pEntry->m_Client).f_Destroy().f_Wrap()
						> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to destroy PostgreSQL connection after connect failure")
					;
					m_FreeConnectionPoolEntries.f_InsertLast(pEntry);
					fp_NotifyConnectionAvailable();

					NConcurrency::TCAsyncResult<CPostgresConnectionCheckout> CheckoutResult;
					CheckoutResult.f_SetException(fg_Move(ConnectResult));

					co_return fg_Move(CheckoutResult);
				}

				pEntry->m_bConnected = true;
			}

			break;
		}

		CPostgresConnectionCheckout Checkout;
		Checkout.m_Client = pEntry->m_Client;
		Checkout.m_pPreparedSelectCache = &pEntry->m_PreparedSelectCache;
		Checkout.m_pPreparedInsertCache = &pEntry->m_PreparedInsertCache;
		Checkout.m_pPreparedUpdateCache = &pEntry->m_PreparedUpdateCache;
		Checkout.m_pPreparedDeleteCache = &pEntry->m_PreparedDeleteCache;
		Checkout.m_pPreparedUpsertCache = &pEntry->m_PreparedUpsertCache;
		Checkout.m_pState = fg_Construct<CPostgresConnectionCheckoutState>();
		Checkout.m_ReleaseSubscription = NConcurrency::g_ActorSubscription / [this, pEntry, pState = Checkout.m_pState]() -> NConcurrency::TCFuture<void>
			{
				co_await fp_ReleaseConnection(pEntry, pState);

				co_return {};
			}
		;

		co_return fg_Move(Checkout);
	}

	auto CPostgresDatabaseBackendActor::fp_ReleaseConnection
		(
			CPostgresConnectionPoolEntry *_pEntry
			, NStorage::TCSharedPointer<CPostgresConnectionCheckoutState> _pState
		)
		-> NConcurrency::TCFuture<void>
	{
		if (!_pEntry)
			co_return {};

		if (_pState && _pState->m_bReusable && _pEntry->m_bConnected)
		{
			m_FreeConnectionPoolEntries.f_InsertFirst(_pEntry);
			fp_NotifyConnectionAvailable();

			co_return {};
		}

		if (_pEntry->m_Client)
			co_await fg_Move(_pEntry->m_Client).f_Destroy();

		_pEntry->m_bConnected = false;
		_pEntry->m_PreparedSelectCache.f_Clear();
		_pEntry->m_PreparedInsertCache.f_Clear();
		_pEntry->m_PreparedUpdateCache.f_Clear();
		_pEntry->m_PreparedDeleteCache.f_Clear();
		_pEntry->m_PreparedUpsertCache.f_Clear();
		_pEntry->m_Client = fg_CreatePostgresClient();
		auto ConnectResult = co_await _pEntry->m_Client(&CPostgresClientActor::f_Connect, m_Settings.m_ConnectionSettings).f_Wrap();
		if (ConnectResult)
			_pEntry->m_bConnected = true;
		else
		{
			co_await fg_Move(_pEntry->m_Client).f_Destroy().f_Wrap()
				> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to destroy PostgreSQL connection after reconnect failure")
			;
			NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to reconnect PostgreSQL connection during release")(fg_Move(ConnectResult));
		}

		m_FreeConnectionPoolEntries.f_InsertLast(_pEntry);
		fp_NotifyConnectionAvailable();

		co_return {};
	}

	void CPostgresDatabaseBackendActor::fp_NotifyConnectionAvailable()
	{
		if (m_ConnectionPoolWaiters.f_IsEmpty())
			return;

		m_ConnectionPoolWaiters.f_PopFirst().f_SetResult();
	}

	CPostgresPreparedSelectCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		// Resolve the row mapping by the description pointer, never the shared QueryID key: a select's mapping is not
		// always determined by its SQL (aliased expressions decode into specific result-struct members without changing
		// the SQL), so a QueryID match could hand back a different statement's mapping. The pointer key is unique per
		// compile-time statement and so always yields this statement's own mapping.
		if (CPostgresPreparedSelectCacheEntry const *pEntry = m_pPreparedSelectCache->f_FindByPointer(_pDescription))
			return *pEntry;

		CPostgresPreparedSelectCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresSelectSql(Entry.m_Description, _pDescription->m_ParameterTypes.m_nTypes);
		Entry.m_CountSql = fg_PostgresSelectCountSql(Entry.m_Description);
		Entry.m_ExistsSql = fg_PostgresSelectExistsSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedSelectName(_pDescription->m_QueryID);

		return Entry;
	}

	CPostgresPreparedInsertCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedInsertCacheEntry const *pEntry = m_pPreparedInsertCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedInsertCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		CSqlInsertOperation Operation;
		Operation.m_TableName = Entry.m_Description.m_TableName;
		Operation.m_pDescription = _pDescription;
		Entry.m_Sql = fg_PostgresInsertSql(Operation);
		Entry.m_Name = fg_PostgresPreparedInsertName(_pDescription->m_QueryID);

		return Entry;
	}

	CPostgresPreparedUpdateCacheEntry CPostgresDatabaseBackendActor::fp_EnsurePreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpdateCacheEntry const *pEntry = m_pPreparedUpdateCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedUpdateCacheEntry Entry = fp_GetPreparedUpdate(_pDescription);

		if (m_pPreparedUpdateCache.f_GetRefCount() != 1)
			m_pPreparedUpdateCache = fg_Construct<CPostgresPreparedUpdateCache>(*m_pPreparedUpdateCache);

		m_pPreparedUpdateCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	CPostgresPreparedUpdateCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpdateCacheEntry const *pEntry = m_pPreparedUpdateCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedUpdateCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresUpdateSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedUpdateName(_pDescription->m_QueryID);

		return Entry;
	}

	CPostgresPreparedDeleteCacheEntry CPostgresDatabaseBackendActor::fp_EnsurePreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedDeleteCacheEntry const *pEntry = m_pPreparedDeleteCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedDeleteCacheEntry Entry = fp_GetPreparedDelete(_pDescription);

		if (m_pPreparedDeleteCache.f_GetRefCount() != 1)
			m_pPreparedDeleteCache = fg_Construct<CPostgresPreparedDeleteCache>(*m_pPreparedDeleteCache);

		m_pPreparedDeleteCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	CPostgresPreparedDeleteCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedDeleteCacheEntry const *pEntry = m_pPreparedDeleteCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedDeleteCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresDeleteSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedDeleteName(_pDescription->m_QueryID);

		return Entry;
	}

	CPostgresPreparedUpsertCacheEntry CPostgresDatabaseBackendActor::fp_EnsurePreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpsertCacheEntry const *pEntry = m_pPreparedUpsertCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedUpsertCacheEntry Entry = fp_GetPreparedUpsert(_pDescription);

		if (m_pPreparedUpsertCache.f_GetRefCount() != 1)
			m_pPreparedUpsertCache = fg_Construct<CPostgresPreparedUpsertCache>(*m_pPreparedUpsertCache);

		m_pPreparedUpsertCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	CPostgresPreparedUpsertCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpsertCacheEntry const *pEntry = m_pPreparedUpsertCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedUpsertCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresUpsertSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedUpsertName(_pDescription->m_QueryID);

		return Entry;
	}
}

namespace NMib::NSQL
{
	NConcurrency::TCWrapped<CSqlSchemaMigrationPlan> fg_PostgresPlanSchemaMigration(ICSqlSchemaVersions const &_SchemaVersions, NStr::CStr const *_pCurrentVersionID)
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

		Plan.m_Statements.f_InsertLast(fg_PostgresCreateSchemaVersionTableSql());
		if (CurrentVersionID.f_IsEmpty())
		{
			Plan.m_Statements.f_InsertLast(co_await fg_PostgresSyncAdditiveSchemaStatements(Versions[Versions.f_GetLen() - 1], nullptr));
			for (umint iVersion = 0; iVersion < Versions.f_GetLen(); ++iVersion)
				Plan.m_Statements.f_InsertLast(fg_PostgresInsertSchemaVersionSql());

			co_return fg_Move(Plan);
		}

		if (iCurrentVersion == Versions.f_GetLen() - 1)
			co_return fg_Move(Plan);

		for (umint iVersion = iCurrentVersion + 1; iVersion < Versions.f_GetLen(); ++iVersion)
		{
			auto const &PreviousVersion = Versions[iVersion - 1];
			auto const &NextVersion = Versions[iVersion];
			CSqlSchemaVersionDescription PlannedPreviousVersion = PreviousVersion;
			if (auto const *pMigration = fg_PostgresFindMigration(Migrations, PreviousVersion.f_ID(), NextVersion.f_ID()))
			{
				for (auto const &Operation : pMigration->m_Operations)
				{
					Plan.m_Statements.f_InsertLast(co_await fg_PostgresMigrationOperationStatements(Operation, NextVersion, &PlannedPreviousVersion));
					fg_PostgresApplyMigrationOperationToPlannedSchema(PlannedPreviousVersion, Operation, NextVersion);
				}
			}

			Plan.m_Statements.f_InsertLast(co_await fg_PostgresSyncAdditiveSchemaStatements(NextVersion, &PlannedPreviousVersion));
			Plan.m_Statements.f_InsertLast(fg_PostgresInsertSchemaVersionSql());
		}

		co_return fg_Move(Plan);
	}
}
