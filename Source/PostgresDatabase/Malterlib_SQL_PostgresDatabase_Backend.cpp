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

	// Parameter types for a prepared statement, taken from the values the encoder sends: the server can infer another width,
	// for example int4 for a parameter compared with LENGTH(...), whose ORM type is int64
	// A NULL value carries no type, so a statement prepared from it would get a type the server infers
	bool fg_PostgresValuesHaveNull(NContainer::TCVector<CPostgresValue> const &_Values)
	{
		for (auto const &Value : _Values)
		{
			if (Value.f_GetTypeID() == EPostgresValueType::mc_Null)
				return true;
		}

		return false;
	}

	NContainer::TCVector<EPostgresValueType> fg_PostgresParameterValueTypes(NContainer::TCVector<CPostgresValue> const &_Values)
	{
		NContainer::TCVector<EPostgresValueType> Types;
		for (auto const &Value : _Values)
			Types.f_InsertLast(Value.f_GetTypeID());

		return Types;
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

	// IF NOT EXISTS also skips the index when another relation already holds its name, such as the index PostgreSQL names
	// after a PRIMARY KEY or UNIQUE constraint, which would leave the declared index missing without an error
	NConcurrency::TCFuture<void> fg_PostgresEnsureIndex(NConcurrency::TCActor<CPostgresClientActor> _Client, CSqlTableDescription _Table, CSqlIndexDescription _Index)
	{
		using namespace NStr;

		co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresCreateIndex(_Table, _Index, true));

		NContainer::TCVector<CPostgresValue> Values;
		Values.f_InsertLast(_Index.f_Name());
		auto Result = co_await _Client
			(
				&CPostgresClientActor::f_ExecuteWithParameters
				, NStr::CStr
				(
					"SELECT tbl.relname::text, idx.indisunique, EXISTS (SELECT 1 FROM pg_constraint con WHERE con.contype IN ('p', 'u', 'x') AND con.conindid = idx.indexrelid), "
					"array_to_string(ARRAY(SELECT octet_length(att.attname)::text || ':' || att.attname::text FROM unnest(idx.indkey::int2[]) WITH ORDINALITY AS key(attnum, ord) "
					"JOIN pg_attribute att ON att.attrelid = idx.indrelid AND att.attnum = key.attnum ORDER BY key.ord), ','), "
					"idx.indpred IS NULL AND idx.indexprs IS NULL, "
					"idx.indisvalid AND idx.indisready, "
					"ARRAY(SELECT unnest(idx.indcollation::oid[])) = ARRAY(SELECT att.attcollation FROM unnest(idx.indkey::int2[]) WITH ORDINALITY AS key(attnum, ord) "
					"JOIN pg_attribute att ON att.attrelid = idx.indrelid AND att.attnum = key.attnum ORDER BY key.ord), "
					"NOT COALESCE((to_jsonb(idx)->>'indnullsnotdistinct')::boolean, false) "
					"FROM pg_index idx JOIN pg_class ic ON ic.oid = idx.indexrelid JOIN pg_class tbl ON tbl.oid = idx.indrelid "
					"WHERE ic.relname = $1 AND ic.relnamespace = current_schema()::regnamespace"
				)
				, fg_Move(Values)
			)
		;

		// Length-prefixed, so a column name containing the separator cannot match another column list
		NStr::CStr Columns;
		for (auto const *pColumn : _Index.m_Columns)
		{
			if (!Columns.f_IsEmpty())
				Columns += ",";
			Columns += "{}:{}"_f << pColumn->f_GetLen() << *pColumn;
		}

		if
		(
			Result.m_Rows.f_GetLen() != 1
			|| Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() != _Table.f_Name()
			|| Result.m_Rows[0].m_Values[2].f_GetAsType<bool>()
		)
		{
			co_return DMibErrorDatabaseInstance("PostgreSQL index name '{}' of table '{}' is already used by another index"_f << _Index.f_Name() << _Table.f_Name());
		}

		if
		(
			Result.m_Rows[0].m_Values[1].f_GetAsType<bool>() == _Index.f_IsUnique()
			&& Result.m_Rows[0].m_Values[3].f_GetAsType<NStr::CStr>() == Columns
			&& Result.m_Rows[0].m_Values[4].f_GetAsType<bool>()
			&& Result.m_Rows[0].m_Values[5].f_GetAsType<bool>()
			&& Result.m_Rows[0].m_Values[6].f_GetAsType<bool>()
			&& Result.m_Rows[0].m_Values[7].f_GetAsType<bool>()
		)
		{
			co_return {};
		}

		// A later schema version changed the definition of this table's index, its key collations differ from the columns'
		// ones the declared index takes, it treats NULLs as equal (PostgreSQL 15 and later, which to_jsonb reads without
		// requiring the column), or a failed concurrent build left it invalid
		NStr::CStr DropSql;
		{
			NStr::CStr::CAppender Appender(DropSql);
			Appender += "DROP INDEX ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Index.f_Name());
		}
		co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(DropSql));
		co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresCreateIndex(_Table, _Index, false));

		co_return {};
	}

	// PostgreSQL silently truncates identifiers longer than 63 bytes, after which catalog lookups by the full name miss them
	NException::CExceptionPointer fg_PostgresValidateSchemaVersions(NContainer::TCVector<CSqlSchemaVersionDescription> const &_Versions)
	{
		using namespace NStr;

		constexpr umint c_MaxIdentifierBytes = 63;
		NStr::CStr const *pTooLong = nullptr;
		auto fCheck = [&](NStr::CStr const &_Identifier)
			{
				if (!pTooLong && umint(_Identifier.f_GetLen()) > c_MaxIdentifierBytes)
					pTooLong = &_Identifier;
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

			for (auto const &Table : Version.m_Database.m_Tables)
			{
				fCheck(Table.f_Name());
				for (auto const &Column : Table.m_Columns)
				{
					fCheck(Column.f_Name());
					if (Column.f_IsGeneratedOnAnyBackend() && !fg_PostgresColumnIsGenerated(Column))
					{
						return DMibErrorDatabaseInstance
							(
								"SQL column '{}.{}' is generated on another backend but has no PostgreSQL generated expression"_f << Table.f_Name() << Column.f_Name()
							)
						;
					}
				}
				for (auto const &Index : Table.m_Indexes)
					fCheck(Index.f_Name());
				for (umint iConstraint = 0; iConstraint < Table.m_Constraints.f_GetLen(); ++iConstraint)
				{
					auto const &Constraint = Table.m_Constraints[iConstraint];
					fCheck(Constraint.f_Name());
					for (umint iOther = 0; iOther < iConstraint; ++iOther)
					{
						if (Table.m_Constraints[iOther].f_Name() == Constraint.f_Name())
						{
							return DMibErrorDatabaseInstance
								(
									"SQL table '{}' declares constraint '{}' more than once"_f << Table.f_Name() << Constraint.f_Name()
								)
							;
						}
					}
				}
			}
		}

		if (pTooLong)
			return DMibErrorDatabaseInstance("PostgreSQL identifier '{}' is longer than {} bytes"_f << *pTooLong << c_MaxIdentifierBytes);

		return {};
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

	NStr::CStr fg_PostgresDropConstraintIfExistsSql(CSqlTableDescription const &_Table, CSqlConstraintDescription const &_Constraint)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "ALTER TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " DROP CONSTRAINT IF EXISTS ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Constraint.f_Name());
		}

		return Sql;
	}

	// A CHECK lists no columns, so any of them may have been dropped with the column and none stays tracked
	void fg_PostgresRemoveConstraintsForColumn(CSqlTableDescription &_Table, NStr::CStr const &_ColumnName)
	{
		for (umint iConstraint = _Table.m_Constraints.f_GetLen(); iConstraint-- > 0; )
		{
			bool bReferencesColumn = _Table.m_Constraints[iConstraint].m_Type == ESqlConstraintType::mc_Check;

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

	// Text the planned schema points at that no declaration holds, kept for as long as the planned schema
	struct CPostgresPlannedSchemaStorage
	{
		NContainer::TCVector<NStorage::TCUniquePointer<NStr::CStr>> m_Strings;
		NContainer::TCVector<NStorage::TCUniquePointer<NContainer::TCVector<CNonPortableColumnOptions>>> m_Options;
	};

	void fg_PostgresApplyMigrationOperationToPlannedSchema
		(
			CSqlSchemaVersionDescription &_Schema
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CPostgresPlannedSchemaStorage &o_Storage
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

				// PostgreSQL also rewrites the generated expressions that use the column
				for (auto &Column : pTable->m_Columns)
				{
					CNonPortableColumnOptions const *pOptions = Column.f_NonPortableOptions(NStr::gc_Str<"postgres">.m_Str);
					if (!pOptions || pOptions->m_pGeneratedSql->f_IsEmpty())
						continue;

					NStr::CStr Renamed = NPrivate::fg_SqlTextRenameIdentifier(*pOptions->m_pGeneratedSql, *_Operation.m_pOldName, *_Operation.m_pNewName);
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

				// PostgreSQL rewrites the CHECK expressions that use the column, which the declared text does not follow
				for (umint iConstraint = pTable->m_Constraints.f_GetLen(); iConstraint-- > 0; )
				{
					if (pTable->m_Constraints[iConstraint].m_Type != ESqlConstraintType::mc_Check)
						continue;

					pTable->m_Constraints.f_Remove(iConstraint);
				}
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

			// The rebuild also drops the foreign keys of other tables that reference it, and leaves the declared ones to
			// the additive pass, which adds them with their target definitions after every operation
			for (auto &Table : _Schema.m_Database.m_Tables)
			{
				for (umint iConstraint = Table.m_Constraints.f_GetLen(); iConstraint-- > 0; )
				{
					auto const &Constraint = Table.m_Constraints[iConstraint];
					if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey && Constraint.f_ReferencedTable() == *_Operation.m_pTableName)
						Table.m_Constraints.f_Remove(iConstraint);
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

	NStr::CStr fg_PostgresResetSerialSequenceSql(NStr::CStr const &_TableName, NStr::CStr const &_ColumnName, NStr::CStr const *_pPreviousTableName = nullptr);
	void fg_PostgresAppendStringLiteral(NStr::CStr::CAppender &_Appender, NStr::CStr const &_Value);

	// PostgreSQL truncates identifiers longer than 63 bytes, which would make the scratch table unreachable under the
	// name used to look up its columns, so a long table name is replaced by a hash of it
	NStr::CStr fg_PostgresRebuildScratchTableName(NStr::CStr const &_TableName)
	{
		NStr::CStr const Prefix = "__mib_rebuild_old_";
		if (Prefix.f_GetLen() + _TableName.f_GetLen() <= 63)
			return Prefix + _TableName;

		uint64 Hash = 14695981039346656037ull;
		for (aint i = 0; i < _TableName.f_GetLen(); ++i)
			Hash = (Hash ^ uint64(uint8(_TableName.f_GetAt(i)))) * 1099511628211ull;

		return NStr::CStr::CFormat("{}{nfh,sj16,sf0}") << Prefix << Hash;
	}

	// PRIMARY KEY and UNIQUE constraints own an index named after the constraint, and index names are unique per schema.
	// The renamed table keeps those index names, so a recreated constraint with the same explicit name would collide.
	// The constraints are read from the catalog, since a column-level constraint carries a name the schema does not declare.
	NStr::CStr fg_PostgresRenameRebuildConstraintsSql(NStr::CStr const &_OldTableName)
	{
		// The body quotes the table name, which must not contain the dollar-quote tag that ends the body
		NStr::CStr Tag = "$mib_rebuild$";
		for (umint iTag = 0; _OldTableName.f_Find(Tag) >= 0; ++iTag)
			Tag = NStr::CStr::CFormat("$mib_rebuild_{}$") << iTag;

		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "DO ";
			Appender += Tag;
			Appender += " DECLARE Constraint_ record; iConstraint integer := 0; BEGIN "
				"FOR Constraint_ IN SELECT conname::text AS name FROM pg_constraint WHERE contype IN ('p', 'u', 'x') AND conrelid = quote_ident("
			;
			fg_PostgresAppendStringLiteral(Appender, _OldTableName);
			Appender += ")::regclass ORDER BY oid LOOP EXECUTE format('ALTER TABLE %I RENAME CONSTRAINT %I TO %I', ";
			fg_PostgresAppendStringLiteral(Appender, _OldTableName);
			Appender += ", Constraint_.name, '__mib_rebuild_old_constraint_' || iConstraint); iConstraint := iConstraint + 1; END LOOP; END ";
			Appender += Tag;
		}

		return Sql;
	}

	NStr::CStr fg_PostgresRebuildCopyRowsSql(CSqlTableDescription const &_Table, NStr::CStr const &_OldTableName, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "INSERT INTO ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
			if (_ExistingColumns.f_IsEmpty())
			{
				// Without shared columns the rows are still kept, every column taking its default
				Appender += " SELECT FROM ";
				fg_PostgresAppendQuotedIdentifier(Appender, _OldTableName);
			}
			else
			{
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
				fg_PostgresAppendQuotedIdentifier(Appender, _OldTableName);
			}
		}

		return Sql;
	}

	// Constraint names are per table, so the child table identifies the foreign key along with its name
	auto fg_PostgresSchemaDeclaresForeignKey
		(
			CSqlSchemaVersionDescription const &_Schema
			, NStr::CStr const &_ChildTable
			, NStr::CStr const &_ConstraintName
			, NStr::CStr const &_ReferencedTable
		)
		-> bool
	{
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			if (Table.f_Name() != _ChildTable)
				continue;

			for (auto const &Constraint : Table.m_Constraints)
			{
				if
				(
					Constraint.m_Type == ESqlConstraintType::mc_ForeignKey
					&& Constraint.f_Name() == _ConstraintName
					&& Constraint.f_ReferencedTable() == _ReferencedTable
				)
				{
					return true;
				}
			}
		}

		return false;
	}

	bool fg_PostgresRebuildColumnIsShared(NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns, NStr::CStr const &_ColumnName)
	{
		for (auto const *pColumnName : _ExistingColumns)
		{
			if (*pColumnName == _ColumnName)
				return true;
		}

		return false;
	}

	NContainer::TCVector<NStr::CStr> fg_PostgresRebuildTableStatements
		(
			CSqlTableDescription const &_Table
			, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns
			, CSqlSchemaVersionDescription const *_pPreviousSchema
			, CSqlSchemaVersionDescription const &_TargetSchema
			, NContainer::TCVector<CPostgresPlannedForeignKey> &o_ForeignKeys
		)
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		NStr::CStr OldTableName = fg_PostgresRebuildScratchTableName(_Table.f_Name());

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

		Statements.f_InsertLast(fg_PostgresRenameRebuildConstraintsSql(OldTableName));

		// Mirror the executor: create the rebuilt table without its foreign keys and let the additive pass add them,
		// so the planned statements match the order the backend actually applies.
		Statements.f_InsertLast(fg_PostgresCreateTable(_Table, false, false));
		Statements.f_InsertLast(fg_PostgresRebuildCopyRowsSql(_Table, OldTableName, _ExistingColumns));

		// The copy inserted existing serial ids explicitly without advancing each owned sequence, so realign them
		// before the next default insert would otherwise reuse a copied id, mirroring the executor.
		for (auto const &Column : _Table.m_Columns)
		{
			if (!fg_IsSet(Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
				continue;

			bool bShared = fg_PostgresRebuildColumnIsShared(_ExistingColumns, Column.f_Name());
			Statements.f_InsertLast(fg_PostgresResetSerialSequenceSql(_Table.f_Name(), Column.f_Name(), bShared ? &OldTableName : nullptr));
		}

		Statements.f_InsertLast(fg_PostgresDropTable(OldTableName));

		// A child foreign key can depend on a unique index rather than a constraint, so the indexes come back first
		for (auto const &Index : _Table.m_Indexes)
			Statements.f_InsertLast(fg_PostgresCreateIndex(_Table, Index, true));

		// Mirroring the executor, the additive pass adds the child foreign keys the target declares, and the others are
		// recreated from their previous definitions once the migration's operations ran
		for (auto const &ChildForeignKey : ChildForeignKeys)
		{
			if (!fg_PostgresSchemaDeclaresForeignKey(_TargetSchema, ChildForeignKey.m_pTable->f_Name(), ChildForeignKey.m_pConstraint->f_Name(), _Table.f_Name()))
				o_ForeignKeys.f_InsertLast({.m_Table = *ChildForeignKey.m_pTable, .m_Constraint = *ChildForeignKey.m_pConstraint});
		}

		return Statements;
	}

	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_PostgresMigrationOperationStatements
		(
			CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
			, NContainer::TCVector<CPostgresPlannedForeignKey> &o_ForeignKeys
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
				Statements = fg_PostgresRebuildTableStatements(*pTable, fg_PostgresCommonColumns(*pTable, pPreviousTable), _pPreviousSchema, _TargetSchema, o_ForeignKeys);

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

	// The definition of an index that a later schema version changed while keeping its name
	bool fg_PostgresIndexDefinitionChanged(CSqlTableDescription const *_pPreviousTable, CSqlIndexDescription const &_Index)
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

	// PostgreSQL does not rebuild a table on its own, so a changed column or constraint needs a migration that rebuilds the table
	NException::CExceptionPointer fg_PostgresCheckExistingDefinitionsUnchanged(CSqlTableDescription const &_Table, CSqlTableDescription const *_pPreviousTable)
	{
		using namespace NStr;

		if (!_pPreviousTable)
			return {};

		for (auto const &Column : _Table.m_Columns)
		{
			for (auto const &PreviousColumn : _pPreviousTable->m_Columns)
			{
				if (PreviousColumn.f_Name() == Column.f_Name() && fg_PostgresColumnDefinitionSql(PreviousColumn) != fg_PostgresColumnDefinitionSql(Column))
				{
					return DMibErrorDatabaseInstance
						(
							"PostgreSQL column '{}.{}' changes its definition, which needs a migration that rebuilds the table"_f
							<< _Table.f_Name()
							<< Column.f_Name()
						)
					;
				}
			}
		}

		for (auto const &Constraint : _Table.m_Constraints)
		{
			for (auto const &PreviousConstraint : _pPreviousTable->m_Constraints)
			{
				if
				(
					PreviousConstraint.f_Name() == Constraint.f_Name()
					&& fg_PostgresConstraintDefinitionSql(PreviousConstraint) != fg_PostgresConstraintDefinitionSql(Constraint)
				)
				{
					return DMibErrorDatabaseInstance
						(
							"PostgreSQL constraint '{}' of table '{}' changes its definition, which needs a migration that rebuilds the table"_f
							<< Constraint.f_Name()
							<< _Table.f_Name()
						)
					;
				}
			}
		}

		return {};
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

			if (auto pException = fg_PostgresCheckExistingDefinitionsUnchanged(Table, pPreviousTable))
				return fg_Move(pException);

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
					if (!Column.f_IsNullable() && fg_PostgresColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey() && !fg_PostgresColumnIsGenerated(Column))
						return DMibErrorDatabaseInstance("Cannot add required PostgreSQL column '{}.{}' without a default value"_f << Table.f_Name() << Column.f_Name());

					Statements.f_InsertLast(fg_PostgresAlterTableAddColumn(Table, Column));
				}
			}

			for (auto const &Index : Table.m_Indexes)
			{
				if (fg_PostgresIndexDefinitionChanged(pPreviousTable, Index))
				{
					NStr::CStr DropSql;
					{
						NStr::CStr::CAppender Appender(DropSql);
						// DROP COLUMN can already have dropped the index
						Appender += "DROP INDEX IF EXISTS ";
						fg_PostgresAppendQuotedIdentifier(Appender, Index.f_Name());
					}
					Statements.f_InsertLast(fg_Move(DropSql));
					Statements.f_InsertLast(fg_PostgresCreateIndex(Table, Index, false));
				}
				else
					Statements.f_InsertLast(fg_PostgresCreateIndex(Table, Index, true));
			}

			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey || fg_PostgresTableHasConstraint(pPreviousTable, Constraint.f_Name()))
					continue;

				// An untracked CHECK can exist on the server with another definition, for example one that survived a
				// column drop or was rewritten by a column rename, so the declared one replaces it
				if (Constraint.m_Type == ESqlConstraintType::mc_Check)
					Statements.f_InsertLast(fg_PostgresDropConstraintIfExistsSql(Table, Constraint));

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

	NStr::CStr fg_PostgresResetSerialSequenceSql(NStr::CStr const &_TableName, NStr::CStr const &_ColumnName, NStr::CStr const *_pPreviousTableName)
	{
		// Set the owned sequence so the next default value is one past the highest ID: the current maximum, or the
		// previous table's sequence, which also covers IDs that were handed out and since deleted. An unused sequence
		// is set to 1 with is_called=false so nextval() returns 1. Otherwise is_called stays true, keeping the value
		// readable through pg_sequence_last_value for a later rebuild.
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "SELECT setval(pg_get_serial_sequence(quote_ident(";
			fg_PostgresAppendStringLiteral(Appender, _TableName);
			Appender += "), ";
			fg_PostgresAppendStringLiteral(Appender, _ColumnName);
			Appender += "), GREATEST(1, \"malterlib_last_id\"), \"malterlib_last_id\" > 0) FROM (SELECT GREATEST((SELECT COALESCE(MAX(";
			fg_PostgresAppendQuotedIdentifier(Appender, _ColumnName);
			Appender += "), 0) FROM ";
			fg_PostgresAppendQuotedIdentifier(Appender, _TableName);
			Appender += ")";
			if (_pPreviousTableName)
			{
				Appender += ", COALESCE(pg_sequence_last_value(pg_get_serial_sequence(quote_ident(";
				fg_PostgresAppendStringLiteral(Appender, *_pPreviousTableName);
				Appender += "), ";
				fg_PostgresAppendStringLiteral(Appender, _ColumnName);
				Appender += ")::regclass), 0)";
			}
			Appender += ") AS \"malterlib_last_id\") AS \"malterlib_sequence\"";
		}

		return Sql;
	}

	struct CPostgresChildForeignKey
	{
		NStr::CStr m_ChildSchema; // The child can live in another schema than the search path resolves
		NStr::CStr m_ChildTable;
		NStr::CStr m_Name;
		NStr::CStr m_Definition;
		NStr::CStr m_ParentTable;
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
				"SELECT nsp.nspname::text, rel.relname::text, con.conname::text, pg_get_constraintdef(con.oid) FROM pg_constraint con "
				"JOIN pg_class rel ON rel.oid = con.conrelid JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace "
				"WHERE con.contype = 'f' AND con.conrelid != con.confrelid AND con.confrelid = quote_ident("
			;
			fg_PostgresAppendStringLiteral(Appender, _TableName);
			Appender += ")::regclass";
		}

		return Sql;
	}

	struct CPostgresSequenceGrant
	{
		NStr::CStr m_Privilege;
		NStr::CStr m_Grantee; // Quoted role or PUBLIC, followed by any WITH GRANT OPTION
	};

	constexpr ch8 const *gc_PostgresGranteeSql = "CASE WHEN Grant_.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(Grant_.grantee)) END";
	constexpr ch8 const *gc_PostgresGrantOptionSql = "CASE WHEN Grant_.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END";

	// Runs each statement in the first column of the query's rows
	NConcurrency::TCFuture<void> fg_PostgresExecuteGeneratedStatements(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql)
	{
		auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(_Sql));
		for (auto const &Row : Result.m_Rows)
			co_await _Client(&CPostgresClientActor::f_Execute, Row.m_Values[0].f_GetAsType<NStr::CStr>());

		co_return {};
	}

	// Returns the foreign keys of other tables that referenced this one, for fg_PostgresRestoreChildForeignKeys to recreate
	auto fg_PostgresRebuildTable(NConcurrency::TCActor<CPostgresClientActor> _Client, CSqlTableDescription _Table)
		-> NConcurrency::TCFuture<NContainer::TCVector<CPostgresChildForeignKey>>
	{
		if (!co_await fg_PostgresHasTable(_Client, _Table.f_Name()))
			co_return NContainer::TCVector<CPostgresChildForeignKey>();

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
				ChildForeignKey.m_ChildSchema = Row.m_Values[0].f_GetAsType<NStr::CStr>();
				ChildForeignKey.m_ChildTable = Row.m_Values[1].f_GetAsType<NStr::CStr>();
				ChildForeignKey.m_Name = Row.m_Values[2].f_GetAsType<NStr::CStr>();
				ChildForeignKey.m_Definition = Row.m_Values[3].f_GetAsType<NStr::CStr>();
				ChildForeignKey.m_ParentTable = _Table.f_Name();
				ChildForeignKeys.f_InsertLast(fg_Move(ChildForeignKey));
			}
		}

		for (auto const &ChildForeignKey : ChildForeignKeys)
		{
			NStr::CStr DropSql;
			{
				NStr::CStr::CAppender Appender(DropSql);
				Appender += "ALTER TABLE ";
				fg_PostgresAppendQuotedIdentifier(Appender, ChildForeignKey.m_ChildSchema);
				Appender += ".";
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
		NStr::CStr OldTableName = fg_PostgresRebuildScratchTableName(_Table.f_Name());

		// Row-level security, its policies and publication membership belong to the old table and cannot be carried over
		{
			NStr::CStr SecuritySql;
			{
				NStr::CStr::CAppender Appender(SecuritySql);
				Appender += "SELECT (Table_.relrowsecurity OR Table_.relforcerowsecurity OR EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = Table_.oid) "
					"OR EXISTS (SELECT 1 FROM pg_publication_tables WHERE schemaname = current_schema() AND tablename = Table_.relname))::text "
					"FROM pg_class AS Table_ WHERE Table_.oid = quote_ident("
				;
				fg_PostgresAppendStringLiteral(Appender, _Table.f_Name());
				Appender += ")::regclass";
			}

			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(SecuritySql));
			if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() == "true")
			{
				co_return DMibErrorDatabaseInstance
					(
						NStr::CStr::CFormat("Cannot rebuild PostgreSQL table '{}' that uses row-level security or is published") << _Table.f_Name()
					)
				;
			}
		}

		// Indexes, triggers and privileges are dropped with the old table, so they are captured while they still name this
		// table. The schema's own indexes and constraints are created again from their declarations.
		NContainer::TCVector<NStr::CStr> RestoreStatements;
		{
			NStr::CStr IndexesSql;
			{
				NStr::CStr::CAppender Appender(IndexesSql);
				Appender += "SELECT pg_get_indexdef(Index_.indexrelid)::text, IndexClass_.relname::text, "
					"ARRAY(SELECT attname::text FROM pg_attribute WHERE attrelid = Index_.indrelid AND attnum > 0 AND (attnum = ANY(Index_.indkey::int2[]) "
					"OR attnum IN (SELECT refobjsubid FROM pg_depend WHERE classid = 'pg_class'::regclass AND objid = Index_.indexrelid "
					"AND refclassid = 'pg_class'::regclass AND refobjid = Index_.indrelid))) "
					"FROM pg_index AS Index_ "
					"JOIN pg_class AS IndexClass_ ON IndexClass_.oid = Index_.indexrelid "
					"WHERE NOT EXISTS (SELECT 1 FROM pg_constraint WHERE contype IN ('p', 'u', 'x') AND conindid = Index_.indexrelid) "
					"AND Index_.indrelid = quote_ident("
				;
				fg_PostgresAppendStringLiteral(Appender, _Table.f_Name());
				Appender += ")::regclass ORDER BY Index_.indexrelid";
			}

			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(IndexesSql));
			for (auto const &Row : Result.m_Rows)
			{
				bool bSkip = false;
				for (auto const &Index : _Table.m_Indexes)
				{
					if (Index.f_Name() == Row.m_Values[1].f_GetAsType<NStr::CStr>())
						bSkip = true;
				}

				// An index on a column the target drops goes with the column, also when only its expression or WHERE uses it
				for (auto const &IndexColumn : Row.m_Values[2].f_GetAsType<TCPostgresArray<NStr::CStr>>().m_Values)
				{
					bool bFound = false;
					for (auto const &Column : _Table.m_Columns)
					{
						if (IndexColumn && Column.f_Name() == *IndexColumn)
							bFound = true;
					}

					bSkip = bSkip || !bFound;
				}

				if (!bSkip)
					RestoreStatements.f_InsertLast(Row.m_Values[0].f_GetAsType<NStr::CStr>());
			}
		}
		{
			NStr::CStr TriggersSql;
			{
				NStr::CStr::CAppender Appender(TriggersSql);
				Appender += "SELECT pg_get_triggerdef(oid)::text, tgname::text, tgenabled::text FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = quote_ident(";
				fg_PostgresAppendStringLiteral(Appender, _Table.f_Name());
				Appender += ")::regclass ORDER BY oid";
			}

			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(TriggersSql));
			for (auto const &Row : Result.m_Rows)
			{
				RestoreStatements.f_InsertLast(Row.m_Values[0].f_GetAsType<NStr::CStr>());

				// pg_get_triggerdef does not include the firing mode
				NStr::CStr Enabled = Row.m_Values[2].f_GetAsType<NStr::CStr>();
				ch8 const *pMode = nullptr;
				if (Enabled == "D")
					pMode = " DISABLE TRIGGER ";
				else if (Enabled == "R")
					pMode = " ENABLE REPLICA TRIGGER ";
				else if (Enabled == "A")
					pMode = " ENABLE ALWAYS TRIGGER ";

				if (pMode)
				{
					NStr::CStr ModeSql;
					{
						NStr::CStr::CAppender Appender(ModeSql);
						Appender += "ALTER TABLE ";
						fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
						Appender += pMode;
						fg_PostgresAppendQuotedIdentifier(Appender, Row.m_Values[1].f_GetAsType<NStr::CStr>());
					}
					RestoreStatements.f_InsertLast(fg_Move(ModeSql));
				}
			}
		}
		// The replacement gets the migration role's owner and default privileges, so the original owner and privileges are
		// captured and restored exactly. Column privileges and those of serial sequences live on objects of their own.
		NStr::CStr TableLiteral;
		{
			NStr::CStr::CAppender Appender(TableLiteral);
			Appender += "quote_ident(";
			fg_PostgresAppendStringLiteral(Appender, _Table.f_Name());
			Appender += ")";
		}

		NContainer::TCVector<NStr::CStr> PrivilegeStatements;
		{
			auto Result = co_await _Client
				(
					&CPostgresClientActor::f_Execute
					, NStr::CStr::CFormat
					(
						"SELECT format('GRANT %s ON TABLE %I TO %s%s', Grant_.privilege_type, Table_.relname, {}, {}) FROM pg_class AS Table_ "
						"CROSS JOIN LATERAL aclexplode(COALESCE(Table_.relacl, acldefault('r', Table_.relowner))) AS Grant_ WHERE Table_.oid = {}::regclass"
					)
					<< gc_PostgresGranteeSql
					<< gc_PostgresGrantOptionSql
					<< TableLiteral
				)
			;
			for (auto const &Row : Result.m_Rows)
				PrivilegeStatements.f_InsertLast(Row.m_Values[0].f_GetAsType<NStr::CStr>());
		}
		{
			auto Result = co_await _Client
				(
					&CPostgresClientActor::f_Execute
					, NStr::CStr::CFormat
					(
						"SELECT Column_.attname::text, format('GRANT %s (%I) ON TABLE %I TO %s%s', Grant_.privilege_type, Column_.attname, Table_.relname, {}, {}) "
						"FROM pg_class AS Table_ JOIN pg_attribute AS Column_ ON Column_.attrelid = Table_.oid AND Column_.attnum > 0 AND NOT Column_.attisdropped "
						"CROSS JOIN LATERAL aclexplode(Column_.attacl) AS Grant_ WHERE Table_.oid = {}::regclass"
					)
					<< gc_PostgresGranteeSql
					<< gc_PostgresGrantOptionSql
					<< TableLiteral
				)
			;
			for (auto const &Row : Result.m_Rows)
			{
				NStr::CStr ColumnName = Row.m_Values[0].f_GetAsType<NStr::CStr>();
				for (auto const &Column : _Table.m_Columns)
				{
					if (Column.f_Name() == ColumnName)
						PrivilegeStatements.f_InsertLast(Row.m_Values[1].f_GetAsType<NStr::CStr>());
				}
			}
		}

		NContainer::TCMap<NStr::CStr, NContainer::TCVector<CPostgresSequenceGrant>> SequenceGrants; // By column name
		for (auto const &Column : _Table.m_Columns)
		{
			if (!fg_IsSet(Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
				continue;

			NStr::CStr SequenceSql;
			{
				NStr::CStr::CAppender Appender(SequenceSql);
				Appender += NStr::CStr::CFormat
					(
						"SELECT Grant_.privilege_type::text, format('%s%s', {}, {}) FROM pg_attribute AS Column_ "
						"JOIN pg_class AS Sequence_ ON Sequence_.oid = pg_get_serial_sequence({}, Column_.attname)::regclass "
						"LEFT JOIN LATERAL aclexplode(COALESCE(Sequence_.relacl, acldefault('S', Sequence_.relowner))) AS Grant_ ON true "
						"WHERE Column_.attrelid = {}::regclass AND NOT Column_.attisdropped AND Column_.attname = "
					)
					<< gc_PostgresGranteeSql
					<< gc_PostgresGrantOptionSql
					<< TableLiteral
					<< TableLiteral
				;
				fg_PostgresAppendStringLiteral(Appender, Column.f_Name());
			}

			// A column without a sequence before the rebuild keeps the new sequence's default privileges. A sequence
			// with an empty ACL returns one row without a privilege.
			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(SequenceSql));
			if (Result.m_Rows.f_IsEmpty())
				continue;

			auto &Grants = SequenceGrants[Column.f_Name()];
			for (auto const &Row : Result.m_Rows)
			{
				if (Row.m_Values[0].f_GetTypeID() == EPostgresValueType::mc_Null)
					continue;

				Grants.f_InsertLast({.m_Privilege = Row.m_Values[0].f_GetAsType<NStr::CStr>(), .m_Grantee = Row.m_Values[1].f_GetAsType<NStr::CStr>()});
			}
		}

		NStr::CStr OwnerStatement;
		{
			auto Result = co_await _Client
				(
					&CPostgresClientActor::f_Execute
					, NStr::CStr::CFormat
					(
						"SELECT format('ALTER TABLE %I OWNER TO %I', relname, pg_get_userbyid(relowner)) FROM pg_class "
						"WHERE oid = {}::regclass AND pg_get_userbyid(relowner) <> current_user"
					)
					<< TableLiteral
				)
			;
			if (Result.m_Rows.f_GetLen() == 1)
				OwnerStatement = Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>();
		}

		NStr::CStr RenameSql;
		{
			NStr::CStr::CAppender Appender(RenameSql);
			Appender += "ALTER TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " RENAME TO ";
			fg_PostgresAppendQuotedIdentifier(Appender, OldTableName);
		}

		co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(RenameSql));

		co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresRenameRebuildConstraintsSql(OldTableName));

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

		co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresRebuildCopyRowsSql(_Table, OldTableName, ExistingColumns));

		// The copy inserted the existing id values explicitly, which does not advance the BIGSERIAL column's new
		// owned sequence. Without this reset the first later default insert would reuse an already-copied id and
		// fail with a duplicate key, so realign each serial sequence with the copied data.
		for (auto const &Column : _Table.m_Columns)
		{
			if (!fg_IsSet(Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
				continue;

			bool bShared = fg_PostgresRebuildColumnIsShared(ExistingColumns, Column.f_Name());
			co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresResetSerialSequenceSql(_Table.f_Name(), Column.f_Name(), bShared ? &OldTableName : nullptr));
		}

		co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresDropTable(OldTableName));

		// A child foreign key can depend on a unique index rather than a constraint, so the indexes come back first
		for (auto const &Index : _Table.m_Indexes)
			co_await fg_PostgresEnsureIndex(_Client, _Table, Index);

		for (auto const &RestoreStatement : RestoreStatements)
			co_await _Client(&CPostgresClientActor::f_Execute, RestoreStatement);

		// Before the privileges, since changing the owner turns the current owner's ACL entries into the new owner's,
		// which would merge restored grants to the migration role into the owner's
		if (!OwnerStatement.f_IsEmpty())
			co_await _Client(&CPostgresClientActor::f_Execute, OwnerStatement);

		co_await fg_PostgresExecuteGeneratedStatements
			(
				_Client
				, NStr::CStr::CFormat
				(
					"SELECT DISTINCT format('REVOKE ALL ON TABLE %I FROM %s', Table_.relname, {}) FROM pg_class AS Table_ "
					"CROSS JOIN LATERAL aclexplode(COALESCE(Table_.relacl, acldefault('r', Table_.relowner))) AS Grant_ WHERE Table_.oid = {}::regclass"
				)
				<< gc_PostgresGranteeSql
				<< TableLiteral
			)
		;
		for (auto const &PrivilegeStatement : PrivilegeStatements)
			co_await _Client(&CPostgresClientActor::f_Execute, PrivilegeStatement);

		for (auto iSequenceGrants = SequenceGrants.f_GetIterator(); iSequenceGrants; ++iSequenceGrants)
		{
			NStr::CStr SequenceNameSql;
			{
				NStr::CStr::CAppender Appender(SequenceNameSql);
				Appender += NStr::CStr::CFormat("SELECT pg_get_serial_sequence({}, ") << TableLiteral;
				fg_PostgresAppendStringLiteral(Appender, iSequenceGrants.f_GetKey());
				Appender += ")";
			}

			auto SequenceName = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(SequenceNameSql));
			if (SequenceName.m_Rows.f_GetLen() != 1 || SequenceName.m_Rows[0].m_Values[0].f_GetTypeID() == EPostgresValueType::mc_Null)
				continue;

			NStr::CStr Sequence = SequenceName.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>();
			NStr::CStr SequenceLiteral;
			{
				NStr::CStr::CAppender Appender(SequenceLiteral);
				fg_PostgresAppendStringLiteral(Appender, Sequence);
			}

			co_await fg_PostgresExecuteGeneratedStatements
				(
					_Client
					, NStr::CStr::CFormat
					(
						"SELECT DISTINCT format('REVOKE ALL ON SEQUENCE %s FROM %s', Sequence_.oid::regclass, {}) FROM pg_class AS Sequence_ "
						"CROSS JOIN LATERAL aclexplode(COALESCE(Sequence_.relacl, acldefault('S', Sequence_.relowner))) AS Grant_ WHERE Sequence_.oid = {}::regclass"
					)
					<< gc_PostgresGranteeSql
					<< SequenceLiteral
				)
			;

			for (auto const &Grant : *iSequenceGrants)
				co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr::CFormat("GRANT {} ON SEQUENCE {} TO {}") << Grant.m_Privilege << Sequence << Grant.m_Grantee);
		}

		co_return fg_Move(ChildForeignKeys);
	}

	// Recreates the foreign keys a rebuild dropped from other tables from their captured definitions. The ones the target
	// schema declares in this schema are left to the additive pass, which adds them with their
	// target definitions and follows renames, as the planned schema records them as dropped. The recreate validates the
	// existing child rows against the copied parent data. A child table dropped since is skipped.
	NConcurrency::TCFuture<void> fg_PostgresRestoreChildForeignKeys
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, NContainer::TCVector<CPostgresChildForeignKey> _ChildForeignKeys
			, NStorage::TCOptional<CSqlSchemaVersionDescription> _TargetSchema
		)
	{
		NStr::CStr CurrentSchema;
		if (_TargetSchema && !_ChildForeignKeys.f_IsEmpty())
		{
			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SELECT current_schema()::text"));
			if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values[0].f_GetTypeID() != EPostgresValueType::mc_Null)
				CurrentSchema = Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>();
		}

		for (auto const &ChildForeignKey : _ChildForeignKeys)
		{
			if
			(
				_TargetSchema
				&& ChildForeignKey.m_ChildSchema == CurrentSchema
				&& fg_PostgresSchemaDeclaresForeignKey(*_TargetSchema, ChildForeignKey.m_ChildTable, ChildForeignKey.m_Name, ChildForeignKey.m_ParentTable)
			)
			{
				continue;
			}

			NContainer::TCVector<CPostgresValue> Values;
			Values.f_InsertLast(ChildForeignKey.m_ChildSchema);
			Values.f_InsertLast(ChildForeignKey.m_ChildTable);
			if (!co_await fg_PostgresHasRows(_Client, NStr::CStr("SELECT 1 WHERE to_regclass(quote_ident($1) || '.' || quote_ident($2)) IS NOT NULL"), fg_Move(Values)))
				continue;

			NStr::CStr AddSql;
			{
				NStr::CStr::CAppender Appender(AddSql);
				Appender += "ALTER TABLE ";
				fg_PostgresAppendQuotedIdentifier(Appender, ChildForeignKey.m_ChildSchema);
				Appender += ".";
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

	// A foreign key a rebuild earlier in the migration dropped from the table follows a later rename of its column, and
	// goes with a later drop of it as PostgreSQL drops such a foreign key. An empty _NewName means the column was dropped.
	auto fg_PostgresUpdateDeferredForeignKeys
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, NStorage::TCSharedPointer<NContainer::TCVector<CPostgresChildForeignKey>> _pChildForeignKeys
			, NStr::CStr _TableName
			, NStr::CStr _OldName
			, NStr::CStr _NewName
		)
		-> NConcurrency::TCFuture<void>
	{
		if (_pChildForeignKeys->f_IsEmpty())
			co_return {};

		auto Result = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SELECT current_schema()::text"));
		NStr::CStr CurrentSchema = Result.m_Rows.f_GetLen() == 1 ? Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() : NStr::CStr();

		for (umint iForeignKey = _pChildForeignKeys->f_GetLen(); iForeignKey-- > 0; )
		{
			auto &ChildForeignKey = (*_pChildForeignKeys)[iForeignKey];
			if (ChildForeignKey.m_ChildSchema != CurrentSchema || ChildForeignKey.m_ChildTable != _TableName)
				continue;

			// pg_get_constraintdef writes FOREIGN KEY (columns) REFERENCES ..., where only the part before REFERENCES
			// names this table's columns
			aint iReferences = ChildForeignKey.m_Definition.f_Find(" REFERENCES ");
			if (iReferences < 0)
				continue;

			NStr::CStr Columns = ChildForeignKey.m_Definition.f_Left(umint(iReferences));
			NStr::CStr References = ChildForeignKey.m_Definition.f_Extract(iReferences);
			if (_NewName.f_IsEmpty())
			{
				if (NPrivate::fg_SqlTextMentionsPostgresColumn(Columns, _OldName))
					_pChildForeignKeys->f_Remove(iForeignKey);
			}
			else
				ChildForeignKey.m_Definition = NPrivate::fg_SqlTextRenameIdentifier(Columns, _OldName, _NewName) + References;
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_PostgresApplyMigrationOperation
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, CSqlSchemaMigrationOperationDescription _Operation
			, CSqlSchemaVersionDescription _TargetSchema
			, NStorage::TCSharedPointer<NContainer::TCVector<CPostgresChildForeignKey>> _pChildForeignKeys
		)
	{
		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable:
			if (co_await fg_PostgresHasTable(_Client, *_Operation.m_pOldName))
			{
				if (co_await fg_PostgresHasTable(_Client, *_Operation.m_pNewName))
				{
					co_return DMibErrorDatabaseInstance
						(
							NStr::CStr::CFormat("Cannot rename PostgreSQL table '{}' to '{}': both tables exist")
							<< *_Operation.m_pOldName
							<< *_Operation.m_pNewName
						)
					;
				}

				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresRenameTable(_Operation));

				// Foreign keys a rebuild earlier in the migration dropped from this table are restored under its new name
				if (!_pChildForeignKeys->f_IsEmpty())
				{
					auto Result = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SELECT current_schema()::text"));
					NStr::CStr CurrentSchema = Result.m_Rows.f_GetLen() == 1 ? Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() : NStr::CStr();
					for (auto &ChildForeignKey : *_pChildForeignKeys)
					{
						if (ChildForeignKey.m_ChildSchema == CurrentSchema && ChildForeignKey.m_ChildTable == *_Operation.m_pOldName)
							ChildForeignKey.m_ChildTable = *_Operation.m_pNewName;
					}
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn:
			if (co_await fg_PostgresHasColumn(_Client, *_Operation.m_pTableName, *_Operation.m_pOldName))
			{
				if (co_await fg_PostgresHasColumn(_Client, *_Operation.m_pTableName, *_Operation.m_pNewName))
				{
					co_return DMibErrorDatabaseInstance
						(
							NStr::CStr::CFormat("Cannot rename PostgreSQL column '{}.{}' to '{}': both columns exist")
							<< *_Operation.m_pTableName
							<< *_Operation.m_pOldName
							<< *_Operation.m_pNewName
						)
					;
				}

				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresRenameColumn(_Operation));
				co_await fg_PostgresUpdateDeferredForeignKeys(_Client, _pChildForeignKeys, *_Operation.m_pTableName, *_Operation.m_pOldName, *_Operation.m_pNewName);
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			{
				auto pTable = fg_PostgresFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					co_return DMibErrorDatabaseInstance(NStr::CStr::CFormat("Cannot rebuild PostgreSQL table that is not present in target schema: {}") << *_Operation.m_pTableName);

				// The rebuild creates this table with the target's definition, so an earlier rebuild's captured foreign key on
				// it would bring back one the target no longer declares
				if (!_pChildForeignKeys->f_IsEmpty())
				{
					auto Result = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SELECT current_schema()::text"));
					NStr::CStr CurrentSchema = Result.m_Rows.f_GetLen() == 1 ? Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() : NStr::CStr();
					for (umint iForeignKey = _pChildForeignKeys->f_GetLen(); iForeignKey-- > 0; )
					{
						auto const &ChildForeignKey = (*_pChildForeignKeys)[iForeignKey];
						if (ChildForeignKey.m_ChildSchema == CurrentSchema && ChildForeignKey.m_ChildTable == pTable->f_Name())
							_pChildForeignKeys->f_Remove(iForeignKey);
					}
				}

				for (auto &ChildForeignKey : co_await fg_PostgresRebuildTable(_Client, *pTable))
					_pChildForeignKeys->f_InsertLast(fg_Move(ChildForeignKey));

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_DropTable:
			if (co_await fg_PostgresHasTable(_Client, *_Operation.m_pTableName))
				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresDropTable(*_Operation.m_pTableName));

			break;
		case ESqlSchemaMigrationOperationType::mc_DropColumn:
			if (co_await fg_PostgresHasColumn(_Client, *_Operation.m_pTableName, *_Operation.m_pOldName))
			{
				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresDropColumn(_Operation));
				co_await fg_PostgresUpdateDeferredForeignKeys(_Client, _pChildForeignKeys, *_Operation.m_pTableName, *_Operation.m_pOldName, NStr::CStr());
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql:
			if (co_await fg_PostgresHasTable(_Client, *_Operation.m_pTableName))
			{
				// Columns a version adds appear only after its operations ran, so a backfill of one would silently do nothing
				if (!co_await fg_PostgresHasColumn(_Client, *_Operation.m_pTableName, *_Operation.m_pNewName))
				{
					co_return DMibErrorDatabaseInstance
						(
							NStr::CStr::CFormat("Cannot update column '{}.{}' before it exists; add the column in an earlier schema version")
							<< *_Operation.m_pTableName
							<< *_Operation.m_pNewName
						)
					;
				}

				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresUpdateColumnSql(_Operation));
			}

			break;
		}

		co_return {};
	}

	// A rebuild creates the target table before the migration's later operations run, so a column that a later operation
	// renames does not exist yet under its old name and its data would not be copied
	NException::CExceptionPointer fg_PostgresValidateRenameAfterRebuild(CSqlSchemaMigrationDescription const &_Migration)
	{
		using namespace NStr;

		for (umint iRebuild = 0; iRebuild < _Migration.m_Operations.f_GetLen(); ++iRebuild)
		{
			auto const &Rebuild = _Migration.m_Operations[iRebuild];
			if (Rebuild.m_Type != ESqlSchemaMigrationOperationType::mc_RebuildTable)
				continue;

			for (umint iRename = iRebuild + 1; iRename < _Migration.m_Operations.f_GetLen(); ++iRename)
			{
				auto const &Rename = _Migration.m_Operations[iRename];
				if (Rename.m_Type == ESqlSchemaMigrationOperationType::mc_RenameColumn && *Rename.m_pTableName == *Rebuild.m_pTableName)
				{
					return DMibErrorDatabaseInstance
						(
							"Migration renames column '{}.{}' after rebuilding its table; order the rename first"_f
							<< *Rename.m_pTableName
							<< *Rename.m_pOldName
						)
					;
				}
			}
		}

		return {};
	}

	NConcurrency::TCFuture<void> fg_PostgresApplyMigrationOperations
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, CSqlSchemaMigrationDescription _Migration
			, CSqlSchemaVersionDescription _TargetSchema
		)
	{
		if (auto pException = fg_PostgresValidateRenameAfterRebuild(_Migration))
			co_return fg_Move(pException);

		// Another rebuild in the migration can change the keys these reference, so they come after every operation
		NStorage::TCSharedPointer<NContainer::TCVector<CPostgresChildForeignKey>> pChildForeignKeys = fg_Construct();
		for (auto const &Operation : _Migration.m_Operations)
			co_await fg_PostgresApplyMigrationOperation(_Client, Operation, _TargetSchema, pChildForeignKeys);

		co_await fg_PostgresRestoreChildForeignKeys(_Client, fg_Move(*pChildForeignKeys), _TargetSchema);

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

	// A rebuild copies only declared columns, so a live column the schema does not declare would be dropped with its data
	NConcurrency::TCFuture<void> fg_PostgresCheckNoUndeclaredColumns(NConcurrency::TCActor<CPostgresClientActor> _Client, CSqlTableDescription _Table)
	{
		using namespace NStr;

		NContainer::TCVector<CPostgresValue> Values;
		Values.f_InsertLast(_Table.f_Name());
		auto Result = co_await _Client
			(
				&CPostgresClientActor::f_ExecuteWithParameters
				, NStr::CStr("SELECT column_name::text FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1")
				, fg_Move(Values)
			)
		;

		for (auto const &Row : Result.m_Rows)
		{
			NStr::CStr LiveColumn = Row.m_Values[0].f_GetAsType<NStr::CStr>();
			bool bDeclared = false;
			for (auto const &Column : _Table.m_Columns)
			{
				if (Column.f_Name() == LiveColumn)
				{
					bDeclared = true;
					break;
				}
			}

			if (!bDeclared)
			{
				co_return DMibErrorDatabaseInstance
					(
						"PostgreSQL table '{}' has column '{}' that the schema does not declare; rebuilding the table would drop it"_f
						<< _Table.f_Name()
						<< LiveColumn
					)
				;
			}
		}

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
					co_await fg_PostgresEnsureIndex(_Client, Table, Index);

				continue;
			}

			// Adopting a table that exists without a recorded definition, also one that a later version first declares:
			// it may lack declared column-level constraints (PRIMARY KEY/UNIQUE/NOT NULL) that cannot be compared against
			// the live table, so rebuild it to the declared definition. Rows that violate it fail the apply. Foreign keys
			// follow in pass 2.
			if (!pPreviousTable)
			{
				co_await fg_PostgresCheckNoUndeclaredColumns(_Client, Table);
				auto ChildForeignKeys = co_await fg_PostgresRebuildTable(_Client, Table);
				co_await fg_PostgresRestoreChildForeignKeys(_Client, fg_Move(ChildForeignKeys), _Schema);
				continue;
			}

			if (auto pException = fg_PostgresCheckExistingDefinitionsUnchanged(Table, pPreviousTable))
				co_return fg_Move(pException);

			for (auto const &Column : Table.m_Columns)
			{
				if (!co_await fg_PostgresHasColumn(_Client, Table.f_Name(), Column.f_Name()))
				{
					if (!Column.f_IsNullable() && fg_PostgresColumnDefaultSql(Column).f_IsEmpty() && !Column.f_IsPrimaryKey() && !fg_PostgresColumnIsGenerated(Column))
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
				co_await fg_PostgresEnsureIndex(_Client, Table, Index);

			// Add the declared non-foreign-key constraints new to this version. The previous version's constraints are
			// already in place, and an adopted table was rebuilt above.
			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey)
					continue;

				if (fg_PostgresTableHasConstraint(pPreviousTable, Constraint.f_Name()))
					continue;

				// An untracked CHECK can exist on the server with another definition, for example one that survived a
				// column drop or was rewritten by a column rename, so the declared one replaces it
				if (Constraint.m_Type == ESqlConstraintType::mc_Check)
					co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresDropConstraintIfExistsSql(Table, Constraint));

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

		if (auto pException = fg_PostgresValidateSchemaVersions(m_SchemaVersions))
			co_return fg_Move(pException);
		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		auto Client = ConnectionCheckout.m_Client;

		// A snapshot taken before the advisory lock is granted would miss the migrations a concurrent applier commits, so
		// the reads after the lock need read committed whatever the server default is
		co_await Client(&CPostgresClientActor::f_BeginTransaction, false, CSqlTransactionSettings{.m_Isolation = ESqlTransactionIsolation::mc_ReadCommitted});

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

		// Concurrent appliers would otherwise read the same applied versions and run the same migration twice. The lock is
		// held until this transaction ends and is scoped to the current database.
		co_await Client(&CPostgresClientActor::f_Execute, NStr::CStr("SELECT pg_advisory_xact_lock(hashtext('malterlib_schema_migrations'))"));
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
					CPostgresPlannedSchemaStorage PlannedSchemaStorage;

					if (CSqlSchemaMigrationDescription const *pMigration = fg_PostgresFindMigration(m_SchemaMigrations, PreviousSchema.f_ID(), NextSchema.f_ID()))
					{
						co_await fg_PostgresApplyMigrationOperations(Client, *pMigration, NextSchema);

						for (auto const &Operation : pMigration->m_Operations)
							fg_PostgresApplyMigrationOperationToPlannedSchema(PlannedPreviousSchema, Operation, NextSchema, PlannedSchemaStorage);
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
			bool bPrepared = ConnectionCheckout.m_pPreparedUpdateCache->f_Find(&Description) != nullptr;
			if (!bPrepared && !fg_PostgresValuesHaveNull(Values))
			{
				co_await ConnectionCheckout.m_Client
					(
						&CPostgresClientActor::f_PrepareStatement
						, StatementEntry.m_Name
						, StatementEntry.m_Sql
						, fg_PostgresParameterValueTypes(Values)
					)
				;
				ConnectionCheckout.m_pPreparedUpdateCache->f_Insert(&Description, StatementEntry);
				bPrepared = true;
			}

			if (bPrepared)
				nAffected = co_await fg_PostgresExecutePreparedAffected(ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
			else
				nAffected = co_await fg_PostgresExecuteAffected(ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));
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
			bool bPrepared = ConnectionCheckout.m_pPreparedDeleteCache->f_Find(&Description) != nullptr;
			if (!bPrepared && !fg_PostgresValuesHaveNull(Values))
			{
				co_await ConnectionCheckout.m_Client
					(
						&CPostgresClientActor::f_PrepareStatement
						, StatementEntry.m_Name
						, StatementEntry.m_Sql
						, fg_PostgresParameterValueTypes(Values)
					)
				;
				ConnectionCheckout.m_pPreparedDeleteCache->f_Insert(&Description, StatementEntry);
				bPrepared = true;
			}

			if (bPrepared)
				nAffected = co_await fg_PostgresExecutePreparedAffected(ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
			else
				nAffected = co_await fg_PostgresExecuteAffected(ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));
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
		auto StatementEntry = _pBackend->fp_EnsurePreparedSelect(&Description);
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
		auto StatementEntry = _pBackend->fp_EnsurePreparedSelect(&Description);
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
		auto StatementEntry = _pBackend->fp_EnsurePreparedSelect(&Description);
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
		// By pointer, since the row mapping is this statement's own, as in fp_GetPreparedSelect
		if (CPostgresPreparedSelectCacheEntry const *pEntry = m_pPreparedSelectCache->f_FindByPointer(_pDescription))
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

	// The pooled connections keep the ORM's named prepared statements, so raw SQL must not DEALLOCATE or DISCARD them
	// Named prepared statements of the ORM live on the pooled connection, so a connection whose statements raw SQL dropped
	// (DEALLOCATE, DISCARD ALL) is reconnected instead of reused, as is one raw SQL left inside a transaction
	// Raw SQL can leave session state behind, such as a search_path or a temporary table that redirects the ORM's
	// unqualified names. This is DISCARD ALL without DEALLOCATE ALL, which would drop the cached prepared statements.
	NConcurrency::TCFuture<bool> fg_PostgresResetSession(NConcurrency::TCActor<CPostgresClientActor> _Client)
	{
		for
		(
			ch8 const *pSql :
			{
				"CLOSE ALL", "SET SESSION AUTHORIZATION DEFAULT", "RESET ALL", "UNLISTEN *", "SELECT pg_advisory_unlock_all()", "DISCARD PLANS"
				, "DISCARD TEMP", "DISCARD SEQUENCES"
			}
		)
		{
			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr(pSql)).f_Wrap();
			if (!Result)
			{
				NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to reset session after raw SQL")(fg_Move(Result));
				co_return false;
			}
		}

		co_return true;
	}

	NConcurrency::TCFuture<bool> fg_PostgresRawConnectionReusable(NConcurrency::TCActor<CPostgresClientActor> _Client, uint64 _nStatementDropsBefore)
	{
		if (co_await _Client(&CPostgresClientActor::f_IsInTransaction))
			co_return false;

		if (co_await _Client(&CPostgresClientActor::f_GetStatementDropCount) != _nStatementDropsBefore)
			co_return false;

		co_return co_await fg_PostgresResetSession(_Client);
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
		uint64 nStatementDropsBefore = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_GetStatementDropCount);
		auto Result = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values));
		umint nAffected = 0;
		if (NException::CExceptionPointer pException = fg_PostgresParseAffectedRows(Result, nAffected))
			co_return pException;

		// A connection that is not marked reusable is discarded and reconnected by fp_ReleaseConnection
		if (co_await fg_PostgresRawConnectionReusable(ConnectionCheckout.m_Client, nStatementDropsBefore))
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
		uint64 nStatementDropsBefore = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_GetStatementDropCount);
		auto QueryResult = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values));

		if (co_await fg_PostgresRawConnectionReusable(ConnectionCheckout.m_Client, nStatementDropsBefore))
			ConnectionCheckout.f_MarkReusable();

		co_return fg_PostgresRawResult(fg_Move(QueryResult));
	}

	static NConcurrency::TCAsyncGenerator<CSqlRawRowBatch> fg_PostgresRawRowStreamFromIterator
		(
			NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>::CPipelinedIterator _iItems
			, CPostgresConnectionCheckout _ConnectionCheckout
			, uint64 _nStatementDropsBefore
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

		if (co_await fg_PostgresRawConnectionReusable(_ConnectionCheckout.m_Client, _nStatementDropsBefore))
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

		uint64 nStatementDropsBefore = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_GetStatementDropCount);
		auto Items = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteRowsStream, fg_Move(Sql), fg_Move(Values), nRowsPerBatch);
		auto iItems = co_await fg_Move(Items).f_GetPipelinedIterator(m_Settings.m_nPipelineLength);
		if (!iItems)
		{
			// The stream produced no items, which means it drained to ReadyForQuery (for example an UPDATE or DDL, or
			// transaction-control SQL such as BEGIN, routed through the stream API)
			if (co_await fg_PostgresRawConnectionReusable(ConnectionCheckout.m_Client, nStatementDropsBefore))
				ConnectionCheckout.f_MarkReusable();

			co_return DMibErrorDatabaseInstance("PostgreSQL raw stream completed without producing a row description");
		}

		CPostgresRowStreamItem FirstItem = fg_Move(*iItems);
		if (!FirstItem.m_Description)
			co_return DMibErrorDatabaseInstance("PostgreSQL raw stream did not produce a row description before rows");

		co_await ++iItems;

		CSqlRawStream Stream;
		Stream.m_Columns = fg_PostgresRawColumns(*FirstItem.m_Description);
		Stream.m_Rows = fg_PostgresRawRowStreamFromIterator(fg_Move(iItems), fg_Move(ConnectionCheckout), nStatementDropsBefore);

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

		if (auto pException = fg_PostgresValidateSchemaVersions(Versions))
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
			CPostgresPlannedSchemaStorage PlannedSchemaStorage;
			if (auto const *pMigration = fg_PostgresFindMigration(Migrations, PreviousVersion.f_ID(), NextVersion.f_ID()))
			{
				if (auto pException = fg_PostgresValidateRenameAfterRebuild(*pMigration))
					co_return fg_Move(pException);

				// Another rebuild in the migration can change the keys these reference, so they come after every operation
				NContainer::TCVector<CPostgresPlannedForeignKey> ForeignKeys;
				for (auto const &Operation : pMigration->m_Operations)
				{
					Plan.m_Statements.f_InsertLast(co_await fg_PostgresMigrationOperationStatements(Operation, NextVersion, &PlannedPreviousVersion, ForeignKeys));
					fg_PostgresApplyMigrationOperationToPlannedSchema(PlannedPreviousVersion, Operation, NextVersion, PlannedSchemaStorage);

					if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_RenameTable)
					{
						for (auto &ForeignKey : ForeignKeys)
						{
							if (ForeignKey.m_Table.f_Name() == *Operation.m_pOldName)
								ForeignKey.m_Table.m_pName = Operation.m_pNewName;
						}
					}
					else if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_RebuildTable)
					{
						// The rebuilt table gets the target's definition, as in the executor
						for (umint iForeignKey = ForeignKeys.f_GetLen(); iForeignKey-- > 0; )
						{
							if (ForeignKeys[iForeignKey].m_Table.f_Name() == *Operation.m_pTableName)
								ForeignKeys.f_Remove(iForeignKey);
						}
					}
				}

				for (auto const &ForeignKey : ForeignKeys)
					Plan.m_Statements.f_InsertLast(fg_PostgresAlterTableAddConstraint(ForeignKey.m_Table, ForeignKey.m_Constraint));
			}

			Plan.m_Statements.f_InsertLast(co_await fg_PostgresSyncAdditiveSchemaStatements(NextVersion, &PlannedPreviousVersion));
			Plan.m_Statements.f_InsertLast(fg_PostgresInsertSchemaVersionSql());
		}

		co_return fg_Move(Plan);
	}
}
