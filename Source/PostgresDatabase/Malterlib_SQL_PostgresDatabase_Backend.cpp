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
			// An existing table keeps its own column types, which CREATE TABLE IF NOT EXISTS does not change
			if (Row.m_Values[0].f_GetTypeID() != EPostgresValueType::mc_Text || Row.m_Values[1].f_GetTypeID() != EPostgresValueType::mc_Text)
				co_return DMibErrorDatabaseInstance("PostgreSQL table schema_migrations has an id or checksum that is not text");

			CPostgresAppliedSchemaVersion Version;
			Version.m_ID = Row.m_Values[0].f_GetAsType<NStr::CStr>();
			Version.m_Checksum = Row.m_Values[1].f_GetAsType<NStr::CStr>();

			Versions.f_InsertLast(fg_Move(Version));
		}

		co_return Versions;
	}

	auto fg_PostgresValidateAppliedSchemaVersions
		(
			NContainer::TCVector<CPostgresAppliedSchemaVersion> const &_AppliedVersions
			, NContainer::TCVector<CSqlSchemaVersionDescription> const &_ExpectedVersions
		)
		-> NException::CExceptionPointer
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

		// A CommandComplete tag may have no row count, as for successful DDL: report zero when there is no trailing count. The count of
		// other commands, like the SELECT 1 of a query, is the number of rows they returned or moved, not of rows they changed.
		auto const &Tag = _Result.m_CommandComplete->m_Tag;
		if (!Tag.f_StartsWith("INSERT ") && !Tag.f_StartsWith("UPDATE ") && !Tag.f_StartsWith("DELETE ") && !Tag.f_StartsWith("MERGE "))
			return {};

		umint iCount = Tag.f_GetLen();
		while (iCount != 0 && Tag[iCount - 1] >= '0' && Tag[iCount - 1] <= '9')
			--iCount;

		if (iCount == Tag.f_GetLen() || iCount == 0 || Tag[iCount - 1] != ' ')
			return {};

		for (umint iChar = iCount; iChar < Tag.f_GetLen(); ++iChar)
			o_nRows = o_nRows * 10 + umint(Tag[iChar] - '0');

		return {};
	}

	// A row the statement returns isn't decoded, so a value that the client can't decode doesn't fail a statement that has already run
	NConcurrency::TCFuture<umint> fg_PostgresExecuteAffected(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values)
	{
		auto Result = co_await _Client(&CPostgresClientActor::f_ExecuteWithParametersSkipRows, fg_Move(_Sql), fg_Move(_Values));
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

	auto fg_PostgresExecuteReturningValue(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values, CSqlRowFieldMapping _Field)
		-> NConcurrency::TCFuture<CSqlValue>
	{
		auto Result = co_await _Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(_Sql), fg_Move(_Values));
		// The statement fails on several rows, and returns none when no row matched
		if (Result.m_Rows.f_IsEmpty())
			co_return DMibErrorSqlInstance("SQL mutation expected one row but matched no rows", fg_SqlErrorData(ESqlErrorCategory::mc_MissingRow));

		if (Result.m_Rows.f_GetLen() != 1 || Result.m_Rows[0].m_Values.f_GetLen() != 1)
			co_return DMibErrorDatabaseInstance("PostgreSQL returning statement did not return exactly one value");

		co_return co_await fg_PostgresSqlValue(fg_Move(Result.m_Rows[0].m_Values[0]), _Field);
	}

	NContainer::TCVector<EPostgresValueType> fg_PostgresParameterTypes(CSqlParameterTypesDescription _ParameterTypes)
	{
		NContainer::TCVector<EPostgresValueType> ParameterTypes;
		for (umint iType = 0; iType < _ParameterTypes.m_nTypes; ++iType)
			ParameterTypes.f_InsertLast(fg_PostgresValueType(_ParameterTypes.f_GetType(iType)));

		return ParameterTypes;
	}

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

	// Parameter types for a prepared statement, taken from the values the encoder sends: the server can infer another width,
	// for example int4 for a parameter compared with LENGTH(...), whose ORM type is int64
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
		// Read indnullsnotdistinct through to_jsonb so the query also works before PostgreSQL 15. The schema is compared by name:
		// current_schema() returns the name as stored, which a regnamespace cast would parse as an identifier and case fold
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
					"FROM pg_index idx JOIN pg_class ic ON ic.oid = idx.indexrelid JOIN pg_namespace ns ON ns.oid = ic.relnamespace JOIN pg_class tbl ON tbl.oid = idx.indrelid "
					"WHERE ic.relname = $1 AND ns.nspname = current_schema()"
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

		if (Result.m_Rows.f_GetLen() != 1 || Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() != _Table.f_Name() || Result.m_Rows[0].m_Values[2].f_GetAsType<bool>())
			co_return DMibErrorDatabaseInstance("PostgreSQL index name '{}' of table '{}' is already used by another index"_f << _Index.f_Name() << _Table.f_Name());

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

	// PostgreSQL truncates identifiers longer than 63 bytes, after which catalog lookups by the full name miss them
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

			// An empty ID means no applied version
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
							return DMibErrorDatabaseInstance("SQL table '{}' declares constraint '{}' more than once"_f << Table.f_Name() << Constraint.f_Name());
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
		for (umint iConstraint = _Table.m_Constraints.f_GetLen(); iConstraint-- > 0;)
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

	// The description of a CHECK has no columns, so any check may have been dropped with the column, and none stays tracked.
	void fg_PostgresRemoveConstraintsForColumn(CSqlTableDescription &_Table, NStr::CStr const &_ColumnName)
	{
		for (umint iConstraint = _Table.m_Constraints.f_GetLen(); iConstraint-- > 0;)
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

	// Owns rewritten strings and column options referenced by the planned schema
	struct CPostgresPlannedSchemaStorage
	{
		NContainer::TCVector<NStorage::TCUniquePointer<NStr::CStr>> m_Strings;
		NContainer::TCVector<NStorage::TCUniquePointer<NContainer::TCVector<CNonPortableColumnOptions>>> m_Options;
	};

	// A rebuild creates only the columns of _TargetTable, which would remove a column that a later operation of the migration drops before
	// the operations in between can use it, so such a column is kept until that drop. _Operation is one of _Migration's operations, and
	// _pPlannedTable describes the table just before _Operation.
	auto fg_PostgresRebuildTableKeepingLaterDrops
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
			if (Later.m_Type != ESqlSchemaMigrationOperationType::mc_DropColumn || *Later.m_pTableName != _TargetTable.f_Name())
				continue;

			bool bInTable = false;
			for (auto const &Column : Table.m_Columns)
				bInTable = bInTable || Column.f_Name() == *Later.m_pOldName;

			if (bInTable)
				continue;

			for (auto const &Column : _pPlannedTable->m_Columns)
			{
				if (Column.f_Name() == *Later.m_pOldName)
				{
					Table.m_Columns.f_InsertLast(Column);
					break;
				}
			}
		}

		return Table;
	}

	void fg_PostgresApplyMigrationOperationToPlannedSchema
		(
			CSqlSchemaVersionDescription &_Schema
			, CSqlSchemaMigrationDescription const &_Migration
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

				// PostgreSQL rewrites the CHECK expressions that use the column, which the declared text does not follow
				for (umint iConstraint = pTable->m_Constraints.f_GetLen(); iConstraint-- > 0;)
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

				// Mirror PostgreSQL dropping this table's constraints on the column, or the additive pass skips a target constraint that reuses a dropped name
				fg_PostgresRemoveConstraintsForColumn(*pTable, *_Operation.m_pOldName);
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			// Record the rebuilt shape without foreign keys: the additive pass adds them once all referenced tables exist
			if (CSqlTableDescription const *pTarget = fg_PostgresFindTable(_TargetSchema, *_Operation.m_pTableName))
			{
				if (CSqlTableDescription *pPlanned = fg_PostgresFindTable(_Schema, *_Operation.m_pTableName))
				{
					*pPlanned = fg_PostgresRebuildTableKeepingLaterDrops(_Migration, _Operation, *pTarget, pPlanned);
					fg_PostgresRemoveForeignKeyConstraints(*pPlanned);
				}
			}

			// The rebuild also drops the foreign keys of other tables that reference it, and leaves the declared ones to
			// the additive pass, which adds them with their target definitions after every operation
			for (auto &Table : _Schema.m_Database.m_Tables)
			{
				for (umint iConstraint = Table.m_Constraints.f_GetLen(); iConstraint-- > 0;)
				{
					auto const &Constraint = Table.m_Constraints[iConstraint];
					if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey && Constraint.f_ReferencedTable() == *_Operation.m_pTableName)
						Table.m_Constraints.f_Remove(iConstraint);
				}
			}

			break;
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql: break;
		}
	}

	// Returns umint(-1) when no migration goes from _FromVersionID to _ToVersionID
	umint fg_PostgresFindMigration(NContainer::TCVector<CSqlSchemaMigrationDescription> const &_Migrations, NStr::CStr const &_FromVersionID, NStr::CStr const &_ToVersionID)
	{
		for (auto const &Migration : _Migrations)
		{
			if (*Migration.m_pFromVersionID == _FromVersionID && *Migration.m_pToVersionID == _ToVersionID)
				return umint(&Migration - _Migrations.f_GetArray());
		}

		return umint(-1);
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

	// PostgreSQL truncates identifiers longer than 63 bytes, which would make the scratch table unreachable under the
	// name used to look up its columns, so a long table name is replaced by a hash of it
	NStr::CStr fg_PostgresScratchTableName(NStr::CStr const &_Prefix, NStr::CStr const &_TableName)
	{
		if (_Prefix.f_GetLen() + _TableName.f_GetLen() <= 63)
			return _Prefix + _TableName;

		uint64 Hash = 14695981039346656037ull;
		for (aint iChar = 0; iChar < _TableName.f_GetLen(); ++iChar)
			Hash = (Hash ^ uint64(uint8(_TableName.f_GetAt(iChar)))) * 1099511628211ull;

		return NStr::CStr::CFormat("{}{nfh,sj16,sf0}") << _Prefix << Hash;
	}

	NStr::CStr fg_PostgresRebuildScratchTableName(NStr::CStr const &_TableName)
	{
		return fg_PostgresScratchTableName("__mib_rebuild_old_", _TableName);
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
				for (umint iExistingColumn = 0; iExistingColumn < _ExistingColumns.f_GetLen(); ++iExistingColumn)
				{
					if (iExistingColumn != 0)
						Appender += ", ";

					fg_PostgresAppendQuotedIdentifier(Appender, *_ExistingColumns[iExistingColumn]);
				}
				Appender += ") SELECT ";
				for (umint iExistingColumn = 0; iExistingColumn < _ExistingColumns.f_GetLen(); ++iExistingColumn)
				{
					if (iExistingColumn != 0)
						Appender += ", ";

					fg_PostgresAppendQuotedIdentifier(Appender, *_ExistingColumns[iExistingColumn]);
				}
				Appender += " FROM ";
				fg_PostgresAppendQuotedIdentifier(Appender, _OldTableName);
			}
		}

		return Sql;
	}

	// Constraint names are unique per table, so a constraint the schema declares under the name of a foreign key on the child table
	// replaces that key, also when it references another table or is no foreign key
	bool fg_PostgresSchemaDeclaresConstraint(CSqlSchemaVersionDescription const &_Schema, NStr::CStr const &_ChildTable, NStr::CStr const &_ConstraintName)
	{
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			if (Table.f_Name() != _ChildTable)
				continue;

			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.f_Name() == _ConstraintName)
					return true;
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

	auto fg_PostgresRebuildTableStatements
		(
			CSqlTableDescription const &_Table
			, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns
			, CSqlSchemaVersionDescription const *_pPreviousSchema
			, NContainer::TCVector<CPostgresPlannedForeignKey> &o_ForeignKeys
		)
		-> NContainer::TCVector<NStr::CStr>
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		NStr::CStr OldTableName = fg_PostgresRebuildScratchTableName(_Table.f_Name());

		// Foreign keys of other tables follow the renamed copy and block its drop, so they are dropped and recreated against
		// the rebuilt table. Self-references are left to the additive pass.
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

		// Without foreign keys, since the tables they reference may not exist before the additive pass
		Statements.f_InsertLast(fg_PostgresCreateTable(_Table, false, false));
		Statements.f_InsertLast(fg_PostgresRebuildCopyRowsSql(_Table, OldTableName, _ExistingColumns));

		// Explicit ids do not advance owned sequences, so a later default insert could reuse a copied id
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

		for (auto const &ChildForeignKey : ChildForeignKeys)
			o_ForeignKeys.f_InsertLast({.m_Table = *ChildForeignKey.m_pTable, .m_Constraint = *ChildForeignKey.m_pConstraint});

		return Statements;
	}

	auto fg_PostgresMigrationOperationStatements
		(
			CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
			, NContainer::TCVector<CPostgresPlannedForeignKey> &o_ForeignKeys
		)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;
		switch (_Operation.m_Type)
		{
		case ESqlSchemaMigrationOperationType::mc_RenameTable: Statements.f_InsertLast(fg_PostgresRenameTable(_Operation)); break;
		case ESqlSchemaMigrationOperationType::mc_RenameColumn: Statements.f_InsertLast(fg_PostgresRenameColumn(_Operation)); break;
		case ESqlSchemaMigrationOperationType::mc_RebuildTable:
			{
				auto pTable = fg_PostgresFindTable(_TargetSchema, *_Operation.m_pTableName);
				if (!pTable)
					return DMibErrorDatabaseInstance("Cannot rebuild PostgreSQL table that is not present in target schema: {}"_f << *_Operation.m_pTableName);

				CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_PostgresFindTable(*_pPreviousSchema, *_Operation.m_pTableName) : nullptr;
				CSqlTableDescription Table = fg_PostgresRebuildTableKeepingLaterDrops(_Migration, _Operation, *pTable, pPreviousTable);
				Statements = fg_PostgresRebuildTableStatements(Table, fg_PostgresCommonColumns(Table, pPreviousTable), _pPreviousSchema, o_ForeignKeys);

				break;
			}
		case ESqlSchemaMigrationOperationType::mc_DropTable: Statements.f_InsertLast(fg_PostgresDropTable(*_Operation.m_pTableName)); break;
		case ESqlSchemaMigrationOperationType::mc_DropColumn: Statements.f_InsertLast(fg_PostgresDropColumn(_Operation)); break;
		case ESqlSchemaMigrationOperationType::mc_UpdateColumnSql: Statements.f_InsertLast(fg_PostgresUpdateColumnSql(_Operation)); break;
		}

		return Statements;
	}

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

			for (umint iColumn = 0; iColumn < _Index.m_Columns.f_GetLen(); ++iColumn)
			{
				if (*PreviousIndex.m_Columns[iColumn] != *_Index.m_Columns[iColumn])
					return true;
			}

			return false;
		}

		return false;
	}

	// Additive sync does not alter a column or constraint that is still tracked after the migration operations, as a rebuild or a drop removes it
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
							"PostgreSQL column '{}.{}' changes its definition, which needs a migration that rebuilds the table"_f << _Table.f_Name() << Column.f_Name()
						)
					;
				}
			}
		}

		for (auto const &Constraint : _Table.m_Constraints)
		{
			for (auto const &PreviousConstraint : _pPreviousTable->m_Constraints)
			{
				if (PreviousConstraint.f_Name() == Constraint.f_Name() && fg_PostgresConstraintDefinitionSql(PreviousConstraint) != fg_PostgresConstraintDefinitionSql(Constraint))
				{
					return DMibErrorDatabaseInstance
						(
							"PostgreSQL constraint '{}' of table '{}' changes its definition, which needs a migration that rebuilds the table"_f << Constraint.f_Name() << _Table.f_Name()
						)
					;
				}
			}
		}

		return {};
	}

	auto fg_PostgresSyncAdditiveSchemaStatements(CSqlSchemaVersionDescription const &_Schema, CSqlSchemaVersionDescription const *_pPreviousSchema)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	{
		using namespace NStr;

		NContainer::TCVector<NStr::CStr> Statements;

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

		// Add foreign keys only after all tables exist, so declaration order and reference cycles are supported
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
		for (aint iChar = 0; iChar < _Value.f_GetLen(); ++iChar)
		{
			auto Character = _Value.f_GetAt(iChar);
			_Appender += Character;
			if (Character == '\'')
				_Appender += "'";
		}
		_Appender += "'";
	}

	NStr::CStr fg_PostgresResetSerialSequenceSql(NStr::CStr const &_TableName, NStr::CStr const &_ColumnName, NStr::CStr const *_pPreviousTableName)
	{
		// Continue past the greater of the copied maximum and the previous sequence's last value, which covers deleted IDs.
		// When neither is positive, is_called=false makes the first nextval() return 1; otherwise is_called stays true so a
		// later rebuild can read the value through pg_sequence_last_value.
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
	};

	NStr::CStr fg_PostgresReferencingForeignKeysSql(NStr::CStr const &_TableName)
	{
		// Casts to ::text keep the name columns as text (OID 25) so the value decoder accepts them. The constraint definition,
		// with later column renames applied, recreates the foreign key; self-references are left to the additive pass. The copies
		// a partitioned child keeps on its partitions go and come back with its own key, and cannot be dropped on their own.
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender +=
				"SELECT nsp.nspname::text, rel.relname::text, con.conname::text, pg_get_constraintdef(con.oid) FROM pg_constraint con "
				"JOIN pg_class rel ON rel.oid = con.conrelid JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace "
				"WHERE con.contype = 'f' AND con.conparentid = 0 AND con.conrelid != con.confrelid AND con.confrelid = quote_ident("
			;
			fg_PostgresAppendStringLiteral(Appender, _TableName);
			Appender += ")::regclass";
		}

		return Sql;
	}

	struct CPostgresSequenceGrant
	{
		NStr::CStr m_Privilege;
		NStr::CStr m_GranteeSql; // Quoted role or PUBLIC, followed by any WITH GRANT OPTION
	};

	constexpr ch8 const *gc_pPostgresGranteeSql = "CASE WHEN Grant_.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(Grant_.grantee)) END";
	constexpr ch8 const *gc_pPostgresGrantOptionSql = "CASE WHEN Grant_.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END";

	// Runs each statement in the first column of the query's rows
	NConcurrency::TCFuture<void> fg_PostgresExecuteGeneratedStatements(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql)
	{
		auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(_Sql));
		for (auto const &Row : Result.m_Rows)
			co_await _Client(&CPostgresClientActor::f_Execute, Row.m_Values[0].f_GetAsType<NStr::CStr>());

		co_return {};
	}

	// Returns the dropped foreign keys of other tables that referenced this one, which the caller must recreate
	auto fg_PostgresRebuildTable(NConcurrency::TCActor<CPostgresClientActor> _Client, CSqlTableDescription _Table) -> NConcurrency::TCFuture<NContainer::TCVector<CPostgresChildForeignKey>>
	{
		if (!co_await fg_PostgresHasTable(_Client, _Table.f_Name()))
			co_return NContainer::TCVector<CPostgresChildForeignKey>();

		// Child foreign keys follow the renamed table object and would block its drop; their captured definitions name this table, so they bind to the new one
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

		// Transactional DDL cannot leave a scratch table after a rollback. Never pre-drop this name: it may belong to a user table, whose rename conflict aborts the migration
		NStr::CStr OldTableName = fg_PostgresRebuildScratchTableName(_Table.f_Name());

		// Reject row-level security, policies and publication membership: this rebuild does not restore them. The replacement is an ordinary
		// table, which would also leave out the rows of a partition or inheritance child from its parent, and lose the partitions of a
		// partitioned table.
		{
			NStr::CStr SecuritySql;
			{
				NStr::CStr::CAppender Appender(SecuritySql);
				Appender += "SELECT (Table_.relrowsecurity OR Table_.relforcerowsecurity OR EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = Table_.oid) "
					"OR EXISTS (SELECT 1 FROM pg_publication_tables WHERE schemaname = Namespace_.nspname AND tablename = Table_.relname))::text, "
					"(Table_.relkind = 'p' OR Table_.relispartition OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = Table_.oid OR inhparent = Table_.oid))::text "
					"FROM pg_class AS Table_ JOIN pg_namespace AS Namespace_ ON Namespace_.oid = Table_.relnamespace WHERE Table_.oid = quote_ident("
				;
				fg_PostgresAppendStringLiteral(Appender, _Table.f_Name());
				Appender += ")::regclass";
			}

			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(SecuritySql));
			if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() == "true")
				co_return DMibErrorDatabaseInstance(NStr::CStr::CFormat("Cannot rebuild PostgreSQL table '{}' that uses row-level security or is published") << _Table.f_Name());

			if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values[1].f_GetAsType<NStr::CStr>() == "true")
			{
				co_return DMibErrorDatabaseInstance
					(
						NStr::CStr::CFormat("Cannot rebuild PostgreSQL table '{}' that is partitioned, a partition, or in an inheritance hierarchy") << _Table.f_Name()
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
				// Schema versions only add indexes: an index that a previous version declared and the target leaves out stays, also when the
				// additive sync runs without a rebuild, so the rebuild keeps it like an index made outside the schema
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
		// The replacement gets the migration role's owner and default privileges, so the owner and the table, column and serial
		// sequence grants are captured and reissued, without their original grantors
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
					<< gc_pPostgresGranteeSql
					<< gc_pPostgresGrantOptionSql
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
					<< gc_pPostgresGranteeSql
					<< gc_pPostgresGrantOptionSql
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

		NContainer::TCMap<NStr::CStr, NContainer::TCVector<CPostgresSequenceGrant>> SequenceGrantsByColumn;
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
					<< gc_pPostgresGranteeSql
					<< gc_pPostgresGrantOptionSql
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

			auto &Grants = SequenceGrantsByColumn[Column.f_Name()];
			for (auto const &Row : Result.m_Rows)
			{
				if (Row.m_Values[0].f_GetTypeID() == EPostgresValueType::mc_Null)
					continue;

				Grants.f_InsertLast({.m_Privilege = Row.m_Values[0].f_GetAsType<NStr::CStr>(), .m_GranteeSql = Row.m_Values[1].f_GetAsType<NStr::CStr>()});
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

		// Defer this table's foreign keys: a referenced table may not exist until the additive pass
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

		// Copying explicit IDs does not advance the owned sequences; realign them so a default insert cannot reuse a copied ID
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
				<< gc_pPostgresGranteeSql
				<< TableLiteral
			)
		;
		for (auto const &PrivilegeStatement : PrivilegeStatements)
			co_await _Client(&CPostgresClientActor::f_Execute, PrivilegeStatement);

		for (auto iSequenceGrants = SequenceGrantsByColumn.f_GetIterator(); iSequenceGrants; ++iSequenceGrants)
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
					<< gc_pPostgresGranteeSql
					<< SequenceLiteral
				)
			;

			for (auto const &Grant : *iSequenceGrants)
				co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr::CFormat("GRANT {} ON SEQUENCE {} TO {}") << Grant.m_Privilege << Sequence << Grant.m_GranteeSql);
		}

		co_return fg_Move(ChildForeignKeys);
	}

	// Replays the captured definitions of the child foreign keys a rebuild dropped, skipping child tables dropped since. A key whose
	// name the target schema declares on the same child table is left to the additive pass, which adds the declared constraint.
	auto fg_PostgresRestoreChildForeignKeys
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, NContainer::TCVector<CPostgresChildForeignKey> _ChildForeignKeys
			, NStorage::TCOptional<CSqlSchemaVersionDescription> _TargetSchema
		)
		-> NConcurrency::TCFuture<void>
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
			if (_TargetSchema && ChildForeignKey.m_ChildSchema == CurrentSchema && fg_PostgresSchemaDeclaresConstraint(*_TargetSchema, ChildForeignKey.m_ChildTable, ChildForeignKey.m_Name))
				continue;

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

		for (umint iForeignKey = _pChildForeignKeys->f_GetLen(); iForeignKey-- > 0;)
		{
			auto &ChildForeignKey = (*_pChildForeignKeys)[iForeignKey];
			if (ChildForeignKey.m_ChildSchema != CurrentSchema || ChildForeignKey.m_ChildTable != _TableName)
				continue;

			// pg_get_constraintdef writes FOREIGN KEY (columns) REFERENCES ...; the column lists of ON DELETE actions are subsets of
			// that first list, so the first list alone tells whether a dropped column takes the key
			if (_NewName.f_IsEmpty())
			{
				aint iReferences = NPrivate::fg_PostgresForeignKeyFindReferences(ChildForeignKey.m_Definition);
				if (iReferences >= 0 && NPrivate::fg_SqlTextMentionsPostgresColumn(ChildForeignKey.m_Definition.f_Left(umint(iReferences)), _OldName))
					_pChildForeignKeys->f_Remove(iForeignKey);
			}
			else
				ChildForeignKey.m_Definition = NPrivate::fg_PostgresForeignKeyRenameColumn(ChildForeignKey.m_Definition, _OldName, _NewName);
		}

		co_return {};
	}

	// A rebuild copies only declared columns, so a live column the schema does not declare would be dropped with its data.
	// _RemovableColumns can go too: the columns of the previous version's table and those the migration drops.
	auto fg_PostgresCheckNoUndeclaredColumns(NConcurrency::TCActor<CPostgresClientActor> _Client, CSqlTableDescription _Table, NContainer::TCVector<NStr::CStr> _RemovableColumns)
		-> NConcurrency::TCFuture<void>
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

			for (auto const &RemovableColumn : _RemovableColumns)
				bDeclared = bDeclared || RemovableColumn == LiveColumn;

			if (!bDeclared)
			{
				co_return DMibErrorDatabaseInstance
					(
						"PostgreSQL table '{}' has column '{}' that the schema does not declare; rebuilding the table would drop it"_f << _Table.f_Name() << LiveColumn
					)
				;
			}
		}

		co_return {};
	}

	// _RebuildTable is the table a rebuild by _Operation creates, and _RemovableColumns are the columns it can drop besides those
	// the table declares
	auto fg_PostgresApplyMigrationOperation
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, CSqlSchemaMigrationOperationDescription _Operation
			, NStorage::TCSharedPointer<NContainer::TCVector<CPostgresChildForeignKey>> _pChildForeignKeys
			, NStorage::TCOptional<CSqlTableDescription> _RebuildTable
			, NContainer::TCVector<NStr::CStr> _RemovableColumns
		)
		-> NConcurrency::TCFuture<void>
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
							NStr::CStr::CFormat("Cannot rename PostgreSQL table '{}' to '{}': both tables exist") << *_Operation.m_pOldName << *_Operation.m_pNewName
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
				if (!_RebuildTable)
					co_return DMibErrorDatabaseInstance(NStr::CStr::CFormat("Cannot rebuild PostgreSQL table that is not present in target schema: {}") << *_Operation.m_pTableName);

				CSqlTableDescription const *pTable = &*_RebuildTable;

				// The rebuild creates this table with the target's definition, so an earlier rebuild's captured foreign key on
				// it would bring back one the target no longer declares
				if (!_pChildForeignKeys->f_IsEmpty())
				{
					auto Result = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SELECT current_schema()::text"));
					NStr::CStr CurrentSchema = Result.m_Rows.f_GetLen() == 1 ? Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() : NStr::CStr();
					for (umint iForeignKey = _pChildForeignKeys->f_GetLen(); iForeignKey-- > 0;)
					{
						auto const &ChildForeignKey = (*_pChildForeignKeys)[iForeignKey];
						if (ChildForeignKey.m_ChildSchema == CurrentSchema && ChildForeignKey.m_ChildTable == pTable->f_Name())
							_pChildForeignKeys->f_Remove(iForeignKey);
					}
				}

				// The rebuild removes the columns of the previous version that the target leaves out, but not one added outside version tracking
				if (co_await fg_PostgresHasTable(_Client, pTable->f_Name()))
					co_await fg_PostgresCheckNoUndeclaredColumns(_Client, *pTable, fg_Move(_RemovableColumns));

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
				// The additive pass adds columns after the migration operations, so a backfill needs a column that already exists
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
					return DMibErrorDatabaseInstance("Migration renames column '{}.{}' after rebuilding its table; order the rename first"_f << *Rename.m_pTableName << *Rename.m_pOldName);
			}
		}

		return {};
	}

	auto fg_PostgresApplyMigrationOperations
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, CSqlSchemaMigrationDescription _Migration
			, CSqlSchemaVersionDescription _PreviousSchema
			, CSqlSchemaVersionDescription _TargetSchema
		)
		-> NConcurrency::TCFuture<void>
	{
		if (auto pException = fg_PostgresValidateRenameAfterRebuild(_Migration))
			co_return fg_Move(pException);

		// Follows the operations, so each rebuild sees the table as the previous version and the operations before it leave it
		CSqlSchemaVersionDescription PlannedSchema = _PreviousSchema;
		CPostgresPlannedSchemaStorage PlannedSchemaStorage;

		// Another rebuild in the migration can change the keys these reference, so they come after every operation
		NStorage::TCSharedPointer<NContainer::TCVector<CPostgresChildForeignKey>> pChildForeignKeys = fg_Construct();
		for (auto const &Operation : _Migration.m_Operations)
		{
			NStorage::TCOptional<CSqlTableDescription> RebuildTable;
			// Besides the columns the rebuilt table declares, the rebuild can drop the columns of the planned table and those the migration drops
			NContainer::TCVector<NStr::CStr> RemovableColumns;
			if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_RebuildTable)
			{
				CSqlTableDescription const *pPlannedTable = fg_PostgresFindTable(PlannedSchema, *Operation.m_pTableName);
				if (CSqlTableDescription const *pTargetTable = fg_PostgresFindTable(_TargetSchema, *Operation.m_pTableName))
					RebuildTable = fg_PostgresRebuildTableKeepingLaterDrops(_Migration, Operation, *pTargetTable, pPlannedTable);

				if (pPlannedTable)
				{
					for (auto const &Column : pPlannedTable->m_Columns)
						RemovableColumns.f_InsertLast(Column.f_Name());
				}

				for (auto const &Drop : _Migration.m_Operations)
				{
					if (Drop.m_Type == ESqlSchemaMigrationOperationType::mc_DropColumn && *Drop.m_pTableName == *Operation.m_pTableName)
						RemovableColumns.f_InsertLast(*Drop.m_pOldName);
				}
			}

			co_await fg_PostgresApplyMigrationOperation(_Client, Operation, pChildForeignKeys, fg_Move(RebuildTable), fg_Move(RemovableColumns));
			fg_PostgresApplyMigrationOperationToPlannedSchema(PlannedSchema, _Migration, Operation, _TargetSchema, PlannedSchemaStorage);
		}

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

	bool fg_PostgresColumnListsEqual(NContainer::TCVector<NStr::CStr const *> const &_Declared, TCPostgresArray<NStr::CStr> const &_Live)
	{
		if (_Declared.f_GetLen() != _Live.m_Values.f_GetLen())
			return false;

		for (umint iColumn = 0; iColumn < _Declared.f_GetLen(); ++iColumn)
		{
			if (!_Live.m_Values[iColumn] || *_Declared[iColumn] != *_Live.m_Values[iColumn])
				return false;
		}

		return true;
	}

	bool fg_PostgresColumnListsEqual(TCPostgresArray<NStr::CStr> const &_Left, TCPostgresArray<NStr::CStr> const &_Right)
	{
		if (_Left.m_Values.f_GetLen() != _Right.m_Values.f_GetLen())
			return false;

		for (umint iColumn = 0; iColumn < _Left.m_Values.f_GetLen(); ++iColumn)
		{
			if (!_Left.m_Values[iColumn] || !_Right.m_Values[iColumn] || *_Left.m_Values[iColumn] != *_Right.m_Values[iColumn])
				return false;
		}

		return true;
	}

	// Gives the OID of the collation as text when the collation is nondeterministic, where strings that differ can compare equal, and
	// otherwise an empty string, since a deterministic collation compares only identical strings equal.
	NStr::CStr fg_PostgresNondeterministicCollationSql(ch8 const *_pCollationOid)
	{
		NStr::CStr Sql = "COALESCE((SELECT CASE WHEN collisdeterministic THEN '' ELSE oid::text END FROM pg_collation WHERE oid = ";
		Sql += _pCollationOid;
		Sql += "), '')";

		return Sql;
	}

	NStr::CStr fg_PostgresConstraintsOfTableSql(NStr::CStr const &_TableReference)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "SELECT Constraint_.conname::text, Constraint_.contype::text, pg_get_constraintdef(Constraint_.oid)::text, "
				"ARRAY(SELECT Attribute_.attname::text FROM unnest(Constraint_.conkey) WITH ORDINALITY AS Key_(attnum, ord) "
				"JOIN pg_attribute AS Attribute_ ON Attribute_.attrelid = Constraint_.conrelid AND Attribute_.attnum = Key_.attnum ORDER BY Key_.ord), "
				"Constraint_.confrelid::oid::text, "
				"ARRAY(SELECT Attribute_.attname::text FROM unnest(Constraint_.confkey) WITH ORDINALITY AS Key_(attnum, ord) "
				"JOIN pg_attribute AS Attribute_ ON Attribute_.attrelid = Constraint_.confrelid AND Attribute_.attnum = Key_.attnum ORDER BY Key_.ord), "
				"COALESCE((SELECT (to_jsonb(Index_)->>'indnullsnotdistinct')::boolean FROM pg_index AS Index_ "
				"WHERE Index_.indexrelid = Constraint_.conindid AND Constraint_.contype IN ('p', 'u')), false), "
				"ARRAY(SELECT "
			;
			Appender += fg_PostgresNondeterministicCollationSql("Key_.colloid");
			Appender += " FROM pg_index AS Index_, unnest(Index_.indcollation::oid[]) WITH ORDINALITY AS Key_(colloid, ord) "
				"WHERE Index_.indexrelid = Constraint_.conindid AND Constraint_.contype IN ('p', 'u') ORDER BY Key_.ord), "
				"Constraint_.confupdtype::text || Constraint_.confdeltype::text || Constraint_.confmatchtype::text "
				"|| CASE WHEN Constraint_.condeferred THEN 'D' WHEN Constraint_.condeferrable THEN 'd' ELSE '' END "
				"|| CASE WHEN jsonb_typeof(to_jsonb(Constraint_)->'confdelsetcols') = 'array' AND to_jsonb(Constraint_)->'confdelsetcols' <> '[]'::jsonb "
				"THEN 'l' ELSE '' END "
				"FROM pg_constraint AS Constraint_ WHERE Constraint_.contype IN ('p', 'u', 'c', 'x', 'f') AND Constraint_.conrelid = "
			;
			Appender += _TableReference;
			Appender += "::regclass ORDER BY Constraint_.oid";
		}

		return Sql;
	}

	// Adopting an untracked table rebuilds it from the declaration, which keeps only the declared constraints, so a live
	// constraint the schema does not declare would be dropped. The declared PRIMARY KEY, UNIQUE, CHECK and EXCLUDE
	// constraints are read back from a temporary table created from the declaration, as PostgreSQL deparses them; a
	// temporary table cannot reference permanent tables, so foreign keys are compared with their declarations. Indexes
	// created on their own are restored by the rebuild, except one named like a declared index, which the declared index
	// replaces.
	NConcurrency::TCFuture<void> fg_PostgresCheckNoUndeclaredConstraints(NConcurrency::TCActor<CPostgresClientActor> _Client, CSqlTableDescription _Table)
	{
		using namespace NStr;

		NStr::CStr TableReference;
		{
			NStr::CStr::CAppender Appender(TableReference);
			Appender += "quote_ident(";
			fg_PostgresAppendStringLiteral(Appender, _Table.f_Name());
			Appender += ")";
		}

		auto LiveConstraints = co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresConstraintsOfTableSql(TableReference));

		NStr::CStr ScratchName = fg_PostgresScratchTableName("__mib_adopt_check_", _Table.f_Name());
		CSqlTableDescription ScratchTable = _Table;
		ScratchTable.m_pName = &ScratchName;

		NStr::CStr CreateScratchSql = fg_PostgresCreateTable(ScratchTable, false, false);
		CreateScratchSql = "CREATE TEMPORARY TABLE " + CreateScratchSql.f_Extract(NStr::CStr("CREATE TABLE ").f_GetLen());
		co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(CreateScratchSql));

		NStr::CStr ScratchReference;
		{
			NStr::CStr::CAppender Appender(ScratchReference);
			Appender += "('pg_temp.' || quote_ident(";
			fg_PostgresAppendStringLiteral(Appender, ScratchName);
			Appender += "))";
		}

		auto ScratchConstraints = co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresConstraintsOfTableSql(ScratchReference));

		// The nullability and collation of the declared columns as the rebuild creates them, which decide the values that the declared keys
		// and indexes treat as equal.
		NStr::CStr ScratchColumnsSql;
		{
			NStr::CStr::CAppender Appender(ScratchColumnsSql);
			Appender += "SELECT attname::text, attnotnull, ";
			Appender += fg_PostgresNondeterministicCollationSql("attcollation");
			Appender += " FROM pg_attribute WHERE attnum > 0 AND NOT attisdropped AND attrelid = ";
			Appender += ScratchReference;
			Appender += "::regclass";
		}
		auto ScratchColumns = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(ScratchColumnsSql));

		{
			NStr::CStr DropScratchSql;
			{
				NStr::CStr::CAppender Appender(DropScratchSql);
				Appender += "DROP TABLE pg_temp.";
				fg_PostgresAppendQuotedIdentifier(Appender, ScratchName);
			}
			co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(DropScratchSql));
		}

		auto fUndeclared = [&](NStr::CStr const &_Name, NStr::CStr const &_Definition) -> NException::CExceptionPointer
			{
				return DMibErrorDatabaseInstance
					(
						"PostgreSQL table '{}' has constraint '{}' ({}) that the schema does not declare; adopting the table would drop it"_f << _Table.f_Name() << _Name << _Definition
					)
				;
			}
		;

		// A declared key replaces a live one on the same columns, and keeps its uniqueness when it compares each value at least as
		// coarsely and, for a live key whose NULL values are not distinct, when no declared key column can be NULL
		auto fDeclaredKeyEnforces = [&](TCPostgresArray<NStr::CStr> const &_LiveColumns, TCPostgresArray<NStr::CStr> const &_LiveCollations, bool _bLiveNullsNotDistinct)
			{
				for (umint iColumn = 0; iColumn < _LiveColumns.m_Values.f_GetLen(); ++iColumn)
				{
					if (!_LiveColumns.m_Values[iColumn])
						return false;

					NStr::CStr const *pLiveCollation = iColumn < _LiveCollations.m_Values.f_GetLen() && _LiveCollations.m_Values[iColumn] ? &*_LiveCollations.m_Values[iColumn] : nullptr;

					bool bFound = false;
					for (auto const &Column : ScratchColumns.m_Rows)
					{
						if (Column.m_Values[0].f_GetAsType<NStr::CStr>() != *_LiveColumns.m_Values[iColumn])
							continue;

						bFound = true;
						if (_bLiveNullsNotDistinct && !Column.m_Values[1].f_GetAsType<bool>())
							return false;

						if (pLiveCollation && !pLiveCollation->f_IsEmpty() && Column.m_Values[2].f_GetAsType<NStr::CStr>() != *pLiveCollation)
							return false;
					}

					if (!bFound)
						return false;
				}

				return true;
			}
		;

		{
			NStr::CStr LiveNotNullSql;
			{
				NStr::CStr::CAppender Appender(LiveNotNullSql);
				Appender += "SELECT attname::text FROM pg_attribute WHERE attnum > 0 AND NOT attisdropped AND attnotnull AND attrelid = ";
				Appender += TableReference;
				Appender += "::regclass";
			}

			auto LiveNotNullColumns = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(LiveNotNullSql));
			for (auto const &Live : LiveNotNullColumns.m_Rows)
			{
				NStr::CStr const &ColumnName = Live.m_Values[0].f_GetAsType<NStr::CStr>();
				bool bDeclared = false;
				for (auto const &Column : ScratchColumns.m_Rows)
					bDeclared = bDeclared || (Column.m_Values[0].f_GetAsType<NStr::CStr>() == ColumnName && Column.m_Values[1].f_GetAsType<bool>());

				if (!bDeclared)
				{
					co_return DMibErrorDatabaseInstance
						(
							"PostgreSQL table '{}' has NOT NULL on column '{}' that the schema does not declare; adopting the table would drop it"_f << _Table.f_Name() << ColumnName
						)
					;
				}
			}
		}

		NContainer::TCVector<bool> ScratchUsed;
		ScratchUsed.f_SetLen(ScratchConstraints.m_Rows.f_GetLen());
		for (auto &bUsed : ScratchUsed)
			bUsed = false;

		// The update and delete actions, MATCH type and deferral of a declared foreign key, as the last column of
		// fg_PostgresConstraintsOfTableSql reports them: a declaration is MATCH SIMPLE, not deferrable, and has no column list for
		// ON DELETE SET NULL or SET DEFAULT, which a live key reports as 'l'
		auto fDeclaredForeignKeyBehavior = [](CSqlConstraintDescription const &_Constraint)
			{
				auto fAction = [](ESqlForeignKeyAction _Action) -> ch8 const *
					{
						switch (_Action)
						{
						case ESqlForeignKeyAction::mc_Default: return "a";
						case ESqlForeignKeyAction::mc_NoAction: return "a";
						case ESqlForeignKeyAction::mc_Restrict: return "r";
						case ESqlForeignKeyAction::mc_Cascade: return "c";
						case ESqlForeignKeyAction::mc_SetNull: return "n";
						case ESqlForeignKeyAction::mc_SetDefault: return "d";
						}

						return "a";
					}
				;

				NStr::CStr Behavior = fAction(_Constraint.m_OnUpdate);
				Behavior += fAction(_Constraint.m_OnDelete);
				Behavior += "s";

				return Behavior;
			}
		;

		// A foreign key keeps its target when the declared name resolves to the relation the live one references, also when a
		// table of the same name exists in another schema
		NContainer::TCVector<NStr::CStr> DeclaredReferencedOids;
		for (auto const &Constraint : _Table.m_Constraints)
		{
			NStr::CStr ReferencedOid;
			if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey)
			{
				NContainer::TCVector<CPostgresValue> Values;
				Values.f_InsertLast(Constraint.f_ReferencedTable());
				auto Result = co_await _Client(&CPostgresClientActor::f_ExecuteWithParameters, NStr::CStr("SELECT COALESCE(to_regclass(quote_ident($1))::oid::text, '')"), fg_Move(Values));
				if (Result.m_Rows.f_GetLen() == 1)
					ReferencedOid = Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>();
			}

			DeclaredReferencedOids.f_InsertLast(fg_Move(ReferencedOid));
		}

		for (auto const &Live : LiveConstraints.m_Rows)
		{
			NStr::CStr const &Name = Live.m_Values[0].f_GetAsType<NStr::CStr>();
			NStr::CStr const &Type = Live.m_Values[1].f_GetAsType<NStr::CStr>();
			NStr::CStr const &Definition = Live.m_Values[2].f_GetAsType<NStr::CStr>();
			auto const &Columns = Live.m_Values[3].f_GetAsType<TCPostgresArray<NStr::CStr>>();

			bool bDeclared = false;
			if (Type == "f")
			{
				NStr::CStr const &ReferencedOid = Live.m_Values[4].f_GetAsType<NStr::CStr>();
				auto const &ReferencedColumns = Live.m_Values[5].f_GetAsType<TCPostgresArray<NStr::CStr>>();
				NStr::CStr const &Behavior = Live.m_Values[8].f_GetAsType<NStr::CStr>();
				for (umint iConstraint = 0; iConstraint < _Table.m_Constraints.f_GetLen(); ++iConstraint)
				{
					auto const &Constraint = _Table.m_Constraints[iConstraint];
					// A replacement that behaves otherwise would change what an update or delete of the parent does to the child rows,
					// or which rows and transactions the key accepts
					if
					(
						Constraint.m_Type == ESqlConstraintType::mc_ForeignKey
						&& !DeclaredReferencedOids[iConstraint].f_IsEmpty()
						&& DeclaredReferencedOids[iConstraint] == ReferencedOid
						&& fg_PostgresColumnListsEqual(Constraint.m_Columns, Columns)
						&& fg_PostgresColumnListsEqual(Constraint.m_ReferencedColumns, ReferencedColumns)
						&& fDeclaredForeignKeyBehavior(Constraint) == Behavior
					)
					{
						bDeclared = true;
					}
				}
			}
			else
			{
				bool bLiveNullsNotDistinct = Live.m_Values[6].f_GetAsType<bool>();
				auto const &LiveCollations = Live.m_Values[7].f_GetAsType<TCPostgresArray<NStr::CStr>>();
				// The deferral of a key, which a replacement must keep: an immediate key would reject a transaction that duplicates a
				// value only until it commits
				NStr::CStr const &Deferral = Live.m_Values[8].f_GetAsType<NStr::CStr>();
				for (umint iScratch = 0; iScratch < ScratchConstraints.m_Rows.f_GetLen() && !bDeclared; ++iScratch)
				{
					if (ScratchUsed[iScratch])
						continue;

					auto const &Scratch = ScratchConstraints.m_Rows[iScratch];
					NStr::CStr const &ScratchType = Scratch.m_Values[1].f_GetAsType<NStr::CStr>();
					bool bMatches = false;
					if (Type == "p" || Type == "u")
					{
						// A declared primary key also keeps a unique constraint on the same columns
						bMatches
							= (ScratchType == Type || (Type == "u" && ScratchType == "p"))
							&& fg_PostgresColumnListsEqual(Columns, Scratch.m_Values[3].f_GetAsType<TCPostgresArray<NStr::CStr>>())
							&& Scratch.m_Values[8].f_GetAsType<NStr::CStr>() == Deferral
							&& fDeclaredKeyEnforces(Columns, LiveCollations, bLiveNullsNotDistinct)
						;
					}
					else
						bMatches = ScratchType == Type && Scratch.m_Values[2].f_GetAsType<NStr::CStr>() == Definition;

					if (bMatches)
					{
						ScratchUsed[iScratch] = true;
						bDeclared = true;
					}
				}

				// A declared unique index enforces the same uniqueness as a unique constraint that is not deferrable, which the
				// constraint's deferral reports as spaces only
				if (!bDeclared && Type == "u" && Deferral.f_Find("d") < 0 && Deferral.f_Find("D") < 0)
				{
					for (auto const &Index : _Table.m_Indexes)
					{
						if (Index.f_IsUnique() && fg_PostgresColumnListsEqual(Index.m_Columns, Columns) && fDeclaredKeyEnforces(Columns, LiveCollations, bLiveNullsNotDistinct))
							bDeclared = true;
					}
				}
			}

			if (!bDeclared)
				co_return fUndeclared(Name, Definition);
		}

		NStr::CStr IndexesSql;
		{
			NStr::CStr::CAppender Appender(IndexesSql);
			// The key columns only, without INCLUDE columns, which do not take part in the uniqueness
			Appender += "SELECT IndexClass_.relname::text, Index_.indisunique::text, "
				"ARRAY(SELECT Attribute_.attname::text FROM unnest(Index_.indkey::int2[]) WITH ORDINALITY AS Key_(attnum, ord) "
				"JOIN pg_attribute AS Attribute_ ON Attribute_.attrelid = Index_.indrelid AND Attribute_.attnum = Key_.attnum WHERE Key_.ord <= Index_.indnkeyatts "
				"ORDER BY Key_.ord), "
				"pg_get_indexdef(Index_.indexrelid)::text, "
				"COALESCE((to_jsonb(Index_)->>'indnullsnotdistinct')::boolean, false), "
				"ARRAY(SELECT "
			;
			Appender += fg_PostgresNondeterministicCollationSql("Collation_.colloid");
			Appender += " FROM unnest(Index_.indkey::int2[]) WITH ORDINALITY AS Key_(attnum, ord) "
				"JOIN unnest(Index_.indcollation::oid[]) WITH ORDINALITY AS Collation_(colloid, ord) ON Collation_.ord = Key_.ord "
				"JOIN pg_attribute AS Attribute_ ON Attribute_.attrelid = Index_.indrelid AND Attribute_.attnum = Key_.attnum ORDER BY Key_.ord) "
				"FROM pg_index AS Index_ JOIN pg_class AS IndexClass_ ON IndexClass_.oid = Index_.indexrelid "
				"WHERE NOT EXISTS (SELECT 1 FROM pg_constraint WHERE contype IN ('p', 'u', 'x') AND conindid = Index_.indexrelid) AND Index_.indrelid = "
			;
			Appender += TableReference;
			Appender += "::regclass";
		}

		auto LiveIndexes = co_await _Client(&CPostgresClientActor::f_Execute, fg_Move(IndexesSql));
		for (auto const &Live : LiveIndexes.m_Rows)
		{
			if (Live.m_Values[1].f_GetAsType<NStr::CStr>() != "true")
				continue;

			NStr::CStr const &Name = Live.m_Values[0].f_GetAsType<NStr::CStr>();
			for (auto const &Index : _Table.m_Indexes)
			{
				if (Index.f_Name() != Name)
					continue;

				// The declared index replaces the live one, which keeps its uniqueness only when the declared index is unique on the same
				// columns; a declared full index is at least as strict as a live partial one
				auto const &LiveColumns = Live.m_Values[2].f_GetAsType<TCPostgresArray<NStr::CStr>>();
				if
				(
					!Index.f_IsUnique()
					|| !fg_PostgresColumnListsEqual(Index.m_Columns, LiveColumns)
					|| !fDeclaredKeyEnforces(LiveColumns, Live.m_Values[5].f_GetAsType<TCPostgresArray<NStr::CStr>>(), Live.m_Values[4].f_GetAsType<bool>())
				)
				{
					co_return fUndeclared(Name, Live.m_Values[3].f_GetAsType<NStr::CStr>());
				}
			}
		}

		co_return {};
	}

	auto fg_PostgresSyncAdditiveSchema
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, CSqlSchemaVersionDescription _Schema
			, CSqlSchemaVersionDescription const *_pPreviousSchema = nullptr
		)
		-> NConcurrency::TCFuture<void>
	{
		// The rebuild that adopts a parent drops the foreign keys of its children that the schema declares, to add them as declared, so
		// every adopted table is checked before the first rebuild
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			if (_pPreviousSchema && fg_PostgresFindTable(*_pPreviousSchema, Table.f_Name()))
				continue;

			if (!co_await fg_PostgresHasTable(_Client, Table.f_Name()))
				continue;

			co_await fg_PostgresCheckNoUndeclaredColumns(_Client, Table, {});
			co_await fg_PostgresCheckNoUndeclaredConstraints(_Client, Table);
		}

		// A recorded table can be missing, dropped outside the migrations along with the foreign keys that reference it
		NContainer::TCSet<NStr::CStr> CreatedTables;

		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_PostgresFindTable(*_pPreviousSchema, Table.f_Name()) : nullptr;
			if (!co_await fg_PostgresHasTable(_Client, Table.f_Name()))
			{
				// Without IF NOT EXISTS, since a relation that information_schema.tables does not list, such as a materialized view or a
				// sequence, can have the name, and must fail the create instead of being taken for the table
				co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresCreateTable(Table, false, false));
				CreatedTables[Table.f_Name()];

				for (auto const &Index : Table.m_Indexes)
					co_await fg_PostgresEnsureIndex(_Client, Table, Index);

				continue;
			}

			// Without a recorded definition, rebuild to enforce declared column constraints the live table may lack; rows that violate them fail the apply
			if (!pPreviousTable)
			{
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
								NStr::CStr::CFormat("Cannot add required PostgreSQL column '{}.{}' without a default value") << Table.f_Name() << Column.f_Name()
							)
						;
					}

					co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresAlterTableAddColumn(Table, Column));
				}
			}

			for (auto const &Index : Table.m_Indexes)
				co_await fg_PostgresEnsureIndex(_Client, Table, Index);

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

		// Add foreign keys only after all tables exist, so declaration order and reference cycles are supported
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			CSqlTableDescription const *pPreviousTable = _pPreviousSchema ? fg_PostgresFindTable(*_pPreviousSchema, Table.f_Name()) : nullptr;
			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.m_Type != ESqlConstraintType::mc_ForeignKey)
					continue;

				// Adopting a table without a recorded version, or with the table or its parent created here, only the live table tells which
				// foreign keys already exist
				bool bCheckLive = !_pPreviousSchema || CreatedTables.f_FindEqual(Table.f_Name()) || CreatedTables.f_FindEqual(Constraint.f_ReferencedTable());
				bool bAlreadyPresent
					= bCheckLive ? co_await fg_PostgresHasConstraint(_Client, Table.f_Name(), Constraint.f_Name()) : fg_PostgresTableHasConstraint(pPreviousTable, Constraint.f_Name())
				;

				if (!bAlreadyPresent)
					co_await _Client(&CPostgresClientActor::f_Execute, fg_PostgresAlterTableAddConstraint(Table, Constraint));
			}
		}

		co_return {};
	}

	CPostgresDatabaseBackendActor::CPostgresDatabaseBackendActor(ICSqlSchemaVersions const *_pSchemaVersions, CPostgresDatabaseBackendSettings _Settings)
		: mp_pSchemaVersions(_pSchemaVersions)
		, mp_Schema(_pSchemaVersions->f_Describe())
		, mp_SchemaVersions(_pSchemaVersions->f_DescribeVersions())
		, mp_SchemaMigrations(_pSchemaVersions->f_DescribeMigrations())
		, mp_Settings(fg_Move(_Settings))
		, mp_pPreparedSelectCache(fg_Construct<CPostgresPreparedSelectCache>())
		, mp_pPreparedInsertCache(fg_Construct<CPostgresPreparedInsertCache>())
		, mp_pPreparedUpdateCache(fg_Construct<CPostgresPreparedUpdateCache>())
		, mp_pPreparedDeleteCache(fg_Construct<CPostgresPreparedDeleteCache>())
		, mp_pPreparedUpsertCache(fg_Construct<CPostgresPreparedUpsertCache>())
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

		// The top-level description carries the left operand's modifiers, and m_SetOperations only the right operands
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

		if (auto pException = fg_PostgresValidateSchemaVersions(mp_SchemaVersions))
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

		if (NException::CExceptionPointer pValidationException = fg_PostgresValidateAppliedSchemaVersions(AppliedVersions, mp_SchemaVersions))
			co_return pValidationException;

		NStr::CStr CurrentSchemaVersionID;
		umint iCurrentSchemaVersion = umint(-1);
		NStr::CStr const *pFirstMissingVersionID = nullptr;
		for (umint iSchemaVersion = 0; iSchemaVersion < mp_SchemaVersions.f_GetLen(); ++iSchemaVersion)
		{
			auto const &SchemaVersion = mp_SchemaVersions[iSchemaVersion];

			bool bApplied = false;
			for (auto const &AppliedVersion : AppliedVersions)
			{
				if (AppliedVersion.m_ID == SchemaVersion.f_ID())
				{
					bApplied = true;
					break;
				}
			}

			if (!bApplied)
			{
				if (!pFirstMissingVersionID)
					pFirstMissingVersionID = &SchemaVersion.f_ID();

				continue;
			}

			// Upgrading resumes after the latest applied version, so a version the history has before it would never run
			if (pFirstMissingVersionID)
			{
				co_return DMibErrorDatabaseInstance
					(
						NStr::CStr::CFormat("PostgreSQL schema version '{}' is not applied, but the later version '{}' is") << *pFirstMissingVersionID << SchemaVersion.f_ID()
					)
				;
			}

			CurrentSchemaVersionID = SchemaVersion.f_ID();
			iCurrentSchemaVersion = iSchemaVersion;
		}

		bool bSchemaChanged = CurrentSchemaVersionID != mp_Schema.f_ID();
		if (bSchemaChanged)
		{
			if (CurrentSchemaVersionID.f_IsEmpty())
			{
				co_await fg_PostgresSyncAdditiveSchema(Client, mp_Schema);

				for (umint iSchemaVersion = 0; iSchemaVersion < mp_SchemaVersions.f_GetLen(); ++iSchemaVersion)
					co_await fg_PostgresMarkSchemaVersionApplied(Client, mp_SchemaVersions[iSchemaVersion]);
			}
			else
			{
				for (umint iSchemaVersion = iCurrentSchemaVersion + 1; iSchemaVersion < mp_SchemaVersions.f_GetLen(); ++iSchemaVersion)
				{
					auto const &PreviousSchema = mp_SchemaVersions[iSchemaVersion - 1];
					auto const &NextSchema = mp_SchemaVersions[iSchemaVersion];
					CSqlSchemaVersionDescription PlannedPreviousSchema = PreviousSchema;
					CPostgresPlannedSchemaStorage PlannedSchemaStorage;

					umint iMigration = fg_PostgresFindMigration(mp_SchemaMigrations, PreviousSchema.f_ID(), NextSchema.f_ID());
					if (iMigration != umint(-1))
					{
						co_await fg_PostgresApplyMigrationOperations(Client, mp_SchemaMigrations[iMigration], PreviousSchema, NextSchema);

						auto const &TargetSchema = mp_SchemaVersions[iSchemaVersion];
						for (auto const &Operation : mp_SchemaMigrations[iMigration].m_Operations)
							fg_PostgresApplyMigrationOperationToPlannedSchema(PlannedPreviousSchema, mp_SchemaMigrations[iMigration], Operation, TargetSchema, PlannedSchemaStorage);
					}

					co_await fg_PostgresSyncAdditiveSchema(Client, mp_SchemaVersions[iSchemaVersion], &PlannedPreviousSchema);
					co_await fg_PostgresMarkSchemaVersionApplied(Client, mp_SchemaVersions[iSchemaVersion]);
				}
			}
		}

		co_await Client(&CPostgresClientActor::f_CommitTransaction);

		bCommitted = true;
		ConnectionCheckout.f_MarkReusable();

		if (bSchemaChanged)
			++mp_SchemaGeneration;

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
				, fg_PostgresParameterBatchGenerator(fg_Move(_Operation.m_RowBatches), mp_Settings.m_nPipelineLength)
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
				co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, StatementEntry.m_Name, StatementEntry.m_Sql, fg_PostgresParameterValueTypes(Values));
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
				co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, StatementEntry.m_Name, StatementEntry.m_Sql, fg_PostgresParameterValueTypes(Values));
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
			Parameters.f_InsertLast(_Operation.m_nResultRowLimit != 0 ? NPrivate::fg_SqlRowCountParameter(_Operation.m_nResultRowLimit) : TCLimitsInt<int64>::mc_Max);
		if (pStatementDescription->m_LimitOffset.m_bHasOffset)
			Parameters.f_InsertLast(NPrivate::fg_SqlRowCountParameter(_Operation.m_nResultRowOffset));
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : _pBackend->mp_Settings.m_nSelectRowsPerBatch;
		auto nPipelineLength = _pBackend->mp_Settings.m_nPipelineLength;
		auto ConnectionCheckout = co_await _pBackend->fp_CheckoutConnection();

		auto Values = co_await fg_PostgresValues(fg_Move(Parameters));
		auto Rows = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteRows, fg_Move(Sql), fg_Move(Values), nRowsPerBatch);

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
		// By pointer, since identical SQL can decode into different result members
		if (CPostgresPreparedSelectCacheEntry const *pEntry = mp_pPreparedSelectCache->f_FindByPointer(_pDescription))
			return *pEntry;

		CPostgresPreparedSelectCacheEntry Entry = fp_GetPreparedSelect(_pDescription);

		if (mp_pPreparedSelectCache.f_GetRefCount() != 1)
			mp_pPreparedSelectCache = fg_Construct<CPostgresPreparedSelectCache>(*mp_pPreparedSelectCache);

		mp_pPreparedSelectCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	CPostgresPreparedInsertCacheEntry CPostgresDatabaseBackendActor::fp_EnsurePreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedInsertCacheEntry const *pEntry = mp_pPreparedInsertCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedInsertCacheEntry Entry = fp_GetPreparedInsert(_pDescription);

		if (mp_pPreparedInsertCache.f_GetRefCount() != 1)
			mp_pPreparedInsertCache = fg_Construct<CPostgresPreparedInsertCache>(*mp_pPreparedInsertCache);

		mp_pPreparedInsertCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	// Clears session state raw SQL can leave behind, such as a search_path or temporary table that redirects the ORM's unqualified names. This is
	// DISCARD ALL without its DEALLOCATE ALL, which would drop the cached prepared statements. A statement that SQL PREPARE created keeps its name
	// taken, and a named DEALLOCATE makes the client forget its whole cache, so such a session is not reused.
	NConcurrency::TCFuture<bool> fg_PostgresResetSession(NConcurrency::TCActor<CPostgresClientActor> _Client)
	{
		{
			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SELECT EXISTS (SELECT 1 FROM pg_prepared_statements WHERE from_sql)")).f_Wrap();
			if (!Result)
			{
				NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to check for SQL prepared statements after raw SQL")(fg_Move(Result));
				co_return false;
			}

			if (Result->m_Rows.f_GetLen() != 1 || Result->m_Rows[0].m_Values[0].f_GetAsType<bool>())
				co_return false;
		}

		for
		(
			ch8 const *pSql :
			{
				"CLOSE ALL"
				, "SET SESSION AUTHORIZATION DEFAULT"
				, "RESET ALL"
				, "UNLISTEN *"
				, "SELECT pg_advisory_unlock_all()"
				, "DISCARD PLANS"
				, "DISCARD TEMP"
				, "DISCARD SEQUENCES"
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
		// A row the statement returns isn't decoded, so a value that the client can't decode doesn't fail a statement that has already committed
		auto Result = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParametersSkipRows, fg_Move(Sql), fg_Move(Values));
		umint nAffected = 0;
		if (NException::CExceptionPointer pException = fg_PostgresParseAffectedRows(Result, nAffected))
			co_return pException;

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

	auto fg_PostgresEmptyRawRowStream() -> NConcurrency::TCAsyncGenerator<CSqlRawRowBatch>
	{
		co_return {};
	}

	static auto fg_PostgresRawRowStreamFromIterator
		(
			NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>::CPipelinedIterator _iItems
			, CPostgresConnectionCheckout _ConnectionCheckout
			, uint64 _nStatementDropsBefore
		)
		-> NConcurrency::TCAsyncGenerator<CSqlRawRowBatch>
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

		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : mp_Settings.m_nSelectRowsPerBatch;
		auto ConnectionCheckout = co_await fp_CheckoutConnection();
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));

		uint64 nStatementDropsBefore = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_GetStatementDropCount);
		auto Items = co_await ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteRowsStream, fg_Move(Sql), fg_Move(Values), nRowsPerBatch);
		auto iItems = co_await fg_Move(Items).f_GetPipelinedIterator(mp_Settings.m_nPipelineLength);
		if (!iItems)
		{
			// ReadyForQuery is already consumed, but a command without a row description, such as BEGIN, can still leave a transaction open
			if (co_await fg_PostgresRawConnectionReusable(ConnectionCheckout.m_Client, nStatementDropsBefore))
				ConnectionCheckout.f_MarkReusable();

			// The command has run, and outside a transaction it has committed, so it succeeds with no columns and no rows, as on SQLite
			co_return CSqlRawStream{.m_Rows = fg_PostgresEmptyRawRowStream()};
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
				, mp_Settings.m_nSelectRowsPerBatch
				, mp_Settings.m_nPipelineLength
				, NConcurrency::fg_ThisActor(this)
				, mp_pPreparedSelectCache
				, mp_pPreparedInsertCache
				, mp_pPreparedUpdateCache
				, mp_pPreparedDeleteCache
				, mp_pPreparedUpsertCache
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
				, mp_Settings.m_nSelectRowsPerBatch
				, mp_Settings.m_nPipelineLength
				, NConcurrency::fg_ThisActor(this)
				, mp_pPreparedSelectCache
				, mp_pPreparedInsertCache
				, mp_pPreparedUpdateCache
				, mp_pPreparedDeleteCache
				, mp_pPreparedUpsertCache
			)
		;
		co_await Transaction(&CPostgresTransactionActor::f_OpenBegin);

		co_return Transaction;
	}

	NConcurrency::TCFuture<void> CPostgresDatabaseBackendActor::fp_Destroy()
	{
		auto pDestroyedException = DMibErrorDatabaseInstance("PostgreSQL database backend destroyed while waiting for connection").f_ExceptionPointer();
		for (NConcurrency::TCPromise<void> &Promise : mp_ConnectionPoolWaiters)
			Promise.f_SetException(pDestroyedException);

		mp_ConnectionPoolWaiters.f_Clear();

		NConcurrency::TCFutureVector<void> DestroyFutures;
		for (CPostgresConnectionPoolEntry &Entry : mp_ConnectionPool)
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
		if (mp_Settings.m_ConnectionPoolSize == 0)
			co_return DMibErrorDatabaseInstance("PostgreSQL connection pool size must be at least 1");

		if (mp_Settings.m_nSelectRowsPerBatch == 0)
			co_return DMibErrorDatabaseInstance("PostgreSQL select rows per batch must be at least 1");

		if (mp_Settings.m_nPipelineLength == 0)
			co_return DMibErrorDatabaseInstance("PostgreSQL pipeline length must be at least 1");

		CPostgresConnectionPoolEntry *pEntry = nullptr;

		for (;;)
		{
			pEntry = mp_FreeConnectionPoolEntries.f_Pop();
			if (!pEntry)
			{
				if (mp_ConnectionPool.f_GetLen() >= mp_Settings.m_ConnectionPoolSize)
				{
					co_await mp_ConnectionPoolWaiters.f_InsertLast().f_Future();

					continue;
				}

				pEntry = &mp_ConnectionPool.f_InsertLast();
			}

			// A statement prepared before this backend changed the schema can fail with a changed result type until it is prepared again
			if (pEntry->m_bConnected && pEntry->m_SchemaGeneration != mp_SchemaGeneration)
			{
				uint64 SchemaGeneration = mp_SchemaGeneration;
				pEntry->m_PreparedSelectCache.f_Clear();
				pEntry->m_PreparedInsertCache.f_Clear();
				pEntry->m_PreparedUpdateCache.f_Clear();
				pEntry->m_PreparedDeleteCache.f_Clear();
				pEntry->m_PreparedUpsertCache.f_Clear();
				auto DeallocateResult = co_await pEntry->m_Client(&CPostgresClientActor::f_Execute, NStr::CStr("DEALLOCATE ALL")).f_Wrap();
				if (DeallocateResult)
				{
					// NOLINTNEXTLINE(malterlib-coroutine-actor-reference): no other checkout uses the entry, and CheckDestroy stops this resuming once the pool's actor is destroyed
					pEntry->m_SchemaGeneration = SchemaGeneration;
				}
				else
				{
					NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to drop prepared statements after a schema change")(fg_Move(DeallocateResult));
					// NOLINTNEXTLINE(malterlib-coroutine-actor-reference): no other checkout uses the entry, and CheckDestroy stops this resuming once the pool's actor is destroyed
					pEntry->m_bConnected = false;
				}
			}

			if (!pEntry->m_bConnected)
			{
				// A connection that closed while the entry was free leaves statements that the new session does not have
				pEntry->m_PreparedSelectCache.f_Clear();
				pEntry->m_PreparedInsertCache.f_Clear();
				pEntry->m_PreparedUpdateCache.f_Clear();
				pEntry->m_PreparedDeleteCache.f_Clear();
				pEntry->m_PreparedUpsertCache.f_Clear();
				if (pEntry->m_Client)
					co_await fg_Move(pEntry->m_Client).f_Destroy().f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to destroy closed PostgreSQL connection");
				// NOLINTNEXTLINE(malterlib-coroutine-actor-reference): no other checkout uses the entry, and CheckDestroy stops this resuming once the pool's actor is destroyed
				pEntry->m_Client = fg_CreatePostgresClient();

				uint64 SchemaGeneration = mp_SchemaGeneration;
				auto ConnectResult = co_await pEntry->m_Client(&CPostgresClientActor::f_Connect, mp_Settings.m_ConnectionSettings).f_Wrap();
				if (!ConnectResult)
				{
					// NOLINTNEXTLINE(malterlib-coroutine-actor-reference): no other checkout uses the entry, and CheckDestroy stops this resuming once the pool's actor is destroyed
					pEntry->m_bConnected = false;
					pEntry->m_PreparedSelectCache.f_Clear();
					pEntry->m_PreparedInsertCache.f_Clear();
					pEntry->m_PreparedUpdateCache.f_Clear();
					pEntry->m_PreparedDeleteCache.f_Clear();
					pEntry->m_PreparedUpsertCache.f_Clear();
					co_await fg_Move(pEntry->m_Client).f_Destroy().f_Wrap()
						> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to destroy PostgreSQL connection after connect failure")
					;
					mp_FreeConnectionPoolEntries.f_InsertLast(pEntry);
					fp_NotifyConnectionAvailable();

					NConcurrency::TCAsyncResult<CPostgresConnectionCheckout> CheckoutResult;
					CheckoutResult.f_SetException(fg_Move(ConnectResult));

					co_return fg_Move(CheckoutResult);
				}

				pEntry->m_bConnected = true;
				pEntry->m_SchemaGeneration = SchemaGeneration;
				co_await fp_WatchConnection(pEntry).f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to watch PostgreSQL connection");
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
		Checkout.m_ReleaseSubscription = fp_MakeReleaseSubscription(pEntry, Checkout.m_pState);

		co_return fg_Move(Checkout);
	}

	auto CPostgresDatabaseBackendActor::fp_MakeReleaseSubscription(CPostgresConnectionPoolEntry *_pEntry, NStorage::TCSharedPointer<CPostgresConnectionCheckoutState> const &_pState)
		-> NConcurrency::CActorSubscription
	{
		return NConcurrency::g_ActorSubscription / [this, pEntry = _pEntry, pState = _pState]() -> NConcurrency::TCFuture<void>
			{
				if (pState && pState->m_bReusable && pEntry->m_bConnected)
				{
					mp_FreeConnectionPoolEntries.f_InsertFirst(pEntry);
					fp_NotifyConnectionAvailable();

					co_return {};
				}

				if (pEntry->m_Client)
					co_await fg_Move(pEntry->m_Client).f_Destroy();

				pEntry->m_bConnected = false;
				pEntry->m_PreparedSelectCache.f_Clear();
				pEntry->m_PreparedInsertCache.f_Clear();
				pEntry->m_PreparedUpdateCache.f_Clear();
				pEntry->m_PreparedDeleteCache.f_Clear();
				pEntry->m_PreparedUpsertCache.f_Clear();
				pEntry->m_Client = fg_CreatePostgresClient();
				uint64 SchemaGeneration = mp_SchemaGeneration;
				auto ConnectResult = co_await pEntry->m_Client(&CPostgresClientActor::f_Connect, mp_Settings.m_ConnectionSettings).f_Wrap();
				if (ConnectResult)
				{
					pEntry->m_bConnected = true;
					pEntry->m_SchemaGeneration = SchemaGeneration;
					co_await fp_WatchConnection(pEntry).f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to watch PostgreSQL connection");
				}
				else
				{
					co_await fg_Move(pEntry->m_Client).f_Destroy().f_Wrap()
						> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to destroy PostgreSQL connection after reconnect failure")
					;
					NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to reconnect PostgreSQL connection during release")(fg_Move(ConnectResult));
				}

				mp_FreeConnectionPoolEntries.f_InsertLast(pEntry);
				fp_NotifyConnectionAvailable();

				co_return {};
			}
		;
	}

	// Marks the entry disconnected when the server or the network closes its current connection. The pool tries to connect it again on
	// release, or else at its next checkout.
	NConcurrency::TCFuture<void> CPostgresDatabaseBackendActor::fp_WatchConnection(CPostgresConnectionPoolEntry *_pEntry)
	{
		uint64 Generation = ++_pEntry->m_ConnectionGeneration;

		return _pEntry->m_Client
			(
				&CPostgresClientActor::f_SetOnConnectionLost
				, NConcurrency::g_ActorFunctorWeak / [pEntry = _pEntry, Generation]() -> NConcurrency::TCFuture<void>
				{
					if (pEntry->m_ConnectionGeneration == Generation)
						pEntry->m_bConnected = false;

					co_return {};
				}
			)
			.f_Call()
		;
	}

	void CPostgresDatabaseBackendActor::fp_NotifyConnectionAvailable()
	{
		if (mp_ConnectionPoolWaiters.f_IsEmpty())
			return;

		mp_ConnectionPoolWaiters.f_PopFirst().f_SetResult();
	}

	CPostgresPreparedSelectCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedSelectCacheEntry const *pEntry = mp_pPreparedSelectCache->f_FindByPointer(_pDescription))
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
		if (CPostgresPreparedInsertCacheEntry const *pEntry = mp_pPreparedInsertCache->f_Find(_pDescription))
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
		if (CPostgresPreparedUpdateCacheEntry const *pEntry = mp_pPreparedUpdateCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedUpdateCacheEntry Entry = fp_GetPreparedUpdate(_pDescription);

		if (mp_pPreparedUpdateCache.f_GetRefCount() != 1)
			mp_pPreparedUpdateCache = fg_Construct<CPostgresPreparedUpdateCache>(*mp_pPreparedUpdateCache);

		mp_pPreparedUpdateCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	CPostgresPreparedUpdateCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpdateCacheEntry const *pEntry = mp_pPreparedUpdateCache->f_Find(_pDescription))
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
		if (CPostgresPreparedDeleteCacheEntry const *pEntry = mp_pPreparedDeleteCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedDeleteCacheEntry Entry = fp_GetPreparedDelete(_pDescription);

		if (mp_pPreparedDeleteCache.f_GetRefCount() != 1)
			mp_pPreparedDeleteCache = fg_Construct<CPostgresPreparedDeleteCache>(*mp_pPreparedDeleteCache);

		mp_pPreparedDeleteCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	CPostgresPreparedDeleteCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedDeleteCacheEntry const *pEntry = mp_pPreparedDeleteCache->f_Find(_pDescription))
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
		if (CPostgresPreparedUpsertCacheEntry const *pEntry = mp_pPreparedUpsertCache->f_Find(_pDescription))
			return *pEntry;

		CPostgresPreparedUpsertCacheEntry Entry = fp_GetPreparedUpsert(_pDescription);

		if (mp_pPreparedUpsertCache.f_GetRefCount() != 1)
			mp_pPreparedUpsertCache = fg_Construct<CPostgresPreparedUpsertCache>(*mp_pPreparedUpsertCache);

		mp_pPreparedUpsertCache->f_Insert(_pDescription, Entry);

		return Entry;
	}

	CPostgresPreparedUpsertCacheEntry CPostgresDatabaseBackendActor::fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpsertCacheEntry const *pEntry = mp_pPreparedUpsertCache->f_Find(_pDescription))
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
			if (umint iMigration = fg_PostgresFindMigration(Migrations, PreviousVersion.f_ID(), NextVersion.f_ID()); iMigration != umint(-1))
			{
				auto const &Migration = Migrations[iMigration];
				if (auto pException = fg_PostgresValidateRenameAfterRebuild(Migration))
					co_return fg_Move(pException);

				// Another rebuild in the migration can change the keys these reference, so they come after every operation
				NContainer::TCVector<CPostgresPlannedForeignKey> ForeignKeys;
				for (auto const &Operation : Migration.m_Operations)
				{
					Plan.m_Statements.f_InsertLast(co_await fg_PostgresMigrationOperationStatements(Migration, Operation, NextVersion, &PlannedPreviousVersion, ForeignKeys));
					fg_PostgresApplyMigrationOperationToPlannedSchema(PlannedPreviousVersion, Migration, Operation, NextVersion, PlannedSchemaStorage);

					if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_RenameTable)
					{
						for (auto &ForeignKey : ForeignKeys)
						{
							if (ForeignKey.m_Table.f_Name() == *Operation.m_pOldName)
								ForeignKey.m_Table.m_pName = Operation.m_pNewName;
						}
					}
					else if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_RenameColumn)
					{
						for (auto &ForeignKey : ForeignKeys)
						{
							if (ForeignKey.m_Table.f_Name() == *Operation.m_pTableName)
								fg_PostgresRenameColumnReference(ForeignKey.m_Constraint.m_Columns, Operation.m_pOldName, Operation.m_pNewName);
						}
					}
					else if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_DropColumn)
					{
						// PostgreSQL drops a foreign key with its column
						for (umint iForeignKey = ForeignKeys.f_GetLen(); iForeignKey-- > 0;)
						{
							auto const &ForeignKey = ForeignKeys[iForeignKey];
							if (ForeignKey.m_Table.f_Name() != *Operation.m_pTableName)
								continue;

							bool bUsesColumn = false;
							for (NStr::CStr const *pColumn : ForeignKey.m_Constraint.m_Columns)
							{
								if (*pColumn == *Operation.m_pOldName)
									bUsesColumn = true;
							}

							if (bUsesColumn)
								ForeignKeys.f_Remove(iForeignKey);
						}
					}
					else if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_DropTable)
					{
						for (umint iForeignKey = ForeignKeys.f_GetLen(); iForeignKey-- > 0;)
						{
							if (ForeignKeys[iForeignKey].m_Table.f_Name() == *Operation.m_pTableName)
								ForeignKeys.f_Remove(iForeignKey);
						}
					}
					else if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_RebuildTable)
					{
						// The rebuild replaces this table's foreign keys with the target's, which the additive pass adds
						for (umint iForeignKey = ForeignKeys.f_GetLen(); iForeignKey-- > 0;)
						{
							if (ForeignKeys[iForeignKey].m_Table.f_Name() == *Operation.m_pTableName)
								ForeignKeys.f_Remove(iForeignKey);
						}
					}
				}

				// The additive pass adds the constraints the target declares under the names of these keys on the tables as the
				// operations left them; only the others keep their previous definitions
				for (auto const &ForeignKey : ForeignKeys)
				{
					if (!fg_PostgresSchemaDeclaresConstraint(NextVersion, ForeignKey.m_Table.f_Name(), ForeignKey.m_Constraint.f_Name()))
						Plan.m_Statements.f_InsertLast(fg_PostgresAlterTableAddConstraint(ForeignKey.m_Table, ForeignKey.m_Constraint));
				}
			}

			Plan.m_Statements.f_InsertLast(co_await fg_PostgresSyncAdditiveSchemaStatements(NextVersion, &PlannedPreviousVersion));
			Plan.m_Statements.f_InsertLast(fg_PostgresInsertSchemaVersionSql());
		}

		co_return fg_Move(Plan);
	}
}
