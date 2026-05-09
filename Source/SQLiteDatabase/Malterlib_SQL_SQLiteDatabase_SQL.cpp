// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLiteDatabase_Internal.h"

namespace NMib::NSQL::NPrivate
{
	CSqlErrorData fg_SqliteErrorData(sqlite3 *_pDatabase)
	{
		int ExtendedCode = sqlite3_extended_errcode(_pDatabase);
		ESqlErrorCategory Category = ESqlErrorCategory::mc_Generic;
		ESqlErrorRetryClass RetryClass = ESqlErrorRetryClass::mc_Permanent;

		switch (ExtendedCode)
		{
		case SQLITE_CONSTRAINT_UNIQUE:
		case SQLITE_CONSTRAINT_PRIMARYKEY:
			Category = ESqlErrorCategory::mc_DuplicateKey;
			break;
		case SQLITE_CONSTRAINT_FOREIGNKEY:
			Category = ESqlErrorCategory::mc_ForeignKeyViolation;
			break;
		default:
			// The remaining extended codes, such as SQLITE_IOERR_READ, share the classification of their primary code
			switch (ExtendedCode & 0xff)
			{
			case SQLITE_CONSTRAINT:
				Category = ESqlErrorCategory::mc_ConstraintViolation;
				break;
			case SQLITE_BUSY:
			case SQLITE_LOCKED:
				Category = ESqlErrorCategory::mc_SerializationFailure;
				RetryClass = ESqlErrorRetryClass::mc_RetryTransaction;
				break;
			case SQLITE_IOERR:
			case SQLITE_CANTOPEN:
				Category = ESqlErrorCategory::mc_ConnectionLoss;
				RetryClass = ESqlErrorRetryClass::mc_RetryConnection;
				break;
			default:
				break;
			}

			break;
		}

		return fg_SqlErrorData
			(
				Category
				, RetryClass
				, "sqlite"
				, NStr::CStr::fs_ToStr(ExtendedCode)
				, sqlite3_errmsg(_pDatabase)
			)
		;
	}

	CExceptionSql fg_SqliteError(sqlite3 *_pDatabase, NStr::CStr const &_Message)
	{
		using namespace NStr;

		CSqlErrorData ErrorData = fg_SqliteErrorData(_pDatabase);
		return DMibErrorSqlInstance("{}: {}"_f << _Message << ErrorData.m_BackendMessage, fg_Move(ErrorData));
	}

	NException::CExceptionPointer fg_SqliteUnsupportedColumnTypeError(ESqlColumnType _Type)
	{
		using namespace NStr;

		return DMibErrorDatabaseInstance("SQLite backend does not support ORM column type '{}'"_f << fg_SqlColumnTypeName(_Type));
	}

	NException::CExceptionPointer fg_SqliteUnsupportedValueTypeError(ESqlValueType _Type)
	{
		using namespace NStr;

		return DMibErrorDatabaseInstance("SQLite backend does not support ORM value type '{}'"_f << fg_SqlValueTypeName(_Type));
	}

	void fg_SqliteAppendQuotedIdentifier(NStr::CStr::CAppender &_Appender, NStr::CStr const &_Identifier)
	{
		_Appender += "\"";
		for (aint i = 0; i < _Identifier.f_GetLen(); ++i)
		{
			auto Character = _Identifier.f_GetAt(i);
			_Appender += Character;
			if (Character == '"')
				_Appender += "\"";
		}
		_Appender += "\"";
	}

	void fg_SqliteAppendStringLiteral(NStr::CStr::CAppender &_Appender, NStr::CStr const &_String)
	{
		_Appender += "'";
		for (aint i = 0; i < _String.f_GetLen(); ++i)
		{
			auto Character = _String.f_GetAt(i);
			_Appender += Character;
			if (Character == '\'')
				_Appender += "'";
		}
		_Appender += "'";
	}

	NStr::CStr fg_SqliteColumnType(ESqlColumnType _Type, NException::CExceptionPointer &o_pError)
	{
		switch (_Type)
		{
		case ESqlColumnType::mc_Integer8:
		case ESqlColumnType::mc_Integer16:
		case ESqlColumnType::mc_Integer32:
		case ESqlColumnType::mc_Integer64:
		case ESqlColumnType::mc_UnsignedInteger8:
		case ESqlColumnType::mc_UnsignedInteger16:
		case ESqlColumnType::mc_UnsignedInteger32:
		case ESqlColumnType::mc_UnsignedInteger64:
		case ESqlColumnType::mc_Boolean:
			return NStr::gc_Str<"INTEGER">;
		case ESqlColumnType::mc_Float32:
		case ESqlColumnType::mc_Float64:
			return NStr::gc_Str<"REAL">;
		case ESqlColumnType::mc_Text:
		case ESqlColumnType::mc_Time:
			return NStr::gc_Str<"TEXT">;
		case ESqlColumnType::mc_Blob:
			return NStr::gc_Str<"BLOB">;
		case ESqlColumnType::mc_Invalid:
		case ESqlColumnType::mc_UUID:
		case ESqlColumnType::mc_Date:
		case ESqlColumnType::mc_TimeOfDay:
		case ESqlColumnType::mc_Timestamp:
		case ESqlColumnType::mc_TimestampTz:
		case ESqlColumnType::mc_Interval:
		case ESqlColumnType::mc_Json:
		case ESqlColumnType::mc_Jsonb:
		case ESqlColumnType::mc_Array_Integer16:
		case ESqlColumnType::mc_Array_Integer32:
		case ESqlColumnType::mc_Array_Integer64:
		case ESqlColumnType::mc_Array_Float32:
		case ESqlColumnType::mc_Array_Float64:
		case ESqlColumnType::mc_Array_Text:
		case ESqlColumnType::mc_Array_Boolean:
		case ESqlColumnType::mc_Array_Bytes:
		case ESqlColumnType::mc_Array_Date:
		case ESqlColumnType::mc_Array_TimeOfDay:
		case ESqlColumnType::mc_Array_Timestamp:
		case ESqlColumnType::mc_Array_TimestampTz:
		case ESqlColumnType::mc_Array_UUID:
		case ESqlColumnType::mc_Array_Json:
		case ESqlColumnType::mc_Array_Jsonb:
		case ESqlColumnType::mc_Array_Interval:
			if (!o_pError)
				o_pError = fg_SqliteUnsupportedColumnTypeError(_Type);
			return NStr::gc_Str<"TEXT">;
		}

		return NStr::gc_Str<"TEXT">;
	}

	NConcurrency::TCWrapped<void> fg_SqliteAppendColumnDefinition(NStr::CStr::CAppender &_Appender, CSqlColumnDescription const &_Column)
	{
		CNonPortableColumnOptions const *pNonPortableOptions = _Column.f_NonPortableOptions(NStr::gc_Str<"sqlite">.m_Str);

		fg_SqliteAppendQuotedIdentifier(_Appender, _Column.f_Name());
		_Appender += " ";

		NException::CExceptionPointer pException;
		NStr::CStr ColumnType = fg_SqliteColumnType(_Column.m_Type, pException);
		if (pException)
			return fg_Move(pException);

		_Appender += ColumnType;

		if (pNonPortableOptions && !pNonPortableOptions->m_pCollationSql->f_IsEmpty())
		{
			_Appender += " COLLATE ";
			_Appender += *pNonPortableOptions->m_pCollationSql;
		}

		if (pNonPortableOptions && !pNonPortableOptions->m_pGeneratedSql->f_IsEmpty())
		{
			_Appender += " GENERATED ALWAYS AS (";
			_Appender += *pNonPortableOptions->m_pGeneratedSql;
			_Appender += ") STORED";
		}

		if (fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_PrimaryKey))
			_Appender += " PRIMARY KEY";

		if (fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
		{
			_Appender += " AUTOINCREMENT";

			// SQLite assigns 64-bit keys, so a narrower key fails the insert instead of storing a value it cannot hold
			ch8 const *pMaxKey = nullptr;
			if (_Column.m_Type == ESqlColumnType::mc_Integer16)
				pMaxKey = "32767";
			else if (_Column.m_Type == ESqlColumnType::mc_Integer32)
				pMaxKey = "2147483647";

			if (pMaxKey)
			{
				_Appender += " CHECK (";
				fg_SqliteAppendQuotedIdentifier(_Appender, _Column.f_Name());
				_Appender += " <= ";
				_Appender += pMaxKey;
				_Appender += ")";
			}
		}

		// An INTEGER PRIMARY KEY aliases the rowid, where inserting NULL assigns a new key. Any other primary key accepts
		// NULL in a rowid table unless declared NOT NULL.
		bool bRowidAlias = fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_PrimaryKey) && ColumnType == "INTEGER";
		if (!_Column.f_IsNullable() && !bRowidAlias)
			_Appender += " NOT NULL";

		// A primary key is already unique, so only emit UNIQUE for a non-primary-key column carrying the flag.
		// Without this the schema checksum records a uniqueness requirement the database would not enforce.
		if (fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_Unique) && !fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_PrimaryKey))
			_Appender += " UNIQUE";

		NStr::CStr const &DefaultSql = (pNonPortableOptions && !pNonPortableOptions->m_pDefaultSql->f_IsEmpty()) ? *pNonPortableOptions->m_pDefaultSql : _Column.f_DefaultSql();
		if (!DefaultSql.f_IsEmpty())
		{
			_Appender += " DEFAULT ";
			_Appender += DefaultSql;
		}

		if (pNonPortableOptions && !pNonPortableOptions->m_pCustomSql->f_IsEmpty())
		{
			_Appender += " ";
			_Appender += *pNonPortableOptions->m_pCustomSql;
		}

		return {};
	}

	void fg_SqliteAppendForeignKeyAction(NStr::CStr::CAppender &_Appender, ESqlForeignKeyAction _Action)
	{
		switch (_Action)
		{
		case ESqlForeignKeyAction::mc_Default:
			break;
		case ESqlForeignKeyAction::mc_Restrict:
			_Appender += " RESTRICT";
			break;
		case ESqlForeignKeyAction::mc_Cascade:
			_Appender += " CASCADE";
			break;
		case ESqlForeignKeyAction::mc_SetNull:
			_Appender += " SET NULL";
			break;
		case ESqlForeignKeyAction::mc_SetDefault:
			_Appender += " SET DEFAULT";
			break;
		case ESqlForeignKeyAction::mc_NoAction:
			_Appender += " NO ACTION";
			break;
		}
	}

	void fg_SqliteAppendColumnList(NStr::CStr::CAppender &_Appender, NContainer::TCVector<NStr::CStr const *> const &_Columns)
	{
		_Appender += "(";
		for (umint iColumn = 0; iColumn < _Columns.f_GetLen(); ++iColumn)
		{
			if (iColumn != 0)
				_Appender += ", ";

			fg_SqliteAppendQuotedIdentifier(_Appender, *_Columns[iColumn]);
		}
		_Appender += ")";
	}

	NStr::CStr fg_SqliteConstraintDefinitionSql(CSqlConstraintDescription const &_Constraint)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			fg_SqliteAppendConstraintDefinition(Appender, _Constraint);
		}

		return Sql;
	}

	NConcurrency::TCWrapped<NStr::CStr> fg_SqliteColumnDefinitionSql(CSqlColumnDescription const &_Column)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			co_await fg_SqliteAppendColumnDefinition(Appender, _Column);
		}

		co_return fg_Move(Sql);
	}

	void fg_SqliteAppendConstraintDefinition(NStr::CStr::CAppender &_Appender, CSqlConstraintDescription const &_Constraint)
	{
		_Appender += "CONSTRAINT ";
		fg_SqliteAppendQuotedIdentifier(_Appender, _Constraint.f_Name());
		_Appender += " ";

		switch (_Constraint.m_Type)
		{
		case ESqlConstraintType::mc_PrimaryKey:
			_Appender += "PRIMARY KEY ";
			fg_SqliteAppendColumnList(_Appender, _Constraint.m_Columns);
			break;
		case ESqlConstraintType::mc_Unique:
			_Appender += "UNIQUE ";
			fg_SqliteAppendColumnList(_Appender, _Constraint.m_Columns);
			break;
		case ESqlConstraintType::mc_Check:
			_Appender += "CHECK (";
			_Appender += _Constraint.f_CheckSql();
			_Appender += ")";
			break;
		case ESqlConstraintType::mc_ForeignKey:
			_Appender += "FOREIGN KEY ";
			fg_SqliteAppendColumnList(_Appender, _Constraint.m_Columns);
			_Appender += " REFERENCES ";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Constraint.f_ReferencedTable());
			_Appender += " ";
			fg_SqliteAppendColumnList(_Appender, _Constraint.m_ReferencedColumns);
			if (_Constraint.m_OnDelete != ESqlForeignKeyAction::mc_Default)
			{
				_Appender += " ON DELETE";
				fg_SqliteAppendForeignKeyAction(_Appender, _Constraint.m_OnDelete);
			}
			if (_Constraint.m_OnUpdate != ESqlForeignKeyAction::mc_Default)
			{
				_Appender += " ON UPDATE";
				fg_SqliteAppendForeignKeyAction(_Appender, _Constraint.m_OnUpdate);
			}
			break;
		}
	}

	NConcurrency::TCWrapped<NStr::CStr> fg_SqliteCreateTable(CSqlTableDescription const &_Table, bool _bIfNotExists)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "CREATE TABLE ";
			if (_bIfNotExists)
				Appender += "IF NOT EXISTS ";

			fg_SqliteAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " (";

			bool bNeedSeparator = false;
			for (umint i = 0; i < _Table.m_Columns.f_GetLen(); ++i)
			{
				if (bNeedSeparator)
					Appender += ", ";

				co_await fg_SqliteAppendColumnDefinition(Appender, _Table.m_Columns[i]);
				bNeedSeparator = true;
			}

			for (auto const &Constraint : _Table.m_Constraints)
			{
				if (bNeedSeparator)
					Appender += ", ";

				fg_SqliteAppendConstraintDefinition(Appender, Constraint);
				bNeedSeparator = true;
			}

			Appender += ")";
		}

		co_return fg_Move(Sql);
	}

	NStr::CStr fg_SqliteCreateIndex(CSqlTableDescription const &_Table, CSqlIndexDescription const &_Index, bool _bIfNotExists)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "CREATE ";
			if (_Index.f_IsUnique())
				Appender += "UNIQUE ";

			Appender += "INDEX ";

			if (_bIfNotExists)
				Appender += "IF NOT EXISTS ";

			fg_SqliteAppendQuotedIdentifier(Appender, _Index.f_Name());
			Appender += " ON ";
			fg_SqliteAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " (";

			for (umint i = 0; i < _Index.m_Columns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_SqliteAppendQuotedIdentifier(Appender, *_Index.m_Columns[i]);
			}

			Appender += ")";
		}

		return Sql;
	}

	NConcurrency::TCWrapped<NStr::CStr> fg_SqliteAlterTableAddColumn(CSqlTableDescription const &_Table, CSqlColumnDescription const &_Column)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "ALTER TABLE ";
			fg_SqliteAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " ADD COLUMN ";
			co_await fg_SqliteAppendColumnDefinition(Appender, _Column);
		}

		co_return fg_Move(Sql);
	}

	// SQLite refuses a rename to a name that differs only in case, as it finds the table itself there, so that goes
	// through an intermediate name
	NContainer::TCVector<NStr::CStr> fg_SqliteRenameTable(CSqlSchemaMigrationOperationDescription const &_Operation)
	{
		auto fRename = [](NStr::CStr const &_From, NStr::CStr const &_To)
			{
				NStr::CStr Sql;
				{
					NStr::CStr::CAppender Appender(Sql);
					Appender += "ALTER TABLE ";
					fg_SqliteAppendQuotedIdentifier(Appender, _From);
					Appender += " RENAME TO ";
					fg_SqliteAppendQuotedIdentifier(Appender, _To);
				}

				return Sql;
			}
		;

		NContainer::TCVector<NStr::CStr> Statements;
		if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(*_Operation.m_pOldName, *_Operation.m_pNewName))
		{
			NStr::CStr IntermediateName = "__mib_rename_" + *_Operation.m_pNewName;
			Statements.f_InsertLast(fRename(*_Operation.m_pOldName, IntermediateName));
			Statements.f_InsertLast(fRename(IntermediateName, *_Operation.m_pNewName));
		}
		else
			Statements.f_InsertLast(fRename(*_Operation.m_pOldName, *_Operation.m_pNewName));

		return Statements;
	}

	NStr::CStr fg_SqliteRenameColumn(CSqlSchemaMigrationOperationDescription const &_Operation)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "ALTER TABLE ";
			fg_SqliteAppendQuotedIdentifier(Appender, *_Operation.m_pTableName);
			Appender += " RENAME COLUMN ";
			fg_SqliteAppendQuotedIdentifier(Appender, *_Operation.m_pOldName);
			Appender += " TO ";
			fg_SqliteAppendQuotedIdentifier(Appender, *_Operation.m_pNewName);
		}

		return Sql;
	}

	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_SqliteCreateSchemaStatements(CSqlSchemaVersionDescription const &_Schema)
	{
		NContainer::TCVector<NStr::CStr> Statements;
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			Statements.f_InsertLast(co_await fg_SqliteCreateTable(Table, false));

			for (auto const &Index : Table.m_Indexes)
				Statements.f_InsertLast(fg_SqliteCreateIndex(Table, Index));
		}

		co_return fg_Move(Statements);
	}

	NStr::CStr fg_SqliteCreateSchemaVersionTableSql()
	{
		return
			"CREATE TABLE IF NOT EXISTS \"schema_migrations\" (\"id\" TEXT PRIMARY KEY, \"name\" TEXT NOT NULL, \"checksum\" TEXT NOT NULL, "
			"\"applied_at\" TEXT NOT NULL, \"applied_by_version\" TEXT NOT NULL)"
		;
	}

	NStr::CStr fg_SqliteHasSchemaVersionSql()
	{
		return NStr::gc_Str<"SELECT \"id\" FROM \"schema_migrations\" WHERE \"id\" = ?">;
	}

	NStr::CStr fg_SqliteReadSchemaVersionsSql()
	{
		return NStr::gc_Str<"SELECT \"id\", \"checksum\" FROM \"schema_migrations\" ORDER BY \"applied_at\", \"id\"">;
	}

	NStr::CStr fg_SqliteInsertSchemaVersionSql()
	{
		return NStr::gc_Str<"INSERT INTO \"schema_migrations\" (\"id\", \"name\", \"checksum\", \"applied_at\", \"applied_by_version\") VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)">;
	}

	NStr::CStr fg_SqliteDropTableSql(NStr::CStr const &_TableName)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "DROP TABLE ";
			fg_SqliteAppendQuotedIdentifier(Appender, _TableName);
		}

		return Sql;
	}

	NStr::CStr fg_SqliteUpdateColumnSql(CSqlSchemaMigrationOperationDescription const &_Operation)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "UPDATE ";
			fg_SqliteAppendQuotedIdentifier(Appender, *_Operation.m_pTableName);
			Appender += " SET ";
			fg_SqliteAppendQuotedIdentifier(Appender, *_Operation.m_pNewName);
			Appender += " = ";
			Appender += *_Operation.m_pSql;
		}

		return Sql;
	}

	NStr::CStr fg_SqliteHasTableSql()
	{
		// SQLite matches table names case-insensitively, as CREATE TABLE IF NOT EXISTS does
		return NStr::gc_Str<"SELECT \"name\" FROM \"sqlite_master\" WHERE \"type\" = 'table' AND \"name\" = ? COLLATE NOCASE">;
	}

	NStr::CStr fg_SqliteInsertSql(CSqlInsertOperation const &_Operation)
	{
		CSqlPreparedInsertStatementDescription Description;
		NContainer::TCVector<NStr::CStr> const *pColumns = nullptr;
		if (_Operation.m_pDescription)
		{
			Description = _Operation.m_pDescription->m_pStatement->f_Describe();
			pColumns = &Description.m_InsertColumns;
		}
		else
		{
			Description.m_TableName = _Operation.m_TableName;
			for (auto const &Value : _Operation.m_Values)
				Description.m_InsertColumns.f_InsertLast(Value.m_ColumnName);

			pColumns = &Description.m_InsertColumns;
		}

		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "INSERT INTO ";
			fg_SqliteAppendQuotedIdentifier(Appender, Description.m_TableName);

			if (pColumns->f_IsEmpty())
			{
				// A table whose only columns are auto-generated (e.g. a lone autoincrement primary key) has no
				// insertable columns. "() VALUES ()" is invalid SQL, so insert a fully defaulted row instead.
				Appender += " DEFAULT VALUES";
			}
			else
			{
				Appender += " (";

				for (umint i = 0; i < pColumns->f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					fg_SqliteAppendQuotedIdentifier(Appender, (*pColumns)[i]);
				}

				Appender += ") VALUES (";

				for (umint i = 0; i < pColumns->f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					Appender += "?";
				}

				Appender += ")";
			}
		}

		return Sql;
	}

	void fg_SqliteAppendPredicateSql(NStr::CStr::CAppender &_Appender, CSqlPredicateDescription const &_Predicate);

	NStr::CStr fg_SqliteUpsertSql(CSqlPreparedUpsertStatementDescription const &_Description)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "INSERT INTO ";
			fg_SqliteAppendQuotedIdentifier(Appender, _Description.m_TableName);
			Appender += " (";

			for (umint i = 0; i < _Description.m_InsertColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_SqliteAppendQuotedIdentifier(Appender, _Description.m_InsertColumns[i]);
			}

			Appender += ") VALUES (";

			for (umint i = 0; i < _Description.m_InsertColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				Appender += "?";
			}

			Appender += ") ON CONFLICT (";

			for (umint i = 0; i < _Description.m_ConflictColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_SqliteAppendQuotedIdentifier(Appender, _Description.m_ConflictColumns[i]);
			}

			Appender += ") DO UPDATE SET ";

			for (umint i = 0; i < _Description.m_UpdateColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_SqliteAppendQuotedIdentifier(Appender, _Description.m_UpdateColumns[i]);
				Appender += " = excluded.";
				fg_SqliteAppendQuotedIdentifier(Appender, _Description.m_UpdateColumns[i]);
			}
		}

		return Sql;
	}

	NStr::CStr fg_SqliteUpsertSql(CSqlUpsertOperation const &_Operation)
	{
		CSqlPreparedUpsertStatementDescription Description = _Operation.m_pDescription->m_pStatement->f_Describe();

		return fg_SqliteUpsertSql(Description);
	}

	NStr::CStr fg_SqliteUpdateSql(CSqlPreparedUpdateStatementDescription const &_Description)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "UPDATE ";
			fg_SqliteAppendQuotedIdentifier(Appender, _Description.m_TableName);
			Appender += " SET ";

			for (umint i = 0; i < _Description.m_UpdateColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_SqliteAppendQuotedIdentifier(Appender, _Description.m_UpdateColumns[i]);
				Appender += " = ?";
			}

			Appender += " WHERE ";
			fg_SqliteAppendPredicateSql(Appender, _Description.m_Predicate);
		}

		return Sql;
	}

	NStr::CStr fg_SqliteUpdateSql(CSqlUpdateOperation const &_Operation)
	{
		CSqlPreparedUpdateStatementDescription Description = _Operation.m_pDescription->m_pStatement->f_Describe();

		return fg_SqliteUpdateSql(Description);
	}

	NStr::CStr fg_SqliteDeleteSql(CSqlPreparedDeleteStatementDescription const &_Description)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "DELETE FROM ";
			fg_SqliteAppendQuotedIdentifier(Appender, _Description.m_TableName);
			Appender += " WHERE ";
			fg_SqliteAppendPredicateSql(Appender, _Description.m_Predicate);
		}

		return Sql;
	}

	NStr::CStr fg_SqliteDeleteSql(CSqlDeleteOperation const &_Operation)
	{
		CSqlPreparedDeleteStatementDescription Description = _Operation.m_pDescription->m_pStatement->f_Describe();

		return fg_SqliteDeleteSql(Description);
	}

	void fg_SqliteAppendSelectExpressionSql(NStr::CStr::CAppender &_Appender, CSqlSelectExpressionDescription const &_Expression);
	void fg_SqliteAppendSelectSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement);

	void fg_SqliteAppendSubquerySql(NStr::CStr::CAppender &_Appender, ICSqlPreparedSelectStatement const *_pStatement)
	{
		DMibCheck(_pStatement != nullptr);
		auto Statement = _pStatement->f_Describe();

		// Without parentheses an operand's LIMIT would apply to the whole compound, as at the top level
		if (fg_SqliteSelectHasSetOperandModifiers(Statement))
			DMibErrorDatabase("SQLite set operation operands cannot have ORDER BY, LIMIT, or OFFSET modifiers");

		_Appender += "(";
		fg_SqliteAppendSelectSql(_Appender, Statement);
		_Appender += ")";
	}

	void fg_SqliteAppendSetOperationType(NStr::CStr::CAppender &_Appender, ESqlSetOperationType _Type)
	{
		switch (_Type)
		{
		case ESqlSetOperationType::mc_Union:
			_Appender += " UNION ";
			break;
		case ESqlSetOperationType::mc_UnionAll:
			_Appender += " UNION ALL ";
			break;
		case ESqlSetOperationType::mc_Intersect:
			_Appender += " INTERSECT ";
			break;
		case ESqlSetOperationType::mc_Except:
			_Appender += " EXCEPT ";
			break;
		}
	}

	template <typename tf_FAppendOperand>
	void fg_SqliteAppendSetOperandSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Operand, tf_FAppendOperand &&_fAppendOperand)
	{
		if (_Operand.m_SetOperations.f_IsEmpty())
		{
			_fAppendOperand(_Operand);
			return;
		}

		// SQLite evaluates compounds left to right and does not accept a parenthesized compound operand, so a compound
		// right operand keeps its grouping as a subquery
		_Appender += "SELECT * FROM (";
		_fAppendOperand(_Operand);
		_Appender += ")";
	}

	void fg_SqliteAppendPredicateSubject(NStr::CStr::CAppender &_Appender, CSqlPredicateDescription const &_Predicate)
	{
		if (_Predicate.m_bExpression)
			fg_SqliteAppendSelectExpressionSql(_Appender, _Predicate.m_Expression);
		else
			fg_SqliteAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
	}

	void fg_SqliteAppendCompareOperator(NStr::CStr::CAppender &_Appender, ESqlPredicateType _Type)
	{
		switch (_Type)
		{
		case ESqlPredicateType::mc_EqualParameter:
			_Appender += " = ";
			break;
		case ESqlPredicateType::mc_NotEqualParameter:
			_Appender += " <> ";
			break;
		case ESqlPredicateType::mc_LessParameter:
			_Appender += " < ";
			break;
		case ESqlPredicateType::mc_LessEqualParameter:
			_Appender += " <= ";
			break;
		case ESqlPredicateType::mc_GreaterParameter:
			_Appender += " > ";
			break;
		case ESqlPredicateType::mc_GreaterEqualParameter:
			_Appender += " >= ";
			break;
		default:
			DMibCheck(false);
		}
	}

	void fg_SqliteAppendPredicateSql(NStr::CStr::CAppender &_Appender, CSqlPredicateDescription const &_Predicate)
	{
		switch (_Predicate.m_Type)
		{
		case ESqlPredicateType::mc_EqualParameter:
			fg_SqliteAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " = ?";
			break;
		case ESqlPredicateType::mc_NotEqualParameter:
			fg_SqliteAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " <> ?";
			break;
		case ESqlPredicateType::mc_LessParameter:
			fg_SqliteAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " < ?";
			break;
		case ESqlPredicateType::mc_LessEqualParameter:
			fg_SqliteAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " <= ?";
			break;
		case ESqlPredicateType::mc_GreaterParameter:
			fg_SqliteAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " > ?";
			break;
		case ESqlPredicateType::mc_GreaterEqualParameter:
			fg_SqliteAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " >= ?";
			break;
		case ESqlPredicateType::mc_LikeParameter:
			fg_SqliteAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " LIKE ?";
			break;
		case ESqlPredicateType::mc_IsNull:
			fg_SqliteAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " IS NULL";
			break;
		case ESqlPredicateType::mc_IsNotNull:
			fg_SqliteAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " IS NOT NULL";
			break;
		case ESqlPredicateType::mc_InParameters:
			fg_SqliteAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " IN (";
			for (umint i = 0; i < _Predicate.m_nParameters; ++i)
			{
				if (i != 0)
					_Appender += ", ";

				_Appender += "?";
			}
			_Appender += ")";
			break;
		case ESqlPredicateType::mc_InSubquery:
			fg_SqliteAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " IN ";
			fg_SqliteAppendSubquerySql(_Appender, _Predicate.m_pSubqueryStatement);
			break;
		case ESqlPredicateType::mc_Exists:
			_Appender += "EXISTS ";
			fg_SqliteAppendSubquerySql(_Appender, _Predicate.m_pSubqueryStatement);
			break;
		case ESqlPredicateType::mc_NotExists:
			_Appender += "NOT EXISTS ";
			fg_SqliteAppendSubquerySql(_Appender, _Predicate.m_pSubqueryStatement);
			break;
		case ESqlPredicateType::mc_And:
		case ESqlPredicateType::mc_Or:
			_Appender += "(";
			fg_SqliteAppendPredicateSql(_Appender, _Predicate.m_Children[0]);
			_Appender += _Predicate.m_Type == ESqlPredicateType::mc_And ? " AND " : " OR ";
			fg_SqliteAppendPredicateSql(_Appender, _Predicate.m_Children[1]);
			_Appender += ")";
			break;
		case ESqlPredicateType::mc_Not:
			_Appender += "(NOT ";
			fg_SqliteAppendPredicateSql(_Appender, _Predicate.m_Children[0]);
			_Appender += ")";
			break;
		case ESqlPredicateType::mc_AllRows:
			_Appender += "1 = 1";
			break;
		}
	}

	void fg_SqliteAppendNumber(NStr::CStr::CAppender &_Appender, umint _Value)
	{
		using namespace NStr;

		{
			auto Committed = _Appender.f_Commit();
			Committed.m_String += "{}"_f << _Value;
		}
	}

	void fg_SqliteAppendTableAlias(NStr::CStr::CAppender &_Appender, umint _iTable)
	{
		_Appender += "t";
		fg_SqliteAppendNumber(_Appender, _iTable);
	}

	void fg_SqliteAppendQualifiedColumn(NStr::CStr::CAppender &_Appender, CSqlQualifiedColumnDescription const &_Column)
	{
		fg_SqliteAppendTableAlias(_Appender, _Column.m_iTable);
		_Appender += ".";
		fg_SqliteAppendQuotedIdentifier(_Appender, _Column.m_ColumnName);
	}

	void fg_SqliteAppendSelectExpressionSql(NStr::CStr::CAppender &_Appender, CSqlSelectExpressionDescription const &_Expression)
	{
		auto AppendBinary = [&_Appender, &_Expression](NStr::CStr const &_Operator)
		{
			_Appender += "(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_LeftColumnName);
			_Appender += _Operator;
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_RightColumnName);
			_Appender += ")";
		};

		switch (_Expression.m_Type)
		{
		case ESqlSelectExpressionType::mc_Column:
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			break;
		case ESqlSelectExpressionType::mc_Count:
			_Appender += "COUNT(*)";
			break;
		case ESqlSelectExpressionType::mc_Sum:
			_Appender += "SUM(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Avg:
			_Appender += "AVG(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Min:
			_Appender += "MIN(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Max:
			_Appender += "MAX(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Add:
			AppendBinary(" + ");
			break;
		case ESqlSelectExpressionType::mc_Subtract:
			AppendBinary(" - ");
			break;
		case ESqlSelectExpressionType::mc_Multiply:
			AppendBinary(" * ");
			break;
		case ESqlSelectExpressionType::mc_Divide:
			_Appender += "(CAST(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_LeftColumnName);
			_Appender += " AS REAL) / ";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_RightColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Lower:
			_Appender += "LOWER(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Upper:
			_Appender += "UPPER(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Length:
			_Appender += "LENGTH(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_CastFloat:
			_Appender += "CAST(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += " AS REAL)";
			break;
		case ESqlSelectExpressionType::mc_BackendFunction:
			_Appender += _Expression.m_FunctionName;
			_Appender += "(";
			fg_SqliteAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		}
	}

	void fg_SqliteAppendLimitOffsetSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement)
	{
		if (_Statement.m_LimitOffset.m_bHasLimit)
			_Appender += " LIMIT ?";
		else if (_Statement.m_LimitOffset.m_bHasOffset)
			_Appender += " LIMIT -1";

		if (_Statement.m_LimitOffset.m_bHasOffset)
			_Appender += " OFFSET ?";
	}

	void fg_SqliteAppendSelectSourceSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement)
	{
		_Appender += " FROM ";
		fg_SqliteAppendQuotedIdentifier(_Appender, _Statement.m_TableName);
		if (!_Statement.m_Joins.f_IsEmpty())
		{
			_Appender += " AS ";
			fg_SqliteAppendTableAlias(_Appender, 0);
			for (umint i = 0; i < _Statement.m_Joins.f_GetLen(); ++i)
			{
				auto const &Join = _Statement.m_Joins[i];
				_Appender += Join.m_Type == ESqlJoinType::mc_Inner ? " INNER JOIN " : " LEFT JOIN ";
				fg_SqliteAppendQuotedIdentifier(_Appender, Join.m_TableName);
				_Appender += " AS ";
				fg_SqliteAppendTableAlias(_Appender, i + 1);
				_Appender += " ON ";
				for (umint iOn = 0; iOn < Join.m_On.f_GetLen(); ++iOn)
				{
					if (iOn != 0)
						_Appender += " AND ";

					auto const &On = Join.m_On[iOn];
					fg_SqliteAppendTableAlias(_Appender, On.m_iLeftTable);
					_Appender += ".";
					fg_SqliteAppendQuotedIdentifier(_Appender, On.m_LeftColumnName);
					fg_SqliteAppendCompareOperator(_Appender, On.m_Type);
					fg_SqliteAppendTableAlias(_Appender, On.m_iRightTable);
					_Appender += ".";
					fg_SqliteAppendQuotedIdentifier(_Appender, On.m_RightColumnName);
				}
			}
		}
		_Appender += " WHERE ";
		fg_SqliteAppendPredicateSql(_Appender, _Statement.m_Predicate);
	}

	void fg_SqliteAppendSelectGroupHavingSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement)
	{
		if (!_Statement.m_GroupBy.f_IsEmpty())
		{
			_Appender += " GROUP BY ";
			for (umint i = 0; i < _Statement.m_GroupBy.f_GetLen(); ++i)
			{
				if (i != 0)
					_Appender += ", ";

				fg_SqliteAppendQuotedIdentifier(_Appender, _Statement.m_GroupBy[i].m_ColumnName);
			}
		}

		if (_Statement.m_bHasHaving)
		{
			_Appender += " HAVING ";
			fg_SqliteAppendPredicateSql(_Appender, _Statement.m_Having);
		}
	}

	void fg_SqliteAppendSelectSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement)
	{
		_Appender += "SELECT ";
		if (_Statement.m_bDistinct)
			_Appender += "DISTINCT ";

		umint nSelectColumns = !_Statement.m_SelectExpressions.f_IsEmpty()
			? _Statement.m_SelectExpressions.f_GetLen()
			: (!_Statement.m_QualifiedSelectColumns.f_IsEmpty() ? _Statement.m_QualifiedSelectColumns.f_GetLen() : _Statement.m_SelectColumns.f_GetLen())
		;
		for (umint i = 0; i < nSelectColumns; ++i)
		{
			if (i != 0)
				_Appender += ", ";

			if (!_Statement.m_SelectExpressions.f_IsEmpty())
				fg_SqliteAppendSelectExpressionSql(_Appender, _Statement.m_SelectExpressions[i]);
			else if (!_Statement.m_QualifiedSelectColumns.f_IsEmpty())
				fg_SqliteAppendQualifiedColumn(_Appender, _Statement.m_QualifiedSelectColumns[i]);
			else
				fg_SqliteAppendQuotedIdentifier(_Appender, _Statement.m_SelectColumns[i]);
		}

		fg_SqliteAppendSelectSourceSql(_Appender, _Statement);

		fg_SqliteAppendSelectGroupHavingSql(_Appender, _Statement);

		if (!_Statement.m_OrderBy.f_IsEmpty())
		{
			_Appender += " ORDER BY ";
			for (umint i = 0; i < _Statement.m_OrderBy.f_GetLen(); ++i)
			{
				if (i != 0)
					_Appender += ", ";

				fg_SqliteAppendQuotedIdentifier(_Appender, _Statement.m_OrderBy[i].m_ColumnName);
				_Appender += _Statement.m_OrderBy[i].m_bDescending ? " DESC" : " ASC";
			}
		}

		fg_SqliteAppendLimitOffsetSql(_Appender, _Statement);

		// The ORDER BY/LIMIT/OFFSET emitted above belongs to the left operand (f_Describe copies the left operand's
		// clauses into the top-level statement). fg_SqliteValidateSelectStatement rejects modifiers on any operand,
		// so they never reach SQLite here, and the typed builder has no compound-level ORDER BY/LIMIT.
		for (auto const &SetOperation : _Statement.m_SetOperations)
		{
			fg_SqliteAppendSetOperationType(_Appender, SetOperation.m_Type);
			fg_SqliteAppendSetOperandSql
				(
					_Appender
					, SetOperation.m_pStatement->f_Describe()
					, [&](CSqlPreparedSelectStatementDescription const &_Operand)
					{
						fg_SqliteAppendSelectSql(_Appender, _Operand);
					}
				)
			;
		}
	}

	void fg_SqliteAppendSelectProjectionSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement)
	{
		umint nSelectColumns = !_Statement.m_SelectExpressions.f_IsEmpty()
			? _Statement.m_SelectExpressions.f_GetLen()
			: (!_Statement.m_QualifiedSelectColumns.f_IsEmpty() ? _Statement.m_QualifiedSelectColumns.f_GetLen() : _Statement.m_SelectColumns.f_GetLen())
		;
		for (umint i = 0; i < nSelectColumns; ++i)
		{
			if (i != 0)
				_Appender += ", ";

			if (!_Statement.m_SelectExpressions.f_IsEmpty())
				fg_SqliteAppendSelectExpressionSql(_Appender, _Statement.m_SelectExpressions[i]);
			else if (!_Statement.m_QualifiedSelectColumns.f_IsEmpty())
				fg_SqliteAppendQualifiedColumn(_Appender, _Statement.m_QualifiedSelectColumns[i]);
			else
				fg_SqliteAppendQuotedIdentifier(_Appender, _Statement.m_SelectColumns[i]);
		}
	}

	void fg_SqliteAppendSelectExistenceProjectionSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement, bool _bPreserveProjection = false)
	{
		_Appender += "SELECT ";
		if (_Statement.m_bDistinct || _bPreserveProjection || !_Statement.m_SetOperations.f_IsEmpty() || fg_SqlSelectIsUngroupedAggregate(_Statement))
		{
			if (_Statement.m_bDistinct)
				_Appender += "DISTINCT ";

			fg_SqliteAppendSelectProjectionSql(_Appender, _Statement);
		}
		else
			_Appender += "1";

		fg_SqliteAppendSelectSourceSql(_Appender, _Statement);
		fg_SqliteAppendSelectGroupHavingSql(_Appender, _Statement);

		for (auto const &SetOperation : _Statement.m_SetOperations)
		{
			fg_SqliteAppendSetOperationType(_Appender, SetOperation.m_Type);
			fg_SqliteAppendSetOperandSql
				(
					_Appender
					, SetOperation.m_pStatement->f_Describe()
					, [&](CSqlPreparedSelectStatementDescription const &_Operand)
					{
						fg_SqliteAppendSelectExistenceProjectionSql(_Appender, _Operand, true);
					}
				)
			;
		}
	}

	NStr::CStr fg_SqliteSelectSql(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			fg_SqliteAppendSelectSql(Appender, _Statement);
		}

		return Sql;
	}

	NStr::CStr fg_SqliteSelectCountSql(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "SELECT COUNT(*) FROM (";
			fg_SqliteAppendSelectExistenceProjectionSql(Appender, _Statement);
			Appender += ") AS ";
			fg_SqliteAppendQuotedIdentifier(Appender, "malterlib_count");
		}

		return Sql;
	}

	NStr::CStr fg_SqliteSelectExistsSql(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "SELECT EXISTS(";
			fg_SqliteAppendSelectExistenceProjectionSql(Appender, _Statement);
			Appender += ")";
		}

		return Sql;
	}

	bool fg_SqliteUsesSingleConnection(NStr::CStr const &_Path)
	{
		// ":memory:" and an empty path both open a database that is private to each connection - an in-memory
		// database and an anonymous temporary on-disk database respectively. A read pool would then open separate
		// databases from the write connection and never observe its schema or data, so both must run on a single
		// shared connection.
		return _Path == ":memory:" || _Path.f_IsEmpty();
	}
}
