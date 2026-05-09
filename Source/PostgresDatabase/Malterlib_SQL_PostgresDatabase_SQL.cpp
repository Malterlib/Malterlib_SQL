// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_PostgresDatabase_Internal.h"

namespace NMib::NSQL::NPrivate
{
	void fg_PostgresAppendQuotedIdentifier(NStr::CStr::CAppender &_Appender, NStr::CStr const &_Identifier)
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

	void fg_PostgresAppendParameterIndex(NStr::CStr::CAppender &_Appender, umint _iParameter);

	NStr::CStr fg_PostgresColumnType(CSqlColumnDescription const &_Column)
	{
		if (fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
			return NStr::gc_Str<"BIGSERIAL">;

		switch (_Column.m_Type)
		{
		case ESqlColumnType::mc_Integer8: return NStr::gc_Str<"SMALLINT">;
		case ESqlColumnType::mc_Integer16: return NStr::gc_Str<"SMALLINT">;
		case ESqlColumnType::mc_Integer32: return NStr::gc_Str<"INTEGER">;
		case ESqlColumnType::mc_Integer64: return NStr::gc_Str<"BIGINT">;
		case ESqlColumnType::mc_UnsignedInteger8: return NStr::gc_Str<"SMALLINT">;
		case ESqlColumnType::mc_UnsignedInteger16: return NStr::gc_Str<"INTEGER">;
		case ESqlColumnType::mc_UnsignedInteger32: return NStr::gc_Str<"BIGINT">;
		case ESqlColumnType::mc_UnsignedInteger64: return NStr::gc_Str<"BIGINT">;
		case ESqlColumnType::mc_Float32: return NStr::gc_Str<"REAL">;
		case ESqlColumnType::mc_Float64: return NStr::gc_Str<"DOUBLE PRECISION">;
		case ESqlColumnType::mc_Boolean: return NStr::gc_Str<"BOOLEAN">;
		case ESqlColumnType::mc_Blob: return NStr::gc_Str<"BYTEA">;
		case ESqlColumnType::mc_Time: return NStr::gc_Str<"TIMESTAMPTZ">;
		case ESqlColumnType::mc_UUID: return NStr::gc_Str<"UUID">;
		case ESqlColumnType::mc_Date: return NStr::gc_Str<"DATE">;
		case ESqlColumnType::mc_TimeOfDay: return NStr::gc_Str<"TIME">;
		case ESqlColumnType::mc_Timestamp: return NStr::gc_Str<"TIMESTAMP">;
		case ESqlColumnType::mc_TimestampTz: return NStr::gc_Str<"TIMESTAMPTZ">;
		case ESqlColumnType::mc_Interval: return NStr::gc_Str<"INTERVAL">;
		case ESqlColumnType::mc_Json: return NStr::gc_Str<"JSON">;
		case ESqlColumnType::mc_Jsonb: return NStr::gc_Str<"JSONB">;
		case ESqlColumnType::mc_Array_Integer16: return NStr::gc_Str<"SMALLINT[]">;
		case ESqlColumnType::mc_Array_Integer32: return NStr::gc_Str<"INTEGER[]">;
		case ESqlColumnType::mc_Array_Integer64: return NStr::gc_Str<"BIGINT[]">;
		case ESqlColumnType::mc_Array_Float32: return NStr::gc_Str<"REAL[]">;
		case ESqlColumnType::mc_Array_Float64: return NStr::gc_Str<"DOUBLE PRECISION[]">;
		case ESqlColumnType::mc_Array_Text: return NStr::gc_Str<"TEXT[]">;
		case ESqlColumnType::mc_Array_Boolean: return NStr::gc_Str<"BOOLEAN[]">;
		case ESqlColumnType::mc_Array_Bytes: return NStr::gc_Str<"BYTEA[]">;
		case ESqlColumnType::mc_Array_Date: return NStr::gc_Str<"DATE[]">;
		case ESqlColumnType::mc_Array_TimeOfDay: return NStr::gc_Str<"TIME[]">;
		case ESqlColumnType::mc_Array_Timestamp: return NStr::gc_Str<"TIMESTAMP[]">;
		case ESqlColumnType::mc_Array_TimestampTz: return NStr::gc_Str<"TIMESTAMPTZ[]">;
		case ESqlColumnType::mc_Array_UUID: return NStr::gc_Str<"UUID[]">;
		case ESqlColumnType::mc_Array_Json: return NStr::gc_Str<"JSON[]">;
		case ESqlColumnType::mc_Array_Jsonb: return NStr::gc_Str<"JSONB[]">;
		case ESqlColumnType::mc_Array_Interval: return NStr::gc_Str<"INTERVAL[]">;
		default: return NStr::gc_Str<"TEXT">;
		}
	}

	NStr::CStr const &fg_PostgresColumnDefaultSql(CSqlColumnDescription const &_Column)
	{
		CNonPortableColumnOptions const *pNonPortableOptions = _Column.f_NonPortableOptions(NStr::gc_Str<"postgres">.m_Str);
		if (pNonPortableOptions && !pNonPortableOptions->m_pDefaultSql->f_IsEmpty())
			return *pNonPortableOptions->m_pDefaultSql;

		return _Column.f_DefaultSql();
	}

	bool fg_PostgresColumnIsGenerated(CSqlColumnDescription const &_Column)
	{
		CNonPortableColumnOptions const *pNonPortableOptions = _Column.f_NonPortableOptions(NStr::gc_Str<"postgres">.m_Str);
		return pNonPortableOptions && !pNonPortableOptions->m_pGeneratedSql->f_IsEmpty();
	}

	void fg_PostgresAppendColumnDefinition(NStr::CStr::CAppender &_Appender, CSqlColumnDescription const &_Column)
	{
		CNonPortableColumnOptions const *pNonPortableOptions = _Column.f_NonPortableOptions(NStr::gc_Str<"postgres">.m_Str);

		fg_PostgresAppendQuotedIdentifier(_Appender, _Column.f_Name());
		_Appender += " ";
		_Appender += fg_PostgresColumnType(_Column);

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

		if (!_Column.f_IsNullable() && !fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_PrimaryKey))
			_Appender += " NOT NULL";

		// A primary key is already unique, so only emit UNIQUE for a non-primary-key column carrying the flag.
		// Without this the schema checksum records a uniqueness requirement the database would not enforce.
		if (fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_Unique) && !fg_IsSet(_Column.m_Flags, ESqlColumnFlag::mc_PrimaryKey))
			_Appender += " UNIQUE";

		NStr::CStr const &DefaultSql = fg_PostgresColumnDefaultSql(_Column);
		if (!DefaultSql.f_IsEmpty())
		{
			_Appender += " DEFAULT ";

			if (_Column.m_Type == ESqlColumnType::mc_Boolean && DefaultSql == "0")
				_Appender += "FALSE";
			else if (_Column.m_Type == ESqlColumnType::mc_Boolean && DefaultSql == "1")
				_Appender += "TRUE";
			else
				_Appender += DefaultSql;
		}

		if (pNonPortableOptions && !pNonPortableOptions->m_pCustomSql->f_IsEmpty())
		{
			_Appender += " ";
			_Appender += *pNonPortableOptions->m_pCustomSql;
		}
	}

	void fg_PostgresAppendForeignKeyAction(NStr::CStr::CAppender &_Appender, ESqlForeignKeyAction _Action)
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

	void fg_PostgresAppendColumnList(NStr::CStr::CAppender &_Appender, NContainer::TCVector<NStr::CStr const *> const &_Columns)
	{
		_Appender += "(";
		for (umint iColumn = 0; iColumn < _Columns.f_GetLen(); ++iColumn)
		{
			if (iColumn != 0)
				_Appender += ", ";

			fg_PostgresAppendQuotedIdentifier(_Appender, *_Columns[iColumn]);
		}
		_Appender += ")";
	}

	void fg_PostgresAppendConstraintDefinition(NStr::CStr::CAppender &_Appender, CSqlConstraintDescription const &_Constraint)
	{
		_Appender += "CONSTRAINT ";
		fg_PostgresAppendQuotedIdentifier(_Appender, _Constraint.f_Name());
		_Appender += " ";

		switch (_Constraint.m_Type)
		{
		case ESqlConstraintType::mc_PrimaryKey:
			_Appender += "PRIMARY KEY ";
			fg_PostgresAppendColumnList(_Appender, _Constraint.m_Columns);
			break;
		case ESqlConstraintType::mc_Unique:
			_Appender += "UNIQUE ";
			fg_PostgresAppendColumnList(_Appender, _Constraint.m_Columns);
			break;
		case ESqlConstraintType::mc_Check:
			_Appender += "CHECK (";
			_Appender += _Constraint.f_CheckSql();
			_Appender += ")";
			break;
		case ESqlConstraintType::mc_ForeignKey:
			_Appender += "FOREIGN KEY ";
			fg_PostgresAppendColumnList(_Appender, _Constraint.m_Columns);
			_Appender += " REFERENCES ";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Constraint.f_ReferencedTable());
			_Appender += " ";
			fg_PostgresAppendColumnList(_Appender, _Constraint.m_ReferencedColumns);
			if (_Constraint.m_OnDelete != ESqlForeignKeyAction::mc_Default)
			{
				_Appender += " ON DELETE";
				fg_PostgresAppendForeignKeyAction(_Appender, _Constraint.m_OnDelete);
			}
			if (_Constraint.m_OnUpdate != ESqlForeignKeyAction::mc_Default)
			{
				_Appender += " ON UPDATE";
				fg_PostgresAppendForeignKeyAction(_Appender, _Constraint.m_OnUpdate);
			}
			break;
		}
	}

	NStr::CStr fg_PostgresColumnDefinition(CSqlColumnDescription const &_Column)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			fg_PostgresAppendColumnDefinition(Appender, _Column);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresCreateTable(CSqlTableDescription const &_Table, bool _bIfNotExists, bool _bIncludeForeignKeys)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "CREATE TABLE ";
			if (_bIfNotExists)
				Appender += "IF NOT EXISTS ";

			fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " (";

			bool bNeedSeparator = false;
			for (umint i = 0; i < _Table.m_Columns.f_GetLen(); ++i)
			{
				if (bNeedSeparator)
					Appender += ", ";

				fg_PostgresAppendColumnDefinition(Appender, _Table.m_Columns[i]);
				bNeedSeparator = true;
			}

			for (auto const &Constraint : _Table.m_Constraints)
			{
				// Foreign keys may reference tables created later (or form a cycle). PostgreSQL resolves the
				// referenced relation when the constraint is created, so callers that build a whole schema defer
				// foreign keys to ALTER TABLE statements emitted once every table exists.
				if (!_bIncludeForeignKeys && Constraint.m_Type == ESqlConstraintType::mc_ForeignKey)
					continue;

				if (bNeedSeparator)
					Appender += ", ";

				fg_PostgresAppendConstraintDefinition(Appender, Constraint);
				bNeedSeparator = true;
			}

			Appender += ")";
		}

		return Sql;
	}

	NStr::CStr fg_PostgresCreateIndex(CSqlTableDescription const &_Table, CSqlIndexDescription const &_Index, bool _bIfNotExists)
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

			fg_PostgresAppendQuotedIdentifier(Appender, _Index.f_Name());
			Appender += " ON ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " (";

			for (umint i = 0; i < _Index.m_Columns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_PostgresAppendQuotedIdentifier(Appender, *_Index.m_Columns[i]);
			}

			Appender += ")";
		}

		return Sql;
	}

	NStr::CStr fg_PostgresAlterTableAddColumn(CSqlTableDescription const &_Table, CSqlColumnDescription const &_Column)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "ALTER TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " ADD COLUMN ";
			fg_PostgresAppendColumnDefinition(Appender, _Column);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresAlterTableAddConstraint(CSqlTableDescription const &_Table, CSqlConstraintDescription const &_Constraint)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "ALTER TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Table.f_Name());
			Appender += " ADD ";
			fg_PostgresAppendConstraintDefinition(Appender, _Constraint);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresRenameTable(CSqlSchemaMigrationOperationDescription const &_Operation)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "ALTER TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pOldName);
			Appender += " RENAME TO ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pNewName);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresRenameColumn(CSqlSchemaMigrationOperationDescription const &_Operation)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "ALTER TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pTableName);
			Appender += " RENAME COLUMN ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pOldName);
			Appender += " TO ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pNewName);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresDropTable(NStr::CStr const &_TableName)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "DROP TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, _TableName);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresDropColumn(CSqlSchemaMigrationOperationDescription const &_Operation)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "ALTER TABLE ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pTableName);
			Appender += " DROP COLUMN ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pOldName);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresUpdateColumnSql(CSqlSchemaMigrationOperationDescription const &_Operation)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "UPDATE ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pTableName);
			Appender += " SET ";
			fg_PostgresAppendQuotedIdentifier(Appender, *_Operation.m_pNewName);
			Appender += " = ";
			Appender += *_Operation.m_pSql;
		}

		return Sql;
	}

	NContainer::TCVector<NStr::CStr> fg_PostgresCreateSchemaStatements(CSqlSchemaVersionDescription const &_Schema)
	{
		NContainer::TCVector<NStr::CStr> Statements;

		// Create every table (and its indexes) without foreign keys first, then add the foreign keys once all
		// referenced tables exist. This keeps schema creation independent of table declaration order and supports
		// reference cycles, which an inline foreign key could never satisfy.
		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			Statements.f_InsertLast(fg_PostgresCreateTable(Table, true, false));

			for (auto const &Index : Table.m_Indexes)
				Statements.f_InsertLast(fg_PostgresCreateIndex(Table, Index, true));
		}

		for (auto const &Table : _Schema.m_Database.m_Tables)
		{
			for (auto const &Constraint : Table.m_Constraints)
			{
				if (Constraint.m_Type == ESqlConstraintType::mc_ForeignKey)
					Statements.f_InsertLast(fg_PostgresAlterTableAddConstraint(Table, Constraint));
			}
		}

		return Statements;
	}

	NStr::CStr fg_PostgresCreateSchemaVersionTableSql()
	{
		return
			"CREATE TABLE IF NOT EXISTS \"schema_migrations\" (\"id\" TEXT PRIMARY KEY, \"name\" TEXT NOT NULL, \"checksum\" TEXT NOT NULL, "
			"\"applied_at\" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, \"applied_by_version\" TEXT NOT NULL)"
		;
	}

	NStr::CStr fg_PostgresReadSchemaVersionsSql()
	{
		return NStr::gc_Str<"SELECT \"id\", \"checksum\" FROM \"schema_migrations\" ORDER BY \"applied_at\", \"id\"">;
	}

	NStr::CStr fg_PostgresHasTableSql()
	{
		return NStr::gc_Str<"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1">;
	}

	NStr::CStr fg_PostgresHasColumnSql()
	{
		return NStr::gc_Str<"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2">;
	}

	NStr::CStr fg_PostgresHasConstraintSql()
	{
		return NStr::gc_Str<"SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = 'public' AND table_name = $1 AND constraint_name = $2">;
	}

	NStr::CStr fg_PostgresInsertSchemaVersionSql()
	{
		return NStr::gc_Str<"INSERT INTO \"schema_migrations\" (\"id\", \"name\", \"checksum\", \"applied_by_version\") VALUES ($1, $2, $3, $4) ON CONFLICT (\"id\") DO NOTHING">;
	}

	NStr::CStr fg_PostgresInsertSql(CSqlInsertOperation const &_Operation)
	{
		CSqlPreparedInsertStatementDescription Description;
		NContainer::TCVector<NStr::CStr> const *pColumns = nullptr;
		NStr::CStr Sql;
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

		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "INSERT INTO ";
			fg_PostgresAppendQuotedIdentifier(Appender, Description.m_TableName);

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

					fg_PostgresAppendQuotedIdentifier(Appender, (*pColumns)[i]);
				}

				Appender += ") VALUES (";

				for (umint i = 0; i < pColumns->f_GetLen(); ++i)
				{
					if (i != 0)
						Appender += ", ";

					fg_PostgresAppendParameterIndex(Appender, i);
				}

				Appender += ")";
			}

			if (_Operation.m_bReturning)
			{
				Appender += " RETURNING ";
				fg_PostgresAppendQuotedIdentifier(Appender, _Operation.m_ReturningColumnName);
			}
		}

		return Sql;
	}

	NStr::CStr fg_PostgresUpsertSql(CSqlPreparedUpsertStatementDescription const &_Description);

	NStr::CStr fg_PostgresUpsertSql(CSqlUpsertOperation const &_Operation)
	{
		CSqlPreparedUpsertStatementDescription Description = _Operation.m_pDescription->m_pStatement->f_Describe();

		NStr::CStr Sql = fg_PostgresUpsertSql(Description);
		if (_Operation.m_bReturning)
		{
			Sql += " RETURNING ";
			NStr::CStr::CAppender Appender(Sql);
			fg_PostgresAppendQuotedIdentifier(Appender, _Operation.m_ReturningColumnName);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresUpsertSql(CSqlPreparedUpsertStatementDescription const &_Description)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "INSERT INTO ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Description.m_TableName);
			Appender += " (";

			for (umint i = 0; i < _Description.m_InsertColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_PostgresAppendQuotedIdentifier(Appender, _Description.m_InsertColumns[i]);
			}

			Appender += ") VALUES (";

			for (umint i = 0; i < _Description.m_InsertColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_PostgresAppendParameterIndex(Appender, i);
			}

			Appender += ") ON CONFLICT (";

			for (umint i = 0; i < _Description.m_ConflictColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_PostgresAppendQuotedIdentifier(Appender, _Description.m_ConflictColumns[i]);
			}

			Appender += ") DO UPDATE SET ";

			for (umint i = 0; i < _Description.m_UpdateColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_PostgresAppendQuotedIdentifier(Appender, _Description.m_UpdateColumns[i]);
				Appender += " = excluded.";
				fg_PostgresAppendQuotedIdentifier(Appender, _Description.m_UpdateColumns[i]);
			}
		}

		return Sql;
	}

	void fg_PostgresAppendParameterIndex(NStr::CStr::CAppender &_Appender, umint _iParameter)
	{
		using namespace NStr;

		_Appender += "$";
		{
			auto Committed = _Appender.f_Commit();
			Committed.m_String += "{}"_f << (_iParameter + 1);
		}
	}

	void fg_PostgresAppendPredicateSql(NStr::CStr::CAppender &_Appender, CSqlPredicateDescription const &_Predicate, umint _iParameterOffset = 0);

	NStr::CStr fg_PostgresUpdateSql(CSqlPreparedUpdateStatementDescription const &_Description)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "UPDATE ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Description.m_TableName);
			Appender += " SET ";

			for (umint i = 0; i < _Description.m_UpdateColumns.f_GetLen(); ++i)
			{
				if (i != 0)
					Appender += ", ";

				fg_PostgresAppendQuotedIdentifier(Appender, _Description.m_UpdateColumns[i]);
				Appender += " = ";
				fg_PostgresAppendParameterIndex(Appender, i);
			}

			Appender += " WHERE ";
			fg_PostgresAppendPredicateSql(Appender, _Description.m_Predicate, _Description.m_UpdateColumns.f_GetLen());
		}

		return Sql;
	}

	NStr::CStr fg_PostgresUpdateSql(CSqlUpdateOperation const &_Operation)
	{
		CSqlPreparedUpdateStatementDescription Description = _Operation.m_pDescription->m_pStatement->f_Describe();

		NStr::CStr Sql = fg_PostgresUpdateSql(Description);
		if (_Operation.m_bReturning)
		{
			Sql += " RETURNING ";
			NStr::CStr::CAppender Appender(Sql);
			fg_PostgresAppendQuotedIdentifier(Appender, _Operation.m_ReturningColumnName);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresDeleteSql(CSqlPreparedDeleteStatementDescription const &_Description)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "DELETE FROM ";
			fg_PostgresAppendQuotedIdentifier(Appender, _Description.m_TableName);
			Appender += " WHERE ";
			fg_PostgresAppendPredicateSql(Appender, _Description.m_Predicate);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresDeleteSql(CSqlDeleteOperation const &_Operation)
	{
		CSqlPreparedDeleteStatementDescription Description = _Operation.m_pDescription->m_pStatement->f_Describe();

		NStr::CStr Sql = fg_PostgresDeleteSql(Description);
		if (_Operation.m_bReturning)
		{
			Sql += " RETURNING ";
			NStr::CStr::CAppender Appender(Sql);
			fg_PostgresAppendQuotedIdentifier(Appender, _Operation.m_ReturningColumnName);
		}

		return Sql;
	}

	void fg_PostgresAppendNumber(NStr::CStr::CAppender &_Appender, umint _Value)
	{
		using namespace NStr;

		{
			auto Committed = _Appender.f_Commit();
			Committed.m_String += "{}"_f << _Value;
		}
	}

	void fg_PostgresAppendTableAlias(NStr::CStr::CAppender &_Appender, umint _iTable)
	{
		_Appender += "t";
		fg_PostgresAppendNumber(_Appender, _iTable);
	}

	void fg_PostgresAppendQualifiedColumn(NStr::CStr::CAppender &_Appender, CSqlQualifiedColumnDescription const &_Column)
	{
		fg_PostgresAppendTableAlias(_Appender, _Column.m_iTable);
		_Appender += ".";
		fg_PostgresAppendQuotedIdentifier(_Appender, _Column.m_ColumnName);
	}

	void fg_PostgresAppendSelectExpressionSql(NStr::CStr::CAppender &_Appender, CSqlSelectExpressionDescription const &_Expression);
	umint fg_PostgresSelectLocalParameterCount(CSqlPreparedSelectStatementDescription const &_Statement);
	void fg_PostgresAppendSelectSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement, umint _iParameterOffset, umint _nWhereParameterCount);

	void fg_PostgresAppendSubquerySql(NStr::CStr::CAppender &_Appender, ICSqlPreparedSelectStatement const *_pStatement, umint _iParameterOffset)
	{
		DMibCheck(_pStatement != nullptr);
		auto Statement = _pStatement->f_Describe();

		_Appender += "(";
		fg_PostgresAppendSelectSql(_Appender, Statement, _iParameterOffset, fg_PostgresSelectLocalParameterCount(Statement));
		_Appender += ")";
	}

	void fg_PostgresAppendSetOperationType(NStr::CStr::CAppender &_Appender, ESqlSetOperationType _Type)
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

	void fg_PostgresAppendPredicateSubject(NStr::CStr::CAppender &_Appender, CSqlPredicateDescription const &_Predicate)
	{
		if (_Predicate.m_bExpression)
			fg_PostgresAppendSelectExpressionSql(_Appender, _Predicate.m_Expression);
		else
			fg_PostgresAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
	}

	void fg_PostgresAppendCompareOperator(NStr::CStr::CAppender &_Appender, ESqlPredicateType _Type)
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

	void fg_PostgresAppendPredicateSql(NStr::CStr::CAppender &_Appender, CSqlPredicateDescription const &_Predicate, umint _iParameterOffset)
	{
		switch (_Predicate.m_Type)
		{
		case ESqlPredicateType::mc_EqualParameter:
			fg_PostgresAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " = ";
			fg_PostgresAppendParameterIndex(_Appender, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_NotEqualParameter:
			fg_PostgresAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " <> ";
			fg_PostgresAppendParameterIndex(_Appender, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_LessParameter:
			fg_PostgresAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " < ";
			fg_PostgresAppendParameterIndex(_Appender, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_LessEqualParameter:
			fg_PostgresAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " <= ";
			fg_PostgresAppendParameterIndex(_Appender, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_GreaterParameter:
			fg_PostgresAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " > ";
			fg_PostgresAppendParameterIndex(_Appender, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_GreaterEqualParameter:
			fg_PostgresAppendPredicateSubject(_Appender, _Predicate);
			_Appender += " >= ";
			fg_PostgresAppendParameterIndex(_Appender, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_LikeParameter:
			fg_PostgresAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " LIKE ";
			fg_PostgresAppendParameterIndex(_Appender, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_IsNull:
			fg_PostgresAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " IS NULL";
			break;
		case ESqlPredicateType::mc_IsNotNull:
			fg_PostgresAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " IS NOT NULL";
			break;
		case ESqlPredicateType::mc_InParameters:
			fg_PostgresAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " IN (";
			for (umint i = 0; i < _Predicate.m_nParameters; ++i)
			{
				if (i != 0)
					_Appender += ", ";

				fg_PostgresAppendParameterIndex(_Appender, _iParameterOffset + _Predicate.m_iParameter + i);
			}
			_Appender += ")";
			break;
		case ESqlPredicateType::mc_InSubquery:
			fg_PostgresAppendQuotedIdentifier(_Appender, _Predicate.m_ColumnName);
			_Appender += " IN ";
			fg_PostgresAppendSubquerySql(_Appender, _Predicate.m_pSubqueryStatement, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_Exists:
			_Appender += "EXISTS ";
			fg_PostgresAppendSubquerySql(_Appender, _Predicate.m_pSubqueryStatement, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_NotExists:
			_Appender += "NOT EXISTS ";
			fg_PostgresAppendSubquerySql(_Appender, _Predicate.m_pSubqueryStatement, _iParameterOffset + _Predicate.m_iParameter);
			break;
		case ESqlPredicateType::mc_And:
		case ESqlPredicateType::mc_Or:
			_Appender += "(";
			fg_PostgresAppendPredicateSql(_Appender, _Predicate.m_Children[0], _iParameterOffset);
			_Appender += _Predicate.m_Type == ESqlPredicateType::mc_And ? " AND " : " OR ";
			fg_PostgresAppendPredicateSql(_Appender, _Predicate.m_Children[1], _iParameterOffset);
			_Appender += ")";
			break;
		case ESqlPredicateType::mc_Not:
			_Appender += "(NOT ";
			fg_PostgresAppendPredicateSql(_Appender, _Predicate.m_Children[0], _iParameterOffset);
			_Appender += ")";
			break;
		case ESqlPredicateType::mc_AllRows:
			_Appender += "TRUE";
			break;
		}
	}

	void fg_PostgresAppendLimitOffsetSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement, umint _iParameterOffset, umint _nWhereParameterCount)
	{
		umint iParam = _iParameterOffset + _nWhereParameterCount;
		if (_Statement.m_LimitOffset.m_bHasLimit)
		{
			_Appender += " LIMIT ";
			fg_PostgresAppendParameterIndex(_Appender, iParam++);
		}

		if (_Statement.m_LimitOffset.m_bHasOffset)
		{
			_Appender += " OFFSET ";
			fg_PostgresAppendParameterIndex(_Appender, iParam++);
		}
	}

	void fg_PostgresAppendSelectSourceSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement, umint _iParameterOffset = 0)
	{
		_Appender += " FROM ";
		fg_PostgresAppendQuotedIdentifier(_Appender, _Statement.m_TableName);
		if (!_Statement.m_Joins.f_IsEmpty())
		{
			_Appender += " AS ";
			fg_PostgresAppendTableAlias(_Appender, 0);
			for (umint i = 0; i < _Statement.m_Joins.f_GetLen(); ++i)
			{
				auto const &Join = _Statement.m_Joins[i];
				_Appender += Join.m_Type == ESqlJoinType::mc_Inner ? " INNER JOIN " : " LEFT JOIN ";
				fg_PostgresAppendQuotedIdentifier(_Appender, Join.m_TableName);
				_Appender += " AS ";
				fg_PostgresAppendTableAlias(_Appender, i + 1);
				_Appender += " ON ";
				for (umint iOn = 0; iOn < Join.m_On.f_GetLen(); ++iOn)
				{
					if (iOn != 0)
						_Appender += " AND ";

					auto const &On = Join.m_On[iOn];
					fg_PostgresAppendTableAlias(_Appender, On.m_iLeftTable);
					_Appender += ".";
					fg_PostgresAppendQuotedIdentifier(_Appender, On.m_LeftColumnName);
					fg_PostgresAppendCompareOperator(_Appender, On.m_Type);
					fg_PostgresAppendTableAlias(_Appender, On.m_iRightTable);
					_Appender += ".";
					fg_PostgresAppendQuotedIdentifier(_Appender, On.m_RightColumnName);
				}
			}
		}
		_Appender += " WHERE ";
		fg_PostgresAppendPredicateSql(_Appender, _Statement.m_Predicate, _iParameterOffset);
	}

	void fg_PostgresAppendSelectGroupHavingSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement, umint _iParameterOffset)
	{
		if (!_Statement.m_GroupBy.f_IsEmpty())
		{
			_Appender += " GROUP BY ";
			for (umint i = 0; i < _Statement.m_GroupBy.f_GetLen(); ++i)
			{
				if (i != 0)
					_Appender += ", ";

				fg_PostgresAppendQuotedIdentifier(_Appender, _Statement.m_GroupBy[i].m_ColumnName);
			}
		}

		if (_Statement.m_bHasHaving)
		{
			_Appender += " HAVING ";
			fg_PostgresAppendPredicateSql(_Appender, _Statement.m_Having, _iParameterOffset);
		}
	}

	void fg_PostgresAppendSelectExpressionSql(NStr::CStr::CAppender &_Appender, CSqlSelectExpressionDescription const &_Expression)
	{
		auto AppendBinary = [&_Appender, &_Expression](NStr::CStr const &_Operator)
		{
			_Appender += "(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_LeftColumnName);
			_Appender += _Operator;
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_RightColumnName);
			_Appender += ")";
		};

		switch (_Expression.m_Type)
		{
		case ESqlSelectExpressionType::mc_Column:
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			break;
		case ESqlSelectExpressionType::mc_Count:
			_Appender += "COUNT(*)";
			break;
		case ESqlSelectExpressionType::mc_Sum:
			// PostgreSQL promotes SUM over a bigint-backed column (int64/uint32/uint64) to NUMERIC, which the typed
			// int64 decoder rejects (OID 1700 is not an accepted integer type). Cast integer sums back to BIGINT -
			// the wire type the declared int64 projection decodes - so they map cleanly; SMALLINT/INTEGER sums
			// already yield BIGINT, making the cast a no-op. Float sums keep their double-precision result, matching
			// the explicit cast AVG applies below.
			if (_Expression.m_ResultType == ESqlValueType::mc_Integer64)
			{
				_Appender += "CAST(SUM(";
				fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
				_Appender += ") AS BIGINT)";
			}
			else
			{
				_Appender += "SUM(";
				fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
				_Appender += ")";
			}
			break;
		case ESqlSelectExpressionType::mc_Avg:
			_Appender += "AVG(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")::double precision";
			break;
		case ESqlSelectExpressionType::mc_Min:
			_Appender += "MIN(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Max:
			_Appender += "MAX(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
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
			_Appender += "(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_LeftColumnName);
			_Appender += "::double precision / ";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_RightColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Lower:
			_Appender += "LOWER(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Upper:
			_Appender += "UPPER(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_Length:
			_Appender += "LENGTH(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		case ESqlSelectExpressionType::mc_CastFloat:
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += "::double precision";
			break;
		case ESqlSelectExpressionType::mc_BackendFunction:
			_Appender += _Expression.m_FunctionName;
			_Appender += "(";
			fg_PostgresAppendQuotedIdentifier(_Appender, _Expression.m_ColumnName);
			_Appender += ")";
			break;
		}
	}

	void fg_PostgresAppendSelectProjectionSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement)
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
				fg_PostgresAppendSelectExpressionSql(_Appender, _Statement.m_SelectExpressions[i]);
			else if (!_Statement.m_QualifiedSelectColumns.f_IsEmpty())
				fg_PostgresAppendQualifiedColumn(_Appender, _Statement.m_QualifiedSelectColumns[i]);
			else
				fg_PostgresAppendQuotedIdentifier(_Appender, _Statement.m_SelectColumns[i]);
		}
	}

	umint fg_PostgresSelectLocalParameterCount(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		umint nParameters = _Statement.m_Predicate.m_nParameters;
		if (_Statement.m_bHasHaving)
			nParameters += _Statement.m_Having.m_nParameters;

		return nParameters;
	}

	umint fg_PostgresSelectParameterCount(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		umint nParameters = fg_PostgresSelectLocalParameterCount(_Statement);
		if (_Statement.m_LimitOffset.m_bHasLimit)
			++nParameters;
		if (_Statement.m_LimitOffset.m_bHasOffset)
			++nParameters;

		for (auto const &SetOperation : _Statement.m_SetOperations)
			nParameters += fg_PostgresSelectParameterCount(SetOperation.m_pStatement->f_Describe());

		return nParameters;
	}

	umint fg_PostgresSelectNoLimitParameterCount(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		umint nParameters = fg_PostgresSelectLocalParameterCount(_Statement);
		for (auto const &SetOperation : _Statement.m_SetOperations)
			nParameters += fg_PostgresSelectNoLimitParameterCount(SetOperation.m_pStatement->f_Describe());

		return nParameters;
	}

	auto fg_PostgresAppendSelectExistenceProjectionSql
		(
			NStr::CStr::CAppender &_Appender
			, CSqlPreparedSelectStatementDescription const &_Statement
			, umint _iParameterOffset
			, bool _bPreserveProjection = false
		)
		-> void
	{
		_Appender += "SELECT ";
		if (_Statement.m_bDistinct || _bPreserveProjection || !_Statement.m_SetOperations.f_IsEmpty() || fg_SqlSelectIsUngroupedAggregate(_Statement))
		{
			if (_Statement.m_bDistinct)
				_Appender += "DISTINCT ";

			fg_PostgresAppendSelectProjectionSql(_Appender, _Statement);
		}
		else
			_Appender += "1";

		fg_PostgresAppendSelectSourceSql(_Appender, _Statement, _iParameterOffset);
		fg_PostgresAppendSelectGroupHavingSql(_Appender, _Statement, _iParameterOffset);

		umint iSetParameterOffset = _iParameterOffset + fg_PostgresSelectLocalParameterCount(_Statement);
		for (auto const &SetOperation : _Statement.m_SetOperations)
		{
			auto SetStatement = SetOperation.m_pStatement->f_Describe();
			fg_PostgresAppendSetOperationType(_Appender, SetOperation.m_Type);
			fg_PostgresAppendSelectExistenceProjectionSql(_Appender, SetStatement, iSetParameterOffset, true);
			iSetParameterOffset += fg_PostgresSelectNoLimitParameterCount(SetStatement);
		}
	}

	void fg_PostgresAppendSelectSql(NStr::CStr::CAppender &_Appender, CSqlPreparedSelectStatementDescription const &_Statement, umint _iParameterOffset, umint _nWhereParameterCount)
	{
		_Appender += "SELECT ";
		if (_Statement.m_bDistinct)
			_Appender += "DISTINCT ";

		fg_PostgresAppendSelectProjectionSql(_Appender, _Statement);

		fg_PostgresAppendSelectSourceSql(_Appender, _Statement, _iParameterOffset);

		fg_PostgresAppendSelectGroupHavingSql(_Appender, _Statement, _iParameterOffset);

		if (!_Statement.m_OrderBy.f_IsEmpty())
		{
			_Appender += " ORDER BY ";
			for (umint i = 0; i < _Statement.m_OrderBy.f_GetLen(); ++i)
			{
				if (i != 0)
					_Appender += ", ";

				fg_PostgresAppendQuotedIdentifier(_Appender, _Statement.m_OrderBy[i].m_ColumnName);
				_Appender += _Statement.m_OrderBy[i].m_bDescending ? " DESC" : " ASC";
			}
		}

		fg_PostgresAppendLimitOffsetSql(_Appender, _Statement, _iParameterOffset, _nWhereParameterCount);

		// Set-operation operands are appended unparenthesized on purpose. The ORDER BY/LIMIT/OFFSET emitted above
		// belongs to the left operand (f_Describe copies the left operand's clauses into the top-level statement),
		// and any operand - left (top-level modifiers) or right (a modified m_SetOperations entry) - that carries
		// those modifiers is rejected by fg_PostgresValidateSelectStatement before this generated SQL is ever
		// executed. The typed builder also exposes no way to attach a trailing modifier to the whole compound, so
		// reaching this loop with modifiers present always means an invalid operand, never a legitimate
		// compound-level ORDER BY/LIMIT. Do not "fix" this by parenthesizing operands - that would make an
		// already-rejected shape look supported. The limit/offset placeholder bump below only ever matters for
		// that already-rejected shape: a valid compound has no top-level LIMIT/OFFSET, so operand $n numbering
		// stays correct without it.
		umint iSetParameterOffset = _iParameterOffset + fg_PostgresSelectLocalParameterCount(_Statement);
		if (_Statement.m_LimitOffset.m_bHasLimit)
			++iSetParameterOffset;
		if (_Statement.m_LimitOffset.m_bHasOffset)
			++iSetParameterOffset;

		for (auto const &SetOperation : _Statement.m_SetOperations)
		{
			auto SetStatement = SetOperation.m_pStatement->f_Describe();
			fg_PostgresAppendSetOperationType(_Appender, SetOperation.m_Type);
			fg_PostgresAppendSelectSql(_Appender, SetStatement, iSetParameterOffset, fg_PostgresSelectLocalParameterCount(SetStatement));
			iSetParameterOffset += fg_PostgresSelectParameterCount(SetStatement);
		}
	}

	NStr::CStr fg_PostgresSelectSql(CSqlPreparedSelectStatementDescription const &_Statement, umint _nWhereParameterCount)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			fg_PostgresAppendSelectSql(Appender, _Statement, 0, _nWhereParameterCount);
		}

		return Sql;
	}

	NStr::CStr fg_PostgresSelectCountSql(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "SELECT COUNT(*) FROM (";
			fg_PostgresAppendSelectExistenceProjectionSql(Appender, _Statement, 0);
			Appender += ") AS ";
			fg_PostgresAppendQuotedIdentifier(Appender, "malterlib_count");
		}

		return Sql;
	}

	NStr::CStr fg_PostgresSelectExistsSql(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			Appender += "SELECT EXISTS(";
			fg_PostgresAppendSelectExistenceProjectionSql(Appender, _Statement, 0);
			Appender += ")";
		}

		return Sql;
	}

	NStr::CStr fg_PostgresPreparedSelectName(CSqlQueryID _QueryID)
	{
		return NStr::CStr::CFormat("MalterlibSelectCache_{}") << _QueryID.m_Value;
	}

	NStr::CStr fg_PostgresPreparedInsertName(CSqlQueryID _QueryID)
	{
		return NStr::CStr::CFormat("MalterlibInsertCache_{}") << _QueryID.m_Value;
	}

	NStr::CStr fg_PostgresPreparedUpdateName(CSqlQueryID _QueryID)
	{
		return NStr::CStr::CFormat("MalterlibUpdateCache_{}") << _QueryID.m_Value;
	}

	NStr::CStr fg_PostgresPreparedDeleteName(CSqlQueryID _QueryID)
	{
		return NStr::CStr::CFormat("MalterlibDeleteCache_{}") << _QueryID.m_Value;
	}

	NStr::CStr fg_PostgresPreparedUpsertName(CSqlQueryID _QueryID)
	{
		return NStr::CStr::CFormat("MalterlibUpsertCache_{}") << _QueryID.m_Value;
	}

	CPostgresDate fg_PostgresElement(CSqlDate &&_Value)
	{
		return {.m_Time = fg_Move(_Value.m_Time)};
	}

	CPostgresTime fg_PostgresElement(CSqlTimeOfDay &&_Value)
	{
		return {.m_Time = fg_Move(_Value.m_Time)};
	}

	CPostgresTimestamp fg_PostgresElement(CSqlTimestamp &&_Value)
	{
		return {.m_Time = fg_Move(_Value.m_Time)};
	}

	CPostgresTimestampTz fg_PostgresElement(CSqlTimestampTz &&_Value)
	{
		return {.m_Time = fg_Move(_Value.m_Time)};
	}

	CPostgresInterval fg_PostgresElement(CSqlInterval &&_Value)
	{
		return {.m_Months = _Value.m_Months, .m_Days = _Value.m_Days, .m_Time = fg_Move(_Value.m_Time)};
	}

	CPostgresUnrecognizedValue fg_PostgresElement(CSqlUnrecognizedBackendValue &&_Value)
	{
		return {.m_TypeOID = _Value.m_TypeID, .m_Bytes = fg_Move(_Value.m_Bytes)};
	}

	template <typename t_CValue>
	t_CValue fg_PostgresElement(t_CValue &&_Value)
	{
		return fg_Move(_Value);
	}

	CSqlDate fg_SqlElement(CPostgresDate &&_Value)
	{
		return {.m_Time = fg_Move(_Value.m_Time)};
	}

	CSqlTimeOfDay fg_SqlElement(CPostgresTime &&_Value)
	{
		return {.m_Time = fg_Move(_Value.m_Time)};
	}

	CSqlTimestamp fg_SqlElement(CPostgresTimestamp &&_Value)
	{
		return {.m_Time = fg_Move(_Value.m_Time)};
	}

	CSqlTimestampTz fg_SqlElement(CPostgresTimestampTz &&_Value)
	{
		return {.m_Time = fg_Move(_Value.m_Time)};
	}

	CSqlInterval fg_SqlElement(CPostgresInterval &&_Value)
	{
		return {.m_Months = _Value.m_Months, .m_Days = _Value.m_Days, .m_Time = fg_Move(_Value.m_Time)};
	}

	CSqlUnrecognizedBackendValue fg_SqlElement(CPostgresUnrecognizedValue &&_Value)
	{
		return {.m_TypeID = _Value.m_TypeOID, .m_Bytes = fg_Move(_Value.m_Bytes)};
	}

	template <typename t_CValue>
	t_CValue fg_SqlElement(t_CValue &&_Value)
	{
		return fg_Move(_Value);
	}

	template <typename t_CTo, typename t_CFrom>
	TCPostgresArray<t_CTo> fg_PostgresArrayFromSql(TCSqlArray<t_CFrom> &&_Array)
	{
		TCPostgresArray<t_CTo> Result;
		for (auto &Dimension : _Array.m_Dimensions)
			Result.m_Dimensions.f_InsertLast({.m_Length = Dimension.m_Length, .m_LowerBound = Dimension.m_LowerBound});

		for (auto &Value : _Array.m_Values)
		{
			if (Value)
				Result.m_Values.f_InsertLast(fg_PostgresElement(fg_Move(*Value)));
			else
				Result.m_Values.f_InsertLast(NStorage::TCOptional<t_CTo>());
		}

		return Result;
	}

	template <typename t_CTo, typename t_CFrom>
	TCSqlArray<t_CTo> fg_SqlArrayFromPostgres(TCPostgresArray<t_CFrom> &&_Array)
	{
		TCSqlArray<t_CTo> Result;
		for (auto &Dimension : _Array.m_Dimensions)
			Result.m_Dimensions.f_InsertLast({.m_Length = Dimension.m_Length, .m_LowerBound = Dimension.m_LowerBound});

		for (auto &Value : _Array.m_Values)
		{
			if (Value)
				Result.m_Values.f_InsertLast(fg_SqlElement(fg_Move(*Value)));
			else
				Result.m_Values.f_InsertLast(NStorage::TCOptional<t_CTo>());
		}

		return Result;
	}

	NConcurrency::TCWrapped<CPostgresValue> fg_PostgresValue(CSqlValue &&_Value)
	{
		switch (_Value.f_GetTypeID())
		{
		case ESqlValueType::mc_Null: return CPostgresValue();
		case ESqlValueType::mc_Integer8: return int16(_Value.f_GetAsType<int8>());
		case ESqlValueType::mc_Integer16: return _Value.f_GetAsType<int16>();
		case ESqlValueType::mc_Integer32: return _Value.f_GetAsType<int32>();
		case ESqlValueType::mc_Integer64: return _Value.f_GetAsType<int64>();
		case ESqlValueType::mc_UnsignedInteger8: return int16(_Value.f_GetAsType<uint8>());
		case ESqlValueType::mc_UnsignedInteger16: return int32(_Value.f_GetAsType<uint16>());
		case ESqlValueType::mc_UnsignedInteger32: return int64(_Value.f_GetAsType<uint32>());
		case ESqlValueType::mc_UnsignedInteger64:
			{
				uint64 Value = _Value.f_GetAsType<uint64>();
				if (Value > uint64(TCLimitsInt<int64>::mc_Max))
					return DMibErrorDatabaseInstance("PostgreSQL unsigned integer parameter exceeds signed 64-bit database range");

				return int64(Value);
			}
		case ESqlValueType::mc_Float32: return _Value.f_GetAsType<fp32>();
		case ESqlValueType::mc_Float64: return _Value.f_GetAsType<fp64>();
		case ESqlValueType::mc_Text: return fg_Move(_Value.f_GetAsType<NStr::CStr>());
		case ESqlValueType::mc_Blob:
			return fg_Move(_Value.f_GetAsType<NContainer::CIOByteVector>());
		case ESqlValueType::mc_Boolean: return _Value.f_GetAsType<bool>();
		case ESqlValueType::mc_Time: return CPostgresTimestampTz{.m_Time = fg_Move(_Value.f_GetAsType<NTime::CTime>())};
		case ESqlValueType::mc_UUID: return fg_Move(_Value.f_GetAsType<NCryptography::CUniversallyUniqueIdentifier>());
		case ESqlValueType::mc_Date: return fg_PostgresElement(fg_Move(_Value.f_GetAsType<CSqlDate>()));
		case ESqlValueType::mc_TimeOfDay: return fg_PostgresElement(fg_Move(_Value.f_GetAsType<CSqlTimeOfDay>()));
		case ESqlValueType::mc_Timestamp: return fg_PostgresElement(fg_Move(_Value.f_GetAsType<CSqlTimestamp>()));
		case ESqlValueType::mc_TimestampTz: return fg_PostgresElement(fg_Move(_Value.f_GetAsType<CSqlTimestampTz>()));
		case ESqlValueType::mc_Interval: return fg_PostgresElement(fg_Move(_Value.f_GetAsType<CSqlInterval>()));
		case ESqlValueType::mc_Json: return fg_Move(_Value.f_GetAsType<NEncoding::CJsonOrdered>());
		case ESqlValueType::mc_Jsonb: return fg_Move(_Value.f_GetAsType<NEncoding::CJsonSorted>());
		case ESqlValueType::mc_UnrecognizedBackend: return fg_PostgresElement(fg_Move(_Value.f_GetAsType<CSqlUnrecognizedBackendValue>()));
		case ESqlValueType::mc_Array_Integer16: return fg_PostgresArrayFromSql<int16>(fg_Move(_Value.f_GetAsType<TCSqlArray<int16>>()));
		case ESqlValueType::mc_Array_Integer32: return fg_PostgresArrayFromSql<int32>(fg_Move(_Value.f_GetAsType<TCSqlArray<int32>>()));
		case ESqlValueType::mc_Array_Integer64: return fg_PostgresArrayFromSql<int64>(fg_Move(_Value.f_GetAsType<TCSqlArray<int64>>()));
		case ESqlValueType::mc_Array_Float32: return fg_PostgresArrayFromSql<fp32>(fg_Move(_Value.f_GetAsType<TCSqlArray<fp32>>()));
		case ESqlValueType::mc_Array_Float64: return fg_PostgresArrayFromSql<fp64>(fg_Move(_Value.f_GetAsType<TCSqlArray<fp64>>()));
		case ESqlValueType::mc_Array_Text: return fg_PostgresArrayFromSql<NStr::CStr>(fg_Move(_Value.f_GetAsType<TCSqlArray<NStr::CStr>>()));
		case ESqlValueType::mc_Array_Boolean: return fg_PostgresArrayFromSql<bool>(fg_Move(_Value.f_GetAsType<TCSqlArray<bool>>()));
		case ESqlValueType::mc_Array_Bytes: return fg_PostgresArrayFromSql<NContainer::CIOByteVector>(fg_Move(_Value.f_GetAsType<TCSqlArray<NContainer::CIOByteVector>>()));
		case ESqlValueType::mc_Array_Date: return fg_PostgresArrayFromSql<CPostgresDate>(fg_Move(_Value.f_GetAsType<TCSqlArray<CSqlDate>>()));
		case ESqlValueType::mc_Array_TimeOfDay: return fg_PostgresArrayFromSql<CPostgresTime>(fg_Move(_Value.f_GetAsType<TCSqlArray<CSqlTimeOfDay>>()));
		case ESqlValueType::mc_Array_Timestamp: return fg_PostgresArrayFromSql<CPostgresTimestamp>(fg_Move(_Value.f_GetAsType<TCSqlArray<CSqlTimestamp>>()));
		case ESqlValueType::mc_Array_TimestampTz: return fg_PostgresArrayFromSql<CPostgresTimestampTz>(fg_Move(_Value.f_GetAsType<TCSqlArray<CSqlTimestampTz>>()));
		case ESqlValueType::mc_Array_UUID:
			return fg_PostgresArrayFromSql<NCryptography::CUniversallyUniqueIdentifier>(fg_Move(_Value.f_GetAsType<TCSqlArray<NCryptography::CUniversallyUniqueIdentifier>>()));
		case ESqlValueType::mc_Array_Json: return fg_PostgresArrayFromSql<NEncoding::CJsonOrdered>(fg_Move(_Value.f_GetAsType<TCSqlArray<NEncoding::CJsonOrdered>>()));
		case ESqlValueType::mc_Array_Jsonb: return fg_PostgresArrayFromSql<NEncoding::CJsonSorted>(fg_Move(_Value.f_GetAsType<TCSqlArray<NEncoding::CJsonSorted>>()));
		case ESqlValueType::mc_Array_Interval: return fg_PostgresArrayFromSql<CPostgresInterval>(fg_Move(_Value.f_GetAsType<TCSqlArray<CSqlInterval>>()));
		}

		DMibNeverGetHere;
		return CPostgresValue();
	}

	NConcurrency::TCWrapped<NContainer::TCVector<CPostgresValue>> fg_PostgresValues(NContainer::TCVector<CSqlValue> &&_Values)
	{
		NContainer::TCVector<CPostgresValue> Values;
		Values.f_Reserve(_Values.f_GetLen());
		for (auto &Value : _Values)
		{
			auto WrappedValue = fg_PostgresValue(fg_Move(Value));
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			Values.f_InsertLast(fg_Move(*WrappedValue));
		}

		return Values;
	}

	NConcurrency::TCWrapped<NContainer::TCVector<CPostgresValue>> fg_PostgresValues(NContainer::TCVector<CSqlColumnValue> &&_Values)
	{
		NContainer::TCVector<CPostgresValue> Values;
		Values.f_Reserve(_Values.f_GetLen());
		for (auto &Value : _Values)
		{
			auto WrappedValue = fg_PostgresValue(fg_Move(Value.m_Value));
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			Values.f_InsertLast(fg_Move(*WrappedValue));
		}

		return Values;
	}

	EPostgresValueType fg_PostgresValueType(ESqlValueType _ValueType)
	{
		switch (_ValueType)
		{
		case ESqlValueType::mc_Null: return EPostgresValueType::mc_Null;
		case ESqlValueType::mc_Integer8: return EPostgresValueType::mc_Integer16;
		case ESqlValueType::mc_Integer16: return EPostgresValueType::mc_Integer16;
		case ESqlValueType::mc_Integer32: return EPostgresValueType::mc_Integer32;
		case ESqlValueType::mc_Integer64: return EPostgresValueType::mc_Integer64;
		case ESqlValueType::mc_UnsignedInteger8: return EPostgresValueType::mc_Integer16;
		case ESqlValueType::mc_UnsignedInteger16: return EPostgresValueType::mc_Integer32;
		case ESqlValueType::mc_UnsignedInteger32: return EPostgresValueType::mc_Integer64;
		case ESqlValueType::mc_UnsignedInteger64: return EPostgresValueType::mc_Integer64;
		case ESqlValueType::mc_Float32: return EPostgresValueType::mc_Float32;
		case ESqlValueType::mc_Float64: return EPostgresValueType::mc_Float64;
		case ESqlValueType::mc_Text: return EPostgresValueType::mc_Text;
		case ESqlValueType::mc_Blob: return EPostgresValueType::mc_Bytes;
		case ESqlValueType::mc_Boolean: return EPostgresValueType::mc_Boolean;
		case ESqlValueType::mc_Time: return EPostgresValueType::mc_TimestampTz;
		case ESqlValueType::mc_UUID: return EPostgresValueType::mc_UUID;
		case ESqlValueType::mc_Date: return EPostgresValueType::mc_Date;
		case ESqlValueType::mc_TimeOfDay: return EPostgresValueType::mc_Time;
		case ESqlValueType::mc_Timestamp: return EPostgresValueType::mc_Timestamp;
		case ESqlValueType::mc_TimestampTz: return EPostgresValueType::mc_TimestampTz;
		case ESqlValueType::mc_Interval: return EPostgresValueType::mc_Interval;
		case ESqlValueType::mc_Json: return EPostgresValueType::mc_Json;
		case ESqlValueType::mc_Jsonb: return EPostgresValueType::mc_Jsonb;
		case ESqlValueType::mc_UnrecognizedBackend: return EPostgresValueType::mc_Unrecognized;
		case ESqlValueType::mc_Array_Integer16: return EPostgresValueType::mc_Array_Integer16;
		case ESqlValueType::mc_Array_Integer32: return EPostgresValueType::mc_Array_Integer32;
		case ESqlValueType::mc_Array_Integer64: return EPostgresValueType::mc_Array_Integer64;
		case ESqlValueType::mc_Array_Float32: return EPostgresValueType::mc_Array_Float32;
		case ESqlValueType::mc_Array_Float64: return EPostgresValueType::mc_Array_Float64;
		case ESqlValueType::mc_Array_Text: return EPostgresValueType::mc_Array_Text;
		case ESqlValueType::mc_Array_Boolean: return EPostgresValueType::mc_Array_Boolean;
		case ESqlValueType::mc_Array_Bytes: return EPostgresValueType::mc_Array_Bytes;
		case ESqlValueType::mc_Array_Date: return EPostgresValueType::mc_Array_Date;
		case ESqlValueType::mc_Array_TimeOfDay: return EPostgresValueType::mc_Array_Time;
		case ESqlValueType::mc_Array_Timestamp: return EPostgresValueType::mc_Array_Timestamp;
		case ESqlValueType::mc_Array_TimestampTz: return EPostgresValueType::mc_Array_TimestampTz;
		case ESqlValueType::mc_Array_UUID: return EPostgresValueType::mc_Array_UUID;
		case ESqlValueType::mc_Array_Json: return EPostgresValueType::mc_Array_Json;
		case ESqlValueType::mc_Array_Jsonb: return EPostgresValueType::mc_Array_Jsonb;
		case ESqlValueType::mc_Array_Interval: return EPostgresValueType::mc_Array_Interval;
		}

		DMibNeverGetHere;
		return EPostgresValueType::mc_Null;
	}

	NConcurrency::TCWrapped<int64> fg_PostgresIntegerValue(CPostgresValue const &_Value, NStr::CStr const &_ColumnName)
	{
		switch (_Value.f_GetTypeID())
		{
		case EPostgresValueType::mc_Integer16: return _Value.f_GetAsType<int16>();
		case EPostgresValueType::mc_Integer32: return _Value.f_GetAsType<int32>();
		case EPostgresValueType::mc_Integer64: return _Value.f_GetAsType<int64>();
		default:
			return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected integer value type {}") << _ColumnName << uint32(_Value.f_GetTypeID()));
		}
	}

	template <typename t_CInteger>
	NConcurrency::TCWrapped<CSqlValue> fg_PostgresSqlIntegerValue(CPostgresValue const &_Value, NStr::CStr const &_ColumnName)
	{
		auto WrappedValue = fg_PostgresIntegerValue(_Value, _ColumnName);
		if (!WrappedValue)
			return fg_Move(WrappedValue).f_GetException();

		int64 Value = *WrappedValue;

		if constexpr (NTraits::cIsSigned<t_CInteger>)
		{
			if (Value < fg_SqlSignedIntegerMin<t_CInteger>() || Value > fg_SqlSignedIntegerMax<t_CInteger>())
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' contains an integer outside target signed type range") << _ColumnName);
		}
		else
		{
			if (Value < 0)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' contains a negative value for an unsigned field") << _ColumnName);

			if (uint64(Value) > fg_SqlUnsignedIntegerMax<t_CInteger>())
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' contains an integer outside target unsigned type range") << _ColumnName);
		}

		return t_CInteger(Value);
	}

	template <typename t_CValue>
	auto fg_PostgresExpectedSqlValue
		(
			CPostgresValue &&_Value
			, CSqlRowFieldMapping const &_Field
			, EPostgresValueType _ExpectedType
			, NStr::CStr const &_ExpectedName
		)
		-> NConcurrency::TCWrapped<CSqlValue>
	{
		if (_Value.f_GetTypeID() != _ExpectedType)
		{
			return DMibErrorDatabaseInstance
				(
					NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected {} value type {}")
					<< _Field.m_ColumnName
					<< _ExpectedName
					<< uint32(_Value.f_GetTypeID())
				)
			;
		}

		return fg_Move(_Value.f_GetAsType<t_CValue>());
	}

	template <typename t_CSqlValue, typename t_CPostgresValue>
	auto fg_PostgresExpectedSqlConvertedValue
		(
			CPostgresValue &&_Value
			, CSqlRowFieldMapping const &_Field
			, EPostgresValueType _ExpectedType
			, NStr::CStr const &_ExpectedName
		)
		-> NConcurrency::TCWrapped<CSqlValue>
	{
		if (_Value.f_GetTypeID() != _ExpectedType)
		{
			return DMibErrorDatabaseInstance
				(
					NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected {} value type {}")
					<< _Field.m_ColumnName
					<< _ExpectedName
					<< uint32(_Value.f_GetTypeID())
				)
			;
		}

		return fg_SqlElement(fg_Move(_Value.f_GetAsType<t_CPostgresValue>()));
	}

	template <typename t_CSqlValue, typename t_CPostgresValue>
	auto fg_PostgresExpectedSqlArrayValue
		(
			CPostgresValue &&_Value
			, CSqlRowFieldMapping const &_Field
			, EPostgresValueType _ExpectedType
			, NStr::CStr const &_ExpectedName
		)
		-> NConcurrency::TCWrapped<CSqlValue>
	{
		if (_Value.f_GetTypeID() != _ExpectedType)
		{
			return DMibErrorDatabaseInstance
				(
					NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected {} value type {}")
					<< _Field.m_ColumnName
					<< _ExpectedName
					<< uint32(_Value.f_GetTypeID())
				)
			;
		}

		return fg_SqlArrayFromPostgres<t_CSqlValue>(fg_Move(_Value.f_GetAsType<TCPostgresArray<t_CPostgresValue>>()));
	}

	NConcurrency::TCWrapped<CSqlValue> fg_PostgresSqlValue(CPostgresValue &&_Value, CSqlRowFieldMapping const &_Field)
	{
		if (_Value.f_GetTypeID() == EPostgresValueType::mc_Null)
			return CSqlValue();

		switch (_Field.m_ValueType)
		{
		case ESqlValueType::mc_Integer8:
			return fg_PostgresSqlIntegerValue<int8>(_Value, _Field.m_ColumnName);
		case ESqlValueType::mc_Integer16:
			return fg_PostgresSqlIntegerValue<int16>(_Value, _Field.m_ColumnName);
		case ESqlValueType::mc_Integer32:
			return fg_PostgresSqlIntegerValue<int32>(_Value, _Field.m_ColumnName);
		case ESqlValueType::mc_Integer64:
			return fg_PostgresSqlIntegerValue<int64>(_Value, _Field.m_ColumnName);
		case ESqlValueType::mc_UnsignedInteger8:
			return fg_PostgresSqlIntegerValue<uint8>(_Value, _Field.m_ColumnName);
		case ESqlValueType::mc_UnsignedInteger16:
			return fg_PostgresSqlIntegerValue<uint16>(_Value, _Field.m_ColumnName);
		case ESqlValueType::mc_UnsignedInteger32:
			return fg_PostgresSqlIntegerValue<uint32>(_Value, _Field.m_ColumnName);
		case ESqlValueType::mc_UnsignedInteger64:
			return fg_PostgresSqlIntegerValue<uint64>(_Value, _Field.m_ColumnName);
		case ESqlValueType::mc_Float32:
			if (_Value.f_GetTypeID() == EPostgresValueType::mc_Float32)
				return _Value.f_GetAsType<fp32>();
			if (_Value.f_GetTypeID() == EPostgresValueType::mc_Float64)
			{
				fp64 Value = _Value.f_GetAsType<fp64>();
				if (!Value.f_IsNan() && !Value.f_IsInfinity() && (Value < fp64(fp32::fs_LimitMin()) || Value > fp64(fp32::fs_LimitMax())))
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' contains a floating point value outside target fp32 range") << _Field.m_ColumnName);

				return fp32(Value);
			}

			return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected float value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));
		case ESqlValueType::mc_Float64:
			if (_Value.f_GetTypeID() == EPostgresValueType::mc_Float32)
				return fp64(_Value.f_GetAsType<fp32>());
			if (_Value.f_GetTypeID() == EPostgresValueType::mc_Float64)
				return _Value.f_GetAsType<fp64>();

			return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected float value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));
		case ESqlValueType::mc_Text:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Text)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected text value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_Move(_Value.f_GetAsType<NStr::CStr>());
		case ESqlValueType::mc_Blob:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Bytes)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected blob value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_Move(_Value.f_GetAsType<NContainer::CIOByteVector>());
		case ESqlValueType::mc_Boolean:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Boolean)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected boolean value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return CSqlValue(_Value.f_GetAsType<bool>());
		case ESqlValueType::mc_Time:
			if (_Value.f_GetTypeID() == EPostgresValueType::mc_TimestampTz)
				return fg_Move(_Value.f_GetAsType<CPostgresTimestampTz>().m_Time);
			if (_Value.f_GetTypeID() == EPostgresValueType::mc_Timestamp)
				return fg_Move(_Value.f_GetAsType<CPostgresTimestamp>().m_Time);

			return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected time value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));
		case ESqlValueType::mc_UUID:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_UUID)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected UUID value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_Move(_Value.f_GetAsType<NCryptography::CUniversallyUniqueIdentifier>());
		case ESqlValueType::mc_Date:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Date)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected date value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresDate>()));
		case ESqlValueType::mc_TimeOfDay:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Time)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected time value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresTime>()));
		case ESqlValueType::mc_Timestamp:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Timestamp)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected timestamp value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresTimestamp>()));
		case ESqlValueType::mc_TimestampTz:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_TimestampTz)
			{
				return DMibErrorDatabaseInstance
					(
						NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected timestamptz value type {}")
						<< _Field.m_ColumnName
						<< uint32(_Value.f_GetTypeID())
					)
				;
			}

			return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresTimestampTz>()));
		case ESqlValueType::mc_Interval:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Interval)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected interval value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresInterval>()));
		case ESqlValueType::mc_Json:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Json)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected JSON value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_Move(_Value.f_GetAsType<NEncoding::CJsonOrdered>());
		case ESqlValueType::mc_Jsonb:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Jsonb)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected JSONB value type {}") << _Field.m_ColumnName << uint32(_Value.f_GetTypeID()));

			return fg_Move(_Value.f_GetAsType<NEncoding::CJsonSorted>());
		case ESqlValueType::mc_UnrecognizedBackend:
			if (_Value.f_GetTypeID() != EPostgresValueType::mc_Unrecognized)
			{
				return DMibErrorDatabaseInstance
					(
						NStr::CStr::CFormat("PostgreSQL column '{}' has unexpected unrecognized value type {}")
						<< _Field.m_ColumnName
						<< uint32(_Value.f_GetTypeID())
					)
				;
			}

			return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresUnrecognizedValue>()));
		case ESqlValueType::mc_Array_Integer16:
			return fg_PostgresExpectedSqlArrayValue<int16, int16>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Integer16, "smallint array");
		case ESqlValueType::mc_Array_Integer32:
			return fg_PostgresExpectedSqlArrayValue<int32, int32>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Integer32, "integer array");
		case ESqlValueType::mc_Array_Integer64:
			return fg_PostgresExpectedSqlArrayValue<int64, int64>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Integer64, "bigint array");
		case ESqlValueType::mc_Array_Float32:
			return fg_PostgresExpectedSqlArrayValue<fp32, fp32>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Float32, "real array");
		case ESqlValueType::mc_Array_Float64:
			return fg_PostgresExpectedSqlArrayValue<fp64, fp64>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Float64, "double precision array");
		case ESqlValueType::mc_Array_Text:
			return fg_PostgresExpectedSqlArrayValue<NStr::CStr, NStr::CStr>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Text, "text array");
		case ESqlValueType::mc_Array_Boolean:
			return fg_PostgresExpectedSqlArrayValue<bool, bool>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Boolean, "boolean array");
		case ESqlValueType::mc_Array_Bytes:
			return fg_PostgresExpectedSqlArrayValue<NContainer::CIOByteVector, NContainer::CIOByteVector>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Bytes, "bytea array");
		case ESqlValueType::mc_Array_Date:
			return fg_PostgresExpectedSqlArrayValue<CSqlDate, CPostgresDate>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Date, "date array");
		case ESqlValueType::mc_Array_TimeOfDay:
			return fg_PostgresExpectedSqlArrayValue<CSqlTimeOfDay, CPostgresTime>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Time, "time array");
		case ESqlValueType::mc_Array_Timestamp:
			return fg_PostgresExpectedSqlArrayValue<CSqlTimestamp, CPostgresTimestamp>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Timestamp, "timestamp array");
		case ESqlValueType::mc_Array_TimestampTz:
			return fg_PostgresExpectedSqlArrayValue<CSqlTimestampTz, CPostgresTimestampTz>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_TimestampTz, "timestamptz array");
		case ESqlValueType::mc_Array_UUID:
			return fg_PostgresExpectedSqlArrayValue<NCryptography::CUniversallyUniqueIdentifier, NCryptography::CUniversallyUniqueIdentifier>
				(
					fg_Move(_Value)
					, _Field
					, EPostgresValueType::mc_Array_UUID
					, "UUID array"
				)
			;
		case ESqlValueType::mc_Array_Json:
			return fg_PostgresExpectedSqlArrayValue<NEncoding::CJsonOrdered, NEncoding::CJsonOrdered>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Json, "JSON array");
		case ESqlValueType::mc_Array_Jsonb:
			return fg_PostgresExpectedSqlArrayValue<NEncoding::CJsonSorted, NEncoding::CJsonSorted>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Jsonb, "JSONB array");
		case ESqlValueType::mc_Array_Interval:
			return fg_PostgresExpectedSqlArrayValue<CSqlInterval, CPostgresInterval>(fg_Move(_Value), _Field, EPostgresValueType::mc_Array_Interval, "interval array");
		case ESqlValueType::mc_Null:
			return {};
		}

		DMibNeverGetHere;
		return {};
	}

	ESqlValueType fg_PostgresSqlValueTypeForBackendType(EPostgresValueType _Type)
	{
		switch (_Type)
		{
		case EPostgresValueType::mc_Null: return ESqlValueType::mc_Null;
		case EPostgresValueType::mc_Integer16: return ESqlValueType::mc_Integer16;
		case EPostgresValueType::mc_Integer32: return ESqlValueType::mc_Integer32;
		case EPostgresValueType::mc_Integer64: return ESqlValueType::mc_Integer64;
		case EPostgresValueType::mc_Float32: return ESqlValueType::mc_Float32;
		case EPostgresValueType::mc_Float64: return ESqlValueType::mc_Float64;
		case EPostgresValueType::mc_Text:
		case EPostgresValueType::mc_Varchar: return ESqlValueType::mc_Text;
		case EPostgresValueType::mc_Boolean: return ESqlValueType::mc_Boolean;
		case EPostgresValueType::mc_Bytes: return ESqlValueType::mc_Blob;
		case EPostgresValueType::mc_Date: return ESqlValueType::mc_Date;
		case EPostgresValueType::mc_Time: return ESqlValueType::mc_TimeOfDay;
		case EPostgresValueType::mc_Timestamp: return ESqlValueType::mc_Timestamp;
		case EPostgresValueType::mc_TimestampTz: return ESqlValueType::mc_TimestampTz;
		case EPostgresValueType::mc_UUID: return ESqlValueType::mc_UUID;
		case EPostgresValueType::mc_Json: return ESqlValueType::mc_Json;
		case EPostgresValueType::mc_Jsonb: return ESqlValueType::mc_Jsonb;
		case EPostgresValueType::mc_Interval: return ESqlValueType::mc_Interval;
		case EPostgresValueType::mc_Array_Integer16: return ESqlValueType::mc_Array_Integer16;
		case EPostgresValueType::mc_Array_Integer32: return ESqlValueType::mc_Array_Integer32;
		case EPostgresValueType::mc_Array_Integer64: return ESqlValueType::mc_Array_Integer64;
		case EPostgresValueType::mc_Array_Float32: return ESqlValueType::mc_Array_Float32;
		case EPostgresValueType::mc_Array_Float64: return ESqlValueType::mc_Array_Float64;
		case EPostgresValueType::mc_Array_Text:
		case EPostgresValueType::mc_Array_Varchar: return ESqlValueType::mc_Array_Text;
		case EPostgresValueType::mc_Array_Boolean: return ESqlValueType::mc_Array_Boolean;
		case EPostgresValueType::mc_Array_Bytes: return ESqlValueType::mc_Array_Bytes;
		case EPostgresValueType::mc_Array_Date: return ESqlValueType::mc_Array_Date;
		case EPostgresValueType::mc_Array_Time: return ESqlValueType::mc_Array_TimeOfDay;
		case EPostgresValueType::mc_Array_Timestamp: return ESqlValueType::mc_Array_Timestamp;
		case EPostgresValueType::mc_Array_TimestampTz: return ESqlValueType::mc_Array_TimestampTz;
		case EPostgresValueType::mc_Array_UUID: return ESqlValueType::mc_Array_UUID;
		case EPostgresValueType::mc_Array_Json: return ESqlValueType::mc_Array_Json;
		case EPostgresValueType::mc_Array_Jsonb: return ESqlValueType::mc_Array_Jsonb;
		case EPostgresValueType::mc_Array_Interval: return ESqlValueType::mc_Array_Interval;
		case EPostgresValueType::mc_Unrecognized: return ESqlValueType::mc_UnrecognizedBackend;
		}

		return ESqlValueType::mc_UnrecognizedBackend;
	}

	CSqlValue fg_PostgresSqlValueRaw(CPostgresValue &&_Value)
	{
		switch (_Value.f_GetTypeID())
		{
		case EPostgresValueType::mc_Null: return {};
		case EPostgresValueType::mc_Integer16: return _Value.f_GetAsType<int16>();
		case EPostgresValueType::mc_Integer32: return _Value.f_GetAsType<int32>();
		case EPostgresValueType::mc_Integer64: return _Value.f_GetAsType<int64>();
		case EPostgresValueType::mc_Float32: return _Value.f_GetAsType<fp32>();
		case EPostgresValueType::mc_Float64: return _Value.f_GetAsType<fp64>();
		case EPostgresValueType::mc_Text:
		case EPostgresValueType::mc_Varchar:
			return fg_Move(_Value.f_GetAsType<NStr::CStr>());
		case EPostgresValueType::mc_Boolean: return _Value.f_GetAsType<bool>();
		case EPostgresValueType::mc_Bytes: return fg_Move(_Value.f_GetAsType<NContainer::CIOByteVector>());
		case EPostgresValueType::mc_Date: return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresDate>()));
		case EPostgresValueType::mc_Time: return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresTime>()));
		case EPostgresValueType::mc_Timestamp: return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresTimestamp>()));
		case EPostgresValueType::mc_TimestampTz: return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresTimestampTz>()));
		case EPostgresValueType::mc_UUID: return fg_Move(_Value.f_GetAsType<NCryptography::CUniversallyUniqueIdentifier>());
		case EPostgresValueType::mc_Json: return fg_Move(_Value.f_GetAsType<NEncoding::CJsonOrdered>());
		case EPostgresValueType::mc_Jsonb: return fg_Move(_Value.f_GetAsType<NEncoding::CJsonSorted>());
		case EPostgresValueType::mc_Interval: return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresInterval>()));
		case EPostgresValueType::mc_Unrecognized: return fg_SqlElement(fg_Move(_Value.f_GetAsType<CPostgresUnrecognizedValue>()));
		case EPostgresValueType::mc_Array_Integer16: return fg_SqlArrayFromPostgres<int16>(fg_Move(_Value.f_GetAsType<TCPostgresArray<int16>>()));
		case EPostgresValueType::mc_Array_Integer32: return fg_SqlArrayFromPostgres<int32>(fg_Move(_Value.f_GetAsType<TCPostgresArray<int32>>()));
		case EPostgresValueType::mc_Array_Integer64: return fg_SqlArrayFromPostgres<int64>(fg_Move(_Value.f_GetAsType<TCPostgresArray<int64>>()));
		case EPostgresValueType::mc_Array_Float32: return fg_SqlArrayFromPostgres<fp32>(fg_Move(_Value.f_GetAsType<TCPostgresArray<fp32>>()));
		case EPostgresValueType::mc_Array_Float64: return fg_SqlArrayFromPostgres<fp64>(fg_Move(_Value.f_GetAsType<TCPostgresArray<fp64>>()));
		case EPostgresValueType::mc_Array_Text:
		case EPostgresValueType::mc_Array_Varchar:
			return fg_SqlArrayFromPostgres<NStr::CStr>(fg_Move(_Value.f_GetAsType<TCPostgresArray<NStr::CStr>>()));
		case EPostgresValueType::mc_Array_Boolean: return fg_SqlArrayFromPostgres<bool>(fg_Move(_Value.f_GetAsType<TCPostgresArray<bool>>()));
		case EPostgresValueType::mc_Array_Bytes: return fg_SqlArrayFromPostgres<NContainer::CIOByteVector>(fg_Move(_Value.f_GetAsType<TCPostgresArray<NContainer::CIOByteVector>>()));
		case EPostgresValueType::mc_Array_Date: return fg_SqlArrayFromPostgres<CSqlDate>(fg_Move(_Value.f_GetAsType<TCPostgresArray<CPostgresDate>>()));
		case EPostgresValueType::mc_Array_Time: return fg_SqlArrayFromPostgres<CSqlTimeOfDay>(fg_Move(_Value.f_GetAsType<TCPostgresArray<CPostgresTime>>()));
		case EPostgresValueType::mc_Array_Timestamp: return fg_SqlArrayFromPostgres<CSqlTimestamp>(fg_Move(_Value.f_GetAsType<TCPostgresArray<CPostgresTimestamp>>()));
		case EPostgresValueType::mc_Array_TimestampTz: return fg_SqlArrayFromPostgres<CSqlTimestampTz>(fg_Move(_Value.f_GetAsType<TCPostgresArray<CPostgresTimestampTz>>()));
		case EPostgresValueType::mc_Array_UUID:
			return fg_SqlArrayFromPostgres<NCryptography::CUniversallyUniqueIdentifier>(fg_Move(_Value.f_GetAsType<TCPostgresArray<NCryptography::CUniversallyUniqueIdentifier>>()));
		case EPostgresValueType::mc_Array_Json: return fg_SqlArrayFromPostgres<NEncoding::CJsonOrdered>(fg_Move(_Value.f_GetAsType<TCPostgresArray<NEncoding::CJsonOrdered>>()));
		case EPostgresValueType::mc_Array_Jsonb: return fg_SqlArrayFromPostgres<NEncoding::CJsonSorted>(fg_Move(_Value.f_GetAsType<TCPostgresArray<NEncoding::CJsonSorted>>()));
		case EPostgresValueType::mc_Array_Interval: return fg_SqlArrayFromPostgres<CSqlInterval>(fg_Move(_Value.f_GetAsType<TCPostgresArray<CPostgresInterval>>()));
		}

		return {};
	}

	NContainer::TCVector<CSqlRawColumnDescription> fg_PostgresRawColumns(CPostgresRowDescription const &_Description)
	{
		NContainer::TCVector<CSqlRawColumnDescription> Columns;
		Columns.f_Reserve(_Description.m_Fields.f_GetLen());
		for (auto const &Field : _Description.m_Fields)
		{
			CSqlRawColumnDescription Column;
			Column.m_Name = Field.m_Name;
			Column.m_BackendTypeID = Field.m_TypeOID;
			Column.m_ValueType = fg_PostgresSqlValueTypeForBackendType(EPostgresValueType(Field.m_TypeOID));
			Columns.f_InsertLast(fg_Move(Column));
		}

		return Columns;
	}

	CSqlRawRow fg_PostgresRawRow(CPostgresDataRow &&_Row)
	{
		CSqlRawRow RawRow;
		RawRow.m_Values.f_Reserve(_Row.m_Values.f_GetLen());
		for (auto &Value : _Row.m_Values)
			RawRow.m_Values.f_InsertLast(fg_PostgresSqlValueRaw(fg_Move(Value)));

		return RawRow;
	}

	CSqlRawResult fg_PostgresRawResult(CPostgresQueryResult &&_QueryResult)
	{
		CSqlRawResult Result;
		if (_QueryResult.m_RowDescription)
			Result.m_Columns = fg_PostgresRawColumns(*_QueryResult.m_RowDescription);

		Result.m_Rows.f_Reserve(_QueryResult.m_Rows.f_GetLen());
		for (auto &Row : _QueryResult.m_Rows)
			Result.m_Rows.f_InsertLast(fg_PostgresRawRow(fg_Move(Row)));

		return Result;
	}

	NConcurrency::TCWrapped<CSqlRowDataPointer> fg_PostgresMapRow(CPostgresDataRow &&_Row, CSqlRowMapping const &_Mapping)
	{
		if (_Row.m_Values.f_GetLen() != _Mapping.m_Fields.f_GetLen())
		{
			return DMibErrorDatabaseInstance
				(
					NStr::CStr::CFormat("PostgreSQL row value count {} does not match mapping field count {}")
					<< _Row.m_Values.f_GetLen()
					<< _Mapping.m_Fields.f_GetLen()
				)
			;
		}

		auto pRow = _Mapping.f_CreateRow();
		for (umint i = 0; i < _Mapping.m_Fields.f_GetLen(); ++i)
		{
			auto const &Field = _Mapping.m_Fields[i];
			auto *pMember = reinterpret_cast<uint8 *>(pRow.f_Get()) + Field.m_Offset;
			auto &Value = _Row.m_Values[i];
			auto WrappedSqlValue = fg_PostgresSqlValue(fg_Move(Value), Field);
			if (!WrappedSqlValue)
				return fg_Move(WrappedSqlValue).f_GetException();

			if (auto pException = Field.m_fSetValue(pMember, fg_Move(*WrappedSqlValue), Field.m_ColumnName))
				return pException;
		}

		return fg_Move(pRow);
	}
}
