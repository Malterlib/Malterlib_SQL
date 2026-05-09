// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/DatabaseSchema>

#include "../SourceGenerated/SQLite/sqlite3.h"

namespace NMib::NSQL::NPrivate
{
	namespace
	{
		// In a UTF-8 database PostgreSQL folds only ASCII letters of an unquoted name to lower case, and SQLite matches names
		// and keywords ignoring only ASCII case
		NStr::CStr fg_PostgresFoldIdentifier(NStr::CStr const &_Name)
		{
			NStr::CStr Folded = _Name;
			ch8 *pChar = Folded.f_GetStrUniqueWritable();
			for (aint iChar = 0; iChar < Folded.f_GetLen(); ++iChar)
			{
				if (pChar[iChar] >= 'A' && pChar[iChar] <= 'Z')
					pChar[iChar] = ch8(pChar[iChar] - 'A' + 'a');
			}

			return Folded;
		}

		bool fg_IsIdentifierStart(ch8 _Char)
		{
			return (_Char >= 'a' && _Char <= 'z') || (_Char >= 'A' && _Char <= 'Z') || _Char == '_' || uint8(_Char) >= 0x80;
		}

		bool fg_IsIdentifierChar(ch8 _Char)
		{
			return fg_IsIdentifierStart(_Char) || (_Char >= '0' && _Char <= '9') || _Char == '$';
		}

		// Calls _fOnToken(iStart, iEnd, Name, bQuoted) for each token that looks like an identifier, skipping the string literals and comments it recognizes.
		// Quoted identifiers use "", and in SQLite also `` or []; a doubled " or ` inside one stands for itself, while the first ] closes [].
		// PostgreSQL has dollar-quoted strings and uses [] for subscripts.
		// _bFromFirstParenthesis skips the identifiers before the first parenthesis, such as an index's name and table.
		template <typename tf_FOnToken>
		void fg_ForEachIdentifier(NStr::CStr const &_Sql, bool _bPostgres, bool _bFromFirstParenthesis, tf_FOnToken &&_fOnToken)
		{
			bool bReporting = !_bFromFirstParenthesis;
			ch8 const *pSql = _Sql.f_GetStr();
			aint Length = _Sql.f_GetLen();
			aint iChar = 0;
			while (iChar < Length)
			{
				ch8 Char = pSql[iChar];
				if (_bPostgres && (Char == 'E' || Char == 'e') && iChar + 1 < Length && pSql[iChar + 1] == '\'' && (iChar == 0 || !fg_IsIdentifierChar(pSql[iChar - 1])))
				{
					// In PostgreSQL E'...' strings a backslash-escaped quote does not end the literal
					for (iChar += 2; iChar < Length; ++iChar)
					{
						if (pSql[iChar] == '\\')
							++iChar;
						else if (pSql[iChar] == '\'')
						{
							if (iChar + 1 < Length && pSql[iChar + 1] == '\'')
								++iChar;
							else
								break;
						}
					}
					++iChar;
				}
				else if (_bPostgres && Char == '$' && (iChar == 0 || !fg_IsIdentifierChar(pSql[iChar - 1])))
				{
					aint iTagEnd = iChar + 1;
					while (iTagEnd < Length && fg_IsIdentifierChar(pSql[iTagEnd]) && pSql[iTagEnd] != '$')
						++iTagEnd;

					if (iTagEnd < Length && pSql[iTagEnd] == '$' && !(iTagEnd > iChar + 1 && pSql[iChar + 1] >= '0' && pSql[iChar + 1] <= '9'))
					{
						NStr::CStr Tag(pSql + iChar, iTagEnd + 1 - iChar);
						aint iBodyEnd = NStr::CStr(pSql + iTagEnd + 1, Length - iTagEnd - 1).f_Find(Tag);
						iChar = iBodyEnd < 0 ? Length : iTagEnd + 1 + iBodyEnd + Tag.f_GetLen();
					}
					else
						++iChar;
				}
				else if (Char == '\'')
				{
					for (++iChar; iChar < Length; ++iChar)
					{
						if (pSql[iChar] == '\'')
						{
							if (iChar + 1 < Length && pSql[iChar + 1] == '\'')
								++iChar;
							else
								break;
						}
					}
					++iChar;
				}
				else if (Char == '-' && iChar + 1 < Length && pSql[iChar + 1] == '-')
				{
					// PostgreSQL also ends the comment at a carriage return, SQLite only at a line feed
					while (iChar < Length && pSql[iChar] != '\n' && !(_bPostgres && pSql[iChar] == '\r'))
						++iChar;
				}
				else if (Char == '/' && iChar + 1 < Length && pSql[iChar + 1] == '*')
				{
					// PostgreSQL nests block comments
					umint nDepth = 1;
					iChar += 2;
					while (iChar < Length && nDepth > 0)
					{
						if (pSql[iChar] == '*' && iChar + 1 < Length && pSql[iChar + 1] == '/')
						{
							--nDepth;
							iChar += 2;
						}
						else if (_bPostgres && pSql[iChar] == '/' && iChar + 1 < Length && pSql[iChar + 1] == '*')
						{
							++nDepth;
							iChar += 2;
						}
						else
							++iChar;
					}
				}
				else if (Char == '"' || (!_bPostgres && (Char == '`' || Char == '[')))
				{
					ch8 Close = Char == '[' ? ']' : Char;
					aint iStart = iChar;
					NStr::CStr Name;
					for (++iChar; iChar < Length; ++iChar)
					{
						if (pSql[iChar] == Close)
						{
							if (Close != ']' && iChar + 1 < Length && pSql[iChar + 1] == Close)
								++iChar;
							else
								break;
						}

						Name.f_AddChar(pSql[iChar]);
					}
					++iChar;
					if (bReporting)
						_fOnToken(iStart, iChar < Length ? iChar : Length, Name, true);
				}
				else if (fg_IsIdentifierStart(Char))
				{
					aint iStart = iChar;
					while (iChar < Length && fg_IsIdentifierChar(pSql[iChar]))
						++iChar;
					if (bReporting)
						_fOnToken(iStart, iChar, NStr::CStr(pSql + iStart, iChar - iStart), false);
				}
				else if (Char >= '0' && Char <= '9')
				{
					while (iChar < Length && (fg_IsIdentifierChar(pSql[iChar]) || pSql[iChar] == '.'))
						++iChar;
				}
				else
				{
					bReporting = bReporting || Char == '(';
					++iChar;
				}
			}
		}
	}

	// The reserved and type or function name keywords, which quote_ident quotes
	constexpr ch8 const *gc_PostgresQuotedKeywords[] =
		{
			"all"
			, "analyse"
			, "analyze"
			, "and"
			, "any"
			, "array"
			, "as"
			, "asc"
			, "asymmetric"
			, "authorization"
			, "binary"
			, "both"
			, "case"
			, "cast"
			, "check"
			, "collate"
			, "collation"
			, "column"
			, "concurrently"
			, "constraint"
			, "create"
			, "cross"
			, "current_catalog"
			, "current_date"
			, "current_role"
			, "current_schema"
			, "current_time"
			, "current_timestamp"
			, "current_user"
			, "default"
			, "deferrable"
			, "desc"
			, "distinct"
			, "do"
			, "else"
			, "end"
			, "except"
			, "false"
			, "fetch"
			, "for"
			, "foreign"
			, "freeze"
			, "from"
			, "full"
			, "grant"
			, "group"
			, "having"
			, "ilike"
			, "in"
			, "initially"
			, "inner"
			, "intersect"
			, "into"
			, "is"
			, "isnull"
			, "join"
			, "lateral"
			, "leading"
			, "left"
			, "like"
			, "limit"
			, "localtime"
			, "localtimestamp"
			, "natural"
			, "not"
			, "notnull"
			, "null"
			, "offset"
			, "on"
			, "only"
			, "or"
			, "order"
			, "outer"
			, "overlaps"
			, "placing"
			, "primary"
			, "references"
			, "returning"
			, "right"
			, "select"
			, "session_user"
			, "similar"
			, "some"
			, "symmetric"
			, "system_user"
			, "table"
			, "tablesample"
			, "then"
			, "to"
			, "trailing"
			, "true"
			, "union"
			, "unique"
			, "user"
			, "using"
			, "variadic"
			, "verbose"
			, "when"
			, "where"
			, "window"
			, "with"
		}
	;

	// Column name keywords that fg_IsColumnReference also reads unquoted as syntax
	constexpr ch8 const *gc_SqlSyntaxColumnNameKeywords[] = {"between", "exists"};

	namespace
	{
		bool fg_IsSqlSpace(ch8 _Char)
		{
			return _Char == ' ' || _Char == '\t' || _Char == '\n' || _Char == '\r';
		}

		// Tells whether the name before _iPrevious, skipping the earlier names of a list, follows UPDATE OF, as in the column list of a
		// trigger
		bool fg_IsInUpdateOfList(ch8 const *_pSql, aint _iPrevious)
		{
			aint iPrevious = _iPrevious;
			while (iPrevious > 0 && _pSql[iPrevious - 1] == ',')
			{
				--iPrevious;
				while (iPrevious > 0 && fg_IsSqlSpace(_pSql[iPrevious - 1]))
					--iPrevious;

				ch8 Close = iPrevious > 0 ? _pSql[iPrevious - 1] : ch8(0);
				if (Close == '"' || Close == '`' || Close == ']')
				{
					ch8 Open = Close == ']' ? '[' : Close;
					--iPrevious;
					for (;;)
					{
						while (iPrevious > 0 && _pSql[iPrevious - 1] != Open)
							--iPrevious;

						if (iPrevious == 0)
							return false;

						--iPrevious;
						// A doubled quote inside the name
						if (Close == ']' || iPrevious == 0 || _pSql[iPrevious - 1] != Open)
							break;

						--iPrevious;
					}
				}
				else
				{
					aint iNameEnd = iPrevious;
					while (iPrevious > 0 && fg_IsIdentifierChar(_pSql[iPrevious - 1]))
						--iPrevious;

					if (iPrevious == iNameEnd)
						return false;
				}

				while (iPrevious > 0 && fg_IsSqlSpace(_pSql[iPrevious - 1]))
					--iPrevious;
			}

			aint iOfEnd = iPrevious;
			while (iPrevious > 0 && fg_IsIdentifierChar(_pSql[iPrevious - 1]))
				--iPrevious;
			if (NStr::CStr(_pSql + iPrevious, iOfEnd - iPrevious).f_CmpNoCase("of") != 0)
				return false;

			while (iPrevious > 0 && fg_IsSqlSpace(_pSql[iPrevious - 1]))
				--iPrevious;

			aint iUpdateEnd = iPrevious;
			while (iPrevious > 0 && fg_IsIdentifierChar(_pSql[iPrevious - 1]))
				--iPrevious;

			return NStr::CStr(_pSql + iPrevious, iUpdateEnd - iPrevious).f_CmpNoCase("update") == 0;
		}

		// Tells likely column references from other identifiers by their neighbouring text; it does not parse the SQL
		bool fg_IsColumnReference(NStr::CStr const &_Sql, aint _iStart, aint _iEnd, bool _bQuoted, bool _bPostgres)
		{
			ch8 const *pSql = _Sql.f_GetStr();
			aint Length = _Sql.f_GetLen();

			aint iPrevious = _iStart;
			while (iPrevious > 0 && fg_IsSqlSpace(pSql[iPrevious - 1]))
				--iPrevious;
			if (iPrevious >= 2 && pSql[iPrevious - 1] == ':' && pSql[iPrevious - 2] == ':')
				return false;

			aint iNext = _iEnd;
			while (iNext < Length && fg_IsSqlSpace(pSql[iNext]))
				++iNext;
			if (iNext < Length && (pSql[iNext] == '(' || pSql[iNext] == '.'))
				return false;

			if (!_bQuoted && iNext < Length && pSql[iNext] == '\'')
				return false;

			// Takes a qualified member, such as NEW.last in a trigger, as a column even when its name is a keyword
			if (iPrevious > 0 && pSql[iPrevious - 1] == '.')
				return true;

			// A trigger's UPDATE OF list names only columns, also one named like a keyword, such as ignore
			if (fg_IsInUpdateOfList(pSql, iPrevious))
				return true;

			aint iPreviousStart = iPrevious;
			while (iPreviousStart > 0 && fg_IsIdentifierChar(pSql[iPreviousStart - 1]))
				--iPreviousStart;

			NStr::CStr PreviousWord = fg_PostgresFoldIdentifier(NStr::CStr(pSql + iPreviousStart, iPrevious - iPreviousStart));

			if (PreviousWord == "collate")
				return false;

			// The later words of a type name such as double precision or timestamp with time zone
			if (!_bQuoted)
			{
				NStr::CStr Word = fg_PostgresFoldIdentifier(NStr::CStr(pSql + _iStart, _iEnd - _iStart));
				if
				(
					(PreviousWord == "double" && Word == "precision")
					|| ((PreviousWord == "character" || PreviousWord == "bit") && Word == "varying")
					|| ((PreviousWord == "time" || PreviousWord == "timestamp") && (Word == "with" || Word == "without"))
					|| ((PreviousWord == "with" || PreviousWord == "without") && Word == "time")
					|| (PreviousWord == "time" && Word == "zone")
				)
				{
					return false;
				}
			}

			// Takes an unquoted keyword, such as the DESC of an index key, as syntax, though SQLite accepts some keywords as column names
			if (!_bQuoted)
			{
				NStr::CStr Token = fg_PostgresFoldIdentifier(NStr::CStr(pSql + _iStart, _iEnd - _iStart));
				for (ch8 const *pKeyword : gc_PostgresQuotedKeywords)
				{
					if (Token == pKeyword)
						return false;
				}
				for (ch8 const *pKeyword : gc_SqlSyntaxColumnNameKeywords)
				{
					if (Token == pKeyword)
						return false;
				}

				// PostgreSQL writes columns with these names unquoted
				if (!_bPostgres)
				{
					for (ch8 const *pKeyword : {"glob", "regexp", "match", "raise", "ignore"})
					{
						if (Token == pKeyword)
							return false;
					}
				}
			}

			bool bAfterAs
				= iPrevious >= 2 && iPrevious < _iStart && NStr::CStr(pSql + iPrevious - 2, 2).f_CmpNoCase("as") == 0 && (iPrevious == 2 || !fg_IsIdentifierChar(pSql[iPrevious - 3]))
			;

			return !bAfterAs;
		}
	}

	// Matches likely column references ignoring ASCII case
	bool fg_SqlTextMentionsSqliteColumn(NStr::CStr const &_Sql, NStr::CStr const &_Identifier, bool _bFromFirstParenthesis)
	{
		bool bMentions = false;
		fg_ForEachIdentifier
			(
				_Sql
				, false
				, _bFromFirstParenthesis
				, [&](aint _iStart, aint _iEnd, NStr::CStr const &_Name, bool _bQuoted)
				{
					if (fg_SqlIdentifiersEqualIgnoringAsciiCase(_Name, _Identifier) && fg_IsColumnReference(_Sql, _iStart, _iEnd, _bQuoted, false))
						bMentions = true;
				}
			)
		;

		return bMentions;
	}

	// Matches likely column references, a quoted name exactly and an unquoted one after folding to lower case
	bool fg_SqlTextMentionsPostgresColumn(NStr::CStr const &_Sql, NStr::CStr const &_ColumnName)
	{
		bool bMentions = false;
		fg_ForEachIdentifier
			(
				_Sql
				, true
				, false
				, [&](aint _iStart, aint _iEnd, NStr::CStr const &_Name, bool _bQuoted)
				{
					if ((_bQuoted ? _Name == _ColumnName : fg_PostgresFoldIdentifier(_Name) == _ColumnName) && fg_IsColumnReference(_Sql, _iStart, _iEnd, _bQuoted, true))
						bMentions = true;
				}
			)
		;

		return bMentions;
	}

	// Matches SQLite identifier tokens with ASCII-only case folding, also a table name before a column list as in INSERT INTO t(...)
	bool fg_SqlTextHasIdentifierToken(NStr::CStr const &_Sql, NStr::CStr const &_Identifier)
	{
		bool bHas = false;
		fg_ForEachIdentifier
			(
				_Sql
				, false
				, false
				, [&](aint _iStart, aint _iEnd, NStr::CStr const &_Name, bool _bQuoted)
				{
					if (fg_SqlIdentifiersEqualIgnoringAsciiCase(_Name, _Identifier))
						bHas = true;
				}
			)
		;

		return bHas;
	}

	// Renames likely column references without resolving names: PostgreSQL folds only unquoted names, SQLite ignores ASCII case
	static NStr::CStr fsg_SqlTextRenameIdentifier(NStr::CStr const &_Sql, NStr::CStr const &_OldName, NStr::CStr const &_NewName, bool _bPostgres)
	{
		// A name that would fold, not read as one identifier or read as a keyword keeps its meaning only when quoted
		bool bNewNameIsPlain = !_NewName.f_IsEmpty() && !(_NewName.f_GetStr()[0] >= '0' && _NewName.f_GetStr()[0] <= '9');
		for (aint iChar = 0; iChar < _NewName.f_GetLen(); ++iChar)
		{
			ch8 Char = _NewName.f_GetStr()[iChar];
			if (!((Char >= 'a' && Char <= 'z') || (!_bPostgres && Char >= 'A' && Char <= 'Z') || (Char >= '0' && Char <= '9') || Char == '_'))
				bNewNameIsPlain = false;
		}

		// SQLite also gets the PostgreSQL keywords quoted, since fg_IsColumnReference reads them unquoted as syntax in both dialects
		for (ch8 const *pKeyword : gc_PostgresQuotedKeywords)
		{
			if (fg_PostgresFoldIdentifier(_NewName) == pKeyword)
				bNewNameIsPlain = false;
		}
		for (ch8 const *pKeyword : gc_SqlSyntaxColumnNameKeywords)
		{
			if (fg_PostgresFoldIdentifier(_NewName) == pKeyword)
				bNewNameIsPlain = false;
		}

		if (bNewNameIsPlain && !_bPostgres && sqlite3_keyword_check(_NewName.f_GetStr(), int(_NewName.f_GetLen())))
			bNewNameIsPlain = false;

		NStr::CStr Result;
		{
			NStr::CStr::CAppender Appender(Result);
			aint iCopied = 0;
			fg_ForEachIdentifier
				(
					_Sql
					, _bPostgres
					, false
					, [&](aint _iStart, aint _iEnd, NStr::CStr const &_Name, bool _bQuoted)
					{
						if (_bPostgres)
						{
							if (_bQuoted ? _Name != _OldName : fg_PostgresFoldIdentifier(_Name) != _OldName)
								return;
						}
						else if (fg_PostgresFoldIdentifier(_Name) != fg_PostgresFoldIdentifier(_OldName))
							return;

						if (!fg_IsColumnReference(_Sql, _iStart, _iEnd, _bQuoted, _bPostgres))
							return;

						Appender += NStr::CStr(_Sql.f_GetStr() + iCopied, _iStart - iCopied);
						if (_bQuoted || !bNewNameIsPlain)
						{
							Appender += "\"";
							Appender += _NewName.f_Replace("\"", "\"\"");
							Appender += "\"";
						}
						else
							Appender += _NewName;

						iCopied = _iEnd;
					}
				)
			;
			Appender += NStr::CStr(_Sql.f_GetStr() + iCopied, _Sql.f_GetLen() - iCopied);
		}

		return Result;
	}

	// Matches SQLite identifier comparison: only ASCII letters are case-folded.
	bool fg_SqlIdentifiersEqualIgnoringAsciiCase(NStr::CStr const &_Left, NStr::CStr const &_Right)
	{
		return fg_PostgresFoldIdentifier(_Left) == fg_PostgresFoldIdentifier(_Right);
	}

	NStr::CStr fg_SqlTextRenameIdentifier(NStr::CStr const &_Sql, NStr::CStr const &_OldName, NStr::CStr const &_NewName)
	{
		return fsg_SqlTextRenameIdentifier(_Sql, _OldName, _NewName, true);
	}

	NStr::CStr fg_SqlTextRenameSqliteIdentifier(NStr::CStr const &_Sql, NStr::CStr const &_OldName, NStr::CStr const &_NewName)
	{
		return fsg_SqlTextRenameIdentifier(_Sql, _OldName, _NewName, false);
	}

	// A quoted column name can contain the separator
	aint fg_PostgresForeignKeyFindReferences(NStr::CStr const &_Definition)
	{
		ch8 const *pDefinition = _Definition.f_GetStr();
		bool bQuoted = false;
		aint iChecked = 0;
		for (aint iFind = _Definition.f_Find(" REFERENCES "); iFind >= 0; iFind = _Definition.f_Find(iFind + 1, " REFERENCES "))
		{
			for (; iChecked < iFind; ++iChecked)
			{
				if (pDefinition[iChecked] == '"')
					bQuoted = !bQuoted;
			}

			if (!bQuoted)
				return iFind;
		}

		return -1;
	}

	// Renames a referencing column in a pg_get_constraintdef definition: FOREIGN KEY (columns) REFERENCES table(columns) ...,
	// which since PostgreSQL 15 can list referencing columns again after ON DELETE SET NULL or SET DEFAULT
	NStr::CStr fg_PostgresForeignKeyRenameColumn(NStr::CStr const &_Definition, NStr::CStr const &_OldName, NStr::CStr const &_NewName)
	{
		aint iReferences = fg_PostgresForeignKeyFindReferences(_Definition);
		if (iReferences < 0)
			return _Definition;

		NStr::CStr References = _Definition.f_Extract(iReferences);
		ch8 const *pReferences = References.f_GetStr();
		aint ReferencesLength = References.f_GetLen();

		aint iActionColumnsStart = -1;
		{
			NStr::CStr PreviousWords[3];
			fg_ForEachIdentifier
				(
					References
					, true
					, false
					, [&](aint _iStart, aint _iEnd, NStr::CStr const &_Name, bool _bQuoted)
					{
						NStr::CStr Word = _bQuoted ? NStr::CStr() : fg_PostgresFoldIdentifier(_Name);
						if (iActionColumnsStart < 0 && (Word == "null" || Word == "default") && PreviousWords[0] == "on" && PreviousWords[1] == "delete" && PreviousWords[2] == "set")
						{
							aint iNext = _iEnd;
							while (iNext < ReferencesLength && fg_IsSqlSpace(pReferences[iNext]))
								++iNext;

							if (iNext < ReferencesLength && pReferences[iNext] == '(')
								iActionColumnsStart = iNext + 1;
						}

						PreviousWords[0] = fg_Move(PreviousWords[1]);
						PreviousWords[1] = fg_Move(PreviousWords[2]);
						PreviousWords[2] = fg_Move(Word);
					}
				)
			;
		}

		NStr::CStr Result = fsg_SqlTextRenameIdentifier(_Definition.f_Left(umint(iReferences)), _OldName, _NewName, true);
		if (iActionColumnsStart < 0)
			return Result + References;

		aint iActionColumnsEnd = iActionColumnsStart;
		while (iActionColumnsEnd < ReferencesLength && pReferences[iActionColumnsEnd] != ')')
		{
			if (pReferences[iActionColumnsEnd] == '"')
			{
				for (++iActionColumnsEnd; iActionColumnsEnd < ReferencesLength; ++iActionColumnsEnd)
				{
					if (pReferences[iActionColumnsEnd] == '"')
					{
						if (iActionColumnsEnd + 1 < ReferencesLength && pReferences[iActionColumnsEnd + 1] == '"')
							++iActionColumnsEnd;
						else
							break;
					}
				}
			}

			++iActionColumnsEnd;
		}

		if (iActionColumnsEnd > ReferencesLength)
			iActionColumnsEnd = ReferencesLength;

		Result += NStr::CStr(pReferences, iActionColumnsStart);
		Result += fsg_SqlTextRenameIdentifier(NStr::CStr(pReferences + iActionColumnsStart, iActionColumnsEnd - iActionColumnsStart), _OldName, _NewName, true);
		Result += NStr::CStr(pReferences + iActionColumnsEnd, ReferencesLength - iActionColumnsEnd);

		return Result;
	}
}

namespace NMib::NSQL
{
	namespace
	{
		auto fg_SqlFindMigration(NContainer::TCVector<CSqlSchemaMigrationDescription> const &_Migrations, NStr::CStr const &_FromVersionID, NStr::CStr const &_ToVersionID)
			-> CSqlSchemaMigrationDescription const *
		{
			for (auto const &Migration : _Migrations)
			{
				if (*Migration.m_pFromVersionID == _FromVersionID && *Migration.m_pToVersionID == _ToVersionID)
					return &Migration;
			}

			return nullptr;
		}

		void fg_SqlInsertMigrationPlanStep
			(
				CSqlSchemaMigrationPlan &o_Plan
				, ESqlSchemaMigrationPlanStepType _Type
				, NStr::CStr const &_FromVersionID
				, NStr::CStr const &_ToVersionID
				, umint _nOperations = 0
			)
		{
			o_Plan.m_Steps.f_InsertLast
				(
					{
						.m_Type = _Type
						, .m_FromVersionID = _FromVersionID
						, .m_ToVersionID = _ToVersionID
						, .m_nOperations = _nOperations
					}
				)
			;
		}
	}

	CSqlSchemaMigrationPlan fg_SqlPlanSchemaMigration(ICSqlSchemaVersions const &_SchemaVersions, NStr::CStr const *_pCurrentVersionID)
	{
		CSqlSchemaMigrationPlan Plan;
		NContainer::TCVector<CSqlSchemaVersionDescription> Versions = _SchemaVersions.f_DescribeVersions();
		NContainer::TCVector<CSqlSchemaMigrationDescription> Migrations = _SchemaVersions.f_DescribeMigrations();
		if (Versions.f_IsEmpty())
			return Plan;

		NStr::CStr CurrentVersionID = _pCurrentVersionID ? *_pCurrentVersionID : NStr::CStr();
		if (CurrentVersionID.f_IsEmpty())
		{
			fg_SqlInsertMigrationPlanStep
				(
					Plan
					, ESqlSchemaMigrationPlanStepType::mc_CreateInitialSchema
					, NStr::CStr()
					, Versions[Versions.f_GetLen() - 1].f_ID()
					, Versions[Versions.f_GetLen() - 1].m_nTables
				)
			;
			for (auto const &Version : Versions)
				fg_SqlInsertMigrationPlanStep(Plan, ESqlSchemaMigrationPlanStepType::mc_MarkSchemaVersionApplied, NStr::CStr(), Version.f_ID());

			return Plan;
		}

		umint iCurrentVersion = umint(-1);
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
			using namespace NStr;

			Plan.m_Warnings.f_InsertLast("Current schema version '{}' is not present in the compiled schema versions"_f << CurrentVersionID);
			return Plan;
		}

		if (iCurrentVersion == Versions.f_GetLen() - 1)
		{
			fg_SqlInsertMigrationPlanStep(Plan, ESqlSchemaMigrationPlanStepType::mc_AlreadyCurrent, CurrentVersionID, CurrentVersionID);
			return Plan;
		}

		for (umint iVersion = iCurrentVersion + 1; iVersion < Versions.f_GetLen(); ++iVersion)
		{
			auto const &PreviousVersion = Versions[iVersion - 1];
			auto const &NextVersion = Versions[iVersion];
			if (auto const *pMigration = fg_SqlFindMigration(Migrations, PreviousVersion.f_ID(), NextVersion.f_ID()))
			{
				fg_SqlInsertMigrationPlanStep
					(
						Plan
						, ESqlSchemaMigrationPlanStepType::mc_ApplyMigrationOperations
						, PreviousVersion.f_ID()
						, NextVersion.f_ID()
						, pMigration->m_Operations.f_GetLen()
					)
				;
			}

			fg_SqlInsertMigrationPlanStep(Plan, ESqlSchemaMigrationPlanStepType::mc_SyncAdditiveSchema, PreviousVersion.f_ID(), NextVersion.f_ID(), NextVersion.m_nTables);
			fg_SqlInsertMigrationPlanStep(Plan, ESqlSchemaMigrationPlanStepType::mc_MarkSchemaVersionApplied, PreviousVersion.f_ID(), NextVersion.f_ID());
		}

		return Plan;
	}
}
