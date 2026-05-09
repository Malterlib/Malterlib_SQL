// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/DatabaseSchema>

namespace NMib::NSQL::NPrivate
{
	namespace
	{
		// PostgreSQL folds only ASCII letters of an unquoted name to lower case, and SQLite matches names and keywords
		// ignoring only ASCII case
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

		// Calls _fOnToken(iStart, iEnd, Name, bQuoted) for each identifier, skipping string literals and comments.
		// Quoted identifiers use "", and in SQLite also `` or []; a doubled closing quote inside one stands for itself.
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
				if
				(
					_bPostgres
					&& (Char == 'E' || Char == 'e')
					&& iChar + 1 < Length
					&& pSql[iChar + 1] == '\''
					&& (iChar == 0 || !fg_IsIdentifierChar(pSql[iChar - 1]))
				)
				{
					// An escape string, where a backslash escapes the next character, also a quote
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
					while (iChar < Length && pSql[iChar] != '\n')
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
		"all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "authorization", "binary", "both", "case", "cast"
		, "check", "collate", "collation", "column", "concurrently", "constraint", "create", "cross", "current_catalog", "current_date"
		, "current_role", "current_schema", "current_time", "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct"
		, "do", "else", "end", "except", "false", "fetch", "for", "foreign", "freeze", "from", "full", "grant", "group", "having", "ilike", "in"
		, "initially", "inner", "intersect", "into", "is", "isnull", "join", "lateral", "leading", "left", "like", "limit", "localtime"
		, "localtimestamp", "natural", "not", "notnull", "null", "offset", "on", "only", "or", "order", "outer", "overlaps", "placing"
		, "primary", "references", "returning", "right", "select", "session_user", "similar", "some", "symmetric", "system_user", "table"
		, "tablesample", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "verbose", "when", "where", "window"
		, "with"
	};

	namespace
	{
		bool fg_IsSqlSpace(ch8 _Char)
		{
			return _Char == ' ' || _Char == '\t' || _Char == '\n' || _Char == '\r';
		}

		// A name followed by ( is a function, one followed by . a qualifier, one after :: or the AS of a CAST a type, and an
		// unquoted one followed by a string a type literal such as DATE '2000-01-01'
		bool fg_IsColumnReference(NStr::CStr const &_Sql, aint _iStart, aint _iEnd, bool _bQuoted)
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

			// A qualified member, such as NEW.last in a trigger, is a column whatever its name
			if (iPrevious > 0 && pSql[iPrevious - 1] == '.')
				return true;

			aint iPreviousStart = iPrevious;
			while (iPreviousStart > 0 && fg_IsIdentifierChar(pSql[iPreviousStart - 1]))
				--iPreviousStart;

			NStr::CStr PreviousWord = fg_PostgresFoldIdentifier(NStr::CStr(pSql + iPreviousStart, iPrevious - iPreviousStart));

			// The name after COLLATE is a collation
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

			// An unquoted reserved word is syntax, such as the DESC of an index key, as a column of that name needs quotes
			if (!_bQuoted)
			{
				NStr::CStr Token = fg_PostgresFoldIdentifier(NStr::CStr(pSql + _iStart, _iEnd - _iStart));
				for (ch8 const *pKeyword : gc_PostgresQuotedKeywords)
				{
					if (Token == pKeyword)
						return false;
				}
				for (ch8 const *pKeyword : {"glob", "regexp", "match", "between", "exists", "raise", "ignore"})
				{
					if (Token == pKeyword)
						return false;
				}
			}

			bool bAfterAs = iPrevious >= 2
				&& iPrevious < _iStart
				&& NStr::CStr(pSql + iPrevious - 2, 2).f_CmpNoCase("as") == 0
				&& (iPrevious == 2 || !fg_IsIdentifierChar(pSql[iPrevious - 3]))
			;

			return !bAfterAs;
		}
	}


	// Matches column references case-insensitively, not text in literals or comments, function names or types
	bool fg_SqlTextMentionsIdentifier(NStr::CStr const &_Sql, NStr::CStr const &_Identifier, bool _bFromFirstParenthesis)
	{
		bool bMentions = false;
		fg_ForEachIdentifier
			(
				_Sql
				, false
				, _bFromFirstParenthesis
				, [&](aint _iStart, aint _iEnd, NStr::CStr const &_Name, bool _bQuoted)
				{
					if (fg_SqlIdentifiersEqualIgnoringAsciiCase(_Name, _Identifier) && fg_IsColumnReference(_Sql, _iStart, _iEnd, _bQuoted))
						bMentions = true;
				}
			)
		;

		return bMentions;
	}

	// Matches column references by PostgreSQL's rules: a quoted name exactly, an unquoted one after folding to lower case
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
					if ((_bQuoted ? _Name == _ColumnName : fg_PostgresFoldIdentifier(_Name) == _ColumnName) && fg_IsColumnReference(_Sql, _iStart, _iEnd, _bQuoted))
						bMentions = true;
				}
			)
		;

		return bMentions;
	}

	// Matches any identifier token case-insensitively, also a table name before a column list as in INSERT INTO t(...)
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

	// Mirrors the database's rewrite of expressions when a column is renamed, and only column references change. In
	// PostgreSQL a quoted name matches exactly and an unquoted one after folding to lower case, while SQLite matches every
	// name ignoring ASCII case
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

		for (ch8 const *pKeyword : gc_PostgresQuotedKeywords)
		{
			if (fg_PostgresFoldIdentifier(_NewName) == pKeyword)
				bNewNameIsPlain = false;
		}

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

						if (!fg_IsColumnReference(_Sql, _iStart, _iEnd, _bQuoted))
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
}

namespace NMib::NSQL
{
	namespace
	{
		CSqlSchemaMigrationDescription const *fg_SqlFindMigration
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

		void fg_SqlInsertMigrationPlanStep
			(
				CSqlSchemaMigrationPlan &o_Plan
				, ESqlSchemaMigrationPlanStepType _Type
				, NStr::CStr _FromVersionID
				, NStr::CStr _ToVersionID
				, umint _nOperations = 0
			)
		{
			o_Plan.m_Steps.f_InsertLast
				(
					{
						.m_Type = _Type
						, .m_FromVersionID = fg_Move(_FromVersionID)
						, .m_ToVersionID = fg_Move(_ToVersionID)
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
