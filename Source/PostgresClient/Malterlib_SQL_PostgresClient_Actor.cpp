// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_PostgresClient_Internal.h"

#include <Mib/Container/Map>
#include <Mib/Container/PagedByteVector>
#include <Mib/Concurrency/AsyncDestroy>
#include <Mib/Concurrency/LogError>
#include <Mib/Cryptography/Certificate>
#include <Mib/Network/AsyncSocket>
#include <Mib/Network/SSL>
#include <Mib/Network/Sockets/SSL>
#include <Mib/Network/Sockets/TCP>
#include <Mib/Stream/ByteVector>
#include <Mib/Storage/SharedPointer>

namespace NMib::NSQL
{
		namespace
	{
		using CPostgresWriteRefStream = NStream::CBinaryStreamMemoryRef<NStream::CBinaryStreamBigEndian, NContainer::CIOByteVector>;

		NStr::CStr fg_PostgresByteVectorToString(NContainer::CIOByteVector const &_Data)
		{
			return NStr::CStr((ch8 const *)_Data.f_GetArray(), _Data.f_GetLen());
		}

		struct CPostgresPreparedStatementKey
		{
			constexpr auto operator <=> (CPostgresPreparedStatementKey const &_Other) const noexcept = default;

			NStr::CStr m_Sql;
			NContainer::TCVector<uint32> m_ParameterTypeOIDs;
		};

		struct CPostgresChannelBindingState
		{
			NThread::CLowLevelLock m_Lock;
			NContainer::CByteVector m_Data;
		};
	}

	struct CPostgresClientActor::CInternal : public NConcurrency::CActorInternal
	{
		CPostgresConnectionSettings m_Settings;
		NConcurrency::TCActor<NNetwork::CAsyncSocketClientActor> m_ClientActor;
		NConcurrency::TCActorInterface<NNetwork::CAsyncSocketActor> m_Socket;
		NConcurrency::CActorSubscription m_KeepAliveTimerSubscription;
		NContainer::CPagedByteVector m_ReceiveBuffer{4096};
		NStorage::TCOptional<NConcurrency::TCPromise<void>> m_ReceivePromise;
		NContainer::TCLinkedList<NConcurrency::TCPromise<void>> m_ProtocolCommandWaiters;
		NStorage::TCOptional<CPostgresScramClientFirstMessage> m_ScramClientFirst;
		NStorage::TCOptional<CPostgresScramClientFinalMessage> m_ScramClientFinal;
		NContainer::TCMap<CPostgresPreparedStatementKey, NStr::CStr> m_PreparedStatementCache;
		uint64 m_NextPreparedStatementID = 0;
		bool m_bProtocolCommandInProgress = false;
		bool m_bClosed = false;
		bool m_bInTransaction = false;

		void f_UpdateReadyStatus(EPostgresReadyForQueryStatus _ReadyStatus)
		{
			m_bInTransaction = _ReadyStatus != EPostgresReadyForQueryStatus::mc_Idle;
		}

		void f_StopKeepAlive()
		{
			m_KeepAliveTimerSubscription.f_Clear();
		}

		NConcurrency::TCFuture<void> f_WaitForProtocolCommand()
		{
			while (m_bProtocolCommandInProgress)
			{
				NConcurrency::TCPromiseFuturePair<void> Waiter;
				m_ProtocolCommandWaiters.f_InsertLast(fg_Move(Waiter.m_Promise));

				co_await fg_Move(Waiter.m_Future);
			}

			co_return {};
		}

		void f_EndProtocolCommand()
		{
			m_bProtocolCommandInProgress = false;

			if (!m_ProtocolCommandWaiters.f_IsEmpty())
				m_ProtocolCommandWaiters.f_PopFirst().f_SetResult();
		}

		NConcurrency::TCFuture<void> f_ResetStartupConnection()
		{
			if (m_Socket)
			{
				co_await m_Socket
					(
						&NNetwork::CAsyncSocketActor::f_CloseWithLinger
						, NNetwork::EAsyncSocketStatus_NormalClosure
						, NStr::CStr("PostgreSQL startup failed")
						, 1.0
					)
					.f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL client", "Failed to close failed PostgreSQL startup socket")
				;
				m_Socket.f_Clear();
			}

			m_bClosed = true;
			m_bInTransaction = false;
			m_ReceiveBuffer.f_RemoveFront(m_ReceiveBuffer.f_GetLen());
			m_ReceivePromise.f_Clear();
			m_ScramClientFirst.f_Clear();
			m_ScramClientFinal.f_Clear();

			co_return {};
		}
	};

	CPostgresClientActor::CPostgresClientActor()
		: mp_pInternal(fg_Construct())
	{
	}

	CPostgresClientActor::~CPostgresClientActor()
	{
	}

	NConcurrency::TCActor<CPostgresClientActor> fg_CreatePostgresClient()
	{
		return NConcurrency::fg_ConstructActor<CPostgresClientActor>();
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::f_Connect(CPostgresConnectionSettings _Settings)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		co_await Internal.f_WaitForProtocolCommand();

		if (Internal.m_Socket && !Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is already connected");

		Internal.m_bProtocolCommandInProgress = true;
		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		if (_Settings.m_Host.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL host is required");

		if (_Settings.m_User.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL user is required");

		if (_Settings.m_Database.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL database is required");

		if (Internal.m_ClientActor)
			co_await fg_Move(Internal.m_ClientActor).f_Destroy();

		Internal.f_StopKeepAlive();
		Internal.m_Settings = _Settings;
		Internal.m_Socket.f_Clear();
		Internal.m_ReceiveBuffer.f_RemoveFront(Internal.m_ReceiveBuffer.f_GetLen());
		Internal.m_ReceivePromise.f_Clear();
		Internal.m_ScramClientFirst.f_Clear();
		Internal.m_ScramClientFinal.f_Clear();
		Internal.m_PreparedStatementCache.f_Clear();
		Internal.m_NextPreparedStatementID = 0;

		Internal.m_ClientActor = NConcurrency::fg_ConstructActor<NNetwork::CAsyncSocketClientActor>();
		co_await Internal.m_ClientActor(&NNetwork::CAsyncSocketClientActor::f_SetDefaultTimeout, _Settings.m_SocketTimeout);
		Internal.m_bClosed = false;
		Internal.m_bInTransaction = false;

		NNetwork::CAsyncSocketNewClientConnection NewConnection = co_await Internal.m_ClientActor
			(
				&NNetwork::CAsyncSocketClientActor::f_Connect
				, _Settings.m_Host
				, NStr::CStr()
				, NNetwork::ENetAddressType_TCPv4
				, _Settings.m_Port
				, NNetwork::CSocket_TCP::fs_GetFactory()
			)
		;

		NConcurrency::TCActor<CPostgresClientActor> ThisActor = NConcurrency::fg_ThisActor(this);
		NNetwork::CAsyncSocketCallbacks Callbacks;
		Callbacks.m_fOnReceiveData = NConcurrency::g_ActorFunctor / [ThisActor](NStorage::TCSharedPointer<NContainer::CIOByteVector> _pData) -> NConcurrency::TCFuture<void>
			{
				co_await ThisActor(&CPostgresClientActor::fp_OnSocketData, fg_Move(_pData));

				co_return {};
			}
		;
		Callbacks.m_fOnClose = NConcurrency::g_ActorFunctor / [ThisActor](NNetwork::EAsyncSocketStatus, NStr::CStr, NNetwork::EAsyncSocketCloseOrigin) -> NConcurrency::TCFuture<void>
			{
				co_await ThisActor(&CPostgresClientActor::fp_OnSocketClose);

				co_return {};
			}
		;
		Internal.m_Socket = co_await NewConnection.f_Accept(fg_Move(Callbacks));

		NStorage::TCSharedPointer<CPostgresChannelBindingState> pChannelBindingState = fg_Construct();
		if (_Settings.m_bRequireTLS)
		{
			co_await fp_Send(fg_PostgresBuildSSLRequest());
			NContainer::CIOByteVector SSLResponse = co_await fp_ReadBytes(1);

			if (SSLResponse.f_GetArray()[0] != 'S')
			{
				co_await Internal.f_ResetStartupConnection();

				co_return DMibErrorInstance("PostgreSQL server refused TLS");
			}

			NNetwork::CSSLSettings SSLSettings = _Settings.m_TLSSettings;
			if (_Settings.m_bVerifyTLS)
			{
				SSLSettings.m_VerificationFlags = NNetwork::CSSLSettings::EVerificationFlag
					(
						SSLSettings.m_VerificationFlags | NNetwork::CSSLSettings::EVerificationFlag_VerifyHostnameMatches
					)
				;
				if (SSLSettings.m_CACertificateData.f_IsEmpty() && SSLSettings.m_CAStoreLocation.f_IsEmpty())
				{
					SSLSettings.m_VerificationFlags = NNetwork::CSSLSettings::EVerificationFlag
						(
							SSLSettings.m_VerificationFlags | NNetwork::CSSLSettings::EVerificationFlag_UseOSStoreIfNoCASpecified
						)
					;
				}
			}
			else
			{
				SSLSettings.m_VerificationFlags = NNetwork::CSSLSettings::EVerificationFlag
					(
						SSLSettings.m_VerificationFlags | NNetwork::CSSLSettings::EVerificationFlag_IgnoreVerificationFailures
					)
				;
			}
			NStorage::TCSharedPointer<NNetwork::CSSLContext> pSSLContext = fg_Construct(NNetwork::CSSLContext::EType_Client, SSLSettings);
			auto fAuthenticationResult = [pChannelBindingState](NNetwork::CSSLConnection::EAuthenticationResult _Result, NNetwork::CSSLConnectionResult const &_ConnectionResult)
				{
					if (_Result != NNetwork::CSSLConnection::EAuthenticationResult_Success)
						return;

					NContainer::CByteVector PeerCertificate = _ConnectionResult.f_GetPeerCertificate();
					if (!PeerCertificate.f_IsEmpty())
					{
						NContainer::CByteVector ChannelBindingData = NCryptography::CCertificate::fs_GetCertificateTLSServerEndPointData(PeerCertificate);

						DMibLock(pChannelBindingState->m_Lock);
						pChannelBindingState->m_Data = fg_Move(ChannelBindingData);
					}
				}
			;
			co_await Internal.m_Socket(&NNetwork::CAsyncSocketActor::f_UpgradeSocket, NNetwork::CSocket_SSL::fs_GetFactory(pSSLContext, fAuthenticationResult), _Settings.m_Host);
		}

		NContainer::CByteVector ChannelBindingData;
		{
			DMibLock(pChannelBindingState->m_Lock);
			ChannelBindingData = pChannelBindingState->m_Data;
		}

		co_await fp_Send(fg_PostgresBuildStartupMessage(_Settings));
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			NStorage::TCOptional<NContainer::CIOByteVector> NextToSend;
			NException::CExceptionPointer StartupException;
			bool bConnected = false;

			{
				CPostgresReadStream Stream;
				Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
				auto MessageCleanup = g_OnScopeExit / [&]
					{
						fp_ConsumeBackendMessage(MessageRef);
					}
				;
				// The stream points into m_ReceiveBuffer; suspending the coroutine while it
				// is open would let new socket data extend the buffer underneath us. Decoders
				// are synchronous (co_await on TCWrapped does not suspend), so any actual
				// suspension here is a bug. Sends and yields are moved outside this scope.
#if DMibEnableSafeCheck > 0
				auto SuspendGuard = NConcurrency::g_OnSuspend / []
					{
						DMibFastCheck(false);
					}
				;
#endif

				switch (MessageRef.m_Type)
				{
				case 'R':
					{
						CPostgresAuthenticationRequest Auth = co_await fg_PostgresDecodeAuthenticationRequest(Stream);
						if (Auth.m_Type == EPostgresAuthenticationRequestType::mc_OK)
							break;

						if (Auth.m_Type == EPostgresAuthenticationRequestType::mc_SASL)
						{
							bool bSupportsScramSHA256 = false;
							bool bSupportsScramSHA256Plus = false;
							for (auto const &Mechanism : Auth.m_SASLMechanisms)
							{
								if (Mechanism == "SCRAM-SHA-256")
									bSupportsScramSHA256 = true;

								if (Mechanism == "SCRAM-SHA-256-PLUS")
									bSupportsScramSHA256Plus = true;
							}

							bool bUseChannelBinding = bSupportsScramSHA256Plus && !ChannelBindingData.f_IsEmpty();
							if (!bSupportsScramSHA256 && !bUseChannelBinding)
							{
								StartupException = DMibErrorInstance("PostgreSQL server does not offer SCRAM-SHA-256");
								break;
							}

							NStr::CStr Mechanism = NStr::gc_Str<"SCRAM-SHA-256">;
							NStr::CStr GS2Header = NStr::gc_Str<"n,,">;
							if (bUseChannelBinding)
							{
								Mechanism = "SCRAM-SHA-256-PLUS";
								GS2Header = "p=tls-server-end-point,,";
							}

							Internal.m_ScramClientFirst = fg_PostgresScramBuildClientFirstMessage(_Settings.m_User, NStr::CStr(), GS2Header);
							NextToSend = fg_PostgresBuildSASLInitialResponse(Mechanism, Internal.m_ScramClientFirst->m_Message);
							break;
						}

						if (Auth.m_Type == EPostgresAuthenticationRequestType::mc_SASLContinue)
						{
							if (!Internal.m_ScramClientFirst)
							{
								StartupException = DMibErrorInstance("PostgreSQL SCRAM server-first message arrived before client-first state");
								break;
							}

							CPostgresScramServerFirstMessage ServerFirst = co_await fg_PostgresScramParseServerFirstMessage
								(
									fg_PostgresByteVectorToString(Auth.m_SASLData)
									, Internal.m_ScramClientFirst->m_ClientNonce
								)
							;
							// Only SCRAM-SHA-256-PLUS (GS2 header "p=...") appends the TLS channel binding to c=. A plain
							// SCRAM-SHA-256 exchange (GS2 header "n,,"), chosen when the server does not offer PLUS, must
							// bind c= from the GS2 header alone; passing the certificate bytes there yields a proof the
							// server rejects.
							bool bChannelBound = !Internal.m_ScramClientFirst->m_GS2Header.f_IsEmpty() && Internal.m_ScramClientFirst->m_GS2Header[0] == 'p';
							NContainer::CByteVector EmptyChannelBinding;
							Internal.m_ScramClientFinal = fg_PostgresScramBuildClientFinalMessage
								(
									_Settings.m_Password
									, *Internal.m_ScramClientFirst
									, ServerFirst
									, bChannelBound ? ChannelBindingData : EmptyChannelBinding
								)
							;
							NextToSend = fg_PostgresBuildSASLResponse(Internal.m_ScramClientFinal->m_Message);
							break;
						}

						if (Auth.m_Type == EPostgresAuthenticationRequestType::mc_SASLFinal)
						{
							if (!Internal.m_ScramClientFinal)
							{
								StartupException = DMibErrorInstance("PostgreSQL SCRAM server-final message arrived before client-final state");
								break;
							}

							CPostgresScramServerFinalMessage ServerFinal = co_await fg_PostgresScramParseServerFinalMessage(fg_PostgresByteVectorToString(Auth.m_SASLData));
							if (!fg_PostgresScramVerifyServerFinalMessage(*Internal.m_ScramClientFinal, ServerFinal))
							{
								StartupException = DMibErrorInstance("PostgreSQL SCRAM server signature verification failed");
								break;
							}
							break;
						}

						StartupException = DMibErrorInstance("PostgreSQL server requested unsupported authentication method");
						break;
					}
				case 'S':
				case 'K':
					break;
				case 'E':
					{
						CPostgresErrorResponse Error = co_await fg_PostgresDecodeErrorResponse(Stream);
						auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL startup failed ({}): {}");
						ErrorMessage << Error.m_Code;
						ErrorMessage << Error.m_Message;

						StartupException = fg_PostgresSqlError(ErrorMessage, Error);
						break;
					}
				case 'Z':
					Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
					bConnected = true;
					break;
				default:
					break;
				}
			}

			if (StartupException)
			{
				co_await Internal.f_ResetStartupConnection();

				co_return StartupException;
			}

			if (NextToSend)
				co_await fp_Send(fg_Move(*NextToSend));

			if (bConnected)
			{
				if (_Settings.m_KeepAliveInterval > 0.0)
				{
					NConcurrency::TCActor<CPostgresClientActor> ThisActor = NConcurrency::fg_ThisActor(this);
					NConcurrency::fg_RegisterTimer
						(
							_Settings.m_KeepAliveInterval
							, [this]() -> NConcurrency::TCFuture<void>
							{
								co_await fp_KeepAlive();

								co_return {};
							}
							, ThisActor
						)
						> [this](NConcurrency::TCAsyncResult<NConcurrency::CActorSubscription> &&_Subscription)
						{
							if (_Subscription)
								mp_pInternal->m_KeepAliveTimerSubscription = fg_Move(*_Subscription);
						}
					;
				}

				co_return {};
			}
		}
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::f_Close()
	{
		auto &Internal = *mp_pInternal;
		Internal.f_StopKeepAlive();

		co_await Internal.f_WaitForProtocolCommand();

		Internal.m_bProtocolCommandInProgress = true;
		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		if (Internal.m_Socket)
		{
			co_await Internal.m_Socket
				(
					&NNetwork::CAsyncSocketActor::f_CloseWithLinger
					, NNetwork::EAsyncSocketStatus_NormalClosure
					, NStr::CStr("PostgreSQL client closed")
					, 1.0
				)
				.f_Wrap()
				> NConcurrency::fg_LogError("PostgreSQL client", "Failed to close PostgreSQL socket")
			;
			Internal.m_Socket.f_Clear();
		}

		Internal.m_bClosed = true;
		Internal.m_bInTransaction = false;
		Internal.m_PreparedStatementCache.f_Clear();
		Internal.m_NextPreparedStatementID = 0;

		co_return {};
	}

	NConcurrency::TCFuture<CPostgresQueryResult> CPostgresClientActor::f_Execute(NStr::CStr _Sql)
	{
		co_return co_await f_ExecuteWithParameters(fg_Move(_Sql), {});
	}

	NConcurrency::TCFuture<CPostgresQueryResult> CPostgresClientActor::f_ExecuteWithParameters(NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Parameters)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		co_await Internal.f_WaitForProtocolCommand();

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		Internal.m_bProtocolCommandInProgress = true;
		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		NContainer::TCVector<uint32> ParameterTypeOIDs = fg_PostgresBuildParameterTypeOIDs(_Parameters);
		CPostgresPreparedStatementKey PreparedStatementKey{_Sql, ParameterTypeOIDs};
		NStr::CStr PreparedStatementName;
		bool bParseStatement = false;
		if (NStr::CStr const *pCachedStatementName = Internal.m_PreparedStatementCache.f_FindEqual(PreparedStatementKey))
			PreparedStatementName = *pCachedStatementName;
		else
		{
			PreparedStatementName = "malterlib_";
			PreparedStatementName += NStr::CStr::fs_ToStr(++Internal.m_NextPreparedStatementID);
			bParseStatement = true;
		}

		CPostgresWriteStream QueryMessages;
		if (bParseStatement)
			fg_PostgresWriteParse(QueryMessages, PreparedStatementName, _Sql, ParameterTypeOIDs);

		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, PreparedStatementName, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		fg_PostgresWriteExecute(QueryMessages, {});
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		CPostgresQueryResult Result;
		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<CPostgresRowDescription> RowDescription;
		bool bParseCompleted = !bParseStatement;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			CPostgresReadStream Stream;
			Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
			auto MessageCleanup = g_OnScopeExit / [&]
				{
					fp_ConsumeBackendMessage(MessageRef);
				}
			;
			// The stream points into m_ReceiveBuffer; suspending the coroutine here would
			// let new socket data extend the buffer underneath us. Decoders are synchronous
			// (co_await on TCWrapped does not suspend), so no suspension should ever happen
			// in this scope.
#if DMibEnableSafeCheck > 0
			auto SuspendGuard = NConcurrency::g_OnSuspend / []
				{
					DMibFastCheck(false);
				}
			;
#endif

			switch (MessageRef.m_Type)
			{
			case '1':
				co_await fg_PostgresDecodeParseComplete(Stream);
				bParseCompleted = true;

				break;
			case '2':
				co_await fg_PostgresDecodeBindComplete(Stream);
				break;
			case 'n':
				co_await fg_PostgresDecodeNoData(Stream);
				break;
			case 'T':
				Result.m_RowDescription = co_await fg_PostgresDecodeRowDescription(Stream);
				RowDescription = *Result.m_RowDescription;

				break;
			case 'D':
				{
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					Result.m_Rows.f_InsertLast(co_await fg_PostgresDecodeDataRow(Stream, *RowDescription));
				}
				break;
			case 'C':
				Result.m_CommandComplete = co_await fg_PostgresDecodeCommandComplete(Stream);
				break;
			case 'N':
				co_await fg_PostgresDecodeErrorResponse(Stream);
				break;
			case 'E':
				ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
				break;
			case 'Z':
				Result.m_ReadyStatus = co_await fg_PostgresDecodeReadyForQuery(Stream);
				Internal.f_UpdateReadyStatus(Result.m_ReadyStatus);
				// Cache a statement that parsed successfully even when a later Bind/Execute failed: the named prepared
				// statement stays allocated on the session, so caching it lets the next call reuse it instead of
				// leaking a fresh orphaned statement for every failed execution.
				if (bParseStatement && bParseCompleted)
					Internal.m_PreparedStatementCache[fg_Move(PreparedStatementKey)] = PreparedStatementName;

				if (ErrorResponse)
				{
					auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
					ErrorMessage << ErrorResponse->m_Code;
					ErrorMessage << ErrorResponse->m_Message;

					co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
				}

				co_return Result;
			default:
				co_return DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL query returned unexpected backend message type {}") << MessageRef.m_Type);
			}
		}
	}

	NConcurrency::TCAsyncGenerator<CPostgresDataRowBatch> CPostgresClientActor::f_ExecuteRows(NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Parameters, umint _nRowsPerBatch)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_nRowsPerBatch == 0)
			co_return DMibErrorInstance("PostgreSQL row stream batch size must be at least one");

		co_await Internal.f_WaitForProtocolCommand();

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		Internal.m_bProtocolCommandInProgress = true;
		bool bStreamFinished = false;
		auto ProtocolCommandCleanup = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					auto pThis = this;
					auto pInternal = &Internal;
					bool bStreamFinishedLocal = bStreamFinished;
					auto EndProtocolCommand = g_OnScopeExit / [pInternal]
						{
							pInternal->f_EndProtocolCommand();
						}
					;

					if (!bStreamFinishedLocal && pInternal->m_Socket)
					{
						if (pInternal->m_bInTransaction)
						{
							while (true)
							{
								auto MessageRef = co_await pThis->fp_ReadBackendMessageRef();
								CPostgresReadStream Stream;
								Stream.f_OpenRead(pInternal->m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
								auto MessageCleanup = g_OnScopeExit / [&]
									{
										pThis->fp_ConsumeBackendMessage(MessageRef);
									}
								;
#if DMibEnableSafeCheck > 0
								auto SuspendGuard = NConcurrency::g_OnSuspend / []
									{
										DMibFastCheck(false);
									}
								;
#endif

								if (MessageRef.m_Type == 'Z')
								{
									pInternal->f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
									break;
								}
							}
						}
						else
						{
							pInternal->m_bClosed = true;
							co_await pInternal->m_Socket
								(
									&NNetwork::CAsyncSocketActor::f_CloseWithLinger
									, NNetwork::EAsyncSocketStatus_NormalClosure
									, NStr::CStr("PostgreSQL row stream abandoned")
									, 1.0
								)
							;
						}
					}

					co_return {};
				}
			)
		;

		NContainer::TCVector<uint32> ParameterTypeOIDs = fg_PostgresBuildParameterTypeOIDs(_Parameters);
		CPostgresPreparedStatementKey PreparedStatementKey{_Sql, ParameterTypeOIDs};
		NStr::CStr PreparedStatementName;
		bool bParseStatement = false;
		if (NStr::CStr const *pCachedStatementName = Internal.m_PreparedStatementCache.f_FindEqual(PreparedStatementKey))
			PreparedStatementName = *pCachedStatementName;
		else
		{
			PreparedStatementName = "malterlib_";
			PreparedStatementName += NStr::CStr::fs_ToStr(++Internal.m_NextPreparedStatementID);
			bParseStatement = true;
		}

		CPostgresWriteStream QueryMessages;
		if (bParseStatement)
			fg_PostgresWriteParse(QueryMessages, PreparedStatementName, _Sql, ParameterTypeOIDs);

		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, PreparedStatementName, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		fg_PostgresWriteExecute(QueryMessages, {});
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<CPostgresRowDescription> RowDescription;
		CPostgresDataRowBatch Batch;
		bool bParseCompleted = !bParseStatement;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			bool bShouldYieldBatch = false;
			bool bShouldFinish = false;

			{
				CPostgresReadStream Stream;
				Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
				auto MessageCleanup = g_OnScopeExit / [&]
					{
						fp_ConsumeBackendMessage(MessageRef);
					}
				;
				// The stream points into m_ReceiveBuffer; suspending the coroutine while it
				// is open would let new socket data extend the buffer underneath us. Decoders
				// are synchronous (co_await on TCWrapped does not suspend), so any actual
				// suspension here is a bug. Yields are moved outside this scope.
#if DMibEnableSafeCheck > 0
				auto SuspendGuard = NConcurrency::g_OnSuspend / []
					{
						DMibFastCheck(false);
					}
				;
#endif

				switch (MessageRef.m_Type)
				{
				case '1':
					co_await fg_PostgresDecodeParseComplete(Stream);
					bParseCompleted = true;
					break;
				case '2':
					co_await fg_PostgresDecodeBindComplete(Stream);
					break;
				case 'n':
					co_await fg_PostgresDecodeNoData(Stream);
					break;
				case 'T':
					RowDescription = co_await fg_PostgresDecodeRowDescription(Stream);
					break;
				case 'D':
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					Batch.f_InsertLast(co_await fg_PostgresDecodeDataRow(Stream, *RowDescription));

					if (Batch.f_GetLen() >= _nRowsPerBatch)
						bShouldYieldBatch = true;
					break;
				case 'C':
					co_await fg_PostgresDecodeCommandComplete(Stream);
					break;
				case 'N':
					co_await fg_PostgresDecodeErrorResponse(Stream);
					break;
				case 'E':
					ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
					break;
				case 'Z':
					Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
					bStreamFinished = true;
					if (ErrorResponse)
					{
						auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
						ErrorMessage << ErrorResponse->m_Code;
						ErrorMessage << ErrorResponse->m_Message;

						co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
					}

					if (bParseStatement && bParseCompleted)
						Internal.m_PreparedStatementCache[fg_Move(PreparedStatementKey)] = PreparedStatementName;

					if (Batch.f_GetLen() != 0)
						bShouldYieldBatch = true;

					bShouldFinish = true;
					break;
				default:
					co_return DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL query returned unexpected backend message type {}") << MessageRef.m_Type);
				}
			}

			if (bShouldYieldBatch)
				co_yield fg_Move(Batch);

			if (bShouldFinish)
				co_return {};
		}
	}

	NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem> CPostgresClientActor::f_ExecuteRowsStream(NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Parameters, umint _nRowsPerBatch)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_nRowsPerBatch == 0)
			co_return DMibErrorInstance("PostgreSQL row stream batch size must be at least one");

		co_await Internal.f_WaitForProtocolCommand();

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		Internal.m_bProtocolCommandInProgress = true;
		bool bStreamFinished = false;
		auto ProtocolCommandCleanup = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					auto pThis = this;
					auto pInternal = &Internal;
					bool bStreamFinishedLocal = bStreamFinished;
					auto EndProtocolCommand = g_OnScopeExit / [pInternal]
						{
							pInternal->f_EndProtocolCommand();
						}
					;

					if (!bStreamFinishedLocal && pInternal->m_Socket)
					{
						if (pInternal->m_bInTransaction)
						{
							while (true)
							{
								auto MessageRef = co_await pThis->fp_ReadBackendMessageRef();
								CPostgresReadStream Stream;
								Stream.f_OpenRead(pInternal->m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
								auto MessageCleanup = g_OnScopeExit / [&]
									{
										pThis->fp_ConsumeBackendMessage(MessageRef);
									}
								;
#if DMibEnableSafeCheck > 0
								auto SuspendGuard = NConcurrency::g_OnSuspend / []
									{
										DMibFastCheck(false);
									}
								;
#endif

								if (MessageRef.m_Type == 'Z')
								{
									pInternal->f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
									break;
								}
							}
						}
						else
						{
							pInternal->m_bClosed = true;
							co_await pInternal->m_Socket
								(
									&NNetwork::CAsyncSocketActor::f_CloseWithLinger
									, NNetwork::EAsyncSocketStatus_NormalClosure
									, NStr::CStr("PostgreSQL row stream abandoned")
									, 1.0
								)
							;
						}
					}

					co_return {};
				}
			)
		;

		NContainer::TCVector<uint32> ParameterTypeOIDs = fg_PostgresBuildParameterTypeOIDs(_Parameters);
		CPostgresPreparedStatementKey PreparedStatementKey{_Sql, ParameterTypeOIDs};
		NStr::CStr PreparedStatementName;
		bool bParseStatement = false;
		if (NStr::CStr const *pCachedStatementName = Internal.m_PreparedStatementCache.f_FindEqual(PreparedStatementKey))
			PreparedStatementName = *pCachedStatementName;
		else
		{
			PreparedStatementName = "malterlib_";
			PreparedStatementName += NStr::CStr::fs_ToStr(++Internal.m_NextPreparedStatementID);
			bParseStatement = true;
		}

		CPostgresWriteStream QueryMessages;
		if (bParseStatement)
			fg_PostgresWriteParse(QueryMessages, PreparedStatementName, _Sql, ParameterTypeOIDs);

		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, PreparedStatementName, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		fg_PostgresWriteExecute(QueryMessages, {});
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<CPostgresRowDescription> RowDescription;
		bool bDescriptionYielded = false;
		CPostgresDataRowBatch Batch;
		bool bParseCompleted = !bParseStatement;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			bool bShouldYieldDescription = false;
			bool bShouldYieldBatch = false;
			bool bShouldFinish = false;

			{
				CPostgresReadStream Stream;
				Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
				auto MessageCleanup = g_OnScopeExit / [&]
					{
						fp_ConsumeBackendMessage(MessageRef);
					}
				;
				// The stream points into m_ReceiveBuffer; suspending the coroutine while it
				// is open would let new socket data extend the buffer underneath us. Decoders
				// are synchronous (co_await on TCWrapped does not suspend), so any actual
				// suspension here is a bug. Yields are moved outside this scope.
#if DMibEnableSafeCheck > 0
				auto SuspendGuard = NConcurrency::g_OnSuspend / []
					{
						DMibFastCheck(false);
					}
				;
#endif

				switch (MessageRef.m_Type)
				{
				case '1':
					co_await fg_PostgresDecodeParseComplete(Stream);
					bParseCompleted = true;
					break;
				case '2':
					co_await fg_PostgresDecodeBindComplete(Stream);
					break;
				case 'n':
					co_await fg_PostgresDecodeNoData(Stream);
					break;
				case 'T':
					RowDescription = co_await fg_PostgresDecodeRowDescription(Stream);
					break;
				case 'D':
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					Batch.f_InsertLast(co_await fg_PostgresDecodeDataRow(Stream, *RowDescription));

					if (!bDescriptionYielded)
					{
						bDescriptionYielded = true;
						bShouldYieldDescription = true;
					}

					if (Batch.f_GetLen() >= _nRowsPerBatch)
						bShouldYieldBatch = true;
					break;
				case 'C':
					co_await fg_PostgresDecodeCommandComplete(Stream);
					break;
				case 'N':
					co_await fg_PostgresDecodeErrorResponse(Stream);
					break;
				case 'E':
					ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
					break;
				case 'Z':
					Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
					bStreamFinished = true;
					if (ErrorResponse)
					{
						auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
						ErrorMessage << ErrorResponse->m_Code;
						ErrorMessage << ErrorResponse->m_Message;

						co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
					}

					if (bParseStatement && bParseCompleted)
						Internal.m_PreparedStatementCache[fg_Move(PreparedStatementKey)] = PreparedStatementName;

					if (!bDescriptionYielded && RowDescription)
					{
						bDescriptionYielded = true;
						bShouldYieldDescription = true;
					}

					if (Batch.f_GetLen() != 0)
						bShouldYieldBatch = true;

					bShouldFinish = true;
					break;
				default:
					co_return DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL query returned unexpected backend message type {}") << MessageRef.m_Type);
				}
			}

			if (bShouldYieldDescription)
				co_yield CPostgresRowStreamItem{.m_Description = *RowDescription};

			if (bShouldYieldBatch)
				co_yield CPostgresRowStreamItem{.m_Rows = fg_Move(Batch)};

			if (bShouldFinish)
				co_return {};
		}
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::f_PrepareStatement(NStr::CStr _Name, NStr::CStr _Sql, NContainer::TCVector<EPostgresValueType> _ParameterTypes)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Name.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement name is required");

		if (_Sql.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement SQL is required");

		co_await Internal.f_WaitForProtocolCommand();

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		Internal.m_bProtocolCommandInProgress = true;
		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		NContainer::TCVector<uint32> ParameterTypeOIDs;
		for (EPostgresValueType ParameterType : _ParameterTypes)
		{
			uint32 TypeOID = fg_PostgresGetValueTypeOID(ParameterType);
			DMibCheck(TypeOID != 0);
			ParameterTypeOIDs.f_InsertLast(TypeOID);
		}

		CPostgresWriteStream QueryMessages;
		fg_PostgresWriteParse(QueryMessages, _Name, _Sql, ParameterTypeOIDs);
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			CPostgresReadStream Stream;
			Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
			auto MessageCleanup = g_OnScopeExit / [&]
				{
					fp_ConsumeBackendMessage(MessageRef);
				}
			;
			// The stream points into m_ReceiveBuffer; suspending the coroutine here would
			// let new socket data extend the buffer underneath us. Decoders are synchronous
			// (co_await on TCWrapped does not suspend), so no suspension should ever happen
			// in this scope.
#if DMibEnableSafeCheck > 0
			auto SuspendGuard = NConcurrency::g_OnSuspend / []
				{
					DMibFastCheck(false);
				}
			;
#endif

			switch (MessageRef.m_Type)
			{
			case '1':
				co_await fg_PostgresDecodeParseComplete(Stream);
				break;
			case 'N':
				co_await fg_PostgresDecodeErrorResponse(Stream);
				break;
			case 'E':
				ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
				break;
			case 'Z':
				Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
				if (ErrorResponse)
				{
					auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
					ErrorMessage << ErrorResponse->m_Code;
					ErrorMessage << ErrorResponse->m_Message;

					co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
				}

				co_return {};
			default:
				co_return DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL query returned unexpected backend message type {}") << MessageRef.m_Type);
			}
		}
	}

	NConcurrency::TCFuture<CPostgresQueryResult> CPostgresClientActor::f_ExecutePrepared(NStr::CStr _Name, NContainer::TCVector<CPostgresValue> _Parameters)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Name.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement name is required");

		co_await Internal.f_WaitForProtocolCommand();

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		Internal.m_bProtocolCommandInProgress = true;
		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		CPostgresWriteStream QueryMessages;
		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, _Name, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		fg_PostgresWriteExecute(QueryMessages, {});
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		CPostgresQueryResult Result;
		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<CPostgresRowDescription> RowDescription;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			CPostgresReadStream Stream;
			Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
			auto MessageCleanup = g_OnScopeExit / [&]
				{
					fp_ConsumeBackendMessage(MessageRef);
				}
			;
			// The stream points into m_ReceiveBuffer; suspending the coroutine here would
			// let new socket data extend the buffer underneath us. Decoders are synchronous
			// (co_await on TCWrapped does not suspend), so no suspension should ever happen
			// in this scope.
#if DMibEnableSafeCheck > 0
			auto SuspendGuard = NConcurrency::g_OnSuspend / []
				{
					DMibFastCheck(false);
				}
			;
#endif

			switch (MessageRef.m_Type)
			{
			case '2':
				co_await fg_PostgresDecodeBindComplete(Stream);
				break;
			case 'n':
				co_await fg_PostgresDecodeNoData(Stream);
				break;
			case 'T':
				Result.m_RowDescription = co_await fg_PostgresDecodeRowDescription(Stream);
				RowDescription = *Result.m_RowDescription;
				break;
			case 'D':
				{
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					Result.m_Rows.f_InsertLast(co_await fg_PostgresDecodeDataRow(Stream, *RowDescription));
				}
				break;
			case 'C':
				Result.m_CommandComplete = co_await fg_PostgresDecodeCommandComplete(Stream);
				break;
			case 'N':
				co_await fg_PostgresDecodeErrorResponse(Stream);
				break;
			case 'E':
				ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
				break;
			case 'Z':
				Result.m_ReadyStatus = co_await fg_PostgresDecodeReadyForQuery(Stream);
				Internal.f_UpdateReadyStatus(Result.m_ReadyStatus);
				if (ErrorResponse)
				{
					auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
					ErrorMessage << ErrorResponse->m_Code;
					ErrorMessage << ErrorResponse->m_Message;

					co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
				}

				co_return Result;
			default:
				co_return DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL query returned unexpected backend message type {}") << MessageRef.m_Type);
			}
		}
	}

	auto CPostgresClientActor::fp_DecodeBulkResponse(CBackendMessageRef _MessageRef) -> NConcurrency::TCFuture<CBulkResponse>
	{
		auto &Internal = *mp_pInternal;

		CPostgresReadStream Stream;
		Stream.f_OpenRead(Internal.m_ReceiveBuffer, _MessageRef.m_PayloadOffset, _MessageRef.m_PayloadLength);
		auto MessageRef = _MessageRef;
		auto MessageCleanup = g_OnScopeExit / [&]
			{
				fp_ConsumeBackendMessage(MessageRef);
			}
		;
		// The stream points into m_ReceiveBuffer; suspending the coroutine here would let new socket data extend the
		// buffer underneath us. Decoders are synchronous (co_await on TCWrapped does not suspend), so no suspension
		// should ever happen in this scope.
#if DMibEnableSafeCheck > 0
		auto SuspendGuard = NConcurrency::g_OnSuspend / []
			{
				DMibFastCheck(false);
			}
		;
#endif

		CBulkResponse Response;
		Response.m_Type = MessageRef.m_Type;

		switch (MessageRef.m_Type)
		{
		case '2':
			co_await fg_PostgresDecodeBindComplete(Stream);

			break;
		case 'C':
			{
				auto Command = co_await fg_PostgresDecodeCommandComplete(Stream);
				auto const &Tag = Command.m_Tag;
				// The affected-row count is the final whitespace-delimited token of the tag ("INSERT oid rows",
				// "UPDATE rows", ...). Scanning back over the trailing non-space characters exits with Tag[iSpace - 1] == ' ',
				// so iSpace lands one past the last space - ON the first digit of the count ("INSERT 0 1" -> '1', "UPDATE 3"
				// -> '3'), never on the separator space. iSpace == length means the tag ended with a space and carried no count.
				umint iSpace = Tag.f_GetLen();
				while (iSpace != 0 && Tag[iSpace - 1] != ' ')
					--iSpace;

				if (iSpace == Tag.f_GetLen())
				{
					Response.m_ProtocolError = DMibErrorInstance("PostgreSQL pipelined mutation CommandComplete tag did not include affected row count");

					break;
				}

				umint nRows = 0;
				bool bNumeric = true;
				// Parse from iSpace (the first digit), not the preceding separator space - exercised by the bulk-insert test,
				// which would fail at the first CommandComplete if a space were parsed here as a non-digit.
				for (umint i = iSpace; i < Tag.f_GetLen(); ++i)
				{
					if (Tag[i] < '0' || Tag[i] > '9')
					{
						Response.m_ProtocolError = DMibErrorInstance("PostgreSQL pipelined mutation CommandComplete affected row count was not numeric");
						bNumeric = false;

						break;
					}

					nRows = nRows * 10 + umint(Tag[i] - '0');
				}

				if (bNumeric)
				{
					Response.m_AffectedRows = nRows;
					Response.m_bAffectedRowsValid = true;
				}
			}
			break;
		case 'N':
			co_await fg_PostgresDecodeErrorResponse(Stream);

			break;
		case 'E':
			Response.m_ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);

			break;
		case 'Z':
			{
				auto ReadyStatus = co_await fg_PostgresDecodeReadyForQuery(Stream);
				Internal.f_UpdateReadyStatus(ReadyStatus);
			}
			break;
		default:
			Response.m_ProtocolError = DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL pipelined execution returned unexpected backend message type {}") << MessageRef.m_Type);

			break;
		}

		co_return Response;
	}

	auto CPostgresClientActor::fp_ReadBulkResponse() -> NConcurrency::TCFuture<CBulkResponse>
	{
		auto MessageRef = co_await fp_ReadBackendMessageRef();

		co_return co_await fp_DecodeBulkResponse(MessageRef);
	}

	auto CPostgresClientActor::fp_TryReadBulkResponse() -> NConcurrency::TCFuture<NStorage::TCOptional<CBulkResponse>>
	{
		auto &Internal = *mp_pInternal;

		// Decode a response only if a whole backend message is already buffered - a 5-byte header plus its declared
		// payload. This mirrors the framing in fp_ReadBackendMessageRef but checks the buffer length instead of
		// awaiting more bytes, so it never suspends and reports "nothing ready yet" as an empty optional.
		if (Internal.m_ReceiveBuffer.f_GetLen() < 5)
			co_return NStorage::TCOptional<CBulkResponse>{};

		uint8 HeaderBytes[5];
		Internal.m_ReceiveBuffer.f_ReadFront
			(
				5
				, [&](umint _iStart, uint8 const *_pData, umint _nReadBytes) -> bool
				{
					NMemory::fg_MemCopy(HeaderBytes + _iStart, _pData, _nReadBytes);
					return true;
				}
			)
		;

		uint32 Length = (uint32(HeaderBytes[1]) << 24) | (uint32(HeaderBytes[2]) << 16) | (uint32(HeaderBytes[3]) << 8) | uint32(HeaderBytes[4]);
		if (Length < 4)
		{
			CBulkResponse Response;
			Response.m_ProtocolError = DMibErrorInstance("PostgreSQL backend message has invalid length");

			co_return Response;
		}

		umint TotalLength = umint(Length) + 1;
		if (Internal.m_ReceiveBuffer.f_GetLen() < TotalLength)
			co_return NStorage::TCOptional<CBulkResponse>{};

		co_return co_await fp_DecodeBulkResponse(CBackendMessageRef{.m_Type = HeaderBytes[0], .m_PayloadOffset = 5, .m_PayloadLength = umint(Length) - 4, .m_TotalLength = TotalLength});
	}

	auto CPostgresClientActor::f_ExecutePreparedBulk
		(
			NStr::CStr _Name
			, NConcurrency::TCAsyncGenerator<NContainer::TCVector<NContainer::TCVector<CPostgresValue>>> _ParameterBatches
		)
		-> NConcurrency::TCFuture<umint>
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Name.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement name is required");

		co_await Internal.f_WaitForProtocolCommand();

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		Internal.m_bProtocolCommandInProgress = true;
		bool bAnythingSent = false;
		bool bResynchronized = false;
		auto ProtocolCommandCleanup = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					auto pThis = this;
					auto pInternal = &Internal;
					bool bAnythingSentLocal = bAnythingSent;
					bool bResynchronizedLocal = bResynchronized;
					auto EndProtocolCommand = g_OnScopeExit / [pInternal]
						{
							pInternal->f_EndProtocolCommand();
						}
					;

					// Normal completion already sent the final Sync and read to ReadyForQuery. If we unwind before that
					// - for example the parameter generator throws after Bind/Execute messages were already written for
					// a row that fails value conversion - the backend is still mid extended-query cycle with responses
					// pending and no Sync has been sent. Send a Sync now and drain to ReadyForQuery so the stream is
					// resynchronised before this client is reused; otherwise the next command would consume the stale
					// bulk responses (or hang).
					if (bAnythingSentLocal && !bResynchronizedLocal && pInternal->m_Socket && !pInternal->m_bClosed)
					{
						CPostgresWriteStream SyncMessage;
						fg_PostgresWriteSync(SyncMessage);
						co_await pThis->fp_Send(SyncMessage.f_MoveVector());

						while (true)
						{
							auto MessageRef = co_await pThis->fp_ReadBackendMessageRef();
							if (MessageRef.m_Type == 'Z')
							{
								CPostgresReadStream Stream;
								Stream.f_OpenRead(pInternal->m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
								pInternal->f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
								pThis->fp_ConsumeBackendMessage(MessageRef);

								break;
							}

							pThis->fp_ConsumeBackendMessage(MessageRef);
						}
					}

					co_return {};
				}
			)
		;

		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));

		umint nExpectedResults = 0;
		umint nTotalAffected = 0;
		umint nResultsReceived = 0;
		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;

		// Keep at most a pipeline-length window of batches in flight - sent but not yet drained - so how far the backend
		// runs ahead of the client, and the unread CommandComplete responses the client buffers in memory, stay
		// bounded by the caller's batch size times the pipeline length (both caller-controlled). This is a
		// memory/throughput control, not deadlock avoidance: the socket layer always drains incoming data into
		// m_ReceiveBuffer - its receive callback runs on this actor even while a send is suspended in fp_Send - so the
		// backend never blocks writing and the exchange cannot deadlock however much is in flight. Rather than draining
		// every response to zero after each window (which idles the socket and serialises send against receive), drain
		// only enough to retire the oldest batch when the window is full, then send the next - so the pipeline stays
		// full. Flushing (not Syncing) keeps the whole bulk insert inside its single implicit transaction.
		umint nMaxInFlightBatches = Internal.m_Settings.m_nPipelineLength != 0 ? Internal.m_Settings.m_nPipelineLength : 1;

		// Ring buffer holding the still-undrained command count of each in-flight batch, oldest at iRingHead. Responses
		// arrive in send order, so each CommandComplete retires a command from the oldest batch; when its entry reaches
		// zero that batch has fully drained and a window slot is free.
		NContainer::TCVector<umint> InFlightBatchRemaining;
		InFlightBatchRemaining.f_SetLen(nMaxInFlightBatches);
		umint iRingHead = 0;
		umint nInFlightBatches = 0;
		bool bUnflushedSends = false;

		// Apply one decoded response to the window: tally affected rows and retire a command from the oldest in-flight
		// batch, freeing a slot when that batch is fully drained. Returns true when draining should stop because a
		// protocol error (captured in ProtocolError) or a backend error response was seen.
		NException::CExceptionPointer ProtocolError;
		auto fApplyResponse = [&](CBulkResponse &&_Response) -> bool
			{
				if (_Response.m_ProtocolError)
				{
					ProtocolError = fg_Move(_Response.m_ProtocolError);

					return true;
				}

				if (_Response.m_bAffectedRowsValid)
				{
					nTotalAffected += _Response.m_AffectedRows;
					++nResultsReceived;
					if (--InFlightBatchRemaining[iRingHead] == 0)
					{
						iRingHead = (iRingHead + 1) % nMaxInFlightBatches;
						--nInFlightBatches;
					}
				}

				if (_Response.m_ErrorResponse)
				{
					ErrorResponse = fg_Move(_Response.m_ErrorResponse);

					return true;
				}

				return false;
			}
		;

		for (auto iBatch = co_await fg_Move(_ParameterBatches).f_GetPipelinedIterator(Internal.m_Settings.m_nPipelineLength); iBatch; co_await ++iBatch)
		{
			if ((*iBatch).f_IsEmpty())
				continue;

			// Eagerly drain every response that has already arrived, without suspending, so window slots are freed and
			// m_ReceiveBuffer stays small whenever the backend has run ahead (typically the burst a previous Flush
			// produced). Only the blocking read below, reached when the window is genuinely full, waits for more.
			for (;;)
			{
				NStorage::TCOptional<CBulkResponse> Response = co_await fp_TryReadBulkResponse();
				if (!Response)
					break;

				if (fApplyResponse(fg_Move(*Response)))
					break;
			}

			if (ProtocolError)
				co_return ProtocolError;
			if (ErrorResponse)
				break;

			// Still full after the opportunistic drain: flush the sent-but-unflushed batches so the backend delivers
			// their responses, then block until the oldest batch retires and frees one slot.
			while (nInFlightBatches >= nMaxInFlightBatches)
			{
				if (bUnflushedSends)
				{
					CPostgresWriteStream FlushMessage;
					fg_PostgresWriteFlush(FlushMessage);
					co_await fp_Send(FlushMessage.f_MoveVector());
					bUnflushedSends = false;
				}

				if (fApplyResponse(co_await fp_ReadBulkResponse()))
					break;
			}

			if (ProtocolError)
				co_return ProtocolError;
			if (ErrorResponse)
				break;

			CPostgresWriteStream BatchMessages;
			for (auto const &Parameters : *iBatch)
			{
				co_await fg_PostgresWriteBind(BatchMessages, {}, _Name, Parameters, ResultFormats);
				fg_PostgresWriteExecute(BatchMessages, {});
			}

			umint nBatchCommands = (*iBatch).f_GetLen();
			nExpectedResults += nBatchCommands;
			co_await fp_Send(BatchMessages.f_MoveVector());
			bAnythingSent = true;
			bUnflushedSends = true;

			InFlightBatchRemaining[(iRingHead + nInFlightBatches) % nMaxInFlightBatches] = nBatchCommands;
			++nInFlightBatches;
		}

		if (nExpectedResults == 0)
			co_return umint(0);

		// A final Sync drains every still-in-flight batch and commits the implicit transaction (a Flush alone leaves it
		// open). After an error the backend has been discarding messages until this Sync, which re-synchronises it and
		// yields ReadyForQuery.
		CPostgresWriteStream SyncMessage;
		fg_PostgresWriteSync(SyncMessage);
		co_await fp_Send(SyncMessage.f_MoveVector());

		for (;;)
		{
			CBulkResponse Response = co_await fp_ReadBulkResponse();
			if (Response.m_ProtocolError)
				co_return Response.m_ProtocolError;

			if (Response.m_bAffectedRowsValid)
			{
				nTotalAffected += Response.m_AffectedRows;
				++nResultsReceived;
			}

			if (Response.m_ErrorResponse)
				ErrorResponse = fg_Move(Response.m_ErrorResponse);

			if (Response.m_Type == 'Z')
				break;
		}

		// The backend is now at ReadyForQuery, so the destroy-time cleanup must not send a second Sync.
		bResynchronized = true;

		if (ErrorResponse)
		{
			auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
			ErrorMessage << ErrorResponse->m_Code;
			ErrorMessage << ErrorResponse->m_Message;

			co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
		}

		if (nResultsReceived != nExpectedResults)
			co_return DMibErrorInstance("PostgreSQL pipelined execution returned fewer CommandComplete messages than expected");

		co_return nTotalAffected;
	}

	auto CPostgresClientActor::f_ExecutePreparedRows
		(
			NStr::CStr _Name
			, NContainer::TCVector<CPostgresValue> _Parameters
			, umint _nRowsPerBatch
		)
		-> NConcurrency::TCAsyncGenerator<CPostgresDataRowBatch>
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Name.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement name is required");

		if (_nRowsPerBatch == 0)
			co_return DMibErrorInstance("PostgreSQL row stream batch size must be at least one");

		co_await Internal.f_WaitForProtocolCommand();

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		Internal.m_bProtocolCommandInProgress = true;
		bool bStreamFinished = false;
		auto ProtocolCommandCleanup = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					auto pThis = this;
					auto pInternal = &Internal;
					bool bStreamFinishedLocal = bStreamFinished;
					auto EndProtocolCommand = g_OnScopeExit / [pInternal]
						{
							pInternal->f_EndProtocolCommand();
						}
					;

					if (!bStreamFinishedLocal && pInternal->m_Socket)
					{
						if (pInternal->m_bInTransaction)
						{
							while (true)
							{
								auto MessageRef = co_await pThis->fp_ReadBackendMessageRef();
								CPostgresReadStream Stream;
								Stream.f_OpenRead(pInternal->m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
								auto MessageCleanup = g_OnScopeExit / [&]
									{
										pThis->fp_ConsumeBackendMessage(MessageRef);
									}
								;
#if DMibEnableSafeCheck > 0
								auto SuspendGuard = NConcurrency::g_OnSuspend / []
									{
										DMibFastCheck(false);
									}
								;
#endif

								if (MessageRef.m_Type == 'Z')
								{
									pInternal->f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
									break;
								}
							}
						}
						else
						{
							pInternal->m_bClosed = true;
							co_await pInternal->m_Socket
								(
									&NNetwork::CAsyncSocketActor::f_CloseWithLinger
									, NNetwork::EAsyncSocketStatus_NormalClosure
									, NStr::CStr("PostgreSQL row stream abandoned")
									, 1.0
								)
							;
						}
					}

					co_return {};
				}
			)
		;

		CPostgresWriteStream QueryMessages;
		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, _Name, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		fg_PostgresWriteExecute(QueryMessages, {});
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<CPostgresRowDescription> RowDescription;
		CPostgresDataRowBatch Batch;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			bool bShouldYieldBatch = false;
			bool bShouldFinish = false;

			{
				CPostgresReadStream Stream;
				Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
				auto MessageCleanup = g_OnScopeExit / [&]
					{
						fp_ConsumeBackendMessage(MessageRef);
					}
				;
				// The stream points into m_ReceiveBuffer; suspending the coroutine while it
				// is open would let new socket data extend the buffer underneath us. Decoders
				// are synchronous (co_await on TCWrapped does not suspend), so any actual
				// suspension here is a bug. Yields are moved outside this scope.
#if DMibEnableSafeCheck > 0
				auto SuspendGuard = NConcurrency::g_OnSuspend / []
					{
						DMibFastCheck(false);
					}
				;
#endif

				switch (MessageRef.m_Type)
				{
				case '2':
					co_await fg_PostgresDecodeBindComplete(Stream);
					break;
				case 'n':
					co_await fg_PostgresDecodeNoData(Stream);
					break;
				case 'T':
					RowDescription = co_await fg_PostgresDecodeRowDescription(Stream);
					break;
				case 'D':
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					Batch.f_InsertLast(co_await fg_PostgresDecodeDataRow(Stream, *RowDescription));

					if (Batch.f_GetLen() >= _nRowsPerBatch)
						bShouldYieldBatch = true;
					break;
				case 'C':
					co_await fg_PostgresDecodeCommandComplete(Stream);
					break;
				case 'N':
					co_await fg_PostgresDecodeErrorResponse(Stream);
					break;
				case 'E':
					ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
					break;
				case 'Z':
					Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
					bStreamFinished = true;
					if (ErrorResponse)
					{
						auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
						ErrorMessage << ErrorResponse->m_Code;
						ErrorMessage << ErrorResponse->m_Message;

						co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
					}

					if (Batch.f_GetLen() != 0)
						bShouldYieldBatch = true;

					bShouldFinish = true;
					break;
				default:
					co_return DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL query returned unexpected backend message type {}") << MessageRef.m_Type);
				}
			}

			if (bShouldYieldBatch)
				co_yield fg_Move(Batch);

			if (bShouldFinish)
				co_return {};
		}
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::f_DeallocatePrepared(NStr::CStr _Name)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Name.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement name is required");

		co_await Internal.f_WaitForProtocolCommand();

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		Internal.m_bProtocolCommandInProgress = true;
		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		CPostgresWriteStream QueryMessages;
		fg_PostgresWriteClose(QueryMessages, EPostgresDescribeTarget::mc_Statement, _Name);
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			CPostgresReadStream Stream;
			Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
			auto MessageCleanup = g_OnScopeExit / [&]
				{
					fp_ConsumeBackendMessage(MessageRef);
				}
			;
			// The stream points into m_ReceiveBuffer; suspending the coroutine here would
			// let new socket data extend the buffer underneath us. Decoders are synchronous
			// (co_await on TCWrapped does not suspend), so no suspension should ever happen
			// in this scope.
#if DMibEnableSafeCheck > 0
			auto SuspendGuard = NConcurrency::g_OnSuspend / []
				{
					DMibFastCheck(false);
				}
			;
#endif

			switch (MessageRef.m_Type)
			{
			case '3':
				co_await fg_PostgresDecodeCloseComplete(Stream);
				break;
			case 'N':
				co_await fg_PostgresDecodeErrorResponse(Stream);
				break;
			case 'E':
				ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
				break;
			case 'Z':
				Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
				if (ErrorResponse)
				{
					auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
					ErrorMessage << ErrorResponse->m_Code;
					ErrorMessage << ErrorResponse->m_Message;

					co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
				}

				co_return {};
			default:
				co_return DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL query returned unexpected backend message type {}") << MessageRef.m_Type);
			}
		}
	}

	namespace
	{
		NStr::CStr fg_PostgresBeginTransactionSql(bool _bReadOnly, CSqlTransactionSettings _Settings)
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "BEGIN TRANSACTION";
				if (_bReadOnly)
					Appender += " READ ONLY";

				auto const bNeedsIsolationSeparator = _bReadOnly && _Settings.m_Isolation != ESqlTransactionIsolation::mc_Default;
				if (bNeedsIsolationSeparator)
					Appender += ",";

				switch (_Settings.m_Isolation)
				{
				case ESqlTransactionIsolation::mc_Default:
					break;
				case ESqlTransactionIsolation::mc_ReadCommitted:
					Appender += " ISOLATION LEVEL READ COMMITTED";
					break;
				case ESqlTransactionIsolation::mc_RepeatableRead:
					Appender += " ISOLATION LEVEL REPEATABLE READ";
					break;
				case ESqlTransactionIsolation::mc_Serializable:
					Appender += " ISOLATION LEVEL SERIALIZABLE";
					break;
				}
			}

			return Sql;
		}
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::f_BeginTransaction(bool _bReadOnly, CSqlTransactionSettings _Settings)
	{
		CPostgresQueryResult Result = co_await f_Execute(fg_PostgresBeginTransactionSql(_bReadOnly, _Settings));
		if (!Result.m_CommandComplete || Result.m_CommandComplete->m_Tag != "BEGIN")
			co_return DMibErrorInstance("PostgreSQL did not acknowledge transaction begin");

		if (Result.m_ReadyStatus != EPostgresReadyForQueryStatus::mc_InTransaction)
			co_return DMibErrorInstance("PostgreSQL transaction begin did not enter transaction state");

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::f_CommitTransaction()
	{
		CPostgresQueryResult Result = co_await f_Execute("COMMIT TRANSACTION");
		if (!Result.m_CommandComplete || Result.m_CommandComplete->m_Tag != "COMMIT")
			co_return DMibErrorInstance("PostgreSQL did not acknowledge transaction commit");

		if (Result.m_ReadyStatus != EPostgresReadyForQueryStatus::mc_Idle)
			co_return DMibErrorInstance("PostgreSQL transaction commit did not return to idle state");

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::f_RollbackTransaction()
	{
		CPostgresQueryResult Result = co_await f_Execute("ROLLBACK TRANSACTION");
		if (!Result.m_CommandComplete || Result.m_CommandComplete->m_Tag != "ROLLBACK")
			co_return DMibErrorInstance("PostgreSQL did not acknowledge transaction rollback");

		if (Result.m_ReadyStatus != EPostgresReadyForQueryStatus::mc_Idle)
			co_return DMibErrorInstance("PostgreSQL transaction rollback did not return to idle state");

		co_return {};
	}

	NConcurrency::TCFuture<bool> CPostgresClientActor::f_IsInTransaction()
	{
		// m_bInTransaction tracks the most recently decoded ReadyForQuery status. Actor calls are serialised, so a
		// caller that runs this after an operation completes observes that operation's final session state.
		co_return mp_pInternal->m_bInTransaction;
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_Destroy()
	{
		auto &Internal = *mp_pInternal;

		co_await f_Close();

		if (Internal.m_ClientActor)
			co_await fg_Move(Internal.m_ClientActor).f_Destroy();

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_OnSocketData(NStorage::TCSharedPointer<NContainer::CIOByteVector> _pData)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		Internal.m_ReceiveBuffer.f_InsertBack(_pData->f_GetArray(), _pData->f_GetLen());

		if (Internal.m_ReceivePromise)
		{
			Internal.m_ReceivePromise->f_SetResult();
			Internal.m_ReceivePromise.f_Clear();
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_OnSocketClose()
	{
		auto &Internal = *mp_pInternal;

		Internal.f_StopKeepAlive();
		Internal.m_bClosed = true;
		Internal.m_bInTransaction = false;

		if (Internal.m_ReceivePromise)
		{
			Internal.m_ReceivePromise->f_SetException(DMibErrorInstance("PostgreSQL socket closed"));
			Internal.m_ReceivePromise.f_Clear();
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_KeepAlive()
	{
		auto &Internal = *mp_pInternal;

		if (Internal.m_bProtocolCommandInProgress || Internal.m_bClosed || !Internal.m_Socket || Internal.m_bInTransaction)
			co_return {};

		co_await f_Execute("SELECT 1").f_Wrap()
			> NConcurrency::fg_LogError("PostgreSQL client", "PostgreSQL keepalive failed")
		;

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_Send(NContainer::CIOByteVector _Data)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return DMibErrorInstance("PostgreSQL socket is not connected");

		co_await Internal.m_Socket(&NNetwork::CAsyncSocketActor::f_SendData, fg_Construct(fg_Move(_Data)), uint32(0));

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_WaitForReceiveBytes(umint _nBytes)
	{
		auto &Internal = *mp_pInternal;

		while (Internal.m_ReceiveBuffer.f_GetLen() < _nBytes)
		{
			if (Internal.m_bClosed)
				co_return DMibErrorInstance("PostgreSQL socket closed while waiting for data");

			NConcurrency::TCPromiseFuturePair<void> Promise;
			Internal.m_ReceivePromise = Promise.m_Promise;

			co_await fg_Move(Promise.m_Future);
		}

		co_return {};
	}

	NConcurrency::TCFuture<NContainer::CIOByteVector> CPostgresClientActor::fp_ReadBytes(umint _nBytes)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		co_await fp_WaitForReceiveBytes(_nBytes);

		NContainer::CIOByteVector Data;
		Data.f_Reserve(_nBytes);
		Internal.m_ReceiveBuffer.f_ReadFront
			(
				_nBytes
				, [&](umint _iStart, uint8 const *_pData, umint _nReadBytes) -> bool
				{
					(void)_iStart;
					Data.f_InsertLast(_pData, _nReadBytes);
					return true;
				}
			)
		;

		Internal.m_ReceiveBuffer.f_RemoveFront(_nBytes);

		co_return Data;
	}

	NConcurrency::TCFuture<CPostgresClientActor::CBackendMessageRef> CPostgresClientActor::fp_ReadBackendMessageRef()
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		co_await fp_WaitForReceiveBytes(5);

		uint8 HeaderBytes[5];
		Internal.m_ReceiveBuffer.f_ReadFront
			(
				5
				, [&](umint _iStart, uint8 const *_pData, umint _nReadBytes) -> bool
				{
					NMemory::fg_MemCopy(HeaderBytes + _iStart, _pData, _nReadBytes);
					return true;
				}
			)
		;

		uint8 Type = HeaderBytes[0];
		uint32 Length = (uint32(HeaderBytes[1]) << 24) | (uint32(HeaderBytes[2]) << 16) | (uint32(HeaderBytes[3]) << 8) | uint32(HeaderBytes[4]);

		if (Length < 4)
			co_return DMibErrorInstance("PostgreSQL backend message has invalid length");

		umint TotalLength = umint(Length) + 1;
		co_await fp_WaitForReceiveBytes(TotalLength);

		co_return CBackendMessageRef{.m_Type = Type, .m_PayloadOffset = 5, .m_PayloadLength = umint(Length) - 4, .m_TotalLength = TotalLength};
	}

	void CPostgresClientActor::fp_ConsumeBackendMessage(CBackendMessageRef const &_Ref)
	{
		mp_pInternal->m_ReceiveBuffer.f_RemoveFront(_Ref.m_TotalLength);
	}
}
