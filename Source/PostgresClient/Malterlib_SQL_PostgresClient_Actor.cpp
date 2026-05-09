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
		using CPostgresWriteRefStream = NStream::TCBinaryStreamMemoryRef<NStream::CBinaryStreamBigEndian, NContainer::CIOByteVector>;

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

		CExceptionSql fg_PostgresConnectionLossError(NStr::CStr const &_Message)
		{
			return DMibErrorSqlInstance
				(
					_Message
					, fg_SqlErrorData(ESqlErrorCategory::mc_ConnectionLoss, ESqlErrorRetryClass::mc_RetryConnection, "postgres", NStr::CStr(), fg_TempCopy(_Message))
				)
			;
		}

		enum class EPostgresStatementDrop
		{
			mc_None
			, mc_One // A named DEALLOCATE, which the command tag does not name
			, mc_All
		};

		EPostgresStatementDrop fg_PostgresCommandStatementDrop(CPostgresCommandComplete const &_Command)
		{
			if (_Command.m_Tag == "DEALLOCATE ALL" || _Command.m_Tag == "DISCARD ALL")
				return EPostgresStatementDrop::mc_All;

			if (_Command.m_Tag.f_StartsWith("DEALLOCATE"))
				return EPostgresStatementDrop::mc_One;

			return EPostgresStatementDrop::mc_None;
		}

		// ParameterStatus (for example after SET) and NotificationResponse can arrive between any other messages, so the readers
		// consume them instead of handing them to the response loops.
		bool fg_PostgresIsAsynchronousMessage(uint8 _Type)
		{
			return _Type == 'S' || _Type == 'A';
		}

		// PostgreSQL reads the Execute row limit as a signed 32-bit integer and runs the portal to completion for a limit of zero or less
		uint32 fg_PostgresRowsPerExecute(umint _nRowsPerBatch)
		{
			DMibFastCheck(_nRowsPerBatch != 0);
			constexpr umint c_MaxRowsPerExecute = umint(TCLimitsInt<int32>::mc_Max);
			return uint32(_nRowsPerBatch < c_MaxRowsPerExecute ? _nRowsPerBatch : c_MaxRowsPerExecute);
		}

		struct CPostgresChannelBindingState
		{
			NThread::CLowLevelLock m_Lock;
			NContainer::CByteVector m_Data;
		};
	}

	struct CPostgresClientActor::CInternal : public NConcurrency::CActorInternal
	{
		void f_UpdateReadyStatus(EPostgresReadyForQueryStatus _ReadyStatus);
		void f_CacheStatement(CPostgresPreparedStatementKey &&_Key, NStr::CStr const &_Name);
		void f_EvictStatement(CPostgresPreparedStatementKey const &_Key);
		EPostgresStatementDrop f_OnCommandComplete(CPostgresCommandComplete const &_Command);
		void f_OnStatementsDropped(EPostgresStatementDrop _Drop);
		umint f_WritePendingStatementCloses(CPostgresWriteStream &_Stream);
		void f_RemoveSentStatementCloses(umint _nSent);
		void f_StopKeepAlive();
		void f_MarkClosed();
		NConcurrency::TCFuture<bool> f_WaitForProtocolCommand();
		void f_FailProtocolCommandWaiters();
		void f_EndProtocolCommand();
		NConcurrency::TCFuture<void> f_ResetStartupConnection();

		CPostgresConnectionSettings m_Settings;
		NConcurrency::TCActor<NNetwork::CAsyncSocketClientActor> m_ClientActor;
		NConcurrency::TCActorInterface<NNetwork::CAsyncSocketActor> m_Socket;
		NConcurrency::CActorSubscription m_KeepAliveTimerSubscription;
		NConcurrency::TCActorFunctorWeak<NConcurrency::TCFuture<void> ()> m_fOnConnectionLost;
		NContainer::CPagedByteVector m_ReceiveBuffer{4096};
		NStorage::TCOptional<NConcurrency::TCPromise<void>> m_ReceivePromise;
		uint64 m_iReceiveWaiter = 0;                                                          // The read that m_ReceivePromise wakes, the latest one started
		NContainer::TCLinkedList<NConcurrency::TCPromise<bool>> m_ProtocolCommandWaiters;
		NStorage::TCOptional<CPostgresScramClientFirstMessage> m_ScramClientFirst;
		NStorage::TCOptional<CPostgresScramClientFinalMessage> m_ScramClientFinal;
		NContainer::TCMap<CPostgresPreparedStatementKey, NStr::CStr> m_PreparedStatementCache;
		NContainer::TCVector<NStr::CStr> m_PendingStatementCloses;                            // Evicted statements, closed by the next request with SQL text, also on a cache hit
		NContainer::TCMap<NStr::CStr, NContainer::TCVector<uint32>> m_PreparedParameterTypes; // Server parameter type OIDs of named statements
		uint64 m_nStatementDrops = 0;                                                         // Command tags that can drop prepared statements, counted even when none existed
		uint64 m_nTransactionEndTags = 0;
		uint64 m_nResetTags = 0;
		uint64 m_NextPreparedStatementID = 0;
		bool m_bProtocolCommandInProgress = false;
		bool m_bClosed = false;
		bool m_bInTransaction = false;
		bool m_bTransactionFailed = false;
		bool m_bResponsePending = false;                                                      // A send was attempted and its ReadyForQuery has not been read
		umint m_nServerWaits = 0;                                                             // Sends and reads in progress, which wait for the server
	};

	void CPostgresClientActor::CInternal::f_UpdateReadyStatus(EPostgresReadyForQueryStatus _ReadyStatus)
	{
		m_bInTransaction = _ReadyStatus != EPostgresReadyForQueryStatus::mc_Idle;
		m_bTransactionFailed = _ReadyStatus == EPostgresReadyForQueryStatus::mc_FailedTransaction;
		m_bResponsePending = false;
	}

	// Bounded, since each distinct SQL text and parameter types keeps a statement on the server
	void CPostgresClientActor::CInternal::f_CacheStatement(CPostgresPreparedStatementKey &&_Key, NStr::CStr const &_Name)
	{
		constexpr umint c_MaxCachedStatements = 256;
		if (!m_PreparedStatementCache.f_FindEqual(_Key) && m_PreparedStatementCache.f_GetLen() >= c_MaxCachedStatements)
		{
			auto iOldest = m_PreparedStatementCache.f_GetIterator();
			m_PendingStatementCloses.f_InsertLast(*iOldest);
			m_PreparedStatementCache.f_Remove(iOldest.f_GetKey());
		}

		m_PreparedStatementCache[fg_Move(_Key)] = _Name;
	}

	void CPostgresClientActor::CInternal::f_EvictStatement(CPostgresPreparedStatementKey const &_Key)
	{
		if (auto *pName = m_PreparedStatementCache.f_FindEqual(_Key))
		{
			m_PendingStatementCloses.f_InsertLast(*pName);
			m_PreparedStatementCache.f_Remove(_Key);
		}
	}

	EPostgresStatementDrop CPostgresClientActor::CInternal::f_OnCommandComplete(CPostgresCommandComplete const &_Command)
	{
		if (_Command.m_Tag == "COMMIT" || _Command.m_Tag == "ROLLBACK" || _Command.m_Tag == "PREPARE TRANSACTION")
			++m_nTransactionEndTags;
		else if (_Command.m_Tag == "RESET")
			++m_nResetTags;

		return fg_PostgresCommandStatementDrop(_Command);
	}

	// After a named DEALLOCATE any cached statement can be gone, so all are forgotten, and closed since the others still
	// exist on the server
	void CPostgresClientActor::CInternal::f_OnStatementsDropped(EPostgresStatementDrop _Drop)
	{
		if (_Drop == EPostgresStatementDrop::mc_None)
			return;

		++m_nStatementDrops;

		// Another statement can take a dropped name, so the types are described again on the next execution
		m_PreparedParameterTypes.f_Clear();

		if (_Drop == EPostgresStatementDrop::mc_One)
		{
			for (auto const &Name : m_PreparedStatementCache)
				m_PendingStatementCloses.f_InsertLast(Name);
		}

		m_PreparedStatementCache.f_Clear();
	}

	// The closes stay pending until f_RemoveSentStatementCloses, since building the rest of the request can fail
	umint CPostgresClientActor::CInternal::f_WritePendingStatementCloses(CPostgresWriteStream &_Stream)
	{
		for (auto const &Name : m_PendingStatementCloses)
			fg_PostgresWriteClose(_Stream, EPostgresDescribeTarget::mc_Statement, Name);

		return m_PendingStatementCloses.f_GetLen();
	}

	void CPostgresClientActor::CInternal::f_RemoveSentStatementCloses(umint _nSent)
	{
		m_PendingStatementCloses.f_Remove(0, _nSent);
	}

	void CPostgresClientActor::CInternal::f_StopKeepAlive()
	{
		m_KeepAliveTimerSubscription.f_Clear();
	}

	void CPostgresClientActor::CInternal::f_MarkClosed()
	{
		bool bWasOpen = !m_bClosed;
		m_bClosed = true;
		m_bInTransaction = false;
		m_bTransactionFailed = false;

		if (bWasOpen && m_fOnConnectionLost)
			DMibLogWarningOrDiscardResult(m_fOnConnectionLost(), "PostgreSQL client", "Failed to report a lost PostgreSQL connection");
	}

	// Returns false without the turn when the connection closes while another call holds it, as a row stream suspended in its
	// consumer does
	NConcurrency::TCFuture<bool> CPostgresClientActor::CInternal::f_WaitForProtocolCommand()
	{
		if (!m_bProtocolCommandInProgress)
		{
			m_bProtocolCommandInProgress = true;
			co_return true;
		}

		if (m_bClosed)
			co_return false;

		NConcurrency::TCPromiseFuturePair<bool> Waiter;
		m_ProtocolCommandWaiters.f_InsertLast(fg_Move(Waiter.m_Promise));

		co_return co_await fg_Move(Waiter.m_Future);
	}

	void CPostgresClientActor::CInternal::f_FailProtocolCommandWaiters()
	{
		auto Waiters = fg_Move(m_ProtocolCommandWaiters);
		for (auto &Waiter : Waiters)
			Waiter.f_SetResult(false);
	}

	void CPostgresClientActor::CInternal::f_EndProtocolCommand()
	{
		// Reusing the connection before ReadyForQuery would hand the rest of this response to the next command
		if (m_bResponsePending)
		{
			f_MarkClosed();
			m_bResponsePending = false;
		}

		if (m_bClosed || !m_Socket)
		{
			m_bProtocolCommandInProgress = false;
			f_FailProtocolCommandWaiters();
			return;
		}

		// The turn goes directly to the next waiter, so no other call starts before it resumes
		if (!m_ProtocolCommandWaiters.f_IsEmpty())
		{
			m_ProtocolCommandWaiters.f_PopFirst().f_SetResult(true);
			return;
		}

		m_bProtocolCommandInProgress = false;
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::CInternal::f_ResetStartupConnection()
	{
		if (m_Socket)
		{
			co_await m_Socket(&NNetwork::CAsyncSocketActor::f_CloseWithLinger, NNetwork::EAsyncSocketStatus_NormalClosure, NStr::CStr("PostgreSQL startup failed"), 1.0).f_Wrap()
				> NConcurrency::fg_LogError("PostgreSQL client", "Failed to close failed PostgreSQL startup socket")
			;
			m_Socket.f_Clear();
		}

		f_MarkClosed();
		m_ReceiveBuffer.f_RemoveFront(m_ReceiveBuffer.f_GetLen());
		m_ReceivePromise.f_Clear();
		m_ScramClientFirst.f_Clear();
		m_ScramClientFinal.f_Clear();

		co_return {};
	}

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

		// A stream of the closed connection can still hold the turn, and would read from a new connection when resumed
		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL connection is still used by an unfinished row stream");

		if (Internal.m_Socket && !Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return DMibErrorInstance("PostgreSQL socket is already connected");
		}

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
		Internal.m_fOnConnectionLost.f_Clear();
		Internal.m_Settings = _Settings;
		Internal.m_Socket.f_Clear();
		Internal.m_ReceiveBuffer.f_RemoveFront(Internal.m_ReceiveBuffer.f_GetLen());
		Internal.m_ReceivePromise.f_Clear();
		Internal.m_ScramClientFirst.f_Clear();
		Internal.m_ScramClientFinal.f_Clear();
		Internal.m_PreparedStatementCache.f_Clear();
		Internal.m_PreparedParameterTypes.f_Clear();
		Internal.m_NextPreparedStatementID = 0;

		Internal.m_ClientActor = NConcurrency::fg_ConstructActor<NNetwork::CAsyncSocketClientActor>();
		co_await Internal.m_ClientActor(&NNetwork::CAsyncSocketClientActor::f_SetDefaultTimeout, _Settings.m_SocketTimeout);
		// Sends are whole frontend messages already in memory, and the messages with caller data are checked against the server's
		// limit as they are written, before anything is sent. A send the socket refused would close the connection.
		co_await Internal.m_ClientActor(&NNetwork::CAsyncSocketClientActor::f_SetDefaultMaxMessageSize, TCLimitsInt<umint>::mc_Max);
		if (_Settings.m_bRequireTLS)
		{
			// The server accepts SSLRequest with a single 'S' and sends nothing more until the TLS handshake
			NNetwork::FAsyncSocketUpgradeCheckFactory fCheckUpgradeFactory = []() -> NNetwork::FAsyncSocketUpgradeCheck
				{
					return [](NContainer::CPagedByteVector const &_Data) -> NNetwork::CAsyncSocketUpgradeCheckResult
						{
							if (_Data.f_IsEmpty())
								return {};

							uint8 Response = 0;
							_Data.f_ReadFront
								(
									1
									, [&](umint _iStart, uint8 const *_pData, umint _nBytes) -> bool
									{
										Response = _pData[0];
										return false;
									}
								)
							;

							if (Response == 'S')
								return {.m_Result = NNetwork::EAsyncSocketUpgradeCheckResult_Upgrade, .m_nBytesConsumed = 1};

							return {.m_Result = NNetwork::EAsyncSocketUpgradeCheckResult_UpgradeWillNeverHappen};
						}
					;
				}
			;
			co_await Internal.m_ClientActor(&NNetwork::CAsyncSocketClientActor::f_SetDefaultUpgradeCheckFactory, fCheckUpgradeFactory);
		}

		Internal.m_bClosed = false;
		Internal.m_bInTransaction = false;
		Internal.m_bTransactionFailed = false;

		NNetwork::CAsyncSocketNewClientConnection NewConnection = co_await Internal.m_ClientActor
			(
				&NNetwork::CAsyncSocketClientActor::f_Connect
				, _Settings.m_Host
				, NStr::CStr()
				, NNetwork::ENetAddressType_None
				, _Settings.m_Port
				, NNetwork::CSocket_TCP::fs_GetFactory()
			)
		;

		// The socket keeps its callbacks and this actor keeps the socket, so callbacks that referenced this actor would keep a client its users
		// released alive, with its session. The direct call actor sends each delivery when the socket makes it, which keeps their order.
		NConcurrency::TCWeakActor<CPostgresClientActor> WeakThis = NConcurrency::fg_ThisActor(this);
		NNetwork::CAsyncSocketCallbacks Callbacks;
		Callbacks.m_fOnReceiveData = NConcurrency::g_ActorFunctor(NConcurrency::fg_DirectCallActor()) / [WeakThis](NContainer::CSharedByteVector _Data) -> NConcurrency::TCFuture<void>
			{
				DMibLogWarningOrDiscardResult(WeakThis(&CPostgresClientActor::fp_OnSocketData, fg_Move(_Data)), "PostgreSQL client", "Failed to deliver PostgreSQL socket data");

				co_return {};
			}
		;
		Callbacks.m_fOnClose = NConcurrency::g_ActorFunctor(NConcurrency::fg_DirectCallActor()) / [WeakThis]
			(NNetwork::EAsyncSocketStatus _Status, NStr::CStr _Message, NNetwork::EAsyncSocketCloseOrigin _CloseOrigin) -> NConcurrency::TCFuture<void>
			{
				DMibLogWarningOrDiscardResult(WeakThis(&CPostgresClientActor::fp_OnSocketClose), "PostgreSQL client", "Failed to deliver PostgreSQL socket close");

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
				// IgnoreVerificationFailures does not cover trust failures, such as a self-signed certificate
				SSLSettings.m_VerificationFlags = NNetwork::CSSLSettings::EVerificationFlag
					(
						SSLSettings.m_VerificationFlags | NNetwork::CSSLSettings::EVerificationFlag_IgnoreVerificationFailures | NNetwork::CSSLSettings::EVerificationFlag_IgnoreTrustFailures
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
		bool bScramStarted = false;
		bool bScramServerVerified = false;
		bool bAuthenticated = false;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			NStorage::TCOptional<NContainer::CIOByteVector> NextToSend;
			NStorage::TCOptional<CPostgresScramServerFirstMessage> ScramServerFirst;
			NException::CExceptionPointer pStartupException;
			bool bConnected = false;

			{
				CPostgresReadStream Stream;
				Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
				auto MessageCleanup = g_OnScopeExit / [&]
					{
						fp_ConsumeBackendMessage(MessageRef);
					}
				;
				// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
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
						{
							// After a started SCRAM exchange AuthenticationOk does not authenticate the server; require the verified server signature
							if (bScramStarted && !bScramServerVerified)
								pStartupException = DMibErrorInstance("PostgreSQL server completed SCRAM authentication without proving its identity");
							else
								bAuthenticated = true;

							break;
						}

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
								pStartupException = DMibErrorInstance("PostgreSQL server does not offer SCRAM-SHA-256");
								break;
							}

							NStr::CStr Mechanism = NStr::gc_Str<"SCRAM-SHA-256">;
							NStr::CStr GS2Header = NStr::gc_Str<"n,,">;
							if (bUseChannelBinding)
							{
								Mechanism = "SCRAM-SHA-256-PLUS";
								GS2Header = "p=tls-server-end-point,,";
							}
							else if (!ChannelBindingData.f_IsEmpty())
							{
								// The client could bind but the server offered no PLUS mechanism, which lets a server that
								// does offer one detect an intermediary that removed it
								GS2Header = "y,,";
							}

							bScramStarted = true;
							Internal.m_ScramClientFirst = fg_PostgresScramBuildClientFirstMessage(_Settings.m_User, NStr::CStr(), GS2Header);
							NextToSend = fg_PostgresBuildSASLInitialResponse(Mechanism, Internal.m_ScramClientFirst->m_Message);
							break;
						}

						if (Auth.m_Type == EPostgresAuthenticationRequestType::mc_SASLContinue)
						{
							if (!Internal.m_ScramClientFirst)
							{
								pStartupException = DMibErrorInstance("PostgreSQL SCRAM server-first message arrived before client-first state");
								break;
							}

							ScramServerFirst = co_await fg_PostgresScramParseServerFirstMessage(fg_PostgresByteVectorToString(Auth.m_SASLData), Internal.m_ScramClientFirst->m_ClientNonce);
							break;
						}

						if (Auth.m_Type == EPostgresAuthenticationRequestType::mc_SASLFinal)
						{
							if (!Internal.m_ScramClientFinal)
							{
								pStartupException = DMibErrorInstance("PostgreSQL SCRAM server-final message arrived before client-final state");
								break;
							}

							CPostgresScramServerFinalMessage ServerFinal = co_await fg_PostgresScramParseServerFinalMessage(fg_PostgresByteVectorToString(Auth.m_SASLData));
							if (!fg_PostgresScramVerifyServerFinalMessage(*Internal.m_ScramClientFinal, ServerFinal))
							{
								pStartupException = DMibErrorInstance("PostgreSQL SCRAM server signature verification failed");
								break;
							}

							bScramServerVerified = true;
							break;
						}

						pStartupException = DMibErrorInstance("PostgreSQL server requested unsupported authentication method");
						break;
					}
				case 'K': break;
				case 'E':
					{
						CPostgresErrorResponse Error = co_await fg_PostgresDecodeErrorResponse(Stream);
						auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL startup failed ({}): {}");
						ErrorMessage << Error.m_Code;
						ErrorMessage << Error.m_Message;

						pStartupException = fg_PostgresSqlError(ErrorMessage, Error);
						break;
					}
				case 'Z':
					Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
					// ReadyForQuery alone does not prove that authentication completed
					if (bAuthenticated)
						bConnected = true;
					else
						pStartupException = DMibErrorInstance("PostgreSQL server reported ready before authentication completed");
					break;
				default: break;
				}
			}

			if (pStartupException)
			{
				co_await Internal.f_ResetStartupConnection();

				co_return pStartupException;
			}

			if (ScramServerFirst)
			{
				// Only a p= GS2 header appends the TLS channel-binding bytes to c=; n,, and y,, encode the header alone
				bool bChannelBound = !Internal.m_ScramClientFirst->m_GS2Header.f_IsEmpty() && Internal.m_ScramClientFirst->m_GS2Header[0] == 'p';
				// The server chooses the PBKDF2 iteration count, which can make the derivation hold up the other actors of this thread
				Internal.m_ScramClientFinal = co_await
					(
						NConcurrency::g_Dispatch(NConcurrency::fg_ConcurrentActorHighCPU()) /
						[
							Password = _Settings.m_Password
							, ClientFirst = *Internal.m_ScramClientFirst
							, ServerFirst = fg_Move(*ScramServerFirst)
							, ChannelBinding = bChannelBound ? ChannelBindingData : NContainer::CByteVector()
						]
						{
							return fg_PostgresScramBuildClientFinalMessage(Password, ClientFirst, ServerFirst, ChannelBinding);
						}
					)
				;
				NextToSend = fg_PostgresBuildSASLResponse(Internal.m_ScramClientFinal->m_Message);
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

		// Without waiting for the turn: a row stream suspended in its consumer holds it, and a command in flight ends its turn,
		// failing if it needs more data from the closed socket
		co_await fp_CloseSocket();
		if (!Internal.m_bProtocolCommandInProgress)
			Internal.f_FailProtocolCommandWaiters();

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_CloseSocket()
	{
		auto &Internal = *mp_pInternal;

		if (Internal.m_Socket)
		{
			co_await Internal.m_Socket(&NNetwork::CAsyncSocketActor::f_CloseWithLinger, NNetwork::EAsyncSocketStatus_NormalClosure, NStr::CStr("PostgreSQL client closed"), 1.0).f_Wrap()
				> NConcurrency::fg_LogError("PostgreSQL client", "Failed to close PostgreSQL socket")
			;
			Internal.m_Socket.f_Clear();
		}

		Internal.f_MarkClosed();
		Internal.m_PreparedStatementCache.f_Clear();
		Internal.m_PreparedParameterTypes.f_Clear();
		Internal.m_NextPreparedStatementID = 0;

		co_return {};
	}

	// Must not suspend, since the caller still reads the message from the receive buffer
	void CPostgresClientActor::fp_CloseAfterProtocolViolation(NStr::CStr const &_Reason)
	{
		auto &Internal = *mp_pInternal;

		if (Internal.m_Socket)
		{
			Internal.m_Socket(&NNetwork::CAsyncSocketActor::f_CloseWithLinger, NNetwork::EAsyncSocketStatus_NormalClosure, _Reason, 1.0) > []
				(NConcurrency::TCAsyncResult<NNetwork::CAsyncSocketActor::CCloseInfo> &&_Result)
				{
					if (!_Result)
						DMibLogWithCategory(PostgreSQL, Info, "Failed to close PostgreSQL socket: {}", _Result.f_GetExceptionStr());
				}
			;
			Internal.m_Socket.f_Clear();
		}

		Internal.f_MarkClosed();
		Internal.m_PreparedStatementCache.f_Clear();
		Internal.m_PreparedParameterTypes.f_Clear();
		Internal.m_NextPreparedStatementID = 0;
	}

	// The rest of the response can't be read without understanding the message, and COPY FROM STDIN waits for data that Sync does
	// not end, so the connection is closed. It must not suspend, since the caller still reads the message from the receive buffer.
	NException::CExceptionPointer CPostgresClientActor::fp_UnexpectedBackendMessage(uint8 _Type)
	{
		fp_CloseAfterProtocolViolation(NStr::CStr("PostgreSQL client closed after an unexpected backend message"));

		if (_Type == 'G' || _Type == 'H' || _Type == 'W')
			return DMibErrorInstance("PostgreSQL COPY is not supported, so the connection was closed").f_ExceptionPointer();

		return DMibErrorInstance(NStr::CStr::CFormat("PostgreSQL returned unexpected backend message type {}, so the connection was closed") << _Type).f_ExceptionPointer();
	}

	NConcurrency::TCFuture<CPostgresQueryResult> CPostgresClientActor::f_Execute(NStr::CStr _Sql)
	{
		co_return co_await f_ExecuteWithParameters(fg_Move(_Sql), {});
	}

	NConcurrency::TCFuture<CPostgresQueryResult> CPostgresClientActor::f_ExecuteWithParameters(NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Parameters)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");
		}

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
		umint nSentStatementCloses = Internal.f_WritePendingStatementCloses(QueryMessages);
		if (bParseStatement)
			co_await fg_PostgresWriteParse(QueryMessages, PreparedStatementName, _Sql, ParameterTypeOIDs);

		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, PreparedStatementName, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		fg_PostgresWriteExecute(QueryMessages, {});
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());
		Internal.f_RemoveSentStatementCloses(nSentStatementCloses);

		CPostgresQueryResult Result;
		NException::CExceptionPointer pDecodeException;
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
			// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
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
			case '2': co_await fg_PostgresDecodeBindComplete(Stream); break;
			case 'n': co_await fg_PostgresDecodeNoData(Stream); break;
			case 'T':
				Result.m_RowDescription = co_await fg_PostgresDecodeRowDescription(Stream);
				RowDescription = *Result.m_RowDescription;

				break;
			case 'D':
				{
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					// A row that fails to decode fails the query only after ReadyForQuery, keeping the connection in sync
					auto Row = fg_PostgresDecodeDataRow(Stream, *RowDescription);
					if (!Row)
					{
						if (!pDecodeException)
							pDecodeException = fg_Move(Row).f_GetException();
					}
					else if (!pDecodeException)
						Result.m_Rows.f_InsertLast(Row.f_Move());
				}
				break;
			case 'C': Result.m_CommandComplete = co_await fg_PostgresDecodeCommandComplete(Stream); break;
			case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'E': ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'Z':
				Result.m_ReadyStatus = co_await fg_PostgresDecodeReadyForQuery(Stream);
				Internal.f_UpdateReadyStatus(Result.m_ReadyStatus);
				// Cache a statement that parsed successfully even when a later Bind/Execute failed: the named prepared
				// statement stays allocated on the session, so caching it lets the next call reuse it instead of
				// leaking a fresh orphaned statement for every failed execution.
				if (bParseStatement && bParseCompleted)
					Internal.f_CacheStatement(fg_Move(PreparedStatementKey), PreparedStatementName);

				if (Result.m_CommandComplete)
					Internal.f_OnStatementsDropped(Internal.f_OnCommandComplete(*Result.m_CommandComplete));

				// A cached statement whose result shape a schema change altered, or that SQL the command tag does not show deallocated,
				// keeps failing until it is prepared again
				if (ErrorResponse && !bParseStatement && (ErrorResponse->m_Code == "0A000" || ErrorResponse->m_Code == "26000"))
					Internal.f_EvictStatement(PreparedStatementKey);

				if (ErrorResponse)
				{
					auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
					ErrorMessage << ErrorResponse->m_Code;
					ErrorMessage << ErrorResponse->m_Message;

					co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
				}

				if (pDecodeException)
					co_return fg_Move(pDecodeException);

				co_return Result;
			case '3': break;
			case 'I': break;
			default: co_return fp_UnexpectedBackendMessage(MessageRef.m_Type);
			}
		}
	}

	NConcurrency::TCAsyncGenerator<CPostgresDataRowBatch> CPostgresClientActor::f_ExecuteRows(NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Parameters, umint _nRowsPerBatch)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_nRowsPerBatch == 0)
			co_return DMibErrorInstance("PostgreSQL row stream batch size must be at least one");

		// A row stream cannot be aborted while it waits here: its iterator is not handed out before the first value, so the waiter
		// always takes its turn and the cleanup registered below releases it
		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");
		}

		bool bStreamFinished = false;
		bool bCommandSent = false;
		bool bSyncSent = false;
		bool bParseStatement = false;
		bool bParseCompleted = false;
		CPostgresPreparedStatementKey PreparedStatementKey;
		NStr::CStr PreparedStatementName;
		auto ProtocolCommandCleanup = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					auto pThis = this;
					auto pInternal = &Internal;
					bool bStreamFinishedLocal = bStreamFinished;
					bool bCommandSentLocal = bCommandSent;
					bool bSyncSentLocal = bSyncSent;
					bool bParseStatementLocal = bParseStatement;
					bool bParseCompletedLocal = bParseCompleted;
					CPostgresPreparedStatementKey PreparedStatementKeyLocal = PreparedStatementKey;
					NStr::CStr PreparedStatementNameLocal = PreparedStatementName;
					auto EndProtocolCommand = g_OnScopeExit / [pInternal]
						{
							pInternal->f_EndProtocolCommand();
						}
					;

					// Before the command is sent the server has nothing to answer. After that the portal runs in bounded
					// chunks, so the rest of the response is cheap to drain and the connection stays usable.
					if (!bStreamFinishedLocal && bCommandSentLocal && pInternal->m_Socket && !pInternal->m_bClosed)
					{
						if (!bSyncSentLocal)
						{
							CPostgresWriteStream SyncMessage;
							fg_PostgresWriteSync(SyncMessage);
							co_await pThis->fp_Send(SyncMessage.f_MoveVector());
						}

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

							if (MessageRef.m_Type == '1')
								bParseCompletedLocal = true;
							else if (MessageRef.m_Type == 'Z')
							{
								pInternal->f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
								break;
							}
						}

						// The parsed statement remains allocated on the session
						if (bParseStatementLocal && bParseCompletedLocal)
							pInternal->f_CacheStatement(fg_Move(PreparedStatementKeyLocal), PreparedStatementNameLocal);
					}

					co_return {};
				}
			)
		;

		NContainer::TCVector<uint32> ParameterTypeOIDs = fg_PostgresBuildParameterTypeOIDs(_Parameters);
		PreparedStatementKey = CPostgresPreparedStatementKey{_Sql, ParameterTypeOIDs};
		if (NStr::CStr const *pCachedStatementName = Internal.m_PreparedStatementCache.f_FindEqual(PreparedStatementKey))
			PreparedStatementName = *pCachedStatementName;
		else
		{
			PreparedStatementName = "malterlib_";
			PreparedStatementName += NStr::CStr::fs_ToStr(++Internal.m_NextPreparedStatementID);
			bParseStatement = true;
		}

		CPostgresWriteStream QueryMessages;
		umint nSentStatementCloses = Internal.f_WritePendingStatementCloses(QueryMessages);
		if (bParseStatement)
			co_await fg_PostgresWriteParse(QueryMessages, PreparedStatementName, _Sql, ParameterTypeOIDs);

		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, PreparedStatementName, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		// Each Execute fetches one batch, so a slow consumer delays delivery; the server can still materialize a result, as for RETURNING
		uint32 const nRowsPerExecute = fg_PostgresRowsPerExecute(_nRowsPerBatch);
		fg_PostgresWriteExecute(QueryMessages, {}, nRowsPerExecute);
		fg_PostgresWriteFlush(QueryMessages);
		// Set before the send suspends: an aborted row stream's send still completes, and its cleanup must drain the response
		bCommandSent = true;
		co_await fp_Send(QueryMessages.f_MoveVector());
		Internal.f_RemoveSentStatementCloses(nSentStatementCloses);

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<CPostgresRowDescription> RowDescription;
		CPostgresDataRowBatch Batch;
		bParseCompleted = !bParseStatement;
		EPostgresStatementDrop StatementDrop = EPostgresStatementDrop::mc_None;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			bool bShouldYieldBatch = false;
			bool bShouldFinish = false;
			bool bShouldSync = false;
			bool bShouldExecuteMore = false;

			{
				CPostgresReadStream Stream;
				Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
				auto MessageCleanup = g_OnScopeExit / [&]
					{
						fp_ConsumeBackendMessage(MessageRef);
					}
				;
				// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
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
				case '2': co_await fg_PostgresDecodeBindComplete(Stream); break;
				case 'n': co_await fg_PostgresDecodeNoData(Stream); break;
				case 'T': RowDescription = co_await fg_PostgresDecodeRowDescription(Stream); break;
				case 'D':
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					Batch.f_InsertLast(co_await fg_PostgresDecodeDataRow(Stream, *RowDescription));

					if (Batch.f_GetLen() >= _nRowsPerBatch)
						bShouldYieldBatch = true;
					break;
				case 'C':
					StatementDrop = Internal.f_OnCommandComplete(co_await fg_PostgresDecodeCommandComplete(Stream));
					bShouldSync = true;
					break;
				case 's': bShouldExecuteMore = true; break;
				case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
				case 'E':
					ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
					bShouldSync = true;
					break;
				case 'Z':
					Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
					bStreamFinished = true;
					// A statement that parsed stays allocated on the session even when its execution failed
					if (bParseStatement && bParseCompleted)
						Internal.f_CacheStatement(fg_Move(PreparedStatementKey), PreparedStatementName);

					Internal.f_OnStatementsDropped(StatementDrop);

					// A cached statement whose result shape a schema change altered, or that SQL the command tag does not show deallocated,
					// keeps failing until it is prepared again
					if (ErrorResponse && !bParseStatement && (ErrorResponse->m_Code == "0A000" || ErrorResponse->m_Code == "26000"))
						Internal.f_EvictStatement(PreparedStatementKey);

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
				case '3': break;
				case 'I': bShouldSync = true; break;
				default: co_return fp_UnexpectedBackendMessage(MessageRef.m_Type);
				}
			}

			if (bShouldSync && !bSyncSent)
			{
				CPostgresWriteStream SyncMessage;
				fg_PostgresWriteSync(SyncMessage);
				// Set before the send suspends: a second Sync from an aborted row stream's cleanup would leave a ReadyForQuery for the next command
				bSyncSent = true;
				co_await fp_Send(SyncMessage.f_MoveVector());
			}
			else if (bShouldExecuteMore)
			{
				CPostgresWriteStream ExecuteMessages;
				fg_PostgresWriteExecute(ExecuteMessages, {}, nRowsPerExecute);
				fg_PostgresWriteFlush(ExecuteMessages);
				co_await fp_Send(ExecuteMessages.f_MoveVector());
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

		// A row stream cannot be aborted while it waits here: its iterator is not handed out before the first value, so the waiter
		// always takes its turn and the cleanup registered below releases it
		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");
		}

		bool bStreamFinished = false;
		bool bCommandSent = false;
		bool bSyncSent = false;
		bool bParseStatement = false;
		bool bParseCompleted = false;
		CPostgresPreparedStatementKey PreparedStatementKey;
		NStr::CStr PreparedStatementName;
		auto ProtocolCommandCleanup = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					auto pThis = this;
					auto pInternal = &Internal;
					bool bStreamFinishedLocal = bStreamFinished;
					bool bCommandSentLocal = bCommandSent;
					bool bSyncSentLocal = bSyncSent;
					bool bParseStatementLocal = bParseStatement;
					bool bParseCompletedLocal = bParseCompleted;
					CPostgresPreparedStatementKey PreparedStatementKeyLocal = PreparedStatementKey;
					NStr::CStr PreparedStatementNameLocal = PreparedStatementName;
					auto EndProtocolCommand = g_OnScopeExit / [pInternal]
						{
							pInternal->f_EndProtocolCommand();
						}
					;

					// Before the command is sent the server has nothing to answer. After that the portal runs in bounded
					// chunks, so the rest of the response is cheap to drain and the connection stays usable.
					if (!bStreamFinishedLocal && bCommandSentLocal && pInternal->m_Socket && !pInternal->m_bClosed)
					{
						if (!bSyncSentLocal)
						{
							CPostgresWriteStream SyncMessage;
							fg_PostgresWriteSync(SyncMessage);
							co_await pThis->fp_Send(SyncMessage.f_MoveVector());
						}

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

							if (MessageRef.m_Type == '1')
								bParseCompletedLocal = true;
							else if (MessageRef.m_Type == 'Z')
							{
								pInternal->f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
								break;
							}
						}

						// The parsed statement remains allocated on the session
						if (bParseStatementLocal && bParseCompletedLocal)
							pInternal->f_CacheStatement(fg_Move(PreparedStatementKeyLocal), PreparedStatementNameLocal);
					}

					co_return {};
				}
			)
		;

		NContainer::TCVector<uint32> ParameterTypeOIDs = fg_PostgresBuildParameterTypeOIDs(_Parameters);
		PreparedStatementKey = CPostgresPreparedStatementKey{_Sql, ParameterTypeOIDs};
		if (NStr::CStr const *pCachedStatementName = Internal.m_PreparedStatementCache.f_FindEqual(PreparedStatementKey))
			PreparedStatementName = *pCachedStatementName;
		else
		{
			PreparedStatementName = "malterlib_";
			PreparedStatementName += NStr::CStr::fs_ToStr(++Internal.m_NextPreparedStatementID);
			bParseStatement = true;
		}

		CPostgresWriteStream QueryMessages;
		umint nSentStatementCloses = Internal.f_WritePendingStatementCloses(QueryMessages);
		if (bParseStatement)
			co_await fg_PostgresWriteParse(QueryMessages, PreparedStatementName, _Sql, ParameterTypeOIDs);

		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, PreparedStatementName, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		// Each Execute fetches one batch, so a slow consumer delays delivery; the server can still materialize a result, as for RETURNING
		uint32 const nRowsPerExecute = fg_PostgresRowsPerExecute(_nRowsPerBatch);
		fg_PostgresWriteExecute(QueryMessages, {}, nRowsPerExecute);
		fg_PostgresWriteFlush(QueryMessages);
		// Set before the send suspends: an aborted row stream's send still completes, and its cleanup must drain the response
		bCommandSent = true;
		co_await fp_Send(QueryMessages.f_MoveVector());
		Internal.f_RemoveSentStatementCloses(nSentStatementCloses);

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<CPostgresRowDescription> RowDescription;
		bool bDescriptionYielded = false;
		CPostgresDataRowBatch Batch;
		bParseCompleted = !bParseStatement;
		EPostgresStatementDrop StatementDrop = EPostgresStatementDrop::mc_None;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			bool bShouldYieldDescription = false;
			bool bShouldYieldBatch = false;
			bool bShouldFinish = false;
			bool bShouldSync = false;
			bool bShouldExecuteMore = false;

			{
				CPostgresReadStream Stream;
				Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
				auto MessageCleanup = g_OnScopeExit / [&]
					{
						fp_ConsumeBackendMessage(MessageRef);
					}
				;
				// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
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
				case '2': co_await fg_PostgresDecodeBindComplete(Stream); break;
				case 'n': co_await fg_PostgresDecodeNoData(Stream); break;
				case 'T': RowDescription = co_await fg_PostgresDecodeRowDescription(Stream); break;
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
					StatementDrop = Internal.f_OnCommandComplete(co_await fg_PostgresDecodeCommandComplete(Stream));
					bShouldSync = true;
					break;
				case 's': bShouldExecuteMore = true; break;
				case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
				case 'E':
					ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
					bShouldSync = true;
					break;
				case 'Z':
					Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
					bStreamFinished = true;
					// A statement that parsed stays allocated on the session even when its execution failed
					if (bParseStatement && bParseCompleted)
						Internal.f_CacheStatement(fg_Move(PreparedStatementKey), PreparedStatementName);

					Internal.f_OnStatementsDropped(StatementDrop);

					// A cached statement whose result shape a schema change altered, or that SQL the command tag does not show deallocated,
					// keeps failing until it is prepared again
					if (ErrorResponse && !bParseStatement && (ErrorResponse->m_Code == "0A000" || ErrorResponse->m_Code == "26000"))
						Internal.f_EvictStatement(PreparedStatementKey);

					if (ErrorResponse)
					{
						auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
						ErrorMessage << ErrorResponse->m_Code;
						ErrorMessage << ErrorResponse->m_Message;

						co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
					}

					if (!bDescriptionYielded && RowDescription)
					{
						bDescriptionYielded = true;
						bShouldYieldDescription = true;
					}

					if (Batch.f_GetLen() != 0)
						bShouldYieldBatch = true;

					bShouldFinish = true;
					break;
				case '3': break;
				case 'I': bShouldSync = true; break;
				default: co_return fp_UnexpectedBackendMessage(MessageRef.m_Type);
				}
			}

			if (bShouldSync && !bSyncSent)
			{
				CPostgresWriteStream SyncMessage;
				fg_PostgresWriteSync(SyncMessage);
				// Set before the send suspends: a second Sync from an aborted row stream's cleanup would leave a ReadyForQuery for the next command
				bSyncSent = true;
				co_await fp_Send(SyncMessage.f_MoveVector());
			}
			else if (bShouldExecuteMore)
			{
				CPostgresWriteStream ExecuteMessages;
				fg_PostgresWriteExecute(ExecuteMessages, {}, nRowsPerExecute);
				fg_PostgresWriteFlush(ExecuteMessages);
				co_await fp_Send(ExecuteMessages.f_MoveVector());
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

		// The protocol counts parameters in 16 bits
		if (_ParameterTypes.f_GetLen() > TCLimitsInt<uint16>::mc_Max)
			co_return DMibErrorDatabaseInstance("PostgreSQL statements can have at most 65535 parameters");

		// The client caches statements of its own under these names, which replacing them would point at other SQL
		if (_Name.f_StartsWith("malterlib_"))
			co_return DMibErrorInstance("PostgreSQL prepared statement names starting with 'malterlib_' are reserved");

		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");
		}

		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		NContainer::TCVector<uint32> ParameterTypeOIDs;
		for (EPostgresValueType ParameterType : _ParameterTypes)
		{
			// String arrays are bound in binary with the text element OID, which a varchar[] parameter rejects, so varchar parameters
			// are prepared as text. An inferred varchar[] parameter, as in $1::varchar[] without a declared type, cannot take them.
			if (ParameterType == EPostgresValueType::mc_Varchar)
				ParameterType = EPostgresValueType::mc_Text;
			else if (ParameterType == EPostgresValueType::mc_Array_Varchar)
				ParameterType = EPostgresValueType::mc_Array_Text;

			// A NULL parameter has no type of its own; OID 0 leaves it to the server to infer
			uint32 TypeOID = fg_PostgresGetValueTypeOID(ParameterType);
			DMibCheck(TypeOID != 0 || ParameterType == EPostgresValueType::mc_Null);
			ParameterTypeOIDs.f_InsertLast(TypeOID);
		}

		// The name can still be allocated when a caller forgot it after raw SQL dropped other statements, and closing a
		// missing statement is not an error, so preparing replaces it
		CPostgresWriteStream QueryMessages;
		umint nSentStatementCloses = Internal.f_WritePendingStatementCloses(QueryMessages);
		fg_PostgresWriteClose(QueryMessages, EPostgresDescribeTarget::mc_Statement, _Name);
		co_await fg_PostgresWriteParse(QueryMessages, _Name, _Sql, ParameterTypeOIDs);
		// The executions check their values against the parameter types the server settles on, which it can infer
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Statement, _Name);
		fg_PostgresWriteSync(QueryMessages);
		Internal.m_PreparedParameterTypes.f_Remove(_Name);
		co_await fp_Send(QueryMessages.f_MoveVector());
		Internal.f_RemoveSentStatementCloses(nSentStatementCloses);

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<NContainer::TCVector<uint32>> ServerParameterTypeOIDs;
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
			// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
#if DMibEnableSafeCheck > 0
			auto SuspendGuard = NConcurrency::g_OnSuspend / []
				{
					DMibFastCheck(false);
				}
			;
#endif

			switch (MessageRef.m_Type)
			{
			case '3': break;
			case '1': co_await fg_PostgresDecodeParseComplete(Stream); break;
			case 't': ServerParameterTypeOIDs = co_await fg_PostgresDecodeParameterDescription(Stream); break;
			case 'T': co_await fg_PostgresDecodeRowDescription(Stream); break;
			case 'n': co_await fg_PostgresDecodeNoData(Stream); break;
			case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'E': ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'Z':
				Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
				if (ErrorResponse)
				{
					auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
					ErrorMessage << ErrorResponse->m_Code;
					ErrorMessage << ErrorResponse->m_Message;

					co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
				}

				if (!ServerParameterTypeOIDs)
					co_return DMibErrorInstance("PostgreSQL did not describe the parameters of a prepared statement");

				Internal.m_PreparedParameterTypes[_Name] = fg_Move(*ServerParameterTypeOIDs);

				co_return {};
			case 'I': break;
			default: co_return fp_UnexpectedBackendMessage(MessageRef.m_Type);
			}
		}
	}

	// Reads the parameter types of a named statement that f_PrepareStatement did not prepare, or that a DEALLOCATE may have replaced.
	// The caller holds the protocol command.
	NConcurrency::TCFuture<void> CPostgresClientActor::fp_DescribePreparedParameterTypes(NStr::CStr _Name)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		CPostgresWriteStream QueryMessages;
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Statement, _Name);
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<NContainer::TCVector<uint32>> ServerParameterTypeOIDs;
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
			// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
#if DMibEnableSafeCheck > 0
			auto SuspendGuard = NConcurrency::g_OnSuspend / []
				{
					DMibFastCheck(false);
				}
			;
#endif

			switch (MessageRef.m_Type)
			{
			case 't': ServerParameterTypeOIDs = co_await fg_PostgresDecodeParameterDescription(Stream); break;
			case 'T': co_await fg_PostgresDecodeRowDescription(Stream); break;
			case 'n': co_await fg_PostgresDecodeNoData(Stream); break;
			case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'E': ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'Z':
				Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
				if (ErrorResponse)
				{
					auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
					ErrorMessage << ErrorResponse->m_Code;
					ErrorMessage << ErrorResponse->m_Message;

					co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
				}

				if (!ServerParameterTypeOIDs)
					co_return DMibErrorInstance("PostgreSQL did not describe the parameters of a prepared statement");

				Internal.m_PreparedParameterTypes[_Name] = fg_Move(*ServerParameterTypeOIDs);

				co_return {};
			case 'I': break;
			default: co_return fp_UnexpectedBackendMessage(MessageRef.m_Type);
			}
		}
	}

	NConcurrency::TCFuture<CPostgresQueryResult> CPostgresClientActor::f_ExecutePrepared(NStr::CStr _Name, NContainer::TCVector<CPostgresValue> _Parameters)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Name.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement name is required");

		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");
		}

		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		NContainer::TCVector<uint32> const *pParameterTypeOIDs = Internal.m_PreparedParameterTypes.f_FindEqual(_Name);
		if (!pParameterTypeOIDs)
		{
			co_await fp_DescribePreparedParameterTypes(_Name);
			pParameterTypeOIDs = Internal.m_PreparedParameterTypes.f_FindEqual(_Name);
			DMibFastCheck(pParameterTypeOIDs);
		}

		if (auto pException = fg_PostgresCheckParameterTypes(_Name, *pParameterTypeOIDs, _Parameters))
			co_return fg_Move(pException);

		CPostgresWriteStream QueryMessages;
		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, _Name, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		fg_PostgresWriteExecute(QueryMessages, {});
		fg_PostgresWriteSync(QueryMessages);
		co_await fp_Send(QueryMessages.f_MoveVector());

		CPostgresQueryResult Result;
		NException::CExceptionPointer pDecodeException;
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
			// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
#if DMibEnableSafeCheck > 0
			auto SuspendGuard = NConcurrency::g_OnSuspend / []
				{
					DMibFastCheck(false);
				}
			;
#endif

			switch (MessageRef.m_Type)
			{
			case '2': co_await fg_PostgresDecodeBindComplete(Stream); break;
			case 'n': co_await fg_PostgresDecodeNoData(Stream); break;
			case 'T':
				Result.m_RowDescription = co_await fg_PostgresDecodeRowDescription(Stream);
				RowDescription = *Result.m_RowDescription;
				break;
			case 'D':
				{
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					// A row that fails to decode fails the query only after ReadyForQuery, keeping the connection in sync
					auto Row = fg_PostgresDecodeDataRow(Stream, *RowDescription);
					if (!Row)
					{
						if (!pDecodeException)
							pDecodeException = fg_Move(Row).f_GetException();
					}
					else if (!pDecodeException)
						Result.m_Rows.f_InsertLast(Row.f_Move());
				}
				break;
			case 'C': Result.m_CommandComplete = co_await fg_PostgresDecodeCommandComplete(Stream); break;
			case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'E': ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'Z':
				Result.m_ReadyStatus = co_await fg_PostgresDecodeReadyForQuery(Stream);
				Internal.f_UpdateReadyStatus(Result.m_ReadyStatus);
				if (Result.m_CommandComplete)
					Internal.f_OnStatementsDropped(Internal.f_OnCommandComplete(*Result.m_CommandComplete));

				if (ErrorResponse)
				{
					auto ErrorMessage = NStr::CStr::CFormat("PostgreSQL query failed ({}): {}");
					ErrorMessage << ErrorResponse->m_Code;
					ErrorMessage << ErrorResponse->m_Message;

					co_return fg_PostgresSqlError(ErrorMessage, *ErrorResponse);
				}

				if (pDecodeException)
					co_return fg_Move(pDecodeException);

				co_return Result;
			case 'I': break;
			default: co_return fp_UnexpectedBackendMessage(MessageRef.m_Type);
			}
		}
	}

	// Returns owned data: the message is consumed from the receive buffer when this returns
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
		// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
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
		case '2': co_await fg_PostgresDecodeBindComplete(Stream); break;
		case 'C':
			{
				auto Command = co_await fg_PostgresDecodeCommandComplete(Stream);
				auto const &Tag = Command.m_Tag;
				// CommandComplete puts the affected-row count last, after the OID for INSERT
				umint iCountStart = Tag.f_GetLen();
				while (iCountStart != 0 && Tag[iCountStart - 1] != ' ')
					--iCountStart;

				if (iCountStart == Tag.f_GetLen())
				{
					Response.m_pProtocolError = DMibErrorInstance("PostgreSQL pipelined mutation CommandComplete tag did not include affected row count");

					break;
				}

				umint nRows = 0;
				bool bNumeric = true;
				for (umint iChar = iCountStart; iChar < Tag.f_GetLen(); ++iChar)
				{
					if (Tag[iChar] < '0' || Tag[iChar] > '9')
					{
						Response.m_pProtocolError = DMibErrorInstance("PostgreSQL pipelined mutation CommandComplete affected row count was not numeric");
						bNumeric = false;

						break;
					}

					nRows = nRows * 10 + umint(Tag[iChar] - '0');
				}

				if (bNumeric)
				{
					Response.m_AffectedRows = nRows;
					Response.m_bAffectedRowsValid = true;
				}
			}
			break;
		case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
		case 'E': Response.m_ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream); break;
		case 'Z':
			{
				auto ReadyStatus = co_await fg_PostgresDecodeReadyForQuery(Stream);
				Internal.f_UpdateReadyStatus(ReadyStatus);
			}
			break;
		default: Response.m_pProtocolError = fp_UnexpectedBackendMessage(MessageRef.m_Type); break;
		}

		co_return Response;
	}

	auto CPostgresClientActor::fp_ReadBulkResponse() -> NConcurrency::TCFuture<CBulkResponse>
	{
		auto MessageRef = co_await fp_ReadBackendMessageRef();

		co_return co_await fp_DecodeBulkResponse(MessageRef);
	}

	// Never waits for socket data; returns an empty optional if no complete backend message is buffered
	auto CPostgresClientActor::fp_TryReadBulkResponse() -> NConcurrency::TCFuture<NStorage::TCOptional<CBulkResponse>>
	{
		auto &Internal = *mp_pInternal;

		while (true)
		{
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
			if (auto pException = fg_PostgresCheckBackendMessageLength(Length))
			{
				CBulkResponse Response;
				Response.m_pProtocolError = fg_Move(pException);

				co_return Response;
			}

			umint TotalLength = umint(Length) + 1;
			if (Internal.m_ReceiveBuffer.f_GetLen() < TotalLength)
				co_return NStorage::TCOptional<CBulkResponse>{};

			CBackendMessageRef MessageRef{.m_Type = HeaderBytes[0], .m_PayloadOffset = 5, .m_PayloadLength = umint(Length) - 4, .m_TotalLength = TotalLength};
			if (fg_PostgresIsAsynchronousMessage(MessageRef.m_Type))
			{
				auto pException = fp_CheckParameterStatus(MessageRef);
				fp_ConsumeBackendMessage(MessageRef);
				if (pException)
				{
					CBulkResponse Response;
					Response.m_pProtocolError = fg_Move(pException);

					co_return Response;
				}

				continue;
			}

			co_return co_await fp_DecodeBulkResponse(MessageRef);
		}
	}

	auto CPostgresClientActor::f_ExecutePreparedBulk(NStr::CStr _Name, NConcurrency::TCAsyncGenerator<NContainer::TCVector<NContainer::TCVector<CPostgresValue>>> _ParameterBatches)
		-> NConcurrency::TCFuture<umint>
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Name.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement name is required");

		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");
		}

		bool bAnythingSent = false;
		bool bFinalSyncSent = false;
		bool bResynchronized = false;
		auto ProtocolCommandCleanup = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					auto pThis = this;
					auto pInternal = mp_pInternal.f_Get();
					bool bAnythingSentLocal = bAnythingSent;
					bool bFinalSyncSentLocal = bFinalSyncSent;
					bool bResynchronizedLocal = bResynchronized;
					auto EndProtocolCommand = g_OnScopeExit / [pInternal]
						{
							pInternal->f_EndProtocolCommand();
						}
					;

					// Unwinding mid extended-query cycle: Sync and drain to ReadyForQuery so the next command does not read stale responses
					if (bAnythingSentLocal && !bResynchronizedLocal && pInternal->m_Socket && !pInternal->m_bClosed)
					{
						// A failure while reading the final Sync's responses only needs them drained to its ReadyForQuery
						if (!bFinalSyncSentLocal)
						{
							CPostgresWriteStream SyncMessage;
							// Outside an explicit transaction the Sync would commit the batches already sent. A statement that
							// fails to parse fails the implicit transaction, so the Sync rolls it back instead.
							if (!pInternal->m_bInTransaction)
								co_await fg_PostgresWriteParse(SyncMessage, NStr::CStr(), NStr::CStr("malterlib abort bulk"));
							fg_PostgresWriteSync(SyncMessage);
							co_await pThis->fp_Send(SyncMessage.f_MoveVector());
						}

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

		// A copy, since the command completions of the batches can clear the described types
		NContainer::TCVector<uint32> ParameterTypeOIDs;
		if (auto const *pParameterTypeOIDs = Internal.m_PreparedParameterTypes.f_FindEqual(_Name))
			ParameterTypeOIDs = *pParameterTypeOIDs;
		else
		{
			co_await fp_DescribePreparedParameterTypes(_Name);
			auto const *pDescribedTypeOIDs = Internal.m_PreparedParameterTypes.f_FindEqual(_Name);
			DMibFastCheck(pDescribedTypeOIDs);
			ParameterTypeOIDs = *pDescribedTypeOIDs;
		}

		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));

		umint nExpectedResults = 0;
		umint nTotalAffected = 0;
		umint nResultsReceived = 0;
		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;

		// Bound the batches in flight without emptying the pipeline. Flush instead of Sync keeps one implicit transaction.
		umint nMaxInFlightBatches = Internal.m_Settings.m_nPipelineLength != 0 ? Internal.m_Settings.m_nPipelineLength : 1;

		// Ring with the oldest batch at iRingHead: responses arrive in send order, so each CommandComplete belongs to the oldest in-flight batch
		NContainer::TCVector<umint> InFlightBatchRemainingCommands;
		InFlightBatchRemainingCommands.f_SetLen(nMaxInFlightBatches);
		umint iRingHead = 0;
		umint nInFlightBatches = 0;
		bool bUnflushedSends = false;

		NException::CExceptionPointer pProtocolError;
		auto fApplyResponseAndCheckError = [&](CBulkResponse &&_Response) -> bool
			{
				if (_Response.m_pProtocolError)
				{
					pProtocolError = fg_Move(_Response.m_pProtocolError);

					return true;
				}

				if (_Response.m_bAffectedRowsValid)
				{
					nTotalAffected += _Response.m_AffectedRows;
					++nResultsReceived;
					if (--InFlightBatchRemainingCommands[iRingHead] == 0)
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

		for (auto iBatch = co_await fg_Move(_ParameterBatches).f_GetPipelinedIterator(nMaxInFlightBatches); iBatch; co_await ++iBatch)
		{
			if ((*iBatch).f_IsEmpty())
				continue;

			// Drain what has already arrived without suspending, to free window slots and keep the receive buffer small
			for (;;)
			{
				NStorage::TCOptional<CBulkResponse> Response = co_await fp_TryReadBulkResponse();
				if (!Response)
					break;

				if (fApplyResponseAndCheckError(fg_Move(*Response)))
					break;
			}

			if (pProtocolError)
				co_return pProtocolError;
			if (ErrorResponse)
				break;

			// The backend may hold replies to unflushed batches; request them before waiting for a slot
			while (nInFlightBatches >= nMaxInFlightBatches)
			{
				if (bUnflushedSends)
				{
					CPostgresWriteStream FlushMessage;
					fg_PostgresWriteFlush(FlushMessage);
					co_await fp_Send(FlushMessage.f_MoveVector());
					bUnflushedSends = false;
				}

				if (fApplyResponseAndCheckError(co_await fp_ReadBulkResponse()))
					break;
			}

			if (pProtocolError)
				co_return pProtocolError;
			if (ErrorResponse)
				break;

			for (auto const &Parameters : *iBatch)
			{
				if (auto pException = fg_PostgresCheckParameterTypes(_Name, ParameterTypeOIDs, Parameters))
					co_return fg_Move(pException);
			}

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

			InFlightBatchRemainingCommands[(iRingHead + nInFlightBatches) % nMaxInFlightBatches] = nBatchCommands;
			++nInFlightBatches;
		}

		if (nExpectedResults == 0)
			co_return umint(0);

		// Sync ends the implicit transaction and, after an error, the backend's discarding of messages; a Flush does neither
		CPostgresWriteStream SyncMessage;
		fg_PostgresWriteSync(SyncMessage);
		co_await fp_Send(SyncMessage.f_MoveVector());
		bFinalSyncSent = true;

		for (;;)
		{
			CBulkResponse Response = co_await fp_ReadBulkResponse();
			if (Response.m_pProtocolError)
				co_return Response.m_pProtocolError;

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

		// ReadyForQuery is consumed, so the destroy-time cleanup must not wait for another one
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

	auto CPostgresClientActor::f_ExecutePreparedRows(NStr::CStr _Name, NContainer::TCVector<CPostgresValue> _Parameters, umint _nRowsPerBatch)
		-> NConcurrency::TCAsyncGenerator<CPostgresDataRowBatch>
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Name.f_IsEmpty())
			co_return DMibErrorInstance("PostgreSQL prepared statement name is required");

		if (_nRowsPerBatch == 0)
			co_return DMibErrorInstance("PostgreSQL row stream batch size must be at least one");

		// A row stream cannot be aborted while it waits here: its iterator is not handed out before the first value, so the waiter
		// always takes its turn and the cleanup registered below releases it
		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");
		}

		bool bStreamFinished = false;
		bool bCommandSent = false;
		bool bSyncSent = false;
		auto ProtocolCommandCleanup = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					auto pThis = this;
					auto pInternal = mp_pInternal.f_Get();
					bool bStreamFinishedLocal = bStreamFinished;
					bool bCommandSentLocal = bCommandSent;
					bool bSyncSentLocal = bSyncSent;
					auto EndProtocolCommand = g_OnScopeExit / [pInternal]
						{
							pInternal->f_EndProtocolCommand();
						}
					;

					// Before the command is sent the server has nothing to answer. After that the portal runs in bounded
					// chunks, so the rest of the response is cheap to drain and the connection stays usable.
					if (!bStreamFinishedLocal && bCommandSentLocal && pInternal->m_Socket && !pInternal->m_bClosed)
					{
						if (!bSyncSentLocal)
						{
							CPostgresWriteStream SyncMessage;
							fg_PostgresWriteSync(SyncMessage);
							co_await pThis->fp_Send(SyncMessage.f_MoveVector());
						}

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

					co_return {};
				}
			)
		;

		NContainer::TCVector<uint32> const *pParameterTypeOIDs = Internal.m_PreparedParameterTypes.f_FindEqual(_Name);
		if (!pParameterTypeOIDs)
		{
			co_await fp_DescribePreparedParameterTypes(_Name);
			pParameterTypeOIDs = Internal.m_PreparedParameterTypes.f_FindEqual(_Name);
			DMibFastCheck(pParameterTypeOIDs);
		}

		if (auto pException = fg_PostgresCheckParameterTypes(_Name, *pParameterTypeOIDs, _Parameters))
			co_return fg_Move(pException);

		CPostgresWriteStream QueryMessages;
		NContainer::TCVector<uint16> ResultFormats;
		ResultFormats.f_InsertLast(uint16(1));
		co_await fg_PostgresWriteBind(QueryMessages, {}, _Name, _Parameters, ResultFormats);
		fg_PostgresWriteDescribe(QueryMessages, EPostgresDescribeTarget::mc_Portal, {});
		// Each Execute fetches one batch, so a slow consumer delays delivery; the server can still materialize a result, as for RETURNING
		uint32 const nRowsPerExecute = fg_PostgresRowsPerExecute(_nRowsPerBatch);
		fg_PostgresWriteExecute(QueryMessages, {}, nRowsPerExecute);
		fg_PostgresWriteFlush(QueryMessages);
		// Set before the send suspends: an aborted row stream's send still completes, and its cleanup must drain the response
		bCommandSent = true;
		co_await fp_Send(QueryMessages.f_MoveVector());

		EPostgresStatementDrop StatementDrop = EPostgresStatementDrop::mc_None;
		NStorage::TCOptional<CPostgresErrorResponse> ErrorResponse;
		NStorage::TCOptional<CPostgresRowDescription> RowDescription;
		CPostgresDataRowBatch Batch;
		while (true)
		{
			auto MessageRef = co_await fp_ReadBackendMessageRef();
			bool bShouldYieldBatch = false;
			bool bShouldFinish = false;
			bool bShouldSync = false;
			bool bShouldExecuteMore = false;

			{
				CPostgresReadStream Stream;
				Stream.f_OpenRead(Internal.m_ReceiveBuffer, MessageRef.m_PayloadOffset, MessageRef.m_PayloadLength);
				auto MessageCleanup = g_OnScopeExit / [&]
					{
						fp_ConsumeBackendMessage(MessageRef);
					}
				;
				// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
#if DMibEnableSafeCheck > 0
				auto SuspendGuard = NConcurrency::g_OnSuspend / []
					{
						DMibFastCheck(false);
					}
				;
#endif

				switch (MessageRef.m_Type)
				{
				case '2': co_await fg_PostgresDecodeBindComplete(Stream); break;
				case 'n': co_await fg_PostgresDecodeNoData(Stream); break;
				case 'T': RowDescription = co_await fg_PostgresDecodeRowDescription(Stream); break;
				case 'D':
					if (!RowDescription)
						co_return DMibErrorInstance("PostgreSQL DataRow arrived before RowDescription");

					Batch.f_InsertLast(co_await fg_PostgresDecodeDataRow(Stream, *RowDescription));

					if (Batch.f_GetLen() >= _nRowsPerBatch)
						bShouldYieldBatch = true;
					break;
				case 'C':
					StatementDrop = Internal.f_OnCommandComplete(co_await fg_PostgresDecodeCommandComplete(Stream));
					bShouldSync = true;
					break;
				case 's': bShouldExecuteMore = true; break;
				case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
				case 'E':
					ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream);
					bShouldSync = true;
					break;
				case 'Z':
					Internal.f_UpdateReadyStatus(co_await fg_PostgresDecodeReadyForQuery(Stream));
					bStreamFinished = true;
					Internal.f_OnStatementsDropped(StatementDrop);
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
				case 'I': bShouldSync = true; break;
				default: co_return fp_UnexpectedBackendMessage(MessageRef.m_Type);
				}
			}

			if (bShouldSync && !bSyncSent)
			{
				CPostgresWriteStream SyncMessage;
				fg_PostgresWriteSync(SyncMessage);
				// Set before the send suspends: a second Sync from an aborted row stream's cleanup would leave a ReadyForQuery for the next command
				bSyncSent = true;
				co_await fp_Send(SyncMessage.f_MoveVector());
			}
			else if (bShouldExecuteMore)
			{
				CPostgresWriteStream ExecuteMessages;
				fg_PostgresWriteExecute(ExecuteMessages, {}, nRowsPerExecute);
				fg_PostgresWriteFlush(ExecuteMessages);
				co_await fp_Send(ExecuteMessages.f_MoveVector());
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

		// Closing one of the client's own cached statements would leave its cache entry naming a missing statement
		if (_Name.f_StartsWith("malterlib_"))
			co_return DMibErrorInstance("PostgreSQL prepared statement names starting with 'malterlib_' are reserved");

		if (!co_await Internal.f_WaitForProtocolCommand())
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			Internal.f_EndProtocolCommand();
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");
		}

		auto ProtocolCommandCleanup = g_OnScopeExit / [&]
			{
				Internal.f_EndProtocolCommand();
			}
		;

		CPostgresWriteStream QueryMessages;
		fg_PostgresWriteClose(QueryMessages, EPostgresDescribeTarget::mc_Statement, _Name);
		fg_PostgresWriteSync(QueryMessages);
		Internal.m_PreparedParameterTypes.f_Remove(_Name);
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
			// Decode without suspending while the stream borrows bytes of m_ReceiveBuffer
#if DMibEnableSafeCheck > 0
			auto SuspendGuard = NConcurrency::g_OnSuspend / []
				{
					DMibFastCheck(false);
				}
			;
#endif

			switch (MessageRef.m_Type)
			{
			case '3': co_await fg_PostgresDecodeCloseComplete(Stream); break;
			case 'N': co_await fg_PostgresDecodeErrorResponse(Stream); break;
			case 'E': ErrorResponse = co_await fg_PostgresDecodeErrorResponse(Stream); break;
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
			case 'I': break;
			default: co_return fp_UnexpectedBackendMessage(MessageRef.m_Type);
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
				case ESqlTransactionIsolation::mc_Default: break;
				case ESqlTransactionIsolation::mc_ReadCommitted: Appender += " ISOLATION LEVEL READ COMMITTED"; break;
				case ESqlTransactionIsolation::mc_RepeatableRead: Appender += " ISOLATION LEVEL REPEATABLE READ"; break;
				case ESqlTransactionIsolation::mc_Serializable: Appender += " ISOLATION LEVEL SERIALIZABLE"; break;
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

	// The cached ReadyForQuery status, also true in a failed transaction; a closed connection can still read a buffered one, so check f_IsConnected too
	NConcurrency::TCFuture<bool> CPostgresClientActor::f_IsInTransaction()
	{
		co_return mp_pInternal->m_bInTransaction;
	}

	NConcurrency::TCFuture<bool> CPostgresClientActor::f_IsTransactionFailed()
	{
		co_return mp_pInternal->m_bTransactionFailed;
	}

	NConcurrency::TCFuture<bool> CPostgresClientActor::f_IsConnected()
	{
		co_return mp_pInternal->m_Socket && !mp_pInternal->m_bClosed;
	}

	// Called when the connection closes, or at once when it is not connected. A new connection clears it.
	NConcurrency::TCFuture<void> CPostgresClientActor::f_SetOnConnectionLost(NConcurrency::TCActorFunctorWeak<NConcurrency::TCFuture<void> ()> _fOnConnectionLost)
	{
		auto &Internal = *mp_pInternal;

		if (!Internal.m_Socket || Internal.m_bClosed)
		{
			DMibLogWarningOrDiscardResult(_fOnConnectionLost(), "PostgreSQL client", "Failed to report a lost PostgreSQL connection");

			co_return {};
		}

		Internal.m_fOnConnectionLost = fg_Move(_fOnConnectionLost);

		co_return {};
	}

	NConcurrency::TCFuture<uint64> CPostgresClientActor::f_GetStatementDropCount()
	{
		co_return mp_pInternal->m_nStatementDrops;
	}

	NConcurrency::TCFuture<CPostgresSessionCounters> CPostgresClientActor::f_GetSessionCounters()
	{
		co_return CPostgresSessionCounters
			{
				.m_nStatementDrops = mp_pInternal->m_nStatementDrops
				, .m_nTransactionEndTags = mp_pInternal->m_nTransactionEndTags
				, .m_nResetTags = mp_pInternal->m_nResetTags
			}
		;
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_Destroy()
	{
		auto &Internal = *mp_pInternal;

		Internal.f_StopKeepAlive();
		co_await fp_CloseSocket();
		Internal.f_FailProtocolCommandWaiters();

		if (Internal.m_ClientActor)
			co_await fg_Move(Internal.m_ClientActor).f_Destroy();

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_OnSocketData(NContainer::CSharedByteVector _Data)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		Internal.m_ReceiveBuffer.f_InsertBack(_Data.f_GetArray(), _Data.f_GetLen());

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
		Internal.f_MarkClosed();

		if (Internal.m_ReceivePromise)
		{
			Internal.m_ReceivePromise->f_SetException(fg_PostgresConnectionLossError("PostgreSQL socket closed"));
			Internal.m_ReceivePromise.f_Clear();
		}

		// The waiters cannot run on the closed connection, also while a suspended row stream keeps the turn
		Internal.f_FailProtocolCommandWaiters();

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_KeepAlive()
	{
		auto &Internal = *mp_pInternal;

		if (Internal.m_bClosed || !Internal.m_Socket)
			co_return {};

		// A row stream waiting for its consumer and an idle transaction keep the connection without a query, which neither can run, and
		// without waiting for the server, so the socket's receive deadline is restarted instead. A send or read in progress keeps it.
		if (Internal.m_bProtocolCommandInProgress || Internal.m_bInTransaction)
		{
			if (Internal.m_nServerWaits == 0)
			{
				co_await Internal.m_Socket(&NNetwork::CAsyncSocketActor::f_SetTimeout, Internal.m_Settings.m_SocketTimeout).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL client", "PostgreSQL keepalive failed to restart the socket timeout")
				;
			}

			co_return {};
		}

		co_await f_Execute("SELECT 1").f_Wrap() > NConcurrency::fg_LogError("PostgreSQL client", "PostgreSQL keepalive failed");

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_Send(NContainer::CIOByteVector _Data)
	{
		auto &Internal = *mp_pInternal;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (!Internal.m_Socket || Internal.m_bClosed)
			co_return fg_PostgresConnectionLossError("PostgreSQL socket is not connected");

		Internal.m_bResponsePending = true;
		++Internal.m_nServerWaits;
		auto ServerWaitCleanup = g_OnScopeExit / [&]
			{
				--Internal.m_nServerWaits;
			}
		;
		auto SendResult = co_await Internal.m_Socket(&NNetwork::CAsyncSocketActor::f_SendData, NContainer::CSharedByteVector(fg_Move(_Data)), uint32(0)).f_Wrap();
		if (!SendResult)
		{
			// The server can have received part of the message, so the stream cannot be continued
			Internal.f_MarkClosed();
			co_return fg_PostgresConnectionLossError(NStr::CStr::CFormat("PostgreSQL send failed: {}") << SendResult.f_GetExceptionStr());
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresClientActor::fp_WaitForReceiveBytes(umint _nBytes)
	{
		auto &Internal = *mp_pInternal;

		// Reads run one after another, so a read still waiting when a later one starts belongs to a row stream aborted while it waited.
		// It must neither go on reading the response the later read now owns nor take the one wakeup the later read waits for.
		uint64 iReceiveWaiter = ++Internal.m_iReceiveWaiter;
		while (Internal.m_ReceiveBuffer.f_GetLen() < _nBytes)
		{
			if (Internal.m_bClosed)
				co_return fg_PostgresConnectionLossError("PostgreSQL socket closed while waiting for data");

			if (Internal.m_ReceivePromise)
				Internal.m_ReceivePromise->f_SetException(DMibErrorInstance("PostgreSQL read was superseded by a later read"));

			NConcurrency::TCPromiseFuturePair<void> Promise;
			Internal.m_ReceivePromise = Promise.m_Promise;

			{
				++Internal.m_nServerWaits;
				auto ServerWaitCleanup = g_OnScopeExit / [&]
					{
						--Internal.m_nServerWaits;
					}
				;
				co_await fg_Move(Promise.m_Future);
			}

			if (Internal.m_iReceiveWaiter != iReceiveWaiter)
				co_return DMibErrorInstance("PostgreSQL read was superseded by a later read");
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

		while (true)
		{
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

			if (auto pException = fg_PostgresCheckBackendMessageLength(Length))
				co_return fg_Move(pException);

			umint TotalLength = umint(Length) + 1;
			co_await fp_WaitForReceiveBytes(TotalLength);

			CBackendMessageRef MessageRef{.m_Type = Type, .m_PayloadOffset = 5, .m_PayloadLength = umint(Length) - 4, .m_TotalLength = TotalLength};

			if (fg_PostgresIsAsynchronousMessage(Type))
			{
				auto pException = fp_CheckParameterStatus(MessageRef);
				fp_ConsumeBackendMessage(MessageRef);
				if (pException)
					co_return fg_Move(pException);

				continue;
			}

			co_return MessageRef;
		}
	}

	// Text is sent and read as UTF-8, and generated string literals keep backslashes only with standard_conforming_strings, so a session
	// that raw SQL changes either in can't be used. The SET has already run, so the connection is closed.
	NException::CExceptionPointer CPostgresClientActor::fp_CheckParameterStatus(CBackendMessageRef const &_Ref)
	{
		using namespace NStr;

		if (_Ref.m_Type != 'S')
			return {};

		CPostgresReadStream Stream;
		Stream.f_OpenRead(mp_pInternal->m_ReceiveBuffer, _Ref.m_PayloadOffset, _Ref.m_PayloadLength);
		auto Status = fg_PostgresDecodeParameterStatus(Stream);
		if (!Status)
		{
			fp_CloseAfterProtocolViolation(NStr::CStr("PostgreSQL client closed after an invalid ParameterStatus message"));
			return fg_Move(Status).f_GetException();
		}

		NStr::CStr const *pRequired = nullptr;
		if (Status->m_Name == "client_encoding")
			pRequired = &NStr::gc_Str<"UTF8">.m_Str;
		else if (Status->m_Name == "standard_conforming_strings")
			pRequired = &NStr::gc_Str<"on">.m_Str;

		if (!pRequired || Status->m_Value == *pRequired)
			return {};

		fp_CloseAfterProtocolViolation(NStr::CStr("PostgreSQL client closed after an unsupported session setting"));

		return DMibErrorInstance("PostgreSQL {} changed to '{}', which the client does not support, so the connection was closed"_f << Status->m_Name << Status->m_Value)
			.f_ExceptionPointer()
		;
	}

	void CPostgresClientActor::fp_ConsumeBackendMessage(CBackendMessageRef const &_Ref)
	{
		mp_pInternal->m_ReceiveBuffer.f_RemoveFront(_Ref.m_TotalLength);
	}
}
