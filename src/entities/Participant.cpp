/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE

This file is part of embeddedRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "rtps/entities/Participant.h"

#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/common/Locator.h"
#include "rtps/messages/MessageReceiver.h"
#include "rtps/utils/Log.h"
#include "rtps/communication/UdpDriver.h"
#include "netif_wasm_add.h"

#if PARTICIPANT_VERBOSE && RTPS_GLOBAL_VERBOSE
#define PARTICIPANT_LOG(...)  \
  if (true)                   \
  {                           \
    printf("[Participant] "); \
    printf(__VA_ARGS__);      \
    printf("\n");             \
  }
#else
#define PARTICIPANT_LOG(...) //
#endif

using rtps::Participant;

Participant::Participant()
    : m_guidPrefix(GUIDPREFIX_UNKNOWN), m_participantId(PARTICIPANT_ID_INVALID),
      m_receiver(this)
{
  if (sys_mutex_new(&m_mutex) != ERR_OK)
  {
    while (1)
      ;
  }
}
Participant::Participant(const GuidPrefix_t &guidPrefix,
                         ParticipantId_t participantId)
    : m_guidPrefix(guidPrefix), m_participantId(participantId),
      m_receiver(this)
{
  if (sys_mutex_new(&m_mutex) != ERR_OK)
  {
    while (1)
      ;
  }
}

Participant::~Participant() { m_spdpAgent.stop(); }

void Participant::reuse(const GuidPrefix_t &guidPrefix,
                        ParticipantId_t participantId)
{
  m_guidPrefix = guidPrefix;
  m_participantId = participantId;
}

bool Participant::isValid()
{
  return m_participantId != PARTICIPANT_ID_INVALID;
}

std::array<uint8_t, 3> Participant::getNextUserEntityKey()
{
  Lock lock{m_mutex};
  const auto result = m_nextUserEntityId;

  ++m_nextUserEntityId[2];
  if (m_nextUserEntityId[2] == 0)
  {
    ++m_nextUserEntityId[1];
    if (m_nextUserEntityId[1] == 0)
    {
      ++m_nextUserEntityId[0];
    }
  }
  return result;
}

bool Participant::registerOnNewPublisherMatchedCallback(
    void (*callback)(void *arg), void *args)
{
  if (!m_hasBuilInEndpoints)
  {
    return false;
  }

  m_sedpAgent.registerOnNewPublisherMatchedCallback(callback, args);
  return true;
}

bool Participant::registerOnNewSubscriberMatchedCallback(
    void (*callback)(void *arg), void *args)
{
  if (!m_hasBuilInEndpoints)
  {
    return false;
  }

  m_sedpAgent.registerOnNewSubscriberMatchedCallback(callback, args);
  return true;
}
// m_writersの配列にポインターを追加して、EDPを動作させてそう
rtps::Participant::EndpointSnapshot Participant::snapshotEndpoints() const
{
  Lock lock{m_mutex};
  EndpointSnapshot result;
  result.writers = m_writers;
  result.readers = m_readers;
  result.numWriters = m_numWriters;
  result.numReaders = m_numReaders;
  return result;
}

void Participant::publishRegisteredWriter(Writer *writer)
{
  Lock lock{m_mutex};
  // Capacity is checked before SEDP admission; app creation is serialized.
  m_writers[m_numWriters++] = writer;
}

rtps::Writer *Participant::addWriter(Writer *writer)
{
  if (writer == nullptr || !m_hasBuilInEndpoints || !writer->isInitialized())
    return nullptr;
  netif_wasm_ip_snapshot_t snapshot{};
#if defined(__wasi__)
  const bool initialIpValid = netif_wasm_get_ip_snapshot(&snapshot) == 0 &&
                              snapshot.valid && snapshot.ip_addr != 0;
#else
  std::array<uint8_t, 4> address{};
  ip4_addr_t current{};
  const bool initialIpValid = UdpDriver::getLocalIpAddress(address);
  if (initialIpValid)
  {
    IP4_ADDR(&current, address[0], address[1], address[2], address[3]);
    snapshot.ip_addr = current.addr;
    snapshot.valid = 1;
  }
#endif
  return m_sedpAgent.registerWriter(*writer, initialIpValid,
                                    snapshot.ip_addr) ? writer : nullptr;
}

bool Participant::isWritersFull()
{
  Lock lock{m_mutex};
  return m_numWriters == m_writers.size();
}

void Participant::publishRegisteredReader(Reader *reader)
{
  Lock lock{m_mutex};
  // Capacity is checked before SEDP admission; app creation is serialized.
  m_readers[m_numReaders++] = reader;
}

rtps::Reader *Participant::addReader(Reader *reader)
{
  if (reader == nullptr || !m_hasBuilInEndpoints || !reader->isInitialized())
    return nullptr;
  netif_wasm_ip_snapshot_t snapshot{};
#if defined(__wasi__)
  const bool initialIpValid = netif_wasm_get_ip_snapshot(&snapshot) == 0 &&
                              snapshot.valid && snapshot.ip_addr != 0;
#else
  std::array<uint8_t, 4> address{};
  ip4_addr_t current{};
  const bool initialIpValid = UdpDriver::getLocalIpAddress(address);
  if (initialIpValid)
  {
    IP4_ADDR(&current, address[0], address[1], address[2], address[3]);
    snapshot.ip_addr = current.addr;
    snapshot.valid = 1;
  }
#endif
  return m_sedpAgent.registerReader(*reader, initialIpValid,
                                    snapshot.ip_addr) ? reader : nullptr;
}

bool Participant::isReadersFull()
{
  Lock lock{m_mutex};
  return m_numReaders == m_readers.size();
}

rtps::Writer *Participant::getWriter(EntityId_t id) const
{
  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numWriters; ++i)
    if (snapshot.writers[i]->m_attributes.endpointGuid.entityId == id)
      return snapshot.writers[i];
  return nullptr;
}

rtps::Reader *Participant::getReader(EntityId_t id) const
{
  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numReaders; ++i)
    if (snapshot.readers[i]->m_attributes.endpointGuid.entityId == id)
      return snapshot.readers[i];
  return nullptr;
}

rtps::Reader *Participant::getReaderByWriterId(const Guid_t &guid) const
{
  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numReaders; ++i)
    if (snapshot.readers[i]->knowWriterId(guid)) return snapshot.readers[i];
  return nullptr;
}

// 二つある？
rtps::Writer *
Participant::getMatchingWriter(const TopicData &readerTopicData) const
{
  // readerTopicDataのtype_nameとtopic_nameを表示
  PARTICIPANT_LOG("[getMatchingWriter] Matching writer topic_name:%s | type_name:%s | reliabilityKind:%d\n ", readerTopicData.topicName, readerTopicData.typeName, readerTopicData.reliabilityKind);

  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numWriters; ++i)
  {
    auto *writer = snapshot.writers[i];
    if (writer->m_attributes.matchesTopicOf(readerTopicData) &&
        (readerTopicData.reliabilityKind == ReliabilityKind_t::BEST_EFFORT ||
         writer->m_attributes.reliabilityKind == ReliabilityKind_t::RELIABLE))
      return writer;
  }
  return nullptr;
}

// 正常に動作
rtps::Reader *
Participant::getMatchingReader(const TopicData &writerTopicData) const
{
  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numReaders; ++i)
  {
    auto *reader = snapshot.readers[i];
    if (reader->m_attributes.matchesTopicOf(writerTopicData) &&
        (writerTopicData.reliabilityKind == ReliabilityKind_t::RELIABLE ||
         reader->m_attributes.reliabilityKind == ReliabilityKind_t::BEST_EFFORT))
      return reader;
  }
  return nullptr;
}

// nullptrを返す うまくwriterとremote readerをマッチングできていない
rtps::Writer *Participant::getMatchingWriter(
    const TopicDataCompressed &readerTopicData) const
{
  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numWriters; ++i)
  {
    auto *writer = snapshot.writers[i];
    if (readerTopicData.matchesTopicOf(writer->m_attributes) &&
        (readerTopicData.reliabilityKind == ReliabilityKind_t::BEST_EFFORT ||
         writer->m_attributes.reliabilityKind == ReliabilityKind_t::RELIABLE))
      return writer;
  }
  return nullptr;
}

rtps::Reader *Participant::getMatchingReader(
    const TopicDataCompressed &writerTopicData) const
{
  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numReaders; ++i)
  {
    auto *reader = snapshot.readers[i];
    if (writerTopicData.matchesTopicOf(reader->m_attributes) &&
        (writerTopicData.reliabilityKind == ReliabilityKind_t::RELIABLE ||
         reader->m_attributes.reliabilityKind == ReliabilityKind_t::BEST_EFFORT))
      return reader;
  }
  return nullptr;
}

bool Participant::addNewRemoteParticipant(
    const ParticipantProxyData &remotePart)
{
  Lock lock{m_mutex};
  return m_remoteParticipants.add(remotePart);
}

bool Participant::removeRemoteParticipant(const GuidPrefix_t &prefix)
{
  auto isElementToRemove = [&](const ParticipantProxyData &proxy)
  {
    return proxy.m_guid.prefix == prefix;
  };
  auto thunk = [](void *arg, const ParticipantProxyData &value)
  {
    return (*static_cast<decltype(isElementToRemove) *>(arg))(value);
  };
  {
    Lock lock{m_mutex};
    if (!m_remoteParticipants.remove(thunk, &isElementToRemove)) return false;
  }
  removeAllEntitiesOfParticipant(prefix);
  m_sedpAgent.removeUnmatchedEntitiesOfParticipant(prefix);
  return true;
}

void Participant::removeAllEntitiesOfParticipant(const GuidPrefix_t &prefix)
{
  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numWriters; ++i)
    snapshot.writers[i]->removeReaderOfParticipant(prefix);
  for (uint8_t i = 0; i < snapshot.numReaders; ++i)
    snapshot.readers[i]->removeWriterOfParticipant(prefix);
}

bool Participant::copyRemoteParticipant(const GuidPrefix_t &prefix,
                                         ParticipantProxyData &snapshot)
{
  auto isElementToFind = [&](const ParticipantProxyData &proxy)
  {
    return proxy.m_guid.prefix == prefix;
  };
  auto thunk = [](void *arg, const ParticipantProxyData &value)
  {
    return (*static_cast<decltype(isElementToFind) *>(arg))(value);
  };
  Lock lock{m_mutex};
  const auto *remote = m_remoteParticipants.find(thunk, &isElementToFind);
  if (remote == nullptr) return false;
  snapshot = *remote;
  return true;
}

bool Participant::hasRemoteParticipant(const GuidPrefix_t &prefix)
{
  Lock lock{m_mutex};
  for (const auto &remote : m_remoteParticipants)
    if (remote.m_guid.prefix == prefix) return true;
  return false;
}

void Participant::refreshRemoteParticipantLiveliness(
    const GuidPrefix_t &prefix)
{
  auto isElementToFind = [&](const ParticipantProxyData &proxy)
  {
    return proxy.m_guid.prefix == prefix;
  };
  auto thunk = [](void *arg, const ParticipantProxyData &value)
  {
    return (*static_cast<decltype(isElementToFind) *>(arg))(value);
  };
  Lock lock{m_mutex};
  auto remoteParticipant = m_remoteParticipants.find(thunk, &isElementToFind);
  if (remoteParticipant != nullptr) remoteParticipant->onAliveSignal();
}

bool Participant::hasReaderWithMulticastLocator(ip4_addr_t address)
{
  const auto snapshot = snapshotEndpoints();
  for (uint8_t i = 0; i < snapshot.numReaders; i++)
  {
    if (snapshot.readers[i]->m_attributes.multicastLocator.isSameAddress(&address))
    {
      return true;
    }
  }
  return false;
}

uint32_t Participant::getRemoteParticipantCount()
{
  Lock lock{m_mutex};
  return m_remoteParticipants.getNumElements();
}

rtps::MessageReceiver *Participant::getMessageReceiver() { return &m_receiver; }

void Participant::addHeartbeat(GuidPrefix_t sourceGuidPrefix)
{
  Lock lock{m_mutex};
  for (auto &remote : m_remoteParticipants)
  {
    if (remote.m_guid.prefix == sourceGuidPrefix)
    {
      remote.onAliveSignal();
      break;
    }
  }
}

bool Participant::checkAndResetHeartbeats()
{
  std::array<GuidPrefix_t, Config::SPDP_MAX_NUMBER_FOUND_PARTICIPANTS> expired{};
  size_t count = 0;
  {
    Lock lock{m_mutex};
    for (auto &remote : m_remoteParticipants)
      if (!remote.isAlive()) expired[count++] = remote.m_guid.prefix;
  }
  // Never hold Participant while acquiring SEDP or endpoint locks.
  PARTICIPANT_LOG("Unmatched remote writers/readers, %u / %u\n",
      static_cast<unsigned int>(m_sedpAgent.getNumRemoteUnmatchedWriters()),
      static_cast<unsigned int>(m_sedpAgent.getNumRemoteUnmatchedReaders()));
  for (size_t i = 0; i < count; ++i) removeRemoteParticipant(expired[i]);
  return true;
}

rtps::SPDPAgent &Participant::getSPDPAgent() { return m_spdpAgent; }

bool Participant::hasBuiltInEndpoints() const
{
  Lock lock{m_mutex};
  return m_hasBuilInEndpoints;
}

bool Participant::addBuiltInEndpoints(BuiltInEndpoints &endpoints)
{
  if (m_hasBuilInEndpoints || endpoints.spdpWriter == nullptr ||
      endpoints.spdpReader == nullptr || endpoints.sedpPubWriter == nullptr ||
      endpoints.sedpPubReader == nullptr || endpoints.sedpSubWriter == nullptr ||
      endpoints.sedpSubReader == nullptr ||
      m_writers.size() - m_numWriters < 3 ||
      m_readers.size() - m_numReaders < 3)
  {
    return false;
  }
  if (!m_spdpAgent.init(*this, endpoints))
  {
    return false;
  }
  if (!m_sedpAgent.init(*this, endpoints))
  {
    return false;
  }

  // Single creation thread; publish all six builtin pointers atomically.
  {
    Lock lock{m_mutex};
    m_writers[m_numWriters++] = endpoints.spdpWriter;
    m_writers[m_numWriters++] = endpoints.sedpPubWriter;
    m_writers[m_numWriters++] = endpoints.sedpSubWriter;
    m_readers[m_numReaders++] = endpoints.spdpReader;
    m_readers[m_numReaders++] = endpoints.sedpPubReader;
    m_readers[m_numReaders++] = endpoints.sedpSubReader;
    m_hasBuilInEndpoints = true;
  }
  return true;
}

void Participant::newMessage(const uint8_t *data, DataSize_t size)
{
  m_receiver.processMessage(data, size);
}
