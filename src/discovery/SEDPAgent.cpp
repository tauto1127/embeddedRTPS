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

#include "rtps/discovery/SEDPAgent.h"
#include "rtps/discovery/TopicData.h"
#include "rtps/discovery/PreparedDiscovery.h"
#include "rtps/discovery/SPDPAgent.h"
#include "rtps/entities/Participant.h"
#include "rtps/entities/StatefulWriter.h"
#include "rtps/entities/StatelessWriter.h"
#include "netif_wasm_add.h"
#include <array>
#include <utility>
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/Log.h"
#include "ucdr/microcdr.h"

using rtps::SEDPAgent;

#if SEDP_VERBOSE && RTPS_GLOBAL_VERBOSE
#define SEDP_LOG(...)    \
  if (true)              \
  {                      \
    printf("[SEDP] ");   \
    printf(__VA_ARGS__); \
    printf("\n");        \
  }
#else
#define SEDP_LOG(...) //
#endif

bool SEDPAgent::init(Participant &part, const BuiltInEndpoints &endpoints)
{
  if (m_part != nullptr || endpoints.sedpPubWriter == nullptr ||
      endpoints.sedpPubReader == nullptr || endpoints.sedpSubWriter == nullptr ||
      endpoints.sedpSubReader == nullptr)
  {
    return false;
  }
  if (sys_mutex_new(&m_mutex) != ERR_OK)
  {
    SEDP_LOG("SEDPAgent failed to create mutex\n");
    return false;
  }

  m_part = &part;
  m_endpoints = endpoints;
  if (m_endpoints.sedpPubReader != nullptr)
  {
    m_endpoints.sedpPubReader->registerCallback(receiveCallbackPublisher, this);
  }
  m_endpoints.sedpSubReader->registerCallback(receiveCallbackSubscriber,
                                              this);
  return true;
}

void SEDPAgent::registerOnNewPublisherMatchedCallback(
    void (*callback)(void *arg), void *args)
{
  mfp_onNewPublisherCallback = callback;
  m_onNewPublisherArgs = args;
}

void SEDPAgent::registerOnNewSubscriberMatchedCallback(
    void (*callback)(void *arg), void *args)
{
  mfp_onNewSubscriberCallback = callback;
  m_onNewSubscriberArgs = args;
}

void SEDPAgent::receiveCallbackPublisher(void *callee,
                                         const ReaderCacheChange &cacheChange)
{
  auto agent = static_cast<SEDPAgent *>(callee);
  agent->onNewPublisher(cacheChange);
}

void SEDPAgent::receiveCallbackSubscriber(
    void *callee, const ReaderCacheChange &cacheChange)
{
  auto agent = static_cast<SEDPAgent *>(callee);
  agent->onNewSubscriber(cacheChange);
}

void SEDPAgent::onNewPublisher(const ReaderCacheChange &change)
{
  Lock lock{m_mutex};
#if SEDP_VERBOSE
  SEDP_LOG("New publisher\n");
#endif

  if (!change.copyInto(m_buffer, sizeof(m_buffer) / sizeof(m_buffer[0])))
  {
#if SEDP_VERBOSE
    SEDP_LOG("EDPAgent: Buffer too small.\n");
#endif
    return;
  }
  ucdrBuffer cdrBuffer;
  ucdr_init_buffer(&cdrBuffer, m_buffer, sizeof(m_buffer));

  TopicData topicData;
  if (topicData.readFromUcdrBuffer(cdrBuffer))
  {
    onNewPublisher(topicData);
  }
}

void SEDPAgent::addUnmatchedRemoteWriter(const TopicData &writerData)
{
  if (m_unmatchedRemoteWriters.isFull())
  {
#if SEDP_VERBOSE
    SEDP_LOG("List of unmatched remote writers is full.\n");
#endif
    return;
  }
  SEDP_LOG("Adding unmatched remote writer %s %s.\n", writerData.topicName,
           writerData.typeName);
  m_unmatchedRemoteWriters.add(TopicDataCompressed(writerData));
}

void SEDPAgent::addUnmatchedRemoteReader(const TopicData &readerData)
{
  if (m_unmatchedRemoteReaders.isFull())
  {
#if SEDP_VERBOSE
    SEDP_LOG("List of unmatched remote readers is full.\n");
#endif
    return;
  }
  SEDP_LOG("Adding unmatched remote reader %s %s.\n", readerData.topicName,
           readerData.typeName);
  m_unmatchedRemoteReaders.add(TopicDataCompressed(readerData));
}

void SEDPAgent::removeUnmatchedEntitiesOfParticipant(
    const GuidPrefix_t &guidPrefix)
{
  Lock lock{m_mutex};
  auto isElementToRemove = [&](const TopicDataCompressed &topicData)
  {
    return topicData.endpointGuid.prefix == guidPrefix;
  };

  auto thunk = [](void *arg, const TopicDataCompressed &value)
  {
    return (*static_cast<decltype(isElementToRemove) *>(arg))(value);
  };

  m_unmatchedRemoteReaders.remove(thunk, &isElementToRemove);
  m_unmatchedRemoteWriters.remove(thunk, &isElementToRemove);
}

uint32_t SEDPAgent::getNumRemoteUnmatchedReaders()
{
  Lock lock{m_mutex};
  return m_unmatchedRemoteReaders.getNumElements();
}

uint32_t SEDPAgent::getNumRemoteUnmatchedWriters()
{
  Lock lock{m_mutex};
  return m_unmatchedRemoteWriters.getNumElements();
}

void SEDPAgent::onNewPublisher(const TopicData &writerData)
{
  // TODO Is it okay to add Endpoint if the respective participant is unknown
  // participant?
  if (!m_part->hasRemoteParticipant(writerData.endpointGuid.prefix))
  {
    return;
  }
#if SEDP_VERBOSE
  SEDP_LOG("PUB T/D %s/%s", writerData.topicName, writerData.typeName);
#endif
  Reader *reader = m_part->getMatchingReader(writerData);
  if (reader == nullptr)
  {
#if SEDP_VERBOSE
    SEDP_LOG("SEDPAgent: Couldn't find reader for new Publisher[%s, %s] \n",
             writerData.topicName, writerData.typeName);
    addUnmatchedRemoteWriter(writerData);
#endif
    return;
  }
  // TODO check policies
#if SEDP_VERBOSE
  SEDP_LOG("Found a new ");
  if (writerData.reliabilityKind == ReliabilityKind_t::RELIABLE)
  {
    SEDP_LOG("reliable ");
  }
  else
  {
    SEDP_LOG("best-effort ");
  }
  SEDP_LOG("publisher\n");
#endif
  reader->addNewMatchedWriter(
      WriterProxy{writerData.endpointGuid, writerData.unicastLocator});
  if (mfp_onNewPublisherCallback != nullptr)
  {
    mfp_onNewPublisherCallback(m_onNewPublisherArgs);
  }
}

void SEDPAgent::onNewSubscriber(const ReaderCacheChange &change)
{
  Lock lock{m_mutex};
#if SEDP_VERBOSE
  SEDP_LOG("New subscriber\n");
#endif

  if (!change.copyInto(m_buffer, sizeof(m_buffer) / sizeof(m_buffer[0])))
  {
#if SEDP_VERBOSE
    SEDP_LOG("SEDPAgent: Buffer too small.");
#endif
    return;
  }
  ucdrBuffer cdrBuffer;
  ucdr_init_buffer(&cdrBuffer, m_buffer, sizeof(m_buffer));

  TopicData topicData;
  if (topicData.readFromUcdrBuffer(cdrBuffer))
  {
    onNewSubscriber(topicData);
  }
}

void SEDPAgent::onNewSubscriber(const TopicData &readerData)
{
  if (!m_part->hasRemoteParticipant(readerData.endpointGuid.prefix))
  {
    return;
  }
  Writer *writer = m_part->getMatchingWriter(readerData); // readerDataは、EDPパケットで送られてきたsubscriberの情報
#if SEDP_VERBOSE
  SEDP_LOG("SUB T/D %s/%s", readerData.topicName, readerData.typeName);
#endif
  if (writer == nullptr) // writerが見つからなかった場合UnmatchedRemoteReaderのリストか何かに追加
  {
#if SEDP_VERBOSE
    SEDP_LOG("SEDPAgent: Couldn't find writer for new subscriber[%s, %s]\n",
             readerData.topicName, readerData.typeName);
    addUnmatchedRemoteReader(readerData);
#endif
    return;
  }

  // TODO check policies
#if SEDP_VERBOSE
  SEDP_LOG("Found a new ");
  if (readerData.reliabilityKind == ReliabilityKind_t::RELIABLE)
  {
    SEDP_LOG("reliable ");
  }
  else
  {
    SEDP_LOG("best-effort ");
  }
  SEDP_LOG("Subscriber\n");
#endif
  if (readerData.multicastLocator.kind ==
      rtps::LocatorKind_t::LOCATOR_KIND_UDPv4)
  {
    writer->addNewMatchedReader(ReaderProxy{readerData.endpointGuid,
                                            readerData.unicastLocator,
                                            readerData.multicastLocator});
  }
  else
  {
    writer->addNewMatchedReader(
        ReaderProxy{readerData.endpointGuid, readerData.unicastLocator});
  }

  if (mfp_onNewSubscriberCallback != nullptr)
  {
    mfp_onNewSubscriberCallback(m_onNewSubscriberArgs);
  }
}

void SEDPAgent::tryMatchUnmatchedEndpoints()
{
  // Try to match remote readers with local writers
  for (auto &proxy : m_unmatchedRemoteReaders)
  {
    auto writer = m_part->getMatchingWriter(proxy);
    if (writer != nullptr)
    {
      writer->addNewMatchedReader(ReaderProxy{
          proxy.endpointGuid, proxy.unicastLocator, proxy.multicastLocator});
    }
  }

  // Try to match remote writers with local readers
  for (auto &proxy : m_unmatchedRemoteWriters)
  {
    auto reader = m_part->getMatchingReader(proxy);
    if (reader != nullptr)
    {
      reader->addNewMatchedWriter(
          WriterProxy{proxy.endpointGuid, proxy.unicastLocator});
    }
  }
}

bool SEDPAgent::projectLocalIp(uint32_t networkOrderIp)
{
  if (networkOrderIp == 0 || m_part == nullptr) return false;

  std::array<StatefulWriter *, Config::NUM_WRITERS_PER_PARTICIPANT> pubWriters{};
  std::array<StatefulWriter *, Config::NUM_READERS_PER_PARTICIPANT> subWriters{};
  uint8_t pubCount = 0;
  uint8_t subCount = 0;
  {
    Lock lock{m_mutex};
    auto &spdp = m_part->m_spdpAgent;
    if (m_part->m_sedpAgent.m_part != m_part || !spdp.initialized ||
        !m_endpoints.sedpPubWriter || !m_endpoints.sedpSubWriter ||
        !m_endpoints.sedpPubWriter->isInitialized() ||
        !m_endpoints.sedpSubWriter->isInitialized() ||
        !m_endpoints.sedpPubReader || !m_endpoints.sedpSubReader ||
        !m_endpoints.sedpPubReader->isInitialized() ||
        !m_endpoints.sedpSubReader->isInitialized() ||
        !spdp.m_buildInEndpoints.spdpWriter ||
        !spdp.m_buildInEndpoints.spdpReader ||
        !spdp.m_buildInEndpoints.spdpWriter->isInitialized() ||
        !spdp.m_buildInEndpoints.spdpReader->isInitialized())
      return false;
    if (spdp.m_lastAppliedIp == networkOrderIp) return true;

    const auto snapshot = m_part->snapshotEndpoints();
    if (snapshot.numWriters > snapshot.writers.size() ||
        snapshot.numReaders > snapshot.readers.size())
      return false;

    ip4_addr_t current{};
    current.addr = networkOrderIp;
    const std::array<uint8_t, 4> ip{{ip4_addr1(&current), ip4_addr2(&current),
                                     ip4_addr3(&current), ip4_addr4(&current)}};
    const FullLengthLocator locator = getUserUnicastLocator(m_part->m_participantId, ip);
    std::array<PBufWrapper, Config::NUM_WRITERS_PER_PARTICIPANT> pubPayloads{};
    std::array<PBufWrapper, Config::NUM_READERS_PER_PARTICIPANT> subPayloads{};

    for (uint8_t i = 0; i < snapshot.numWriters; ++i)
    {
      Writer *writer = snapshot.writers[i];
      if (!writer || !writer->isInitialized()) return false;
      const auto kind = writer->m_attributes.endpointGuid.entityId.entityKind;
      if (kind == EntityKind_t::BUILD_IN_WRITER_WITH_KEY ||
          kind == EntityKind_t::BUILD_IN_WRITER_WITHOUT_KEY) continue;
      if ((kind != EntityKind_t::USER_DEFINED_WRITER_WITH_KEY &&
           kind != EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY) ||
          pubCount >= Config::HISTORY_SIZE_STATEFUL)
        return false;
      if (!discovery_detail::prepareTopic(writer->m_attributes, locator,
                                           pubPayloads[pubCount])) return false;
      pubWriters[pubCount++] = m_endpoints.sedpPubWriter;
    }
    for (uint8_t i = 0; i < snapshot.numReaders; ++i)
    {
      Reader *reader = snapshot.readers[i];
      if (!reader || !reader->isInitialized()) return false;
      const auto kind = reader->m_attributes.endpointGuid.entityId.entityKind;
      if (kind == EntityKind_t::BUILD_IN_READER_WITH_KEY ||
          kind == EntityKind_t::BUILD_IN_READER_WITHOUT_KEY) continue;
      if (kind != EntityKind_t::USER_DEFINED_READER_WITH_KEY &&
          kind != EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY)
        return false;
      if (subCount >= Config::HISTORY_SIZE_STATEFUL) return false;
      if (!discovery_detail::prepareTopic(reader->m_attributes, locator,
                                           subPayloads[subCount])) return false;
      subWriters[subCount++] = m_endpoints.sedpSubWriter;
    }

    std::array<uint8_t, 400> announcement{};
    DataSize_t announcementLength = 0;
    PBufWrapper spdpPayload;
    if (!spdp.prepareAnnouncement(ip, announcement, announcementLength,
                                  spdpPayload)) return false;

    // Everything fallible is complete. These private typed admissions are
    // guaranteed moves into already validated fixed histories.
#if defined(RTPS_T10_TEST_ACCESS)
    extern void rtpsT10CommitAudit(int);
    rtpsT10CommitAudit(1);
#endif
    for (uint8_t i = 0; i < pubCount; ++i)
      pubWriters[i]->commitPreparedDiscoveryAlive(std::move(pubPayloads[i]));
    for (uint8_t i = 0; i < subCount; ++i)
      subWriters[i]->commitPreparedDiscoveryAlive(std::move(subPayloads[i]));
    spdp.commitPreparedAnnouncement(std::move(spdpPayload), announcement,
                                    announcementLength);
    for (uint8_t i = 0; i < snapshot.numWriters; ++i)
    {
      Writer *writer = snapshot.writers[i];
      const auto kind = writer->m_attributes.endpointGuid.entityId.entityKind;
      if (kind == EntityKind_t::USER_DEFINED_WRITER_WITH_KEY ||
          kind == EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY)
        writer->m_attributes.unicastLocator = locator;
    }
    for (uint8_t i = 0; i < snapshot.numReaders; ++i)
    {
      Reader *reader = snapshot.readers[i];
      const auto kind = reader->m_attributes.endpointGuid.entityId.entityKind;
      if (kind == EntityKind_t::USER_DEFINED_READER_WITH_KEY ||
          kind == EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY)
        reader->m_attributes.unicastLocator = locator;
    }
    spdp.m_lastAppliedIp = networkOrderIp;
#if defined(RTPS_T10_TEST_ACCESS)
    rtpsT10CommitAudit(0);
#endif
  }

  for (uint8_t i = 0; i < pubCount; ++i)
    pubWriters[i]->notifyPreparedDiscovery();
  for (uint8_t i = 0; i < subCount; ++i)
    subWriters[i]->notifyPreparedDiscovery();
  m_part->m_spdpAgent.notifyPreparedAnnouncement();
  return true;
}

bool SEDPAgent::registerWriter(Writer &writer, bool initialIpValid,
                              uint32_t initialIp)
{
  if (m_part == nullptr || !writer.isInitialized() ||
      m_endpoints.sedpPubWriter == nullptr ||
      !m_endpoints.sedpPubWriter->isInitialized()) return false;
  const EntityKind_t kind = writer.m_attributes.endpointGuid.entityId.entityKind;
  if (kind != EntityKind_t::USER_DEFINED_WRITER_WITH_KEY &&
      kind != EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY) return false;

  PBufWrapper payload;
  {
    Lock lock{m_mutex};
    const auto snapshot = m_part->snapshotEndpoints();
    if (snapshot.numWriters >= snapshot.writers.size()) return false;
    size_t userWriters = 0;
    for (uint8_t i = 0; i < snapshot.numWriters; ++i)
    {
      if (snapshot.writers[i] == nullptr) return false;
      const auto existingKind = snapshot.writers[i]->m_attributes.endpointGuid.entityId.entityKind;
      if (existingKind == EntityKind_t::USER_DEFINED_WRITER_WITH_KEY ||
          existingKind == EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY) ++userWriters;
    }
    if (userWriters >= Config::HISTORY_SIZE_STATEFUL) return false;
    const uint32_t selectedIp = m_part->m_spdpAgent.m_lastAppliedIp != 0
                                    ? m_part->m_spdpAgent.m_lastAppliedIp
                                    : (initialIpValid ? initialIp : 0);
    if (selectedIp == 0) return false;

    ip4_addr_t address{};
    address.addr = selectedIp;
    const std::array<uint8_t, 4> bytes{{ip4_addr1(&address), ip4_addr2(&address),
                                        ip4_addr3(&address), ip4_addr4(&address)}};
    const auto locator = getUserUnicastLocator(m_part->m_participantId, bytes);
    if (!discovery_detail::prepareTopic(writer.m_attributes, locator, payload)) return false;

    m_endpoints.sedpPubWriter->commitPreparedDiscoveryAlive(std::move(payload));
    writer.m_attributes.unicastLocator = locator;
    m_part->publishRegisteredWriter(&writer);
    tryMatchUnmatchedEndpoints();
  }
  m_endpoints.sedpPubWriter->notifyPreparedDiscovery();
  return true;
}

bool SEDPAgent::registerReader(Reader &reader, bool initialIpValid,
                               uint32_t initialIp)
{
  if (m_part == nullptr || !reader.isInitialized() ||
      m_endpoints.sedpSubWriter == nullptr ||
      !m_endpoints.sedpSubWriter->isInitialized()) return false;
  const EntityKind_t kind = reader.m_attributes.endpointGuid.entityId.entityKind;
  if (kind != EntityKind_t::USER_DEFINED_READER_WITH_KEY &&
      kind != EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY) return false;

  PBufWrapper payload;
  {
    Lock lock{m_mutex};
    const auto snapshot = m_part->snapshotEndpoints();
    if (snapshot.numReaders >= snapshot.readers.size()) return false;
    size_t userReaders = 0;
    for (uint8_t i = 0; i < snapshot.numReaders; ++i)
    {
      if (snapshot.readers[i] == nullptr) return false;
      const auto existingKind = snapshot.readers[i]->m_attributes.endpointGuid.entityId.entityKind;
      if (existingKind == EntityKind_t::USER_DEFINED_READER_WITH_KEY ||
          existingKind == EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY) ++userReaders;
    }
    if (userReaders >= Config::HISTORY_SIZE_STATEFUL) return false;
    const uint32_t selectedIp = m_part->m_spdpAgent.m_lastAppliedIp != 0
                                    ? m_part->m_spdpAgent.m_lastAppliedIp
                                    : (initialIpValid ? initialIp : 0);
    if (selectedIp == 0) return false;

    ip4_addr_t address{};
    address.addr = selectedIp;
    const std::array<uint8_t, 4> bytes{{ip4_addr1(&address), ip4_addr2(&address),
                                        ip4_addr3(&address), ip4_addr4(&address)}};
    const auto locator = getUserUnicastLocator(m_part->m_participantId, bytes);
    if (!discovery_detail::prepareTopic(reader.m_attributes, locator, payload)) return false;

    m_endpoints.sedpSubWriter->commitPreparedDiscoveryAlive(std::move(payload));
    reader.m_attributes.unicastLocator = locator;
    m_part->publishRegisteredReader(&reader);
    tryMatchUnmatchedEndpoints();
  }
  m_endpoints.sedpSubWriter->notifyPreparedDiscovery();
  return true;
}
