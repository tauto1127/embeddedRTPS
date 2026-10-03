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

#include "rtps/entities/Domain.h"
#include "rtps/utils/Log.h"
#include "rtps/utils/udpUtils.h"

#if DOMAIN_VERBOSE && RTPS_GLOBAL_VERBOSE
#define DOMAIN_LOG(...)  \
  if (true)              \
  {                      \
    printf("[Domain] "); \
    printf(__VA_ARGS__); \
    printf("\n");        \
  }
#else
#define DOMAIN_LOG(...) //
#endif

using rtps::Domain;

Domain::Domain()
    : m_threadPool(receiveJumppad, this),
      m_transport(ThreadPool::readCallback, &m_threadPool)
{
  m_transport.createUdpConnection(getUserMulticastPort());
  m_transport.createUdpConnection(getBuiltInMulticastPort());
  m_transport.joinMultiCastGroup(transformIP4ToU32(239, 255, 0, 1));
}

Domain::~Domain() { stop(); }

// SPDPagentをスタートさせる
bool Domain::completeInit()
{
  m_initComplete = m_threadPool.startThreads();

  if (!m_initComplete)
  {
    DOMAIN_LOG("Failed starting threads\n");
  }

  bool hasPublishedParticipant = false;
  if (m_initComplete)
  {
    for (auto i = 0; i < m_nextParticipantId - PARTICIPANT_START_ID; i++)
    {
      if (m_participants[i].hasBuiltInEndpoints())
      {
        hasPublishedParticipant = true;
        m_participants[i].getSPDPAgent().start();
      }
    }
  }
  return m_initComplete && hasPublishedParticipant;
}

void Domain::stop() { m_threadPool.stopThreads(); }

// readerにメッセージが来ると待機スレッド(rtps::ThreadPool::doReaderWork)から、このコールバックが呼ばれる
void Domain::receiveJumppad(void *callee, const PacketInfo &packet)
{
  auto domain = static_cast<Domain *>(callee);
  domain->receiveCallback(packet);
}

void Domain::receiveCallback(const PacketInfo &packet)
{
  if (packet.buffer.firstElement->next != nullptr)
  {

    DOMAIN_LOG("Cannot handle multiple elements chained. You might "
               "want to increase PBUF_POOL_BUFSIZE\n");
  }

  if (isMetaMultiCastPort(packet.destPort))
  {
    // Pass to all
    DOMAIN_LOG("Domain: Multicast to port %u\n", packet.destPort);
    for (auto i = 0; i < m_nextParticipantId - PARTICIPANT_START_ID; ++i)
    {
      m_participants[i].newMessage(
          static_cast<uint8_t *>(packet.buffer.firstElement->payload),
          packet.buffer.firstElement->len);
    }
    // First Check if UserTraffic Multicast
  }
  else if (isUserMultiCastPort(packet.destPort))
  {
    // Pass to Participant with assigned Multicast Adress (Port ist everytime
    // the same)
    DOMAIN_LOG("Domain: Got user multicast message on port %u\n",
               packet.destPort);
    for (auto i = 0; i < m_nextParticipantId - PARTICIPANT_START_ID; ++i)
    {
      if (m_participants[i].hasReaderWithMulticastLocator(packet.destAddr))
      {
        DOMAIN_LOG("Domain: Forward Multicast only to Participant: %u\n", i);
        m_participants[i].newMessage(
            static_cast<uint8_t *>(packet.buffer.firstElement->payload),
            packet.buffer.firstElement->len);
      }
    }
  }
  else
  {
    // Pass to addressed one only (Unicast, by Port)
    ParticipantId_t id = getParticipantIdFromUnicastPort(
        packet.destPort, isUserPort(packet.destPort));
    if (id != PARTICIPANT_ID_INVALID)
    {
      DOMAIN_LOG("Domain: Got unicast message on port %u\n", packet.destPort);
      if (id < m_nextParticipantId &&
          id >= PARTICIPANT_START_ID)
      { // added extra check to avoid segfault
        // (id below START_ID)
        m_participants[id - PARTICIPANT_START_ID].newMessage(
            static_cast<uint8_t *>(packet.buffer.firstElement->payload),
            packet.buffer.firstElement->len); // ここでパケットを処理している
      }
      else
      {
        DOMAIN_LOG("Domain: Participant id too high or unplausible.\n");
      }
    }
    else
    {
      DOMAIN_LOG("Domain: Got message to port %u: no matching participant\n",
                 packet.destPort);
    }
  }
}

rtps::Participant *Domain::createParticipant()
{

  DOMAIN_LOG("Domain: Creating new participant.\n");

  auto nextSlot =
      static_cast<uint8_t>(m_nextParticipantId - PARTICIPANT_START_ID);
  if (m_initComplete || m_participants.size() <= nextSlot)
  {
    return nullptr;
  }

  auto &entry = m_participants[nextSlot];
  entry.reuse(generateGuidPrefix(m_nextParticipantId), m_nextParticipantId);
  registerPort(entry);
  if (!createBuiltinWritersAndReaders(entry))
  {
    DOMAIN_LOG("Domain: Failed initializing builtin endpoints.\n");
    // A partially initialized fixed-pool slot cannot be initialized again.
    // Retire each endpoint object which successfully acquired resources.
    m_numStatelessWriters +=
        m_statelessWriters[m_numStatelessWriters].isInitialized() ? 1 : 0;
    m_numStatelessReaders +=
        m_statelessReaders[m_numStatelessReaders].isInitialized() ? 1 : 0;
    const auto initializedStatefulWriters =
        static_cast<uint8_t>(m_statefulWriters[m_numStatefulWriters].isInitialized()) +
        static_cast<uint8_t>(m_statefulWriters[m_numStatefulWriters + 1].isInitialized());
    const auto initializedStatefulReaders =
        static_cast<uint8_t>(m_statefulReaders[m_numStatefulReaders].isInitialized()) +
        static_cast<uint8_t>(m_statefulReaders[m_numStatefulReaders + 1].isInitialized());
    m_numStatefulWriters += initializedStatefulWriters;
    m_numStatefulReaders += initializedStatefulReaders;
    ++m_nextParticipantId;
    return nullptr;
  }
  ++m_nextParticipantId;
  return &entry;
}

bool Domain::createBuiltinWritersAndReaders(Participant &part)
{
  if (m_numStatelessWriters >= m_statelessWriters.size() ||
      m_numStatelessReaders >= m_statelessReaders.size() ||
      m_numStatefulWriters + 2 > m_statefulWriters.size() ||
      m_numStatefulReaders + 2 > m_statefulReaders.size())
  {
    return false;
  }
  // SPDP
  StatelessWriter &spdpWriter = m_statelessWriters[m_numStatelessWriters];
  StatelessReader &spdpReader = m_statelessReaders[m_numStatelessReaders];

  TopicData spdpWriterAttributes;
  spdpWriterAttributes.topicName[0] = '\0';
  spdpWriterAttributes.typeName[0] = '\0';
  spdpWriterAttributes.reliabilityKind = ReliabilityKind_t::BEST_EFFORT;
  spdpWriterAttributes.durabilityKind = DurabilityKind_t::TRANSIENT_LOCAL;
  spdpWriterAttributes.endpointGuid.prefix = part.m_guidPrefix;
  spdpWriterAttributes.endpointGuid.entityId =
      ENTITYID_SPDP_BUILTIN_PARTICIPANT_WRITER;
  spdpWriterAttributes.unicastLocator = getBuiltInMulticastLocator();

  if (!spdpWriter.init(spdpWriterAttributes, TopicKind_t::WITH_KEY,
                       &m_threadPool, m_transport))
  {
    return false;
  }
  spdpWriter.addNewMatchedReader(
      ReaderProxy{{part.m_guidPrefix, ENTITYID_SPDP_BUILTIN_PARTICIPANT_READER},
                  getBuiltInMulticastLocator()});

  TopicData spdpReaderAttributes;
  spdpReaderAttributes.endpointGuid = {
      part.m_guidPrefix, ENTITYID_SPDP_BUILTIN_PARTICIPANT_READER};
  spdpReader.init(spdpReaderAttributes);
  if (!spdpReader.isInitialized())
  {
    return false;
  }

  // SEDP
  StatefulReader &sedpPubReader = m_statefulReaders[m_numStatefulReaders];
  StatefulReader &sedpSubReader = m_statefulReaders[m_numStatefulReaders + 1];
  StatefulWriter &sedpPubWriter = m_statefulWriters[m_numStatefulWriters];
  StatefulWriter &sedpSubWriter = m_statefulWriters[m_numStatefulWriters + 1];

  // Prepare attributes from one safe current-IP copy; never seed a zero locator
  // when the snapshot is unavailable.
  std::array<uint8_t, 4> initialIp{};
  if (!UdpDriver::getLocalIpAddress(initialIp) ||
      (initialIp[0] == 0 && initialIp[1] == 0 &&
       initialIp[2] == 0 && initialIp[3] == 0))
  {
    return false;
  }
  TopicData sedpAttributes;
  sedpAttributes.topicName[0] = '\0';
  sedpAttributes.typeName[0] = '\0';
  sedpAttributes.reliabilityKind = ReliabilityKind_t::RELIABLE;
  sedpAttributes.durabilityKind = DurabilityKind_t::TRANSIENT_LOCAL;
  sedpAttributes.endpointGuid.prefix = part.m_guidPrefix;
  sedpAttributes.unicastLocator =
      getBuiltInUnicastLocator(part.m_participantId, initialIp);

  // READER
  sedpAttributes.endpointGuid.entityId =
      ENTITYID_SEDP_BUILTIN_PUBLICATIONS_READER;
  sedpPubReader.init(sedpAttributes, m_transport);
  if (!sedpPubReader.isInitialized())
  {
    return false;
  }
  sedpAttributes.endpointGuid.entityId =
      ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_READER;
  sedpSubReader.init(sedpAttributes, m_transport);
  if (!sedpSubReader.isInitialized())
  {
    return false;
  }

  // WRITER
  sedpAttributes.endpointGuid.entityId =
      ENTITYID_SEDP_BUILTIN_PUBLICATIONS_WRITER;
  if (!sedpPubWriter.init(sedpAttributes, TopicKind_t::NO_KEY,
                          &m_threadPool, m_transport))
  {
    return false;
  }

  sedpAttributes.endpointGuid.entityId =
      ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_WRITER;
  if (!sedpSubWriter.init(sedpAttributes, TopicKind_t::NO_KEY,
                          &m_threadPool, m_transport))
  {
    return false;
  }

  // COLLECT
  BuiltInEndpoints endpoints{};
  endpoints.spdpWriter = &spdpWriter;
  endpoints.spdpReader = &spdpReader;
  endpoints.sedpPubReader = &sedpPubReader;
  endpoints.sedpSubReader = &sedpSubReader;
  endpoints.sedpPubWriter = &sedpPubWriter;
  endpoints.sedpSubWriter = &sedpSubWriter;

  if (!part.addBuiltInEndpoints(endpoints))
  {
    return false;
  }
  ++m_numStatelessWriters;
  ++m_numStatelessReaders;
  m_numStatefulReaders += 2;
  m_numStatefulWriters += 2;
  return true;
}

void Domain::registerPort(const Participant &part)
{
  m_transport.createUdpConnection(getUserUnicastPort(part.m_participantId));
  m_transport.createUdpConnection(getBuiltInUnicastPort(part.m_participantId));
}

void Domain::registerMulticastPort(FullLengthLocator mcastLocator)
{
  if (mcastLocator.kind == LocatorKind_t::LOCATOR_KIND_UDPv4)
  {
    m_transport.createUdpConnection(mcastLocator.getLocatorPort());
  }
}

rtps::Reader *Domain::readerExists(Participant &part, const char *topicName,
                                   const char *typeName, bool reliable)
{
  if (reliable)
  {
    for (unsigned int i = 0; i < m_numStatefulReaders; i++)
    {
      if (m_statefulReaders[i].isInitialized())
      {
        if (strncmp(m_statefulReaders[i].m_attributes.topicName, topicName,
                    Config::MAX_TYPENAME_LENGTH) != 0)
        {
          continue;
        }

        if (strncmp(m_statefulReaders[i].m_attributes.typeName, typeName,
                    Config::MAX_TYPENAME_LENGTH) != 0)
        {
          continue;
        }

        DOMAIN_LOG("StatefulReader exists already [%s, %s]\n", topicName,
                   typeName);

        return &m_statefulReaders[i];
      }
    }
  }
  else
  {
    for (unsigned int i = 0; i < m_numStatelessReaders; i++)
    {
      if (m_statelessReaders[i].isInitialized())
      {
        if (strncmp(m_statelessReaders[i].m_attributes.topicName, topicName,
                    Config::MAX_TYPENAME_LENGTH) != 0)
        {
          continue;
        }

        if (strncmp(m_statelessReaders[i].m_attributes.typeName, typeName,
                    Config::MAX_TYPENAME_LENGTH) != 0)
        {
          continue;
        }

        DOMAIN_LOG("StatelessReader exists [%s, %s]\n", topicName, typeName);

        return &m_statelessReaders[i];
      }
    }
  }

  return nullptr;
}

rtps::Writer *Domain::writerExists(Participant &part, const char *topicName,
                                   const char *typeName, bool reliable)
{
  if (reliable)
  {
    for (unsigned int i = 0; i < m_numStatefulWriters; i++)
    {
      if (m_statefulWriters[i].isInitialized())
      {
        if (strncmp(m_statefulWriters[i].m_attributes.topicName, topicName,
                    Config::MAX_TYPENAME_LENGTH) != 0)
        {
          continue;
        }

        if (strncmp(m_statefulWriters[i].m_attributes.typeName, typeName,
                    Config::MAX_TYPENAME_LENGTH) != 0)
        {
          continue;
        }

        DOMAIN_LOG("StatefulWriter exists [%s, %s]\n", topicName, typeName);

        return &m_statefulWriters[i];
      }
    }
  }
  else
  {
    for (unsigned int i = 0; i < m_numStatelessWriters; i++)
    {
      if (m_statelessWriters[i].isInitialized())
      {
        if (strncmp(m_statelessWriters[i].m_attributes.topicName, topicName,
                    Config::MAX_TYPENAME_LENGTH) != 0)
        {
          continue;
        }

        if (strncmp(m_statelessWriters[i].m_attributes.typeName, typeName,
                    Config::MAX_TYPENAME_LENGTH) != 0)
        {
          continue;
        }

        DOMAIN_LOG("StatelessWriter exists [%s, %s]\n", topicName, typeName);

        return &m_statelessWriters[i];
      }
    }
  }

  return nullptr;
}

// 初期設定でenforceUnicast= false
rtps::Writer *Domain::createWriter(Participant &part, const char *topicName,
                                   const char *typeName, bool reliable,
                                   bool enforceUnicast)
{

  // Check if there is enough capacity for more writers
  if ((reliable && m_statefulWriters.size() <= m_numStatefulWriters) ||
      (!reliable && m_statelessWriters.size() <= m_numStatelessWriters) ||
      part.isWritersFull())
  {

    DOMAIN_LOG("No Writer created. Max Number of Writers reached.\n");

    return nullptr;
  }

  // TODO Distinguish WithKey and NoKey (Also changes EntityKind)
  TopicData attributes;

  if (strlen(topicName) > Config::MAX_TOPICNAME_LENGTH ||
      strlen(typeName) > Config::MAX_TYPENAME_LENGTH)
  {
    DOMAIN_LOG("No Writer created. stopping  over 40 topic_len=%d type_len%d\n", strlen(topicName), strlen(typeName)); // でない
    return nullptr;
  }
  strcpy(attributes.topicName, topicName);
  strcpy(attributes.typeName, typeName);
  attributes.endpointGuid.prefix = part.m_guidPrefix;
  attributes.endpointGuid.entityId = {
      part.getNextUserEntityKey(),
      EntityKind_t::USER_DEFINED_WRITER_WITHOUT_KEY};
  std::array<uint8_t, 4> currentIp{};
  if (!UdpDriver::getLocalIpAddress(currentIp) ||
      (currentIp[0] == 0 && currentIp[1] == 0 &&
       currentIp[2] == 0 && currentIp[3] == 0))
    return nullptr;
  attributes.unicastLocator =
      getUserUnicastLocator(part.m_participantId, currentIp);
  attributes.durabilityKind = DurabilityKind_t::TRANSIENT_LOCAL;

  DOMAIN_LOG("Creating writer[%s, %s]\n", topicName, typeName);

  if (reliable)
  {
    attributes.reliabilityKind = ReliabilityKind_t::RELIABLE;

    StatefulWriter &writer = m_statefulWriters[m_numStatefulWriters];
    if (!writer.init(attributes, TopicKind_t::NO_KEY, &m_threadPool, m_transport,
                     enforceUnicast))
      return nullptr;
    ++m_numStatefulWriters;
    return part.addWriter(&writer) ? &writer : nullptr;
  }
  else
  {
    attributes.reliabilityKind = ReliabilityKind_t::BEST_EFFORT;

    StatelessWriter &writer = m_statelessWriters[m_numStatelessWriters];
    if (!writer.init(attributes, TopicKind_t::NO_KEY, &m_threadPool, m_transport,
                     enforceUnicast))
      return nullptr;
    ++m_numStatelessWriters;
    return part.addWriter(&writer) ? &writer : nullptr;
  }
}

rtps::Reader *Domain::createReader(Participant &part, const char *topicName,
                                   const char *typeName, bool reliable,
                                   ip4_addr_t mcastaddress)
{
  if ((reliable && m_statefulReaders.size() <= m_numStatefulReaders) ||
      (!reliable && m_statelessReaders.size() <= m_numStatelessReaders) ||
      part.isReadersFull())
  {

    DOMAIN_LOG("No Reader created. Max Number of Readers reached.\n");

    return nullptr;
  }

  // TODO Distinguish WithKey and NoKey (Also changes EntityKind)
  TopicData attributes;

  if (strlen(topicName) > Config::MAX_TOPICNAME_LENGTH ||
      strlen(typeName) > Config::MAX_TYPENAME_LENGTH)
  {
    return nullptr;
  }
  strcpy(attributes.topicName, topicName);
  strcpy(attributes.typeName, typeName);
  attributes.endpointGuid.prefix = part.m_guidPrefix;
  attributes.endpointGuid.entityId = {
      part.getNextUserEntityKey(),
      EntityKind_t::USER_DEFINED_READER_WITHOUT_KEY};
  std::array<uint8_t, 4> currentIp{};
  if (!UdpDriver::getLocalIpAddress(currentIp) ||
      (currentIp[0] == 0 && currentIp[1] == 0 &&
       currentIp[2] == 0 && currentIp[3] == 0))
    return nullptr;
  attributes.unicastLocator =
      getUserUnicastLocator(part.m_participantId, currentIp);
  if (!isZeroAddress(mcastaddress))
  {
    if (ip4_addr_ismulticast(&mcastaddress))
    {
      attributes.multicastLocator = rtps::FullLengthLocator::createUDPv4Locator(
          ip4_addr1(&mcastaddress), ip4_addr2(&mcastaddress),
          ip4_addr3(&mcastaddress), ip4_addr4(&mcastaddress),
          getUserMulticastPort());
      m_transport.joinMultiCastGroup(
          attributes.multicastLocator.getIp4Address());
      registerMulticastPort(attributes.multicastLocator);

      DOMAIN_LOG("Multicast enabled!\n");
    }
    else
    {

      DOMAIN_LOG("This is not a Multicastaddress!\n");
    }
  }
  attributes.durabilityKind = DurabilityKind_t::VOLATILE;

  DOMAIN_LOG("Creating reader[%s, %s]\n", topicName, typeName);

  if (reliable)
  {
    if (m_numStatefulReaders == m_statefulReaders.size())
    {
      return nullptr;
    }

    attributes.reliabilityKind = ReliabilityKind_t::RELIABLE;

    StatefulReader &reader = m_statefulReaders[m_numStatefulReaders];
    reader.init(attributes, m_transport);
    if (!reader.isInitialized()) return nullptr;
    ++m_numStatefulReaders;
    return part.addReader(&reader) ? &reader : nullptr;
  }
  else
  {
    if (m_numStatelessReaders == m_statelessReaders.size())
    {
      return nullptr;
    }

    attributes.reliabilityKind = ReliabilityKind_t::BEST_EFFORT;

    StatelessReader &reader = m_statelessReaders[m_numStatelessReaders];
    reader.init(attributes);
    if (!reader.isInitialized()) return nullptr;
    ++m_numStatelessReaders;
    return part.addReader(&reader) ? &reader : nullptr;
  }
}

rtps::GuidPrefix_t Domain::generateGuidPrefix(ParticipantId_t id) const
{
  GuidPrefix_t prefix = Config::BASE_GUID_PREFIX;
#if defined(unix) || defined(__unix__)
  srand(time(nullptr));
#else
  unsigned int seed = (int)xTaskGetTickCount();
  std::array<uint8_t, 4> ip{};
  if (UdpDriver::getLocalIpAddress(ip))
  {
    seed ^= (static_cast<unsigned int>(ip[0]) << 24) |
            (static_cast<unsigned int>(ip[1]) << 16) |
            (static_cast<unsigned int>(ip[2]) << 8) |
            static_cast<unsigned int>(ip[3]);
  }
  srand(seed);
#endif
  for (auto i = 0; i < rtps::Config::BASE_GUID_PREFIX.id.size(); i++)
  {
    prefix.id[i] = (rand() % 256);
  }
  prefix.id[prefix.id.size() - 1] = *reinterpret_cast<uint8_t *>(&id);
  return prefix;
}
