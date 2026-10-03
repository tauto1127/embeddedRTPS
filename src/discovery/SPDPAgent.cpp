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

#include "rtps/discovery/SPDPAgent.h"
#include "lwip/sys.h"
#include "rtps/discovery/ParticipantProxyData.h"
#include "rtps/discovery/PreparedDiscovery.h"
#include "rtps/entities/Participant.h"
#include "rtps/communication/UdpDriver.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/Log.h"
#include "rtps/utils/udpUtils.h"
#include "netif_wasm_add.h"
#include <cstring>
#include <utility>

using rtps::SPDPAgent;
using rtps::SMElement::BuildInEndpointSet;
using rtps::SMElement::ParameterId;

SPDPAgent::~SPDPAgent()
{
  if (initialized)
  {
    sys_mutex_free(&m_mutex);
  }
}

bool SPDPAgent::init(Participant &participant, BuiltInEndpoints &endpoints)
{
  if (initialized || endpoints.spdpWriter == nullptr ||
      endpoints.spdpReader == nullptr || endpoints.sedpPubWriter == nullptr ||
      endpoints.sedpPubReader == nullptr || endpoints.sedpSubWriter == nullptr ||
      endpoints.sedpSubReader == nullptr)
  {
    return false;
  }
  if (sys_mutex_new(&m_mutex) != ERR_OK)
  {
    SPDP_LOG("Could not alloc mutex");
    return false;
  }
  std::array<uint8_t, 4> bytes{};
  if (!UdpDriver::getLocalIpAddress(bytes) ||
      (bytes[0] == 0 && bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 0))
  {
    sys_mutex_free(&m_mutex);
    return false;
  }
  ip4_addr_t current{};
  IP4_ADDR(&current, bytes[0], bytes[1], bytes[2], bytes[3]);
  const std::array<uint8_t, 4> ip{{ip4_addr1(&current), ip4_addr2(&current),
                                   ip4_addr3(&current), ip4_addr4(&current)}};
  mp_participant = &participant;
  m_buildInEndpoints = endpoints;
  m_buildInEndpoints.spdpReader->registerCallback(receiveCallback, this);

  ucdr_init_buffer(&m_microbuffer, m_outputBuffer.data(),
                   m_outputBuffer.size());
  // addInlineQos();
  if (!serializeParticipantParameters(m_microbuffer, ip))
  {
    sys_mutex_free(&m_mutex);
    return false;
  }
  initialized = true;
  return true;
}

void SPDPAgent::start()
{
  if (m_running || !initialized || mp_participant == nullptr ||
      !mp_participant->hasBuiltInEndpoints())
  {
    return;
  }
  m_running = true;
  sys_thread_new("SPDPThread", runBroadcast, this,
                 Config::SPDP_WRITER_STACKSIZE, Config::SPDP_WRITER_PRIO);
}

void SPDPAgent::stop() { m_running = false; }

bool SPDPAgent::refreshLocalIp()
{
  int refreshResult = NETIF_WASM_REFRESH_UNCHANGED;
  netif_wasm_ip_snapshot_t snapshot{};
#if defined(__wasi__)
  refreshResult = netif_wasm_refresh();
  if (refreshResult == NETIF_WASM_REFRESH_BUSY) return false;
  // FAILED may still use the last coherent valid snapshot, never fabricated IP.
  if (netif_wasm_get_ip_snapshot(&snapshot) != 0) return false;
#else
  std::array<uint8_t, 4> bytes{};
  if (!UdpDriver::getLocalIpAddress(bytes)) return false;
  ip4_addr_t current{};
  IP4_ADDR(&current, bytes[0], bytes[1], bytes[2], bytes[3]);
  snapshot.ip_addr = current.addr;
  snapshot.valid = 1;
#endif
  if (!snapshot.valid || snapshot.ip_addr == 0) return false;
  if (refreshResult == NETIF_WASM_REFRESH_FAILED)
    SPDP_LOG("Could not refresh local IP; projecting last valid snapshot\n");
  return mp_participant->m_sedpAgent.projectLocalIp(snapshot.ip_addr);
}

bool SPDPAgent::prepareAnnouncement(const std::array<uint8_t, 4> &ip,
                                    std::array<uint8_t, 400> &bytes,
                                    DataSize_t &length,
                                    PBufWrapper &payload) const
{
  ucdrBuffer temporary{};
  ucdr_init_buffer(&temporary, bytes.data(), bytes.size());
  if (!serializeParticipantParameters(temporary, ip) ||
      ucdr_buffer_has_error(&temporary)) return false;
  const uint32_t serializedLength = ucdr_buffer_length(&temporary);
  if (serializedLength == 0 || serializedLength > bytes.size() ||
      serializedLength > UINT16_MAX) return false;
  length = static_cast<DataSize_t>(serializedLength);
  return discovery_detail::preparePayload(bytes.data(), length, payload);
}

void SPDPAgent::commitPreparedAnnouncement(
    PBufWrapper &&payload, const std::array<uint8_t, 400> &bytes,
    DataSize_t length) noexcept
{
  m_buildInEndpoints.spdpWriter->replacePreparedDiscoveryAlive(
      std::move(payload));
  std::memcpy(m_outputBuffer.data(), bytes.data(), m_outputBuffer.size());
  ucdr_init_buffer_offset(&m_microbuffer, m_outputBuffer.data(),
                          m_outputBuffer.size(), length);
}

void SPDPAgent::notifyPreparedAnnouncement() noexcept
{
  m_buildInEndpoints.spdpWriter->notifyPreparedDiscovery();
}

// mros::initから呼び出し　PDPパケットを送信？
void SPDPAgent::runBroadcast(void *args)
{
  SPDPAgent &agent = *static_cast<SPDPAgent *>(args);
  // Startup has applied-IP sentinel zero, so use the same atomic projection
  // path as periodic refresh rather than admitting a second unchecked record.
  agent.refreshLocalIp();
  while (agent.m_running)
  {
#ifdef OS_IS_FREERTOS
    vTaskDelay(pdMS_TO_TICKS(Config::SPDP_RESEND_PERIOD_MS));
#else
    sys_msleep(Config::SPDP_RESEND_PERIOD_MS);
#endif
    agent.refreshLocalIp();
    // The latest-only prepared history is the sole periodic announcement.
    agent.m_buildInEndpoints.spdpWriter->setAllChangesToUnsent();
    if (agent.m_cycleHB == Config::SPDP_CYCLECOUNT_HEARTBEAT)
    {
      agent.m_cycleHB = 0;
      agent.mp_participant->checkAndResetHeartbeats();
    }
    else
    {
      agent.m_cycleHB++;
    }
  }
}

void SPDPAgent::receiveCallback(void *callee,
                                const ReaderCacheChange &cacheChange)
{
  auto agent = static_cast<SPDPAgent *>(callee);
  agent->handleSPDPPackage(cacheChange);
}

void SPDPAgent::handleSPDPPackage(const ReaderCacheChange &cacheChange)
{
  if (!initialized)
  {
    SPDP_LOG("Callback called without initialization\n");
    return;
  }

  Lock lock{m_mutex};
  if (cacheChange.size > m_inputBuffer.size())
  {
    SPDP_LOG("Input buffer to small\n");
    return;
  }

  // Something went wrong deserializing remote participant
  if (!cacheChange.copyInto(m_inputBuffer.data(), m_inputBuffer.size()))
  {
    return;
  }

  ucdrBuffer buffer;
  ucdr_init_buffer(&buffer, m_inputBuffer.data(), m_inputBuffer.size());

  if (cacheChange.kind == ChangeKind_t::ALIVE)
  {
    configureEndianessAndOptions(buffer);
    volatile bool success =
        m_proxyDataBuffer.readFromUcdrBuffer(buffer, mp_participant);
    if (success)
    {
      // TODO In case we store the history we can free the history mutex here
      processProxyData();
    }
    else
    {
      SPDP_LOG("ParticipantProxyData deserializtaion failed\n");
    }
  }
  else
  {
    // TODO RemoveParticipant
  }
}

void SPDPAgent::configureEndianessAndOptions(ucdrBuffer &buffer)
{
  std::array<uint8_t, 2> encapsulation{};
  // Endianess doesn't matter for this since those are single bytes
  ucdr_deserialize_array_uint8_t(&buffer, encapsulation.data(),
                                 encapsulation.size());
  if (encapsulation == SMElement::SCHEME_PL_CDR_LE)
  {
    buffer.endianness = UCDR_LITTLE_ENDIANNESS;
  }
  else
  {
    buffer.endianness = UCDR_BIG_ENDIANNESS;
  }
  // Reuse encapsulation buffer to skip options
  ucdr_deserialize_array_uint8_t(&buffer, encapsulation.data(),
                                 encapsulation.size());
}

void SPDPAgent::processProxyData()
{
  if (m_proxyDataBuffer.m_guid.prefix.id == mp_participant->m_guidPrefix.id)
  {
    return; // Our own packet
  }

  ParticipantProxyData remoteSnapshot{};
  const auto *remote_part = &remoteSnapshot;
  if (mp_participant->copyRemoteParticipant(m_proxyDataBuffer.m_guid.prefix,
                                          remoteSnapshot))
  {
    const auto locator_list_changed = [](const auto &old_list,
                                         const auto &new_list) {
      for (size_t i = 0; i < old_list.size(); ++i)
      {
        if (old_list[i].kind != new_list[i].kind ||
            old_list[i].port != new_list[i].port ||
            old_list[i].address != new_list[i].address)
        {
          return true;
        }
      }
      return false;
    };
    const bool metatraffic_locator_changed = locator_list_changed(
        remote_part->m_metatrafficUnicastLocatorList,
        m_proxyDataBuffer.m_metatrafficUnicastLocatorList);
    const bool default_locator_changed = locator_list_changed(
        remote_part->m_defaultUnicastLocatorList,
        m_proxyDataBuffer.m_defaultUnicastLocatorList);
    SPDP_LOG("Not adding remote participant guid.prefix = %u \n",
             (unsigned int)Guid_t::sum(remote_part->m_guid));

    if (metatraffic_locator_changed || default_locator_changed)
    {
      const GuidPrefix_t prefix = m_proxyDataBuffer.m_guid.prefix;
      mp_participant->removeRemoteParticipant(prefix);
      if (mp_participant->addNewRemoteParticipant(m_proxyDataBuffer))
      {
        addProxiesForBuiltInEndpoints();
        m_buildInEndpoints.spdpWriter->setAllChangesToUnsent();
      }
      return;
    }

    mp_participant->refreshRemoteParticipantLiveliness(
        m_proxyDataBuffer.m_guid.prefix);
    return; // Already in our list
  }

  if (mp_participant->addNewRemoteParticipant(m_proxyDataBuffer))
  {
    addProxiesForBuiltInEndpoints();
    m_buildInEndpoints.spdpWriter->setAllChangesToUnsent();
#if SPDP_VERBOSE && RTPS_GLOBAL_VERBOSE
    SPDP_LOG("Added new participant with guid: ");
    printGuidPrefix(m_proxyDataBuffer.m_guid.prefix);
  }
  else
  {
    SPDP_LOG("Failed to add new participant");
  }
#else
  }
  else
  {
    while (1)
    {
      SPDP_LOG("failed to add remote participant");
    }
  }
#endif
}

bool SPDPAgent::addProxiesForBuiltInEndpoints()
{

  LocatorIPv4 *locator = nullptr;

  // Check if the remote participants has a locator in our subnet
  for (unsigned int i = 0;
       i < m_proxyDataBuffer.m_metatrafficUnicastLocatorList.size(); i++)
  {
    LocatorIPv4 *l = &(m_proxyDataBuffer.m_metatrafficUnicastLocatorList[i]);
    if (l->isValid() && l->isSameSubnet())
    {
      locator = l;
      break;
    }
  }

  if (!locator)
  {
    return false;
  }

  ip4_addr_t ip4addr = locator->getIp4Address();
  const char *addr = ip4addr_ntoa(&ip4addr);
  SPDP_LOG("Adding IPv4 Locator %s\n", addr);

  if (m_proxyDataBuffer.hasPublicationWriter())
  {
    const WriterProxy proxy{{m_proxyDataBuffer.m_guid.prefix,
                             ENTITYID_SEDP_BUILTIN_PUBLICATIONS_WRITER},
                            *locator};
    m_buildInEndpoints.sedpPubReader->addNewMatchedWriter(proxy);
  }

  if (m_proxyDataBuffer.hasSubscriptionWriter())
  {
    const WriterProxy proxy{{m_proxyDataBuffer.m_guid.prefix,
                             ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_WRITER},
                            *locator};
    m_buildInEndpoints.sedpSubReader->addNewMatchedWriter(proxy);
  }

  if (m_proxyDataBuffer.hasPublicationReader())
  {
    const ReaderProxy proxy{{m_proxyDataBuffer.m_guid.prefix,
                             ENTITYID_SEDP_BUILTIN_PUBLICATIONS_READER},
                            *locator};
    m_buildInEndpoints.sedpPubWriter->addNewMatchedReader(proxy);
  }

  if (m_proxyDataBuffer.hasSubscriptionReader())
  {
    const ReaderProxy proxy{{m_proxyDataBuffer.m_guid.prefix,
                             ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_READER},
                            *locator};
    m_buildInEndpoints.sedpSubWriter->addNewMatchedReader(proxy);
  }

  return true;
}

void SPDPAgent::addInlineQos()
{
  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_KEY_HASH);
  ucdr_serialize_uint16_t(&m_microbuffer, 16);
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               mp_participant->m_guidPrefix.id.data(),
                               sizeof(GuidPrefix_t::id));
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               ENTITYID_BUILD_IN_PARTICIPANT.entityKey.data(),
                               sizeof(EntityId_t::entityKey));
  ucdr_serialize_uint8_t(
      &m_microbuffer,
      static_cast<uint8_t>(ENTITYID_BUILD_IN_PARTICIPANT.entityKind));

  endCurrentList();
}

void SPDPAgent::endCurrentList()
{
  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_SENTINEL);
  ucdr_serialize_uint16_t(&m_microbuffer, 0);
}

bool SPDPAgent::serializeParticipantParameters(ucdrBuffer &output,
    const std::array<uint8_t, 4> &ip) const
{
  // All writes below target the caller-owned buffer, never the live member.
  auto &m_microbuffer = output;
  const uint16_t zero_options = 0;
  const uint16_t protocolVersionSize =
      sizeof(PROTOCOLVERSION.major) + sizeof(PROTOCOLVERSION.minor);
  const uint16_t vendorIdSize = Config::VENDOR_ID.vendorId.size();
  const uint16_t locatorSize = sizeof(FullLengthLocator);
  const uint16_t durationSize =
      sizeof(Duration_t::seconds) + sizeof(Duration_t::fraction);
  const uint16_t entityKeySize = 3;
  const uint16_t entityKindSize = 1;
  const uint16_t entityIdSize = entityKeySize + entityKindSize;
  const uint16_t guidSize = sizeof(GuidPrefix_t::id) + entityIdSize;

  const FullLengthLocator userUniCastLocator =
      getUserUnicastLocator(mp_participant->m_participantId, ip);
  const FullLengthLocator builtInUniCastLocator =
      getBuiltInUnicastLocator(mp_participant->m_participantId, ip);
  const FullLengthLocator builtInMultiCastLocator =
      getBuiltInMulticastLocator();

  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               rtps::SMElement::SCHEME_PL_CDR_LE.data(),
                               rtps::SMElement::SCHEME_PL_CDR_LE.size());
  ucdr_serialize_uint16_t(&m_microbuffer, zero_options);

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_PROTOCOL_VERSION);
  ucdr_serialize_uint16_t(&m_microbuffer, protocolVersionSize + 2);
  ucdr_serialize_uint8_t(&m_microbuffer, PROTOCOLVERSION.major);
  ucdr_serialize_uint8_t(&m_microbuffer, PROTOCOLVERSION.minor);
  ucdr_serialize_uint16_t(&m_microbuffer, 0); // checked sticky padding

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_VENDORID);
  ucdr_serialize_uint16_t(&m_microbuffer, vendorIdSize + 2);
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               Config::VENDOR_ID.vendorId.data(), vendorIdSize);
  ucdr_serialize_uint16_t(&m_microbuffer, 0); // checked sticky padding

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_DEFAULT_UNICAST_LOCATOR);
  ucdr_serialize_uint16_t(&m_microbuffer, locatorSize);
  ucdr_serialize_array_uint8_t(
      &m_microbuffer, reinterpret_cast<const uint8_t *>(&userUniCastLocator),
      locatorSize);

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_METATRAFFIC_UNICAST_LOCATOR);
  ucdr_serialize_uint16_t(&m_microbuffer, locatorSize);
  ucdr_serialize_array_uint8_t(
      &m_microbuffer, reinterpret_cast<const uint8_t *>(&builtInUniCastLocator),
      locatorSize);

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_METATRAFFIC_MULTICAST_LOCATOR);
  ucdr_serialize_uint16_t(&m_microbuffer, locatorSize);
  ucdr_serialize_array_uint8_t(
      &m_microbuffer,
      reinterpret_cast<const uint8_t *>(&builtInMultiCastLocator), locatorSize);

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_PARTICIPANT_LEASE_DURATION);
  ucdr_serialize_uint16_t(&m_microbuffer, durationSize);
  ucdr_serialize_int32_t(&m_microbuffer,
                         Config::SPDP_DEFAULT_REMOTE_LEASE_DURATION.seconds);
  ucdr_serialize_uint32_t(&m_microbuffer,
                          Config::SPDP_DEFAULT_REMOTE_LEASE_DURATION.fraction);

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_PARTICIPANT_GUID);
  ucdr_serialize_uint16_t(&m_microbuffer, guidSize);
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               mp_participant->m_guidPrefix.id.data(),
                               sizeof(GuidPrefix_t::id));
  ucdr_serialize_array_uint8_t(&m_microbuffer,
                               ENTITYID_BUILD_IN_PARTICIPANT.entityKey.data(),
                               entityKeySize);
  ucdr_serialize_uint8_t(
      &m_microbuffer,
      static_cast<uint8_t>(ENTITYID_BUILD_IN_PARTICIPANT.entityKind));

  ucdr_serialize_uint16_t(&m_microbuffer,
                          ParameterId::PID_BUILTIN_ENDPOINT_SET);
  ucdr_serialize_uint16_t(&m_microbuffer, sizeof(BuildInEndpointSet));
  ucdr_serialize_uint32_t(
      &m_microbuffer, BuildInEndpointSet::DISC_BIE_PARTICIPANT_ANNOUNCER |
                          BuildInEndpointSet::DISC_BIE_PARTICIPANT_DETECTOR |
                          BuildInEndpointSet::DISC_BIE_PUBLICATION_ANNOUNCER |
                          BuildInEndpointSet::DISC_BIE_PUBLICATION_DETECTOR |
                          BuildInEndpointSet::DISC_BIE_SUBSCRIPTION_ANNOUNCER |
                          BuildInEndpointSet::DISC_BIE_SUBSCRIPTION_DETECTOR);

  ucdr_serialize_uint16_t(&m_microbuffer, ParameterId::PID_SENTINEL);
  ucdr_serialize_uint16_t(&m_microbuffer, 0);
  return !ucdr_buffer_has_error(&m_microbuffer);
}

#undef SPDP_VERBOSE
