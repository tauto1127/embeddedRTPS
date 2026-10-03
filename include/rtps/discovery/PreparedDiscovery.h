#ifndef RTPS_PREPARED_DISCOVERY_H
#define RTPS_PREPARED_DISCOVERY_H

// Internal discovery preparation only. No history, publication or queue effects.
#include "rtps/discovery/TopicData.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/storages/PBufWrapper.h"
#include "ucdr/microcdr.h"
#include <array>
#include <cstring>

namespace rtps {
namespace discovery_detail {
inline bool preparePayload(const uint8_t *bytes, DataSize_t length,
                           PBufWrapper &owned) {
  return length != 0 && owned.reserve(length) && owned.isValid() &&
         owned.append(bytes, length) && owned.spaceUsed() == length;
}

inline bool prepareTopic(const TopicData &attributes,
                         const FullLengthLocator &locator,
                         PBufWrapper &owned) {
  // Immutable names are bounded before the existing serializer's strlen.
  if (std::memchr(attributes.topicName, '\0', sizeof(attributes.topicName)) == nullptr ||
      std::memchr(attributes.typeName, '\0', sizeof(attributes.typeName)) == nullptr)
    return false;
  TopicData temporary = attributes;
  temporary.unicastLocator = locator;
  std::array<uint8_t, 600> bytes{};
  ucdrBuffer cdr{};
  ucdr_init_buffer(&cdr, bytes.data(), bytes.size());
  const bool serialized =
      ucdr_serialize_array_uint8_t(&cdr, SMElement::SCHEME_PL_CDR_LE.data(),
                                  SMElement::SCHEME_PL_CDR_LE.size()) &&
      ucdr_serialize_uint16_t(&cdr, 0) &&
      temporary.serializeIntoUcdrBuffer(cdr);
  if (!serialized || ucdr_buffer_has_error(&cdr)) return false;
  const auto length = ucdr_buffer_length(&cdr);
  if (length == 0 || length > bytes.size()) return false;
  return preparePayload(bytes.data(), static_cast<DataSize_t>(length), owned);
}
} // namespace discovery_detail
} // namespace rtps
#endif
