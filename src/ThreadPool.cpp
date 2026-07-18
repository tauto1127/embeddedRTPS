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

#include "rtps/ThreadPool.h"

#include "lwip/tcpip.h"
#include "rtps/entities/Writer.h"
#include "rtps/utils/Log.h"
#include "rtps/utils/udpUtils.h"

using rtps::ThreadPool;

namespace {

bool containsDataFrag(const uint8_t *data, size_t size) {
  // RTPS header is 20 bytes. Parse only enough submessage headers to identify
  // DATA_FRAG (0x16); malformed packets are left to the normal RTPS parser.
  constexpr size_t kRtpHeaderSize = 20;
  constexpr uint8_t kDataFrag = 0x16;
  if (data == nullptr || size <= kRtpHeaderSize) {
    return false;
  }

  size_t offset = kRtpHeaderSize;
  while (offset + 4 <= size) {
    const uint8_t id = data[offset];
    const uint8_t flags = data[offset + 1];
    const uint16_t payload_size = (flags & 0x01) != 0
                                      ? static_cast<uint16_t>(data[offset + 2]) |
                                            (static_cast<uint16_t>(data[offset + 3]) << 8)
                                      : (static_cast<uint16_t>(data[offset + 2]) << 8) |
                                            static_cast<uint16_t>(data[offset + 3]);
    if (id == kDataFrag) {
      return true;
    }
    if (payload_size == 0 || offset + 4 + payload_size > size) {
      return false;
    }
    offset += 4 + payload_size;
  }
  return false;
}

} // namespace

#define THREAD_POOL_VERBOSE 0
#if THREAD_POOL_VERBOSE && RTPS_GLOBAL_VERBOSE
#define THREAD_POOL_LOG(...)
if (true) {
  printf("[ThreadPool] ");
  printf(__VA_ARGS__);
  printf("\n");
}
#else
#define THREAD_POOL_LOG(...) //
#endif

ThreadPool::ThreadPool(receiveJumppad_fp receiveCallback, void *callee)
    : m_receiveJumppad(receiveCallback), m_callee(callee) {

  if (!m_queueOutgoing.init() || !m_queueIncoming.init()) {
    return;
  }
  err_t inputErr = sys_sem_new(&m_readerNotificationSem, 0);
  err_t outputErr = sys_sem_new(&m_writerNotificationSem, 0);

  if (inputErr != ERR_OK || outputErr != ERR_OK) {
    THREAD_POOL_LOG("ThreadPool: Failed to create Semaphores.\n");
  }
}

ThreadPool::~ThreadPool() {
  if (m_running) {
    stopThreads();
    sys_msleep(500);
  }

  if (sys_sem_valid(&m_readerNotificationSem)) {
    sys_sem_free(&m_readerNotificationSem);
  }
  if (sys_sem_valid(&m_writerNotificationSem)) {
    sys_sem_free(&m_writerNotificationSem);
  }
}

bool ThreadPool::startThreads() {
  if (m_running) {
    return true;
  }
  if (!sys_sem_valid(&m_readerNotificationSem) ||
      !sys_sem_valid(&m_writerNotificationSem)) {
    return false;
  }

  m_running = true;
  for (auto &thread : m_writers) {
    // TODO ID, err check, waitOnStop
    thread = sys_thread_new("WriterThread", writerThreadFunction, this,
                            Config::THREAD_POOL_WRITER_STACKSIZE,
                            Config::THREAD_POOL_WRITER_PRIO);
  }

  for (auto &thread : m_readers) {
    // TODO ID, err check, waitOnStop
    thread = sys_thread_new("ReaderThread", readerThreadFunction, this,
                            Config::THREAD_POOL_READER_STACKSIZE,
                            Config::THREAD_POOL_READER_PRIO);
  }
  return true;
}

void ThreadPool::stopThreads() {
  m_running = false;
  // This should call all the semaphores for each thread once, so they don't
  // stuck before ended.
  for (auto &thread : m_writers) {
    sys_sem_signal(&m_writerNotificationSem);
    sys_msleep(10);
  }
  for (auto &thread : m_readers) {
    sys_sem_signal(&m_readerNotificationSem);
    sys_msleep(10);
  }
  // TODO make sure they have finished. Seems to be sufficient for tests.
  // Not sufficient if threads shall actually be stopped during runtime.
  sys_msleep(10);
}

void ThreadPool::clearQueues() {
  m_queueOutgoing.clear();
  m_queueIncoming.clear();
}

bool ThreadPool::addWorkload(Writer *workload) {
  bool res = m_queueOutgoing.moveElementIntoBuffer(std::move(workload));
  if (res) {
    sys_sem_signal(&m_writerNotificationSem);
  } else {
    printf("[RTPS TX] DROP: outgoing writer-work queue is full\n");
  }

  return res;
}

bool ThreadPool::addNewPacket(PacketInfo &&packet) {
  bool res = m_queueIncoming.moveElementIntoBuffer(std::move(packet));
  if (res) {
    sys_sem_signal(&m_readerNotificationSem);
  }
  return res;
}

void ThreadPool::writerThreadFunction(void *arg) {
  auto pool = static_cast<ThreadPool *>(arg);
  if (pool == nullptr) {

    THREAD_POOL_LOG("nullptr passed to writer function\n");

    return;
  }

  pool->doWriterWork();
}

void ThreadPool::doWriterWork() {
  while (m_running) {
    Writer *workload;
    auto isWorkToDo = m_queueOutgoing.moveFirstInto(workload);
    if (!isWorkToDo) {
      sys_sem_wait(&m_writerNotificationSem);
      continue;
    }

    workload->progress();
  }
}

void ThreadPool::readCallback(void *args, udp_pcb *target, pbuf *pbuf,
                              const ip_addr_t * /*addr*/, Ip4Port_t port) {
  auto &pool = *static_cast<ThreadPool *>(args);

  PacketInfo packet;

  // Domain::receiveCallback consumes packet.buffer.firstElement->payload/len,
  // therefore it requires one contiguous pbuf.  PBUF_POOL is unsuitable for
  // coalescing: allocating a frame larger than PBUF_POOL_BUFSIZE simply creates
  // another chain and only its first fragment was subsequently parsed.  Large
  // DDS service replies (e.g. Cartographer SubmapQuery textures) were thus
  // silently truncated before RTPS deserialization.
  if (pbuf->next != nullptr) {
    const u16_t packet_length = pbuf->tot_len;
    printf("[RTPS RX] coalescing chained UDP packet: local_port=%u remote_port=%u bytes=%u\n",
           target->local_port, port, packet_length);
    struct pbuf *test = pbuf_alloc(PBUF_RAW, packet_length, PBUF_RAM);
    if (test == nullptr) {
      printf("[RTPS RX] DROP: cannot allocate contiguous buffer: bytes=%u\n",
             packet_length);
      pbuf_free(pbuf);
      return;
    }
    if (pbuf_copy(test, pbuf) != ERR_OK) {
      printf("[RTPS RX] DROP: cannot copy chained UDP packet: bytes=%u\n",
             packet_length);
      pbuf_free(test);
      pbuf_free(pbuf);
      return;
    }
    pbuf_free(pbuf);
    pbuf = test;
    printf("[RTPS RX] coalesced UDP packet: bytes=%u\n", packet_length);
  }

  packet.destAddr = {0}; // not relevant
  packet.destPort = target->local_port;
  packet.srcPort = port;
  packet.buffer = PBufWrapper{pbuf};
  const u16_t packet_length = pbuf->tot_len;
  const Ip4Port_t local_port = target->local_port;

  if (containsDataFrag(static_cast<const uint8_t *>(pbuf->payload),
                       packet_length)) {
    printf("[RTPS RX] DATA_FRAG: local_port=%u remote_port=%u bytes=%u\n",
           local_port, port, packet_length);
  }

  if (!pool.addNewPacket(std::move(packet))) {
    printf("[RTPS RX] DROP: incoming RTPS queue is full: local_port=%u remote_port=%u bytes=%u\n",
           local_port, port, packet_length);
  }
}

void ThreadPool::readerThreadFunction(void *arg) {
  auto pool = static_cast<ThreadPool *>(arg);
  if (pool == nullptr) {

    THREAD_POOL_LOG("nullptr passed to reader function\n");

    return;
  }
  pool->doReaderWork();
}

void ThreadPool::doReaderWork() {

  while (m_running) {
    PacketInfo packet;
    auto isWorkToDo = m_queueIncoming.moveFirstInto(packet);
    if (!isWorkToDo) {
      sys_sem_wait(&m_readerNotificationSem);
      continue;
    }

    m_receiveJumppad(m_callee, const_cast<const PacketInfo &>(packet));
  }
}

#undef THREAD_POOL_VERBOSE
