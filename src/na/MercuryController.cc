/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* ================================================================ */
/*                                                                  */
/* Licensed Materials - Property of IBM                             */
/*                                                                  */
/* Blue Gene/Q                                                      */
/*                                                                  */
/* (C) Copyright IBM Corp.  2011, 2012                              */
/*                                                                  */
/* US Government Users Restricted Rights -                          */
/* Use, duplication or disclosure restricted                        */
/* by GSA ADP Schedule Contract with IBM Corp.                      */
/*                                                                  */
/* This software is available to you under the                      */
/* Eclipse Public License (EPL).                                    */
/*                                                                  */
/* ================================================================ */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */

//! \file  MercuryController.cc
//! \brief Methods for bgcios::stdio::MercuryController class.

// Includes
#include "MercuryController.h"
#include <ramdisk/include/services/common/RdmaError.h>
#include <ramdisk/include/services/common/RdmaDevice.h>
#include <ramdisk/include/services/common/RdmaCompletionQueue.h>
#include <ramdisk/include/services/common/logging.h>
#include <ramdisk/include/services/MessageUtility.h>
#include <ramdisk/include/services/ServicesConstants.h>
#include <poll.h>
#include <errno.h>
#include <iomanip>
#include <sstream>
#include <queue>
#include <stdio.h>
#include <thread>

#include "cscs_messages.h"

using namespace bgcios::stdio;

LOG_DECLARE_FILE("jb");

/*
#undef LOG_CIOS_DEBUG_MSG
#undef LOG_DEBUG_MSG
#define LOG_CIOS_DEBUG_MSG(x) LOG_CIOS_DEBUG_MSG(x);
#define LOG_DEBUG_MSG(x) LOG_CIOS_DEBUG_MSG(x);
*/

const uint64_t LargeRegionSize  = 8192;
/*---------------------------------------------------------------------------*/
MercuryController::MercuryController(const char *device, const char *interface, int port)
{
  this->_device = device;
  this->_interface = interface;
  this->_port = port;
 }
/*---------------------------------------------------------------------------*/
MercuryController::~MercuryController()
{
  LOG_CIOS_DEBUG_MSG("MercuryController destructor clearing clients");
  _dequeUnexpectedInClient.clear();
  _dequeExpectedInClient.clear();
  _clients.clear();
  LOG_CIOS_DEBUG_MSG("MercuryController destructor closing server");
  this->_rdmaListener.reset();
  LOG_CIOS_DEBUG_MSG("MercuryController destructor closing regions");
  this->_largeRegion.reset();
  this->_protectionDomain.reset();
  this->_completionChannel.reset();
  LOG_CIOS_DEBUG_MSG("MercuryController destructor done");
}

/*---------------------------------------------------------------------------*/
int MercuryController::startup()
{
   // Find the address of the I/O link device.
   RdmaDevicePtr linkDevice;
   try {
      LOG_CIOS_DEBUG_MSG("creating InfiniBand device for " << _device << " using interface " << _interface);
      linkDevice = RdmaDevicePtr(new RdmaDevice(_device, _interface ));
   }
   catch (bgcios::RdmaError& e) {
      LOG_ERROR_MSG("error opening InfiniBand device: " << e.what());
      return e.errcode();
   }
   LOG_CIOS_DEBUG_MSG("created InfiniBand device for " << linkDevice->getDeviceName() << " using interface " << linkDevice->getInterfaceName());

   in_addr_t addr2 = linkDevice->getAddress();
   LOG_CIOS_DEBUG_MSG("Device returns IP address "
           << (int)((uint8_t*)&addr2)[0] << "."
           << (int)((uint8_t*)&addr2)[1] << "."
           << (int)((uint8_t*)&addr2)[2] << "."
           << (int)((uint8_t*)&addr2)[3] << "."
       );

   // Create listener for RDMA connections.
   try {
      _rdmaListener = bgcios::RdmaServerPtr(new bgcios::RdmaServer(linkDevice->getAddress(), this->_port));
      if (_rdmaListener->getLocalPort()!=this->_port) {
        this->_port = _rdmaListener->getLocalPort();
        LOG_CIOS_DEBUG_MSG("RdmaServer port changed to " << this->_port);
      }
   }
   catch (bgcios::RdmaError& e) {
      LOG_ERROR_MSG("error creating listening RDMA connection: " << e.what());
      return e.errcode();
   }

   in_addr_t addr = linkDevice->getAddress();
   LOG_CIOS_DEBUG_MSG("created listening RDMA connection on port " << this->_port << " using address " << linkDevice->getAddress()
       << "\t IP address "
       << (int)((uint8_t*)&addr)[0] << "."
       << (int)((uint8_t*)&addr)[1] << "."
       << (int)((uint8_t*)&addr)[2] << "."
       << (int)((uint8_t*)&addr)[3] << "."
   );

   // Create a protection domain object.
   try {
      _protectionDomain = RdmaProtectionDomainPtr(new RdmaProtectionDomain(_rdmaListener->getContext()));
   }
   catch (bgcios::RdmaError& e) {
      LOG_ERROR_MSG("error allocating protection domain: " << e.what());
      return e.errcode();
   }
   LOG_CIOS_DEBUG_MSG("created protection domain " << _protectionDomain->getHandle());

   // Create a completion channel object.
   try {
      _completionChannel = RdmaCompletionChannelPtr(new RdmaCompletionChannel(_rdmaListener->getContext(), false));
   }
   catch (bgcios::RdmaError& e) {
      LOG_ERROR_MSG("error constructing completion channel: " << e.what());
      return e.errcode();
   }
   LOG_CIOS_DEBUG_MSG("created completion channel using fd " << _completionChannel->getChannelFd());

   // Listen for connections.
   int err = _rdmaListener->listen(256);
   if (err != 0) {
      LOG_ERROR_MSG("error listening for new RDMA connections: " << bgcios::errorString(err));
      return err;
   }
   LOG_CIOS_DEBUG_MSG("listening for new RDMA connections on fd " << _rdmaListener->getEventChannelFd());

   // Create a large memory region.
   _largeRegion = RdmaMemoryRegionPtr(new RdmaMemoryRegion());
   err = _largeRegion->allocate(_protectionDomain, LargeRegionSize);
   if (err != 0)
   {
       LOG_ERROR_MSG("error allocating large memory region: " << bgcios::errorString(err));
       return err;
   }
   LOG_CIOS_DEBUG_MSG("created large memory region with local key " << _largeRegion->getLocalKey());

   return 0;
}
/*---------------------------------------------------------------------------*/
int MercuryController::cleanup(void)
{
  return 0;
}

/*---------------------------------------------------------------------------*/
void MercuryController::eventMonitor(int Nevents)
{
  const int eventChannel = 0;
  const int compChannel  = 1;
  const int numFds       = 2;
  //
  bool _done = false;

  pollfd pollInfo[numFds];
  int polltimeout = 0; // seconds*1000; // 10000 == 10 sec

  pollInfo[eventChannel].fd = _rdmaListener->getEventChannelFd();
  pollInfo[eventChannel].events = POLLIN;
  pollInfo[eventChannel].revents = 0;

  pollInfo[compChannel].fd = _completionChannel->getChannelFd();
  pollInfo[compChannel].events = POLLIN;
  pollInfo[compChannel].revents = 0;

  // Process events until told to stop - or timeout.
  while (!_done)
  {
    pollInfo[eventChannel].revents = 0;
    pollInfo[compChannel].revents = 0;

    // Wait for an event on one of the descriptors.
    int rc = poll(pollInfo, numFds, polltimeout);

    // If there were no events/messages
    if (rc == 0) {
      // if we were told to wait for at least one event, retry
      if (Nevents>0) continue;
      // otherwise, leave
      else break;
    }
    LOG_CIOS_TRACE_MSG("A channel has received an event/message " << std::this_thread::get_id() );

    // There was an error so log the failure and try again.
    if (rc == -1)
    {
      int err = errno;
      if (err == EINTR)
      {
        LOG_CIOS_TRACE_MSG("poll returned EINTR, continuing ...");
        continue;
      }
      LOG_ERROR_MSG("error polling socket descriptors: " << bgcios::errorString(err));
      return;
    }

    // Check for an event on the event channel.
    if (pollInfo[eventChannel].revents & POLLIN)
    {
      LOG_CIOS_TRACE_MSG("input event available on event channel");
      eventChannelHandler();
      pollInfo[eventChannel].revents = 0;
      Nevents--;
    }

    // Check for an event on the completion channel.
    if (pollInfo[compChannel].revents & POLLIN)
    {
      LOG_CIOS_TRACE_MSG("input event available on data channel");
      completionChannelHandler(0);
      pollInfo[compChannel].revents = 0;
      Nevents--;
    }

    if (Nevents<=0)
    {
      _done = true;
    }
  }
}

/*---------------------------------------------------------------------------*/
void MercuryController::eventChannelHandler(void)
{
   int err;

   // Wait for the event (it should be here now).
   err = _rdmaListener->waitForEvent();

   if (err != 0) {
      return;
   }

   // Handle the event.
   rdma_cm_event_type type = _rdmaListener->getEventType();

   switch (type) {

      case RDMA_CM_EVENT_CONNECT_REQUEST:
      {
         printf("RDMA_CM_EVENT_CONNECT_REQUEST in event channel handler\n");
         // Construct a RdmaCompletionQueue object for the new client.
         RdmaCompletionQueuePtr completionQ;
         try {
             completionQ = RdmaCompletionQueuePtr(new RdmaCompletionQueue(_rdmaListener->getEventContext(), RdmaCompletionQueue::MaxQueueSize, _completionChannel->getChannel()));
         }
         catch (bgcios::RdmaError& e) {
            LOG_ERROR_MSG("error creating completion queue: " << e.what());
            return;
         }

         // Construct a new RdmaClient object for the new client.
         RdmaClientPtr client;
         try {
             client = RdmaClientPtr(new RdmaClient(_rdmaListener->getEventId(), _protectionDomain, completionQ));
         }
         catch (bgcios::RdmaError& e) {
           LOG_ERROR_MSG("error creating rdma client: %s\n" << e.what());
           completionQ.reset();
           return;
         }

         printf("qpnum = %d\n", client->getQpNum());
         // Add new client to map of active clients.
         _clients.add(client->getQpNum(), client);

         // Add completion queue to completion channel.
         _completionChannel->addCompletionQ(completionQ);

         // Post a receive to get the first message.
         client->postRecvMessage();
         // we need the wr_id that the request corresponds to
         std::pair<uint32_t,uint64_t> temp = std::make_pair(client->getQpNum(), client->getLastPostRecvKey());
         LOG_DEBUG_MSG("New connection Pre-posted an unexpected receive with wr_id " << temp.second);

         // Accept the connection from the new client.
         err = client->accept();
         if (err != 0) {
             printf("error accepting client connection: %s\n", bgcios::errorString(err));
            _clients.remove(client->getQpNum());
            _completionChannel->removeCompletionQ(completionQ);
            client->reject(); // Tell client the bad news
            client.reset();
            completionQ.reset();
            break;
         }
         LOG_DEBUG_MSG("accepted connection from " << client->getRemoteAddressString().c_str());

         break;
      }

      case RDMA_CM_EVENT_ESTABLISHED:
      {
         printf("RDMA_CM_EVENT_ESTABLISHED\n");
         // Find connection associated with this event.
         RdmaClientPtr client = _clients.get(_rdmaListener->getEventQpNum());
         LOG_CIOS_INFO_MSG(client->getTag() << "connection established with " << client->getRemoteAddressString());

         break;
      }

      case RDMA_CM_EVENT_DISCONNECTED:
      {
         printf("RDMA_CM_EVENT_DISCONNECTED\n");
         // Find connection associated with this event.
         uint32_t qp = _rdmaListener->getEventQpNum();
         RdmaClientPtr client = _clients.get(qp);
         RdmaCompletionQueuePtr completionQ = client->getCompletionQ();
         // Complete disconnect initiated by peer.
         err = client->disconnect(false);
         if (err == 0) {
            LOG_CIOS_INFO_MSG(client->getTag() << "disconnected from " << client->getRemoteAddressString());
         }
         else {
            LOG_ERROR_MSG(client->getTag() << "error disconnecting from peer: " << bgcios::errorString(err));
         }

         // Acknowledge the event (must be done before removing the rdma cm id).
         _rdmaListener->ackEvent();

         // Remove connection from map of active connections.
         _clients.remove(qp);

         // Destroy connection object.
         LOG_CIOS_DEBUG_MSG("destroying RDMA connection to client " << client->getRemoteAddressString());
         client.reset();

         // Remove completion queue from the completion channel.
         _completionChannel->removeCompletionQ(completionQ);

         // Destroy the completion queue.
         LOG_CIOS_DEBUG_MSG("destroying completion queue " << completionQ->getHandle());
         completionQ.reset();

         break;
      }

      default:
      {
          printf("RDMA event: %s is not supported\n", rdma_event_str(type));
          break;
      }
   }

   // Acknowledge the event.  Should this always be done?
   if (type != RDMA_CM_EVENT_DISCONNECTED) {
      _rdmaListener->ackEvent();
   }

   return;
}

/*---------------------------------------------------------------------------*/
bool MercuryController::completionChannelHandler(uint64_t requestId)
{
  bool rc = false;
  uint64_t* ptr;
  uint32_t rdma_rkey;
  uint32_t rdma_len;
  uint64_t rdma_addr;
  uint32_t rdma_err;
  char     *Message_text;
  char     *Data_text;
  ErrorAckMessage *outMsg;
  RdmaClientPtr client;
  try {
    // Get the notification event from the completion channel.
    RdmaCompletionQueuePtr completionQ = _completionChannel->getEvent();

    // Remove work completions from the completion queue until it is empty.
    while (completionQ->removeCompletions() != 0) {

      // Get the next work completion.
      struct ibv_wc *completion = completionQ->popCompletion();
      LOG_DEBUG_MSG("Removing wr_id " << completion->wr_id);
      // Find the connection that received the message.
      client = _clients.get(completion->qp_num);

      if (this->_completionFunction) {
        printf("* * * Calling completion function\n\n");
        this->_completionFunction(completion, client);
        printf("* * * Finished Calling completion function\n\n");
      }
    }
    printf("finished completionChannelHandler\n");
  }

  catch (const RdmaError& e) {
    LOG_ERROR_MSG("error removing work completions from completion queue: " << bgcios::errorString(e.errcode()));
  }

  return rc;
}

/*---------------------------------------------------------------------------*/
bool MercuryController::addUnexpectedMsg(const RdmaClientPtr & client, uint32_t qp_id)
{
  // Get pointer to inbound WriteStdio message.
  CSCS_user_message::UserRDMA_message *inMsg = (CSCS_user_message::UserRDMA_message *)client->getInboundMessagePtr();

  /*
   // Build WriteStdioAck message in outbound message region.
   WriteStdioAckMessage *outMsg = (WriteStdioAckMessage *)client->getOutboundMessagePtr();
   memcpy(&(outMsg->header), &(inMsg->header), sizeof(MessageHeader));
   outMsg->header.type = inMsg->header.type == WriteStdout ? WriteStdoutAck : WriteStderrAck;
   outMsg->header.length = sizeof(WriteStdioAckMessage);
   client->setOutboundMessageLength(outMsg->header.length);
   */
  // Validate the job id.
  //   const JobPtr& job = _jobs.get(inMsg->header.jobId);

  _dequeUnexpectedInClient.push_back(ClientMapPair(qp_id,client));
  return true;
}

/*---------------------------------------------------------------------------*/
bool MercuryController::addExpectedMsg(const RdmaClientPtr & client, uint32_t qp_id)
{
  // Get pointer to inbound WriteStdio message.
  CSCS_user_message::UserRDMA_message *inMsg = (CSCS_user_message::UserRDMA_message *)client->getInboundMessagePtr();

  /*
   // Build WriteStdioAck message in outbound message region.
   WriteStdioAckMessage *outMsg = (WriteStdioAckMessage *)client->getOutboundMessagePtr();
   memcpy(&(outMsg->header), &(inMsg->header), sizeof(MessageHeader));
   outMsg->header.type = inMsg->header.type == WriteStdout ? WriteStdoutAck : WriteStderrAck;
   outMsg->header.length = sizeof(WriteStdioAckMessage);
   client->setOutboundMessageLength(outMsg->header.length);
   */
  // Validate the job id.
  //   const JobPtr& job = _jobs.get(inMsg->header.jobId);

  _dequeExpectedInClient.push_back(ClientMapPair(qp_id,client));
  return true;
}

/*---------------------------------------------------------------------------*/
bool MercuryController::fetchUnexpectedMsg(void *buf, uint64_t buf_size, uint32_t &qp_id)
{
  RdmaClientPtr client;
  if (!_dequeUnexpectedInClient.empty() )
  {
    client = _dequeUnexpectedInClient.front().second;
    qp_id = _dequeUnexpectedInClient.front().first;
    _dequeUnexpectedInClient.pop_front();
  }
  else
  {
    throw std::runtime_error("Nothing to receive in unexpected queue");
    return false;
  }
  CSCS_user_message::UserRDMA_message *inMsg = (CSCS_user_message::UserRDMA_message *)client->getInboundMessagePtr();
//  if (buf_size>inMsg->header2.cnk_bytes) throw std::runtime_error("Not enough space for message in UserRDMA msg");
  //
//  memcpy(buf, inMsg->MessageData, inMsg->header2.cnk_bytes);

  return true;
}

/*---------------------------------------------------------------------------*/
bool MercuryController::fetchExpectedMsg(void *buf, uint64_t buf_size, uint32_t &qp_id)
{
  RdmaClientPtr client;
  if (!_dequeExpectedInClient.empty() )
  {
    client = _dequeExpectedInClient.front().second;
    qp_id = _dequeExpectedInClient.front().first;
    _dequeExpectedInClient.pop_front();
  }
  else
  {
    return false;
  }
  CSCS_user_message::UserRDMA_message *inMsg = (CSCS_user_message::UserRDMA_message *)client->getInboundMessagePtr();
//  if (buf_size>inMsg->header2.cnk_bytes) throw std::runtime_error("Not enough space for message in UserRDMA msg");
  //
//  memcpy(buf, inMsg->MessageData, inMsg->header2.cnk_bytes);

  return true;
}

