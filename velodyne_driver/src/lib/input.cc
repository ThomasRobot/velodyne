/*
 *  Copyright (C) 2007 Austin Robot Technology, Patrick Beeson
 *  Copyright (C) 2009, 2010 Austin Robot Technology, Jack O'Quin
 *  Copyright (C) 2015, Jack O'Quin
 *
 *  License: Modified BSD Software License Agreement
 *
 *  $Id$
 */

/** \file
 *
 *  Input classes for the Velodyne HDL-64E 3D LIDAR:
 *
 *     Input -- base class used to access the data independently of
 *              its source
 *
 *     InputSocket -- derived class reads live data from the device
 *              via a UDP socket
 *
 *     InputPCAP -- derived class provides a similar interface from a
 *              PCAP dump
 */

#include <unistd.h>
#include <string>
#include <sstream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <poll.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/file.h>
#include <boost/thread.hpp>
#include <velodyne_driver/input.h>

namespace velodyne_driver
{
  static const size_t packet_size =
    sizeof(velodyne_msgs::VelodynePacket().data);

  static const size_t position_packet_size = 512;

  ////////////////////////////////////////////////////////////////////////
  // Input base class implementation
  ////////////////////////////////////////////////////////////////////////

  /** @brief constructor
   *
   *  @param private_nh ROS private handle for calling node.
   *  @param port UDP port number.
   */
  Input::Input(ros::NodeHandle private_nh, uint16_t port):
    private_nh_(private_nh),
    port_(port)
  {
    private_nh.param("device_ip", devip_str_, std::string(""));
    if (!devip_str_.empty())
      ROS_INFO_STREAM("Only accepting packets from IP address: "
                      << devip_str_);
  }

  ////////////////////////////////////////////////////////////////////////
  // InputSocket class implementation
  ////////////////////////////////////////////////////////////////////////

  /** @brief constructor
   *
   *  @param private_nh ROS private handle for calling node.
   *  @param port UDP port number
   */
  InputSocket::InputSocket(ros::NodeHandle private_nh, uint16_t port, uint16_t port2):
    Input(private_nh, port)
  {
    sockfd_ = -1;
    sockfd2_ = -1;
    
    if (!devip_str_.empty()) {
      inet_aton(devip_str_.c_str(),&devip_);
    }    

    // connect to Velodyne UDP port
    ROS_INFO_STREAM("Opening UDP socket: port " << port);
    sockfd_ = socket(PF_INET, SOCK_DGRAM, 0);
    if (sockfd_ == -1)
      {
        perror("socket");               // TODO: ROS_ERROR errno
        return;
      }

    sockfd2_ = socket(PF_INET, SOCK_DGRAM, 0);
    if (sockfd2_ == -1)
      {
        perror("socket2");               // TODO: ROS_ERROR errno
        return;
      }
  
    sockaddr_in my_addr;                     // my address information
    memset(&my_addr, 0, sizeof(my_addr));    // initialize to zeros
    my_addr.sin_family = AF_INET;            // host byte order
    my_addr.sin_port = htons(port);          // port in network byte order
    my_addr.sin_addr.s_addr = INADDR_ANY;    // automatically fill in my IP
    if (bind(sockfd_, (sockaddr *)&my_addr, sizeof(sockaddr)) == -1)
      {
        perror("bind");                 // TODO: ROS_ERROR errno
        return;
      }

    sockaddr_in my_addr2;                     // my address information
    memset(&my_addr2, 0, sizeof(my_addr2));    // initialize to zeros
    my_addr2.sin_family = AF_INET;            // host byte order
    my_addr2.sin_port = htons(port2);          // port in network byte order
    my_addr2.sin_addr.s_addr = INADDR_ANY;    // automatically fill in my IP
    if (bind(sockfd2_, (sockaddr *)&my_addr2, sizeof(sockaddr)) == -1)
      {
        perror("bind2");                 // TODO: ROS_ERROR errno
        return;
      }
  
    if (fcntl(sockfd_,F_SETFL, O_NONBLOCK|FASYNC) < 0)
      {
        perror("non-block");
        return;
      }

    if (fcntl(sockfd2_,F_SETFL, O_NONBLOCK|FASYNC) < 0)
      {
        perror("non-block2");
        return;
      }

    ROS_DEBUG("Velodyne socket fd is %d\n", sockfd_);
    ROS_DEBUG("Velodyne socket fd 2 is %d\n", sockfd2_);

    positionPacketPollThread_ = boost::shared_ptr< boost::thread >
      (new boost::thread(boost::bind(&InputSocket::positionPacketPoll, this)));

    this->imu_pc_sub_ = private_nh_.subscribe("/mti/sensor/packet_counter", 20, &InputSocket::imuPacketCounterCallback, this);
  }

  /** @brief destructor */
  InputSocket::~InputSocket(void)
  {
    (void) close(sockfd_);
    (void) close(sockfd2_);
  }

  /** @brief Get one velodyne packet. */
  int InputSocket::getPacket(velodyne_msgs::VelodynePacket *pkt, const double time_offset)
  {
    double time1 = ros::Time::now().toSec();

    struct pollfd fds[1];
    fds[0].fd = sockfd_;
    fds[0].events = POLLIN;
    static const int POLL_TIMEOUT = 1000; // one second (in msec)

    sockaddr_in sender_address;
    socklen_t sender_address_len = sizeof(sender_address);

    while (true)
      {
        // Unfortunately, the Linux kernel recvfrom() implementation
        // uses a non-interruptible sleep() when waiting for data,
        // which would cause this method to hang if the device is not
        // providing data.  We poll() the device first to make sure
        // the recvfrom() will not block.
        //
        // Note, however, that there is a known Linux kernel bug:
        //
        //   Under Linux, select() may report a socket file descriptor
        //   as "ready for reading", while nevertheless a subsequent
        //   read blocks.  This could for example happen when data has
        //   arrived but upon examination has wrong checksum and is
        //   discarded.  There may be other circumstances in which a
        //   file descriptor is spuriously reported as ready.  Thus it
        //   may be safer to use O_NONBLOCK on sockets that should not
        //   block.

        // poll() until input available
        do
          {
            int retval = poll(fds, 1, POLL_TIMEOUT);
            if (retval < 0)             // poll() error?
              {
                if (errno != EINTR)
                  ROS_ERROR("poll() error: %s", strerror(errno));
                return 1;
              }
            if (retval == 0)            // poll() timeout?
              {
                ROS_WARN("Velodyne poll() timeout");
                return 1;
              }
            if ((fds[0].revents & POLLERR)
                || (fds[0].revents & POLLHUP)
                || (fds[0].revents & POLLNVAL)) // device error?
              {
                ROS_ERROR("poll() reports Velodyne error");
                return 1;
              }
          } while ((fds[0].revents & POLLIN) == 0);

        // Receive packets that should now be available from the
        // socket using a blocking read.
        ssize_t nbytes = recvfrom(sockfd_, &pkt->data[0],
                                  packet_size,  0,
                                  (sockaddr*) &sender_address,
                                  &sender_address_len);

        if (nbytes < 0)
          {
            if (errno != EWOULDBLOCK)
              {
                perror("recvfail");
                ROS_INFO("recvfail");
                return 1;
              }
          }
        else if ((size_t) nbytes == packet_size)
          {
            // read successful,
            // if packet is not from the lidar scanner we selected by IP,
            // continue otherwise we are done
            if(devip_str_ != ""
               && sender_address.sin_addr.s_addr != devip_.s_addr)
              continue;
            else
              break; //done
          }

        ROS_DEBUG_STREAM("incomplete Velodyne packet read: "
                         << nbytes << " bytes");
      }

    // Average the times at which we begin and end reading.  Use that to
    // estimate when the scan occurred. Add the time offset.
    double time2 = ros::Time::now().toSec();
    pkt->stamp = ros::Time((time2 + time1) / 2.0 + time_offset);

    // Update timestamp by IMU
    if (imu_pc_queue_.size() != 0)
    {
      uint32_t h_offset;
      *((uint8_t*)&h_offset + 0) = pkt->data[packet_size-6];
      *((uint8_t*)&h_offset + 1) = pkt->data[packet_size-5];
      *((uint8_t*)&h_offset + 2) = pkt->data[packet_size-4];
      *((uint8_t*)&h_offset + 3) = pkt->data[packet_size-3];

      static uint64_t last_ts = 0L; // in us
      uint64_t ts1 = pos_pkt_.hh     * 3600000000 + h_offset;
      uint64_t ts2 = (pos_pkt_.hh+1) * 3600000000 + h_offset;
      if (std::abs<uint64_t>(ts1-last_ts) < std::abs<uint64_t>(ts2-last_ts))
      {
        last_ts = ts1;
      }
      else
      {
        last_ts = ts2;
      }

      double pc = fmod(((double)last_ts / 1000 / 1000 * 400), 1<<16);
      double min_t_diff = std::numeric_limits<double>::max();
      int min_i = -1;
//      ROS_DEBUG("pc=%lf", pc);
      boost::mutex::scoped_lock lock(imu_pc_queue_mutex_);
      for (int i = 0; i < imu_pc_queue_.size(); ++i)
      {
        double t_diff = std::abs(imu_pc_queue_[i]->counter - pc);
//        ROS_DEBUG("pc[%d]=%d", i, imu_pc_queue_[i]->counter);
        if (min_t_diff > t_diff)
        {
          min_t_diff = t_diff;
          min_i = i;
        }
      }
      assert(min_i != -1);
      ROS_DEBUG("min_t_diff:%lf ms, min_i:%d", min_t_diff/400*1000, min_i);
      if (min_t_diff > 1)
        ROS_WARN("No IMU packet counter message received");
      else
        pkt->stamp.fromSec(imu_pc_queue_[min_i]->header.stamp.toSec() + (pc - imu_pc_queue_[min_i]->counter)/400);
    }
    else
    {
      ROS_WARN("No IMU packet counter message received");
    }

    return 0;
  }

  int InputSocket::getPositionPacket(PositionPacket *pkt)
  {
//    double time1 = ros::Time::now().toSec();
    uint8_t position_pkt[position_packet_size];

    struct pollfd fds[1];
    fds[0].fd = sockfd2_;
    fds[0].events = POLLIN;
    static const int POLL_TIMEOUT = 1000; // one second (in msec)

    sockaddr_in sender_address;
    socklen_t sender_address_len = sizeof(sender_address);

    while (true)
      {
        // Unfortunately, the Linux kernel recvfrom() implementation
        // uses a non-interruptible sleep() when waiting for data,
        // which would cause this method to hang if the device is not
        // providing data.  We poll() the device first to make sure
        // the recvfrom() will not block.
        //
        // Note, however, that there is a known Linux kernel bug:
        //
        //   Under Linux, select() may report a socket file descriptor
        //   as "ready for reading", while nevertheless a subsequent
        //   read blocks.  This could for example happen when data has
        //   arrived but upon examination has wrong checksum and is
        //   discarded.  There may be other circumstances in which a
        //   file descriptor is spuriously reported as ready.  Thus it
        //   may be safer to use O_NONBLOCK on sockets that should not
        //   block.

        // poll() until input available
        do
          {
            int retval = poll(fds, 1, POLL_TIMEOUT);
            if (retval < 0)             // poll() error?
              {
                if (errno != EINTR)
                  ROS_ERROR("poll2() error: %s", strerror(errno));
                return 1;
              }
            if (retval == 0)            // poll() timeout?
              {
                ROS_WARN("Velodyne poll2() timeout");
                return 1;
              }
            if ((fds[0].revents & POLLERR)
                || (fds[0].revents & POLLHUP)
                || (fds[0].revents & POLLNVAL)) // device error?
              {
                ROS_ERROR("poll2() reports Velodyne error");
                return 1;
              }
          } while ((fds[0].revents & POLLIN) == 0);

        // Receive packets that should now be available from the
        // socket using a blocking read.
        ssize_t nbytes = recvfrom(sockfd2_, &position_pkt,
                                  position_packet_size,  0,
                                  (sockaddr*) &sender_address,
                                  &sender_address_len);

        if (nbytes < 0)
          {
            if (errno != EWOULDBLOCK)
              {
                perror("recvfail2");
                ROS_INFO("recvfail2");
                return 1;
              }
          }
        else if ((size_t) nbytes == position_packet_size)
          {
            // read successful,
            // if packet is not from the lidar scanner we selected by IP,
            // continue otherwise we are done
            if(devip_str_ != ""
               && sender_address.sin_addr.s_addr != devip_.s_addr)
              continue;
            else
              break; //done
          }

        ROS_DEBUG_STREAM("incomplete Velodyne position packet read: "
                         << nbytes << " bytes");
      }

    // Average the times at which we begin and end reading.  Use that to
    // estimate when the scan occurred. Add the time offset.
//    double time2 = ros::Time::now().toSec();
//    pkt->stamp = ros::Time((time2 + time1) / 2.0 + time_offset);
    char gprmc[73];
    memcpy(gprmc, position_pkt + 206, 72);
    gprmc[72] = '\0';
    ROS_DEBUG("NMEA $GPRMC sentence: `%s`", gprmc);

    pkt->hh = (gprmc[ 7] -  '0') * 10 + gprmc[ 8] - '0';
    pkt->mm = (gprmc[ 9] -  '0') * 10 + gprmc[10] - '0';
    pkt->ss = (gprmc[11] -  '0') * 10 + gprmc[12] - '0';
    return 0;
  }

  bool InputSocket::pollPositionPacket()
  {
    while (true)
      {
        // keep reading until full packet received
//        PositionPacket pkt;
        int rc = this->getPositionPacket(&pos_pkt_);
        if (rc == 0) break;       // got a full packet?
        if (rc < 0) return false; // end of file reached?
      }

    return true;
  }


  /** @brief Position poll thread main loop. */
  void InputSocket::positionPacketPoll()
  {
    while(ros::ok())
      {
        // poll device until end of file
        if (!pollPositionPacket())
          break;
      }
  }

  void InputSocket::imuPacketCounterCallback(const custom_msgs::PacketCounterConstPtr &msg)
  {
    boost::mutex::scoped_lock lock(imu_pc_queue_mutex_);

    imu_pc_queue_.push_back(msg);
    if (imu_pc_queue_.size() > 20)
    {
      imu_pc_queue_.pop_front();
    }
  }

  ////////////////////////////////////////////////////////////////////////
  // InputPCAP class implementation
  ////////////////////////////////////////////////////////////////////////

  /** @brief constructor
   *
   *  @param private_nh ROS private handle for calling node.
   *  @param port UDP port number
   *  @param packet_rate expected device packet frequency (Hz)
   *  @param filename PCAP dump file name
   */
  InputPCAP::InputPCAP(ros::NodeHandle private_nh, uint16_t port,
                       double packet_rate, std::string filename,
                       bool read_once, bool read_fast, double repeat_delay):
    Input(private_nh, port),
    packet_rate_(packet_rate),
    filename_(filename)
  {
    pcap_ = NULL;  
    empty_ = true;

    // get parameters using private node handle
    private_nh.param("read_once", read_once_, false);
    private_nh.param("read_fast", read_fast_, false);
    private_nh.param("repeat_delay", repeat_delay_, 0.0);

    if (read_once_)
      ROS_INFO("Read input file only once.");
    if (read_fast_)
      ROS_INFO("Read input file as quickly as possible.");
    if (repeat_delay_ > 0.0)
      ROS_INFO("Delay %.3f seconds before repeating input file.",
               repeat_delay_);

    // Open the PCAP dump file
    ROS_INFO("Opening PCAP file \"%s\"", filename_.c_str());
    if ((pcap_ = pcap_open_offline(filename_.c_str(), errbuf_) ) == NULL)
      {
        ROS_FATAL("Error opening Velodyne socket dump file.");
        return;
      }

    std::stringstream filter;
    if( devip_str_ != "" )              // using specific IP?
      {
        filter << "src host " << devip_str_ << " && ";
      }
    filter << "udp dst port " << port;
    pcap_compile(pcap_, &pcap_packet_filter_,
                 filter.str().c_str(), 1, PCAP_NETMASK_UNKNOWN);
  }

  /** destructor */
  InputPCAP::~InputPCAP(void)
  {
    pcap_close(pcap_);
  }

  /** @brief Get one velodyne packet. */
  int InputPCAP::getPacket(velodyne_msgs::VelodynePacket *pkt, const double time_offset)
  {
    struct pcap_pkthdr *header;
    const u_char *pkt_data;

    while (true)
      {
        int res;
        if ((res = pcap_next_ex(pcap_, &header, &pkt_data)) >= 0)
          {
            // Skip packets not for the correct port and from the
            // selected IP address.
            if (!devip_str_.empty() &&
                (0 == pcap_offline_filter(&pcap_packet_filter_,
                                          header, pkt_data)))
              continue;

            // Keep the reader from blowing through the file.
            if (read_fast_ == false)
              packet_rate_.sleep();
            
            memcpy(&pkt->data[0], pkt_data+42, packet_size);
            pkt->stamp = ros::Time::now(); // time_offset not considered here, as no synchronization required
            empty_ = false;
            return 0;                   // success
          }

        if (empty_)                 // no data in file?
          {
            ROS_WARN("Error %d reading Velodyne packet: %s", 
                     res, pcap_geterr(pcap_));
            return -1;
          }

        if (read_once_)
          {
            ROS_INFO("end of file reached -- done reading.");
            return -1;
          }
        
        if (repeat_delay_ > 0.0)
          {
            ROS_INFO("end of file reached -- delaying %.3f seconds.",
                     repeat_delay_);
            usleep(rint(repeat_delay_ * 1000000.0));
          }

        ROS_DEBUG("replaying Velodyne dump file");

        // I can't figure out how to rewind the file, because it
        // starts with some kind of header.  So, close the file
        // and reopen it with pcap.
        pcap_close(pcap_);
        pcap_ = pcap_open_offline(filename_.c_str(), errbuf_);
        empty_ = true;              // maybe the file disappeared?
      } // loop back and try again
  }

} // velodyne namespace
