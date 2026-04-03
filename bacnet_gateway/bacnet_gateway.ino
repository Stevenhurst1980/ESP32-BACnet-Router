/*
 * BACnet Gateway - FIXED MSTP Implementation for ESP32-C6
 *
 * Fixes applied:
 * 1. Binary-safe RS485 read (uint8_t buffer, no String for binary data)
 * 2. Minimal token bus state machine (Poll-For-Master response, Token pass)
 * 3. Correct NPDU control byte (0x00 instead of 0x20)
 * 4. Fixed RS485 direction switching timing
 * 5. processBACnetFrame now takes uint8_t* buffer
 * 6. Loop uses binary-safe reader
 */

#include <Arduino.h>
#include <WiFi.h>
#include <WebServer.h>
#include <EEPROM.h>

// ========== BACNET CONSTANTS ==========
#define BACNET_PROTOCOL_VERSION 1
#define BACNET_MAX_APDU 480

// BACnet MSTP Frame Types
#define FRAME_TYPE_TOKEN                          0
#define FRAME_TYPE_POLL_FOR_MASTER                1
#define FRAME_TYPE_REPLY_TO_POLL_FOR_MASTER       2
#define FRAME_TYPE_TEST_REQUEST                   3
#define FRAME_TYPE_TEST_RESPONSE                  4
#define FRAME_TYPE_BACNET_DATA_EXPECTING_REPLY    5
#define FRAME_TYPE_BACNET_DATA_NOT_EXPECTING_REPLY 6
#define FRAME_TYPE_REPLY_POSTPONED                7

// Network Constants
#define BACNET_BROADCAST_NETWORK  0xFFFF
#define BACNET_BROADCAST_MAC      0xFF
#define BACNET_MAX_INSTANCE       4194303

// ========== CONFIGURATION ==========
struct Config {
  char ap_ssid[32];
  char ap_password[32];
  uint8_t ip_octet4;

  uint8_t device_mac;
  uint32_t device_id;
  char device_name[32];
  uint16_t network_number;
  uint32_t baud_rate;

  uint8_t initialized;
};

Config config;

// ========== DISCOVERED DEVICES ==========
struct DiscoveredDevice {
  uint32_t device_id;
  uint8_t mac_address;
  uint16_t network_number;
  uint16_t max_apdu;
  uint8_t segmentation;
  uint16_t vendor_id;
  char device_name[32];
  unsigned long last_seen;
  bool online;
};

#define MAX_DISCOVERED_DEVICES 50
DiscoveredDevice discovered_devices[MAX_DISCOVERED_DEVICES];
int discovered_count = 0;

// ========== HARDWARE PINS ==========
#define RS485_EN_PIN   23
#define RS485_TX_PIN   17
#define RS485_RX_PIN   16

// ========== GLOBALS ==========
WebServer server(80);
HardwareSerial RS485Serial(1);
String receivedData = "";
unsigned long lastDataTime = 0;

// Token bus state
bool has_token = false;
unsigned long token_received_time = 0;

// ========== CRC TABLES ==========
static const uint8_t CRC8_Table[256] = {
    0x00, 0xfe, 0xff, 0x01, 0xfd, 0x03, 0x02, 0xfc,
    0xf9, 0x07, 0x06, 0xf8, 0x04, 0xfa, 0xfb, 0x05,
    0xf1, 0x0f, 0x0e, 0xf0, 0x0c, 0xf2, 0xf3, 0x0d,
    0x08, 0xf6, 0xf7, 0x09, 0xf5, 0x0b, 0x0a, 0xf4,
    0xe1, 0x1f, 0x1e, 0xe0, 0x1c, 0xe2, 0xe3, 0x1d,
    0x18, 0xe6, 0xe7, 0x19, 0xe5, 0x1b, 0x1a, 0xe4,
    0x10, 0xee, 0xef, 0x11, 0xed, 0x13, 0x12, 0xec,
    0xe9, 0x17, 0x16, 0xe8, 0x14, 0xea, 0xeb, 0x15,
    0xc1, 0x3f, 0x3e, 0xc0, 0x3c, 0xc2, 0xc3, 0x3d,
    0x38, 0xc6, 0xc7, 0x39, 0xc5, 0x3b, 0x3a, 0xc4,
    0x30, 0xce, 0xcf, 0x31, 0xcd, 0x33, 0x32, 0xcc,
    0xc9, 0x37, 0x36, 0xc8, 0x34, 0xca, 0xcb, 0x35,
    0x20, 0xde, 0xdf, 0x21, 0xdd, 0x23, 0x22, 0xdc,
    0xd9, 0x27, 0x26, 0xd8, 0x24, 0xda, 0xdb, 0x25,
    0xd1, 0x2f, 0x2e, 0xd0, 0x2c, 0xd2, 0xd3, 0x2d,
    0x28, 0xd6, 0xd7, 0x29, 0xd5, 0x2b, 0x2a, 0xd4,
    0x81, 0x7f, 0x7e, 0x80, 0x7c, 0x82, 0x83, 0x7d,
    0x78, 0x86, 0x87, 0x79, 0x85, 0x7b, 0x7a, 0x84,
    0x70, 0x8e, 0x8f, 0x71, 0x8d, 0x73, 0x72, 0x8c,
    0x89, 0x77, 0x76, 0x88, 0x74, 0x8a, 0x8b, 0x75,
    0x60, 0x9e, 0x9f, 0x61, 0x9d, 0x63, 0x62, 0x9c,
    0x99, 0x67, 0x66, 0x98, 0x64, 0x9a, 0x9b, 0x65,
    0x91, 0x6f, 0x6e, 0x90, 0x6c, 0x92, 0x93, 0x6d,
    0x68, 0x96, 0x97, 0x69, 0x95, 0x6b, 0x6a, 0x94,
    0x40, 0xbe, 0xbf, 0x41, 0xbd, 0x43, 0x42, 0xbc,
    0xb9, 0x47, 0x46, 0xb8, 0x44, 0xba, 0xbb, 0x45,
    0xb1, 0x4f, 0x4e, 0xb0, 0x4c, 0xb2, 0xb3, 0x4d,
    0x48, 0xb6, 0xb7, 0x49, 0xb5, 0x4b, 0x4a, 0xb4,
    0xa1, 0x5f, 0x5e, 0xa0, 0x5c, 0xa2, 0xa3, 0x5d,
    0x58, 0xa6, 0xa7, 0x59, 0xa5, 0x5b, 0x5a, 0xa4,
    0x50, 0xae, 0xaf, 0x51, 0xad, 0x53, 0x52, 0xac,
    0xa9, 0x57, 0x56, 0xa8, 0x54, 0xaa, 0xab, 0x55
};

static const uint16_t CRC16_Table[256] = {
    0x0000, 0x1189, 0x2312, 0x329b, 0x4624, 0x57ad, 0x6536, 0x74bf,
    0x8c48, 0x9dc1, 0xaf5a, 0xbed3, 0xca6c, 0xdbe5, 0xe97e, 0xf8f7,
    0x1081, 0x0108, 0x3393, 0x221a, 0x56a5, 0x472c, 0x75b7, 0x643e,
    0x9cc9, 0x8d40, 0xbfdb, 0xae52, 0xdaed, 0xcb64, 0xf9ff, 0xe876,
    0x2102, 0x308b, 0x0210, 0x1399, 0x6726, 0x76af, 0x4434, 0x55bd,
    0xad4a, 0xbcc3, 0x8e58, 0x9fd1, 0xeb6e, 0xfae7, 0xc87c, 0xd9f5,
    0x3183, 0x200a, 0x1291, 0x0318, 0x77a7, 0x662e, 0x54b5, 0x453c,
    0xbdcb, 0xac42, 0x9ed9, 0x8f50, 0xfbef, 0xea66, 0xd8fd, 0xc974,
    0x4242, 0x538d, 0x6116, 0x709f, 0x0420, 0x15a9, 0x2732, 0x36bb,
    0xce4c, 0xdfc5, 0xed5e, 0xfcd7, 0x8868, 0x99e1, 0xab7a, 0xbaf3,
    0x5285, 0x430c, 0x7197, 0x601e, 0x14a1, 0x0528, 0x37b3, 0x263a,
    0xdecd, 0xcf44, 0xfddf, 0xec56, 0x98e9, 0x8960, 0xbbfb, 0xaa72,
    0x6306, 0x728f, 0x4014, 0x519d, 0x2522, 0x34ab, 0x0630, 0x17b9,
    0xef4e, 0xfec7, 0xcc5c, 0xddd5, 0xa96a, 0xb8e3, 0x8a78, 0x9bf1,
    0x7387, 0x620e, 0x5095, 0x411c, 0x35a3, 0x242a, 0x16b1, 0x0738,
    0xffcf, 0xee46, 0xdcdd, 0xcd54, 0xb9eb, 0xa862, 0x9af9, 0x8b70,
    0x8408, 0x9581, 0xa71a, 0xb693, 0xc22c, 0xd3a5, 0xe13e, 0xf0b7,
    0x0840, 0x19c9, 0x2b52, 0x3adb, 0x4e64, 0x5fed, 0x6d76, 0x7cff,
    0x9489, 0x8500, 0xb79b, 0xa612, 0xd2ad, 0xc324, 0xf1bf, 0xe036,
    0x18c1, 0x0948, 0x3bd3, 0x2a5a, 0x5ee5, 0x4f6c, 0x7df7, 0x6c7e,
    0xa50a, 0xb483, 0x8618, 0x9791, 0xe32e, 0xf2a7, 0xc03c, 0xd1b5,
    0x2942, 0x38cb, 0x0a50, 0x1bd9, 0x6f66, 0x7eef, 0x4c74, 0x5dfd,
    0xb58b, 0xa402, 0x9699, 0x8710, 0xf3af, 0xe226, 0xd0bd, 0xc134,
    0x39c3, 0x284a, 0x1ad1, 0x0b58, 0x7fe7, 0x6e6e, 0x5cf5, 0x4d7c,
    0xc60c, 0xd785, 0xe51e, 0xf497, 0x8028, 0x91a1, 0xa33a, 0xb2b3,
    0x4a44, 0x5bcd, 0x6956, 0x78df, 0x0c60, 0x1de9, 0x2f72, 0x3efb,
    0xd68d, 0xc704, 0xf59f, 0xe416, 0x90a9, 0x8120, 0xb3bb, 0xa232,
    0x5ac5, 0x4b4c, 0x79d7, 0x685e, 0x1ce1, 0x0d68, 0x3ff3, 0x2e7a,
    0xe70e, 0xf687, 0xc41c, 0xd595, 0xa12a, 0xb0a3, 0x8238, 0x93b1,
    0x6b46, 0x7acf, 0x4854, 0x59dd, 0x2d62, 0x3ceb, 0x0e70, 0x1ff9,
    0xf78f, 0xe606, 0xd49d, 0xc514, 0xb1ab, 0xa022, 0x92b9, 0x8330,
    0x7bc7, 0x6a4e, 0x58d5, 0x495c, 0x3de3, 0x2c6a, 0x1ef1, 0x0f78
};

// ========== FUNCTION DECLARATIONS ==========
void loadConfig();
void saveConfig();
void setupNetwork();
void setupWebServer();
void sendRS485(const uint8_t* data, size_t len);
int  readRS485(uint8_t* buf, int maxLen);
void processBACnetFrame(uint8_t* data, int len);
void sendBACnetWhoIs();
void sendBACnetIAm();
void parseIAmFrame(uint8_t* data, uint16_t len, uint8_t src_mac);
void addDiscoveredDevice(uint32_t device_id, uint8_t mac, uint16_t network,
                         uint16_t max_apdu, uint8_t segmentation, uint16_t vendor_id);
String getDevicesJSON();
uint8_t  CRC_Calc_Header(uint8_t dataValue, uint8_t crcValue);
uint16_t CRC_Calc_Data(uint8_t dataValue, uint16_t crcValue);
void send_MSTP_frame(uint8_t frame_type, uint8_t dest_mac,
                     uint8_t* data, uint16_t data_len);
void passToken(uint8_t dest_mac);
void replyToPollForMaster(uint8_t dest_mac);

// ========== CRC FUNCTIONS ==========
uint8_t CRC_Calc_Header(uint8_t dataValue, uint8_t crcValue) {
  return CRC8_Table[crcValue ^ dataValue];
}

uint16_t CRC_Calc_Data(uint8_t dataValue, uint16_t crcValue) {
  uint16_t crcLow = (crcValue & 0xff) ^ dataValue;
  return ((crcValue >> 8) ^ CRC16_Table[crcLow]);
}

// ========== SEND MSTP FRAME ==========
void send_MSTP_frame(uint8_t frame_type, uint8_t dest_mac,
                     uint8_t* data, uint16_t data_len) {
  uint8_t frame[600];
  int idx = 0;

  // MSTP Header
  frame[idx++] = 0x55;               // Preamble 1
  frame[idx++] = 0xFF;               // Preamble 2
  frame[idx++] = frame_type;         // Frame Type
  frame[idx++] = dest_mac;           // Destination MAC
  frame[idx++] = config.device_mac;  // Source MAC
  frame[idx++] = (data_len >> 8) & 0xFF;
  frame[idx++] = data_len & 0xFF;

  // Header CRC (covers bytes 2-6, seed 0xFF, result complemented)
  uint8_t header_crc = 0xFF;
  for (int i = 2; i < 7; i++) {
    header_crc = CRC_Calc_Header(frame[i], header_crc);
  }
  frame[idx++] = ~header_crc;

  // Data payload
  for (int i = 0; i < data_len; i++) {
    frame[idx++] = data[i];
  }

  // Data CRC (only if there is a payload)
  if (data_len > 0) {
    uint16_t data_crc = 0xFFFF;
    for (int i = 0; i < data_len; i++) {
      data_crc = CRC_Calc_Data(data[i], data_crc);
    }
    data_crc = ~data_crc;
    frame[idx++] = data_crc & 0xFF;
    frame[idx++] = (data_crc >> 8) & 0xFF;
  }

  sendRS485(frame, idx);

  Serial.print("TX MSTP [");
  Serial.print(idx);
  Serial.print(" bytes] Type=");
  Serial.print(frame_type);
  Serial.print(" Dest=0x");
  Serial.print(dest_mac, HEX);
  Serial.print(" DataLen=");
  Serial.println(data_len);
}

// ========== TOKEN BUS HELPERS ==========

// Pass the token to the next station
void passToken(uint8_t dest_mac) {
  send_MSTP_frame(FRAME_TYPE_TOKEN, dest_mac, nullptr, 0);
  has_token = false;
  Serial.print("  Passed token to MAC 0x");
  Serial.println(dest_mac, HEX);
}

// Reply to a Poll-For-Master directed at us
void replyToPollForMaster(uint8_t dest_mac) {
  send_MSTP_frame(FRAME_TYPE_REPLY_TO_POLL_FOR_MASTER, dest_mac, nullptr, 0);
  Serial.print("  Replied to Poll-For-Master from MAC 0x");
  Serial.println(dest_mac, HEX);
}

// ========== BACNET WHO-IS ==========
void sendBACnetWhoIs() {
  uint8_t message[4];
  int idx = 0;

  // NPDU
  message[idx++] = 0x01;  // BACnet protocol version
  message[idx++] = 0x00;  // Control: no routing, normal priority  <-- FIX: was 0x20

  // APDU - Unconfirmed-Request, Who-Is (no range = discover all)
  message[idx++] = 0x10;  // PDU Type: Unconfirmed-Request
  message[idx++] = 0x08;  // Service Choice: Who-Is

  send_MSTP_frame(FRAME_TYPE_BACNET_DATA_NOT_EXPECTING_REPLY,
                  BACNET_BROADCAST_MAC, message, idx);

  Serial.println("  Sent BACnet Who-Is (broadcast)");
}

// ========== BACNET I-AM ==========
void sendBACnetIAm() {
  uint8_t message[30];
  int idx = 0;

  // NPDU
  message[idx++] = 0x01;  // Protocol version
  message[idx++] = 0x00;  // Control: no routing  <-- FIX: was 0x20

  // APDU - Unconfirmed-Request, I-Am
  message[idx++] = 0x10;  // PDU Type: Unconfirmed-Request
  message[idx++] = 0x00;  // Service Choice: I-Am

  // Device Object Identifier — app tag 12 (0xC), length 4
  uint32_t obj_id = ((uint32_t)8 << 22) | (config.device_id & 0x3FFFFF);
  message[idx++] = 0xC4;
  message[idx++] = (obj_id >> 24) & 0xFF;
  message[idx++] = (obj_id >> 16) & 0xFF;
  message[idx++] = (obj_id >> 8)  & 0xFF;
  message[idx++] =  obj_id        & 0xFF;

  // Max APDU Length Accepted — app tag 2 (Unsigned), length 2 → 480 = 0x01E0
  message[idx++] = 0x22;
  message[idx++] = 0x01;
  message[idx++] = 0xE0;

  // Segmentation Supported — app tag 9 (Enumerated), length 1 → 3 = NO_SEGMENTATION
  message[idx++] = 0x91;
  message[idx++] = 0x03;

  // Vendor ID — app tag 2 (Unsigned), length 2 → 999 = 0x03E7
  message[idx++] = 0x22;
  message[idx++] = 0x03;
  message[idx++] = 0xE7;

  send_MSTP_frame(FRAME_TYPE_BACNET_DATA_NOT_EXPECTING_REPLY,
                  BACNET_BROADCAST_MAC, message, idx);

  Serial.print("  Sent BACnet I-Am: Device ID=");
  Serial.print(config.device_id);
  Serial.print(", MAC=");
  Serial.println(config.device_mac);
}

// ========== RS485 SEND ==========
void sendRS485(const uint8_t* data, size_t len) {
  digitalWrite(RS485_EN_PIN, HIGH);   // Enable TX
  delayMicroseconds(100);             // DE settle time

  RS485Serial.write(data, len);
  RS485Serial.flush();                // Wait for TX shift register to empty

  // At 9600 baud worst case, 1 byte = ~1042µs. Add a small margin.
  // flush() handles the wait, but give the line a moment to settle.
  delayMicroseconds(200);

  digitalWrite(RS485_EN_PIN, LOW);    // Back to RX mode
}

// ========== RS485 BINARY-SAFE READ ==========  <-- FIX: was String-based
int readRS485(uint8_t* buf, int maxLen) {
  int count = 0;
  unsigned long last_byte_time = millis();

  while (count < maxLen) {
    if (RS485Serial.available()) {
      buf[count++] = (uint8_t)RS485Serial.read();
      last_byte_time = millis();
    } else {
      // Inter-frame gap: stop collecting after 5ms silence
      if (millis() - last_byte_time > 5) break;
    }
  }
  return count;
}

// ========== PROCESS RECEIVED BACNET FRAME ==========
// Now takes uint8_t* buffer instead of String  <-- FIX
void processBACnetFrame(uint8_t* data, int len) {
  if (len < 8) return;

  // Check preamble
  if (data[0] != 0x55 || data[1] != 0xFF) return;

  uint8_t  frame_type = data[2];
  uint8_t  dest_mac   = data[3];
  uint8_t  src_mac    = data[4];
  uint16_t data_len   = ((uint16_t)data[5] << 8) | data[6];
  // data[7] = header CRC (not verified here but structure is sound)

  Serial.print("RX MSTP: Type=");
  Serial.print(frame_type);
  Serial.print(" Dest=0x");
  Serial.print(dest_mac, HEX);
  Serial.print(" Src=0x");
  Serial.print(src_mac, HEX);
  Serial.print(" Len=");
  Serial.println(data_len);

  // ---- TOKEN BUS PARTICIPATION ----  <-- FIX: entire section is new

  // Someone is polling for masters — reply if the poll is addressed to us
  if (frame_type == FRAME_TYPE_POLL_FOR_MASTER && dest_mac == config.device_mac) {
    replyToPollForMaster(src_mac);
    return;
  }

  // We received the token — use it to send a Who-Is then pass it on
  if (frame_type == FRAME_TYPE_TOKEN && dest_mac == config.device_mac) {
    has_token = true;
    token_received_time = millis();
    Serial.println("  Token received — sending Who-Is then passing token");

    sendBACnetWhoIs();

    // Pass token to the next logical station (simple round-robin)
    uint8_t next_mac = (config.device_mac + 1) % 128;
    passToken(next_mac);
    return;
  }

  // ---- Filter: only process frames for us or broadcast ----
  if (dest_mac != config.device_mac && dest_mac != BACNET_BROADCAST_MAC) return;

  // ---- BACnet Data Frames ----
  if ((frame_type == FRAME_TYPE_BACNET_DATA_NOT_EXPECTING_REPLY ||
       frame_type == FRAME_TYPE_BACNET_DATA_EXPECTING_REPLY) && data_len >= 4) {

    // Payload starts at byte 8
    uint8_t* payload = data + 8;

    // Validate NPDU version
    if (payload[0] != 0x01) return;

    // NPDU control byte — check if routing info is present
    uint8_t npdu_ctrl = payload[1];
    int apdu_offset = 2;

    // If DNET/DLEN present (bit 5 set), skip those bytes
    if (npdu_ctrl & 0x20) {
      // DNET (2) + DLEN (1) + DADR (variable) — simplified: skip 4 bytes
      apdu_offset += 4;
    }
    // If SNET/SLEN present (bit 3 set), skip those bytes
    if (npdu_ctrl & 0x08) {
      apdu_offset += 4;
    }
    // If hop count present (bit 5 of ctrl), skip 1 byte
    if (npdu_ctrl & 0x20) {
      apdu_offset += 1;
    }

    if ((apdu_offset + 2) > (int)data_len) return;

    uint8_t pdu_type = (payload[apdu_offset] >> 4) & 0x0F;
    uint8_t service  = payload[apdu_offset + 1];

    // Unconfirmed-Request (type = 1)
    if (pdu_type == 1) {
      if (service == 0x00) {  // I-Am
        Serial.println("  Received I-Am — parsing device info");
        parseIAmFrame(payload + apdu_offset, data_len - apdu_offset, src_mac);
      }
      else if (service == 0x08) {  // Who-Is
        Serial.println("  Received Who-Is — sending I-Am");
        delay(random(10, 200));  // Random backoff to avoid bus collision
        sendBACnetIAm();
      }
    }
  }
}

// ========== PARSE I-AM FRAME ==========
void parseIAmFrame(uint8_t* apdu, uint16_t len, uint8_t src_mac) {
  // apdu[0] = 0x10  PDU type
  // apdu[1] = 0x00  Service I-Am
  // apdu[2] = 0xC4  App tag 12, length 4
  // apdu[3..6]      Object identifier (4 bytes)
  // apdu[7] = 0x22  App tag 2, length 2
  // apdu[8..9]      Max APDU
  // apdu[10]= 0x91  App tag 9, length 1
  // apdu[11]        Segmentation
  // apdu[12]= 0x22  App tag 2, length 2
  // apdu[13..14]    Vendor ID

  if (len < 13) return;

  // Device Object Identifier
  uint32_t obj_id = ((uint32_t)apdu[3] << 24) |
                    ((uint32_t)apdu[4] << 16) |
                    ((uint32_t)apdu[5] << 8)  |
                               apdu[6];
  uint32_t device_id = obj_id & 0x3FFFFF;

  // Max APDU
  uint16_t max_apdu = ((uint16_t)apdu[8] << 8) | apdu[9];

  // Segmentation
  uint8_t segmentation = apdu[11];

  // Vendor ID
  uint16_t vendor_id = 0;
  if (len >= 15) {
    vendor_id = ((uint16_t)apdu[13] << 8) | apdu[14];
  }

  Serial.print("  I-Am: Device ID=");
  Serial.print(device_id);
  Serial.print(", MaxAPDU=");
  Serial.print(max_apdu);
  Serial.print(", Vendor=");
  Serial.print(vendor_id);
  Serial.print(", SrcMAC=0x");
  Serial.println(src_mac, HEX);

  addDiscoveredDevice(device_id, src_mac, config.network_number,
                      max_apdu, segmentation, vendor_id);
}

// ========== DEVICE DISCOVERY MANAGEMENT ==========
void addDiscoveredDevice(uint32_t device_id, uint8_t mac, uint16_t network,
                         uint16_t max_apdu, uint8_t segmentation, uint16_t vendor_id) {
  // Update if already known
  for (int i = 0; i < MAX_DISCOVERED_DEVICES; i++) {
    if (discovered_devices[i].device_id == device_id) {
      discovered_devices[i].mac_address = mac;
      discovered_devices[i].last_seen   = millis();
      discovered_devices[i].online      = true;
      Serial.print("  Updated device ");
      Serial.println(device_id);
      return;
    }
  }

  // Add new entry
  for (int i = 0; i < MAX_DISCOVERED_DEVICES; i++) {
    if (discovered_devices[i].device_id == 0) {
      discovered_devices[i].device_id    = device_id;
      discovered_devices[i].mac_address  = mac;
      discovered_devices[i].network_number = network;
      discovered_devices[i].max_apdu     = max_apdu;
      discovered_devices[i].segmentation = segmentation;
      discovered_devices[i].vendor_id    = vendor_id;
      discovered_devices[i].last_seen    = millis();
      discovered_devices[i].online       = true;
      snprintf(discovered_devices[i].device_name, 32, "Device %lu", (unsigned long)device_id);
      discovered_count++;

      Serial.print("  Added device ");
      Serial.print(device_id);
      Serial.print(" (total: ");
      Serial.print(discovered_count);
      Serial.println(")");
      return;
    }
  }
}

String getDevicesJSON() {
  String json = "[";
  bool first = true;
  unsigned long now = millis();

  for (int i = 0; i < MAX_DISCOVERED_DEVICES; i++) {
    if (discovered_devices[i].device_id == 0) continue;

    if (now - discovered_devices[i].last_seen > 300000UL)
      discovered_devices[i].online = false;

    if (!first) json += ",";
    first = false;

    json += "{";
    json += "\"device_id\":"      + String(discovered_devices[i].device_id)      + ",";
    json += "\"device_name\":\""  + String(discovered_devices[i].device_name)     + "\",";
    json += "\"mac_address\":"    + String(discovered_devices[i].mac_address)     + ",";
    json += "\"network_number\":" + String(discovered_devices[i].network_number)  + ",";
    json += "\"vendor_id\":"      + String(discovered_devices[i].vendor_id)       + ",";
    json += "\"last_seen\":"      + String(discovered_devices[i].last_seen / 1000) + ",";
    json += "\"online\":"         + String(discovered_devices[i].online ? "true" : "false");
    json += "}";
  }

  json += "]";
  return json;
}

// ========== CONFIGURATION FUNCTIONS ==========
void loadConfig() {
  EEPROM.get(0, config);

  if (config.initialized != 0xAA) {
    Serial.println("Loading default configuration");

    strcpy(config.ap_ssid,     "BACnet-Gateway");
    strcpy(config.ap_password, "12345678");
    config.ip_octet4     = 1;
    config.device_id     = 1000;
    config.device_mac    = 100;
    config.network_number = 0;
    config.baud_rate     = 38400;
    config.initialized   = 0xAA;
    strcpy(config.device_name, "BACnet Gateway");

    saveConfig();
  } else {
    Serial.println("Config loaded from EEPROM");
  }
}

void saveConfig() {
  EEPROM.put(0, config);
  EEPROM.commit();
  Serial.println("Configuration saved");
}

void setupNetwork() {
  WiFi.mode(WIFI_AP);

  IPAddress localIP(192, 168, 4, config.ip_octet4);
  IPAddress gateway(192, 168, 4, 1);
  IPAddress subnet(255, 255, 255, 0);

  WiFi.softAPConfig(localIP, gateway, subnet);
  WiFi.softAP(config.ap_ssid, config.ap_password);

  Serial.print("AP: ");
  Serial.println(config.ap_ssid);
  Serial.print("IP: ");
  Serial.println(WiFi.softAPIP());
}

// ========== HTML PAGE ==========
const char* INDEX_HTML = R"=====(
<!DOCTYPE html>
<html>
<head>
    <title>BACnet Gateway</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #001885 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1200px; margin: 0 auto; background: white;
            border-radius: 10px; padding: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        h1 { color: #001885; margin-bottom: 20px; text-align: center; }
        .tab-buttons {
            display: flex; flex-wrap: wrap; gap: 10px;
            margin-bottom: 30px; padding: 10px;
            background: #f8f9fa; border-radius: 8px;
        }
        .tab-btn {
            padding: 12px 24px; background: #e9ecef; border: none;
            border-radius: 5px; cursor: pointer; font-weight: bold;
            color: #495057; transition: all 0.3s;
        }
        .tab-btn:hover { background: #dee2e6; }
        .tab-btn.active { background: #001885; color: white; }
        .tab-content { display: none; padding: 20px; background: #f8f9fa; border-radius: 8px; margin-bottom: 20px; }
        .tab-content.active { display: block; }
        .status-bar { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .status-item { background: white; padding: 15px; border-radius: 8px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .status-label { font-size: 0.9rem; color: #666; margin-bottom: 5px; }
        .status-value { font-size: 1.2rem; font-weight: bold; color: #001885; }
        .card { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        h2 { color: #001885; margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid #e9ecef; }
        input, select { width: 100%; padding: 12px; margin: 10px 0; border: 2px solid #e0e0e0; border-radius: 5px; font-size: 16px; }
        input:focus, select:focus { outline: none; border-color: #001885; }
        button { background: #001885; color: white; border: none; padding: 12px 24px; margin: 5px; border-radius: 5px; cursor: pointer; font-size: 16px; font-weight: bold; transition: background 0.3s; }
        button:hover { background: #0033cc; }
        .btn-success { background: #28a745; }
        .btn-success:hover { background: #218838; }
        .data-box { background: #1a1a1a; color: #00ff00; padding: 15px; border-radius: 5px; font-family: monospace; font-size: 13px; max-height: 300px; overflow-y: auto; margin: 10px 0; white-space: pre; }
        .device-table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        .device-table th, .device-table td { padding: 12px; text-align: left; border-bottom: 1px solid #e0e0e0; }
        .device-table th { background: #f8f9fa; color: #001885; font-weight: bold; }
        .device-table tr:hover { background: #f8f9fa; }
        .online-badge  { background: #28a745; color: white; padding: 3px 10px; border-radius: 12px; font-size: 0.8rem; }
        .offline-badge { background: #6c757d; color: white; padding: 3px 10px; border-radius: 12px; font-size: 0.8rem; }
        .alert { padding: 15px; border-radius: 5px; margin: 10px 0; display: none; }
        .alert-success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .alert-error   { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .form-group { margin-bottom: 20px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; color: #495057; }
        small { display: block; margin-top: 5px; color: #6c757d; font-size: 0.875rem; }
    </style>
</head>
<body>
<div class="container">
    <h1>Steve's BACnet MSTP Gateway</h1>

    <div class="tab-buttons">
        <button class="tab-btn active" onclick="showTab('dashboard',event)">Dashboard</button>
        <button class="tab-btn" onclick="showTab('devices',event)">Devices</button>
        <button class="tab-btn" onclick="showTab('gateway',event)">Gateway Config</button>
        <button class="tab-btn" onclick="showTab('network',event)">Network Config</button>
        <button class="tab-btn" onclick="showTab('monitor',event)">Monitor</button>
    </div>

    <!-- DASHBOARD -->
    <div id="dashboard" class="tab-content active">
        <div class="status-bar">
            <div class="status-item"><div class="status-label">Gateway IP</div><div class="status-value" id="dashIp">%IP_ADDRESS%</div></div>
            <div class="status-item"><div class="status-label">Device ID</div><div class="status-value" id="dashDeviceId">%DEVICE_ID%</div></div>
            <div class="status-item"><div class="status-label">MAC Address</div><div class="status-value" id="dashMac">%MAC_ADDRESS%</div></div>
            <div class="status-item"><div class="status-label">Network Number</div><div class="status-value" id="dashNet">%NETWORK_NUMBER%</div></div>
            <div class="status-item"><div class="status-label">Baud Rate</div><div class="status-value" id="dashBaud">%BAUD_RATE%</div></div>
        </div>
        <div class="card">
            <h2>Quick Actions</h2>
            <button onclick="sendCommand('whois')">🔍 Discover Devices</button>
            <button onclick="sendCommand('iam')">📢 Announce Gateway</button>
            <button onclick="sendCommand('test')">🔧 Test RS485</button>
            <button onclick="restartGateway()">🔄 Restart</button>
        </div>
        <div class="card">
            <h2>Statistics</h2>
            <div class="status-bar">
                <div class="status-item"><div class="status-label">Devices Online</div><div class="status-value" id="onlineCount">0</div></div>
                <div class="status-item"><div class="status-label">Total Discovered</div><div class="status-value" id="totalDevices">0</div></div>
            </div>
        </div>
    </div>

    <!-- DEVICES -->
    <div id="devices" class="tab-content">
        <div class="card">
            <h2>Discovered BACnet Devices</h2>
            <button onclick="sendCommand('whois')">🔍 Refresh</button>
            <button onclick="clearDevices()">🗑️ Clear</button>
            <table class="device-table">
                <thead><tr><th>Device ID</th><th>Name</th><th>MAC</th><th>Network</th><th>Vendor</th><th>Status</th></tr></thead>
                <tbody id="devicesTable"><tr><td colspan="6" style="text-align:center">No devices yet</td></tr></tbody>
            </table>
        </div>
    </div>

    <!-- GATEWAY CONFIG -->
    <div id="gateway" class="tab-content">
        <div class="card">
            <h2>Gateway Configuration</h2>
            <div class="form-group">
                <label>Device ID (0–4194303):</label>
                <input type="number" id="deviceId" min="0" max="4194303" value="%DEVICE_ID%">
            </div>
            <div class="form-group">
                <label>MAC Address (0–127):</label>
                <input type="number" id="macAddress" min="0" max="127" value="%MAC_ADDRESS%">
                <small>Must be unique on the MSTP network</small>
            </div>
            <button class="btn-success" onclick="saveGatewayConfig()">💾 Save</button>
            <button onclick="sendCommand('iam')">📢 Announce</button>
        </div>
    </div>

    <!-- NETWORK CONFIG -->
    <div id="network" class="tab-content">
        <div class="card">
            <h2>WiFi Configuration</h2>
            <div class="form-group"><label>AP SSID:</label><input type="text" id="apSsid" value="%AP_SSID%"></div>
            <div class="form-group"><label>AP Password:</label><input type="password" id="apPassword" value="%AP_PASSWORD%"></div>
            <div class="form-group">
                <label>IP Address (192.168.4.X):</label>
                <input type="number" id="ipOctet" min="1" max="254" value="%IP_OCTET%">
            </div>
            <button class="btn-success" onclick="saveWiFiConfig()">💾 Save WiFi</button>
        </div>
        <div class="card">
            <h2>BACnet Network Configuration</h2>
            <div class="form-group">
                <label>Network Number (0–65535):</label>
                <input type="number" id="networkNumber" min="0" max="65535" value="%NETWORK_NUMBER%">
                <small>0 = local network, 1–65535 = routed network</small>
            </div>
            <div class="form-group">
                <label>Baud Rate:</label>
                <select id="baudRate">
                    <option value="9600">9600</option>
                    <option value="19200">19200</option>
                    <option value="38400">38400</option>
                    <option value="76800">76800</option>
                </select>
                <small>⚠️ Must match your MSTP network!</small>
            </div>
            <button class="btn-success" onclick="saveNetworkConfig()">💾 Save Network</button>
        </div>
    </div>

    <!-- MONITOR -->
    <div id="monitor" class="tab-content">
        <div class="card">
            <h2>BACnet Data Monitor</h2>
            <div class="data-box" id="dataMonitor">%RECEIVED_DATA%</div>
            <button onclick="clearMonitor()">🗑️ Clear</button>
            <button onclick="refreshMonitor()">🔄 Refresh</button>
        </div>
    </div>

    <div id="alertSuccess" class="alert alert-success"></div>
    <div id="alertError"   class="alert alert-error"></div>
</div>

<script>
    document.getElementById('baudRate').value = '%BAUD_RATE%';

    function showTab(tabName, e) {
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.getElementById(tabName).classList.add('active');
        if (e && e.currentTarget) e.currentTarget.classList.add('active');
        if (tabName === 'devices') refreshDevices();
    }

    function showAlert(type, msg) {
        const el = document.getElementById('alert' + type[0].toUpperCase() + type.slice(1));
        el.textContent = msg; el.style.display = 'block';
        setTimeout(() => el.style.display = 'none', 3000);
    }

    function sendCommand(cmd) {
        fetch('/cmd?c=' + cmd)
            .then(r => r.text())
            .then(d => { showAlert('success', d); if (cmd === 'whois') setTimeout(refreshDevices, 3000); })
            .catch(e => showAlert('error', 'Error: ' + e));
    }

    function saveGatewayConfig() {
        const data = { deviceId: document.getElementById('deviceId').value, macAddress: document.getElementById('macAddress').value };
        fetch('/save/gateway', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(data) })
            .then(r => r.text()).then(d => { showAlert('success', d); document.getElementById('dashDeviceId').textContent = data.deviceId; document.getElementById('dashMac').textContent = data.macAddress; })
            .catch(e => showAlert('error', 'Error: ' + e));
    }

    function saveNetworkConfig() {
        const data = { networkNumber: document.getElementById('networkNumber').value, baudRate: document.getElementById('baudRate').value };
        fetch('/save/network', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(data) })
            .then(r => r.text()).then(d => { showAlert('success', d); document.getElementById('dashNet').textContent = data.networkNumber; document.getElementById('dashBaud').textContent = data.baudRate + ' bps'; })
            .catch(e => showAlert('error', 'Error: ' + e));
    }

    function saveWiFiConfig() {
        const data = { apSsid: document.getElementById('apSsid').value, apPassword: document.getElementById('apPassword').value, ipOctet: document.getElementById('ipOctet').value };
        fetch('/save/wifi', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(data) })
            .then(r => r.text()).then(d => showAlert('success', d))
            .catch(e => showAlert('error', 'Error: ' + e));
    }

    function refreshDevices() {
        fetch('/devices').then(r => r.json()).then(devices => {
            const table = document.getElementById('devicesTable');
            let onlineCount = 0;
            if (!devices || devices.length === 0) {
                table.innerHTML = '<tr><td colspan="6" style="text-align:center">No devices yet</td></tr>';
                document.getElementById('onlineCount').textContent = 0;
                document.getElementById('totalDevices').textContent = 0;
                return;
            }
            let html = '';
            devices.forEach(d => {
                if (d.online) onlineCount++;
                html += `<tr>
                    <td>${d.device_id}</td><td>${d.device_name}</td>
                    <td>${d.mac_address}</td><td>${d.network_number}</td>
                    <td>${d.vendor_id}</td>
                    <td><span class="${d.online ? 'online-badge' : 'offline-badge'}">${d.online ? 'ONLINE' : 'OFFLINE'}</span></td>
                </tr>`;
            });
            table.innerHTML = html;
            document.getElementById('onlineCount').textContent = onlineCount;
            document.getElementById('totalDevices').textContent = devices.length;
        });
    }

    function clearDevices() {
        if (confirm('Clear all devices?')) {
            fetch('/clear/devices').then(r => r.text()).then(d => { showAlert('success', d); refreshDevices(); });
        }
    }

    function clearMonitor() { document.getElementById('dataMonitor').textContent = ''; fetch('/clear'); }
    function refreshMonitor() { fetch('/data').then(r => r.text()).then(d => document.getElementById('dataMonitor').textContent = d); }

    function restartGateway() {
        if (confirm('Restart gateway?')) { fetch('/restart'); showAlert('success', 'Restarting...'); }
    }

    setInterval(refreshDevices, 5000);
    setInterval(refreshMonitor, 2000);
</script>
</body>
</html>
)=====";

// ========== WEB SERVER SETUP ==========
void setupWebServer() {
  server.on("/", HTTP_GET, []() {
    String html = String(INDEX_HTML);
    html.replace("%IP_ADDRESS%",    "192.168.4." + String(config.ip_octet4));
    html.replace("%DEVICE_ID%",     String(config.device_id));
    html.replace("%MAC_ADDRESS%",   String(config.device_mac));
    html.replace("%NETWORK_NUMBER%",String(config.network_number));
    html.replace("%BAUD_RATE%",     String(config.baud_rate));
    html.replace("%AP_SSID%",       String(config.ap_ssid));
    html.replace("%AP_PASSWORD%",   String(config.ap_password));
    html.replace("%IP_OCTET%",      String(config.ip_octet4));
    html.replace("%RECEIVED_DATA%", receivedData.length() > 0 ? receivedData : "Waiting for data...");
    server.send(200, "text/html", html);
  });

  server.on("/save/gateway", HTTP_POST, []() {
    String body = server.arg("plain");
    int idStart  = body.indexOf("\"deviceId\":") + 11;
    int idEnd    = body.indexOf(",", idStart); if (idEnd  == -1) idEnd  = body.indexOf("}", idStart);
    int macStart = body.indexOf("\"macAddress\":") + 13;
    int macEnd   = body.indexOf(",", macStart); if (macEnd == -1) macEnd = body.indexOf("}", macStart);

    if (idStart > 11 && macStart > 13) {
      String newId  = body.substring(idStart, idEnd);   newId.trim();
      String newMac = body.substring(macStart, macEnd); newMac.trim();
      config.device_id  = newId.toInt();
      config.device_mac = newMac.toInt();
      saveConfig();
      server.send(200, "text/plain", "Gateway configuration saved");
    } else {
      server.send(400, "text/plain", "Invalid data format");
    }
  });

  server.on("/save/network", HTTP_POST, []() {
    String body = server.arg("plain");
    int netStart  = body.indexOf("\"networkNumber\":") + 16;
    int netEnd    = body.indexOf(",", netStart); if (netEnd == -1) netEnd = body.indexOf("}", netStart);
    int baudStart = body.indexOf("\"baudRate\":") + 11;
    if (body.charAt(baudStart) == '"') baudStart++;
    int baudEnd = body.indexOf(",", baudStart);
    if (baudEnd == -1) baudEnd = body.indexOf("}", baudStart);
    if (baudEnd == -1) baudEnd = body.indexOf("\"", baudStart);

    if (netStart > 16 && baudStart > 11) {
      String newNet  = body.substring(netStart, netEnd);   newNet.trim();
      String newBaud = body.substring(baudStart, baudEnd); newBaud.trim(); newBaud.replace("\"","");

      config.network_number = newNet.toInt();
      config.baud_rate      = newBaud.toInt();
      if (config.baud_rate != 9600 && config.baud_rate != 19200 &&
          config.baud_rate != 38400 && config.baud_rate != 76800)
        config.baud_rate = 38400;

      RS485Serial.updateBaudRate(config.baud_rate);
      saveConfig();
      server.send(200, "text/plain", "Network configuration saved");
    } else {
      server.send(400, "text/plain", "Invalid data format");
    }
  });

  server.on("/save/wifi", HTTP_POST, []() {
    String body = server.arg("plain");
    int ssidStart = body.indexOf("\"apSsid\":\"") + 10;
    int ssidEnd   = body.indexOf("\"", ssidStart);
    int passStart = body.indexOf("\"apPassword\":\"") + 14;
    int passEnd   = body.indexOf("\"", passStart);
    int ipStart   = body.indexOf("\"ipOctet\":") + 10;
    int ipEnd     = body.indexOf(",", ipStart); if (ipEnd == -1) ipEnd = body.indexOf("}", ipStart);

    if (ssidStart > 10 && passStart > 14 && ipStart > 10) {
      String newSsid = body.substring(ssidStart, ssidEnd);
      String newPass = body.substring(passStart, passEnd);
      String newIp   = body.substring(ipStart, ipEnd); newIp.trim();

      newSsid.toCharArray(config.ap_ssid,     32);
      newPass.toCharArray(config.ap_password,  32);
      config.ip_octet4 = newIp.toInt();
      saveConfig();
      server.send(200, "text/plain", "WiFi saved. Restart to apply.");
    } else {
      server.send(400, "text/plain", "Invalid data format");
    }
  });

  server.on("/devices", HTTP_GET, []() {
    server.send(200, "application/json", getDevicesJSON());
  });

  server.on("/clear/devices", HTTP_GET, []() {
    memset(discovered_devices, 0, sizeof(discovered_devices));
    discovered_count = 0;
    server.send(200, "text/plain", "Devices cleared");
  });

  server.on("/cmd", HTTP_GET, []() {
    String cmd = server.arg("c");
    String response = "Unknown command";

    if (cmd == "test") {
      uint8_t testFrame[] = {0x55, 0xAA, 0x55, 0xAA};
      sendRS485(testFrame, sizeof(testFrame));
      response = "Test frame sent";
    } else if (cmd == "whois") {
      sendBACnetWhoIs();
      response = "Who-Is sent";
    } else if (cmd == "iam") {
      sendBACnetIAm();
      response = "I-Am sent";
    }

    server.send(200, "text/plain", response);
  });

  server.on("/restart", HTTP_GET, []() {
    server.send(200, "text/plain", "Restarting...");
    delay(100);
    ESP.restart();
  });

  server.on("/data", HTTP_GET, []() {
    server.send(200, "text/plain", receivedData);
  });

  server.on("/clear", HTTP_GET, []() {
    receivedData = "";
    server.send(200, "text/plain", "Cleared");
  });

  server.begin();
}

// ========== SETUP ==========
void setup() {
  Serial.begin(115200);
  delay(1000);

  Serial.println("\n╔══════════════════════════════════════════╗");
  Serial.println("║   BACnet MSTP Gateway - FIXED VERSION   ║");
  Serial.println("║  Token Bus + Binary-Safe RS485 + NPDU   ║");
  Serial.println("╚══════════════════════════════════════════╝\n");

  EEPROM.begin(sizeof(Config));
  loadConfig();

  // RS485 setup
  pinMode(RS485_EN_PIN, OUTPUT);
  digitalWrite(RS485_EN_PIN, LOW);  // Start in receive mode
  RS485Serial.begin(config.baud_rate, SERIAL_8N1, RS485_RX_PIN, RS485_TX_PIN);

  setupNetwork();
  setupWebServer();

  Serial.println("BACnet Gateway Ready!");
  Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  Serial.print("  Device ID:  "); Serial.println(config.device_id);
  Serial.print("  MAC:        "); Serial.println(config.device_mac);
  Serial.print("  Network:    "); Serial.println(config.network_number);
  Serial.print("  Baud Rate:  "); Serial.println(config.baud_rate);
  Serial.print("  IP:         192.168.4."); Serial.println(config.ip_octet4);
  Serial.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

  // Initial announcement after bus settles
  delay(2000);
  Serial.println("Sending initial I-Am...");
  sendBACnetIAm();
  delay(500);
  Serial.println("Sending initial Who-Is...");
  sendBACnetWhoIs();
}

// ========== MAIN LOOP ==========
void loop() {
  server.handleClient();

  // Binary-safe RS485 read  <-- FIX
  uint8_t buf[600];
  int len = readRS485(buf, sizeof(buf));

  if (len > 0) {
    lastDataTime = millis();

    // Format hex for web monitor (first 50 bytes)
    String formatted = "[" + String(millis() / 1000) + "s] ";
    for (int i = 0; i < len && i < 50; i++) {
      char hex[4];
      sprintf(hex, "%02X ", buf[i]);
      formatted += hex;
    }
    receivedData = formatted + "\n" + receivedData;

    // Keep last 30 lines
    int lines = 0;
    for (size_t i = 0; i < receivedData.length(); i++) {
      if (receivedData[i] == '\n') lines++;
      if (lines > 30) { receivedData = receivedData.substring(0, i); break; }
    }

    processBACnetFrame(buf, len);
  }

  delay(5);  // Slightly tighter loop for better frame response time
}
