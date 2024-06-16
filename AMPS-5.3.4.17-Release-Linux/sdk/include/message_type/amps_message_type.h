////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2012-2024 60East Technologies Inc., All Rights Reserved.
//
// This computer software is owned by 60East Technologies Inc. and is
// protected by U.S. copyright laws and other laws and by international
// treaties.  This computer software is furnished by 60East Technologies
// Inc. pursuant to a written license agreement and may be used, copied,
// transmitted, and stored only in accordance with the terms of such
// license agreement and with the inclusion of the above copyright notice.
// This computer software or any other copies thereof may not be provided
// or otherwise made available to any other person.
//
// U.S. Government Restricted Rights.  This computer software: (a) was
// developed at private expense and is in all respects the proprietary
// information of 60East Technologies Inc.; (b) was not developed with
// government funds; (c) is a trade secret of 60East Technologies Inc.
// for all purposes of the Freedom of Information Act; and (d) is a
// commercial item and thus, pursuant to Section 12.212 of the Federal
// Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc.
//
////////////////////////////////////////////////////////////////////////////

#ifndef _AMPS_SDK_MESSAGE_TYPE_AMPS_MESSAGE_TYPE_H_
#define _AMPS_SDK_MESSAGE_TYPE_AMPS_MESSAGE_TYPE_H_

#include <amps_common.h>
#include <stdint.h>
#include <cstddef>
#include <string>
#include <string.h>

///
/// @file message_type/amps_message_type.h
/// @brief Core type and function declarations for the AMPS SDK Message Type Plugin API.
///


/// Identifiers for the type of an message constructed by AMPS.

#define AMPS_MESSAGE_BODY_GENERIC       0
#define AMPS_MESSAGE_BODY_CLIENT_STATUS 1
#define AMPS_MESSAGE_BODY_STOP_TIMER    2
#define AMPS_MESSAGE_BODY_SOW_STATS     3
#define AMPS_MESSAGE_BODY_VIEW          4
#define AMPS_MESSAGE_BODY_DELTA         5

///
/// Identifiers for the value of the 'event' field of a an AMPS 'Client
/// Status' message.
///
#define AMPS_CLIENT_STATUS_LOGON        0
#define AMPS_CLIENT_STATUS_CONNECT      1
#define AMPS_CLIENT_STATUS_DISCONNECT   2
#define AMPS_CLIENT_STATUS_AUTH_FAILURE 3
#define AMPS_CLIENT_STATUS_SUBSCRIBE    4
#define AMPS_CLIENT_STATUS_UNSUBSCRIBE  5
#define AMPS_CLIENT_STATUS_SOW          6
#define AMPS_CLIENT_STATUS_SOW_DELETE   7

///
/// Identifiers for the values of an AMPS 'Stop Timer' message.
///
#define AMPS_STOP_TIMER_ELAPSED_TIME_FIELD_ID       0
#define AMPS_STOP_TIMER_MESSAGE_COUNT_FIELD_ID      1
#define AMPS_STOP_TIMER_BYTE_COUNT_FIELD_ID         2
#define AMPS_STOP_TIMER_MSGS_PER_SEC_FIELD_ID       3
#define AMPS_STOP_TIMER_MEGA_BYTES_PER_SEC_FIELD_ID 4
#define AMPS_STOP_TIMER_AVG_MSG_SIZE_FIELD_ID       5
#define AMPS_STOP_TIMER_MATCH_COUNT_FIELD_ID        6
#define AMPS_STOP_TIMER_MIN_LATENCY_FIELD_ID        7
#define AMPS_STOP_TIMER_MAX_LATENCY_FIELD_ID        8
#define AMPS_STOP_TIMER_MEAN_LATENCY_FIELD_ID       9
#define AMPS_STOP_TIMER_MEDIAN_LATENCY_FIELD_ID     10
#define AMPS_STOP_TIMER_90TH_LATENCY_FIELD_ID       11
#define AMPS_STOP_TIMER_95TH_LATENCY_FIELD_ID       12
#define AMPS_STOP_TIMER_99TH_LATENCY_FIELD_ID       13
#define AMPS_STOP_TIMER_STD_DEVIATION_FIELD_ID      14

///
/// Identifiers for the values of an AMPS 'Client Status' message.
///
#define AMPS_CLIENT_STATUS_TIMESTAMP_FIELD_ID     15
#define AMPS_CLIENT_STATUS_EVENT_FIELD_ID         16
#define AMPS_CLIENT_STATUS_CLIENT_NAME_FIELD_ID   17
#define AMPS_CLIENT_STATUS_CONN_NAME_FIELD_ID     18
#define AMPS_CLIENT_STATUS_CORRELATIONID_FIELD_ID 19
#define AMPS_CLIENT_STATUS_TOPIC_FIELD_ID         20
#define AMPS_CLIENT_STATUS_FILTER_FIELD_ID        21
#define AMPS_CLIENT_STATUS_OPTIONS_FIELD_ID       22
#define AMPS_CLIENT_STATUS_SUBID_FIELD_ID         23
#define AMPS_CLIENT_STATUS_QUERYID_FIELD_ID       24
// -- NOTE: Additional fields have been added, see below

///
/// Identifiers for the values of an AMPS 'SOW Stats' message.
///
#define AMPS_SOW_STATS_TIMESTAMP_FIELD_ID       25
#define AMPS_SOW_STATS_TOPIC_FIELD_ID           26
#define AMPS_SOW_STATS_RECORDS_FIELD_ID         27
// LEGACY FIELD IDS FOR VERSION 2 COMPLIANCE
#define AMPS_SOW_STATS_TIMESTAMP_V2_FIELD_ID    28
#define AMPS_SOW_STATS_TOPIC_V2_FIELD_ID        29
#define AMPS_SOW_STATS_RECORDS_V2_FIELD_ID      30

// Additional client status fields
#define AMPS_CLIENT_STATUS_CLIENTADDRESS_FIELD_ID 31
#define AMPS_CLIENT_STATUS_AUTHID_FIELD_ID        32

// Additional SOW stats fields
#define AMPS_SOW_STATS_MESSAGE_TYPE_FIELD_ID 33

// Additional client status fields
#define AMPS_CLIENT_STATUS_ENTITLEMENT_FILTER_FIELD_ID 34
#define AMPS_CLIENT_STATUS_REASON_FIELD_ID             35

extern "C"
{
  /// the type of xpath hash used to locate xpaths in the find and value sets
  typedef uint64_t amps_message_xpath_hash;

  /// Structure that holds the value for a given xpath. The value can be
  /// a string, an unsigned integer, a signed integer or a double
  /// value. The structure can hold a string representation of the
  /// value, a numeric value or both a string representation and a
  /// numeric value. The type_hint is set during parsing to help
  /// identify the type of the value. AMPS will use the type_hint to
  /// speed up conversions to and from a string representation.
  struct amps_message_xpath_value
  {
    /// hint of what the type might be if represented by a string
    uint32_t    type_hint;
    /// value mask that identifies the actual type of the value union
    uint32_t    value_mask;
    /// pointer to the start of string (null if there is not string rep of the xpath value)
    const char* string_rep;
    /// length of the string
    size_t      string_rep_length;
    /// pointer to the start of the raw bytes
    const char* raw_bytes;
    /// length of the raw bytes.
    size_t      raw_bytes_length;
    /// union to hold the value of non-string types
    union
    {
      /// holds the value as a unsigned long (see value_mask)
      unsigned long  u;
      /// holds the value as a signed long (see value_mask)
      long           l;
      /// holds the value as a double (see value_mask)
      double         f;
      /// holds the value as a boolean (see value_mask)
      bool           b;
    } value;
  };

  /// Structure that contains the name and value for a named value in an
  /// AMPS output message (Client Status, Stop Timer and SOW Stats).  An
  /// array of this structure is passed to the message type serialize
  /// message function so that a message can be serialized in the format
  /// of the message type.
  struct amps_message_value
  {
    /// pointer to the start of the path string
    const char*              path;
    /// length of the path
    size_t                   path_length;
    /// a hash of the xpath string
    amps_message_xpath_hash  path_hash;
    /// the field id for one of the AMPS messages
    uint32_t                 stock_tag;
    /// the depth of this element
    uint32_t                 depth;
    /// Flags on this value
    uint32_t                 flags;
    /// the value for the field id
    amps_message_xpath_value xpath_value;
  };

  inline void amps_message_value_init(amps_message_value* pMessageValue_)
  {
    pMessageValue_->path = nullptr;
    pMessageValue_->path_length = 0;
    pMessageValue_->path_hash = 0;
    pMessageValue_->stock_tag = 0;
    pMessageValue_->depth = 0;
    pMessageValue_->flags = 0;
    pMessageValue_->xpath_value.type_hint = 0;
    pMessageValue_->xpath_value.value_mask = 0;
    pMessageValue_->xpath_value.string_rep = nullptr;
    pMessageValue_->xpath_value.string_rep_length = 0;
    pMessageValue_->xpath_value.raw_bytes = 0;
    pMessageValue_->xpath_value.raw_bytes_length = 0;
    pMessageValue_->xpath_value.value.u = 0;
  }

  /// A list of values for a message.
  struct amps_message_value_list
  {
    /// the number of values in the list
    size_t              count;
    /// pointer to the first value
    amps_message_value* values;
  };

  ///
  /// Valid values for the 'type_hint' field of amps_message_xpath_value.
  ///

  enum amps_message_type_constants : uint32_t
  {
    /// indicates that the value is a string and no type hint exists
    amps_message_xpath_value_type_string             = 0x0,
    /// indicates that the value might be a double value
    amps_message_xpath_value_type_double             = 0x1,
    /// indicates that the value might be an integer value
    amps_message_xpath_value_type_int                = 0x2,
    /// indicates that the value might be a bool value
    amps_message_xpath_value_type_bool               = 0x4,
    /// indicates that this value contains a sub-document or child elements
    amps_message_xpath_value_type_document           = 0xF0,
    /// indicates that this value contains an array
    amps_message_xpath_value_type_array              = 0xD0,

    ///
    /// Valid values for the 'value_mask' field of amps_message_xpath_value.
    ///

    amps_message_xpath_value_mask_string             = 0x0,
    /// indicates that the value union has the 'f' field set to a valid value
    amps_message_xpath_value_mask_double             = 0x1,
    /// indicates that the value union has the 'l' field set to a valid value
    amps_message_xpath_value_mask_long               = 0x2,
    /// indicates that the value union has the 'u' field set to a valid value
    amps_message_xpath_value_mask_ulong              = 0x4,
    /// indicates that the value union has the 'b' field set to a valid value
    amps_message_xpath_value_mask_bool               = 0x8,
    amps_message_xpath_value_mask_null               = 0x10,

    amps_message_xpath_value_mask_invalid_conversion = 0x0,
    /// indicates that the value union has the 'f' field set as a result of a conversion.
    amps_message_xpath_value_mask_double_conversion  = 0x8,
    /// indicates that the value union has the 'l' field set as a result of a conversion.
    amps_message_xpath_value_mask_long_conversion    = 0x10,
    /// indicates that the value union has the 'u' field set as a result of a conversion.
    amps_message_xpath_value_mask_ulong_conversion   = 0x20,

    amps_message_xpath_value_mask_string_to_numeric  = 0x40,
    amps_message_xpath_value_mask_long_to_string     = 0x80,
    amps_message_xpath_value_mask_ulong_to_string    = 0x100,
    amps_message_xpath_value_mask_double_to_string   = 0x200,
    amps_message_xpath_value_mask_is_memoized        = 0x400,
    /// indicates that the value was generated from enrichment instead of parsing
    amps_message_xpath_value_mask_is_enrichment      = 0x800,

    ///
    /// Valid values for the amps_message_value.flags bitmask.
    ///

    /// Indicates that the value should be regarded as an attribute.
    /// This flag is set for values whose XPath is an "@". (e.g. "/Orders/@Status")
    ///
    amps_message_value_flags_attribute = 0x1,
    amps_message_value_flags_binary    = 0x8
  };

/// \defgroup macros Convenience MACROS to set the initialize an amps_message_xpath_value with a type and value
///  set the value of a stock tag that contains a value of string type
#define AMPS_MESSAGE_SET_VALUE_STOCK_STRING(V, STK, S, SL) \
  (V).path = 0; \
  (V).path_length = 0; \
  (V).path_hash = 0; \
  (V).stock_tag = STK; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_string; \
  (V).xpath_value.value_mask = amps_message_xpath_value_type_string; \
  (V).xpath_value.string_rep = (S); \
  (V).xpath_value.string_rep_length = (SL); \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.l = 0;
/// \ingroup macros
///  set the value of a stock tag that contains a value of long type
#define AMPS_MESSAGE_SET_VALUE_STOCK_LONG(V, STK, L) \
  (V).path = 0; \
  (V).path_length = 0; \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).stock_tag = STK; \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_int; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_long; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0; \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.l = (L);
/// \ingroup macros
///  set the value of a stock tag that contains a value of ulong type
#define AMPS_MESSAGE_SET_VALUE_STOCK_ULONG(V, STK, U) \
  (V).path = 0; \
  (V).path_length = 0; \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).stock_tag = STK; \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_int; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_ulong; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0; \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.u = (U);
/// \ingroup macros
///  set the value of a stock tag that contains a value of double type
#define AMPS_MESSAGE_SET_VALUE_STOCK_DOUBLE(V, STK, D) \
  (V).path = 0; \
  (V).path_length = 0; \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).stock_tag = STK; \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_double; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_double; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.f = (D);
/// \ingroup macros
////  set the value to be null
#define AMPS_MESSAGE_SET_VALUE_DOCUMENT(V, T, TL) \
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_document; \
  (V).xpath_value.value_mask = amps_message_xpath_value_type_document; \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.string_rep_length = 0;
////  set the value to be null
#define AMPS_MESSAGE_SET_VALUE_ARRAY(V, T, TL) \
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_array; \
  (V).xpath_value.value_mask = amps_message_xpath_value_type_array; \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.string_rep_length = 0;
/// Just set the tag of a value, no type or data information yet.
#define AMPS_MESSAGE_SET_VALUE_TAG(V, T, TL) \
  (V).depth = (V).flags = 0;\
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).path_hash = 0;
#define AMPS_MESSAGE_SET_VALUE_NULL(V, T, TL) \
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_string; \
  (V).xpath_value.value_mask = amps_message_xpath_value_type_string; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0; \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.l = 0;
///  set the value of a stock tag that contains a value of double type
#define AMPS_MESSAGE_SET_VALUE_STRING(V, T, TL, S, SL) \
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_string; \
  (V).xpath_value.value_mask = amps_message_xpath_value_type_string; \
  (V).xpath_value.string_rep = (S); \
  (V).xpath_value.string_rep_length = (SL); \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.l = 0;
/// \ingroup macros
///  set the value of a tag that contains a value of string type
#define AMPS_MESSAGE_SET_VALUE_LONG(V, T, TL, L) \
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_int; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_long; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.l = (L);
/// \ingroup macros
///  set the value of a tag that contains a value of string type
#define AMPS_MESSAGE_SET_VALUE_ULONG(V, T, TL, U) \
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_int; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_ulong; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.u = (U);
/// \ingroup macros
///  set the value of a tag that contains a value of double type
#define AMPS_MESSAGE_SET_VALUE_DOUBLE(V, T, TL, D) \
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_double; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_double; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.f = (D);
/// \ingroup macros
///  set the value of a tag that contains a value of bool type
#define AMPS_MESSAGE_SET_VALUE_BOOL(V, T, TL, B) \
  (V).path = (T); \
  (V).path_length = (TL); \
  (V).path_hash = 0; \
  (V).depth = (V).flags = 0;\
  (V).xpath_value.type_hint = amps_message_xpath_value_type_bool; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_bool; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.b = (B);
///  set the value of a stock tag that contains a NULL
#define AMPS_MESSAGE_UPDATE_VALUE_NULL(V) \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_string; \
  (V).xpath_value.value_mask = amps_message_xpath_value_type_string; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0; \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.l = 0;
///  set the value of a stock tag that contains a value of string type
#define AMPS_MESSAGE_UPDATE_VALUE_STRING(V, S, SL) \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_string; \
  (V).xpath_value.value_mask = amps_message_xpath_value_type_string; \
  (V).xpath_value.string_rep = (S); \
  (V).xpath_value.string_rep_length = (SL); \
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.l = 0;
/// \ingroup macros
///  set the value of a tag that contains a value of long integer type
#define AMPS_MESSAGE_UPDATE_VALUE_LONG(V, L) \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_int; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_long; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.l = (L);
/// \ingroup macros
///  set the value of a tag that contains a value of long integer type
#define AMPS_MESSAGE_UPDATE_VALUE_ULONG(V, U) \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_int; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_ulong; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.u = (U);
/// \ingroup macros
///  set the value of a tag that contains a value of double type
#define AMPS_MESSAGE_UPDATE_VALUE_DOUBLE(V, D) \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_double; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_double; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.f = (D);
/// \ingroup macros
///  set the value of a tag that contains a value of bool type
#define AMPS_MESSAGE_UPDATE_VALUE_BOOL(V, B) \
  (V).xpath_value.type_hint = amps_message_xpath_value_type_bool; \
  (V).xpath_value.value_mask = amps_message_xpath_value_mask_bool; \
  (V).xpath_value.string_rep = 0; \
  (V).xpath_value.string_rep_length = 0;\
  (V).xpath_value.raw_bytes = 0; \
  (V).xpath_value.raw_bytes_length = 0; \
  (V).xpath_value.value.b = (B);


  ///
  /// \brief Represents a single value or an array within a message.
  ///
  /// A message value set is an associative structure that maps xpath
  /// hash values to value set entries and is produced inside the
  /// message type plugin 'parse_message' function. The value set entry
  /// structure maintains the data for a single xpath.
  ///
  /// Value set entries are typed. A value may be a string, an integer
  /// or a floating point value. In addition, the value might be an
  /// array of values and thus the array, last and array_count members
  /// are used to identify that a given xpath value is an array and
  /// informs how the array values can be navigated for retrieval within
  /// the value set. Finally, the value might be an escaped string and
  /// the 'escaping' string will be maintained in a special escaped
  /// value buffer. The string_rep offset and length members identify
  /// the boundaries of the a string rep for the value. This can be the
  /// string rep of long/double or an unescaped string.
  ///

  enum amps_message_value_constats : uint32_t
  {
    amps_message_value_invalid_index              = ~0U,
    amps_message_value_array_child                = ~1U,

    amps_message_value_set_entry_flag_string_rep  = 0x1,
    amps_message_value_set_entry_flag_attribute   = 0x2,
    amps_message_value_set_entry_flag_required    = 0x4,
    amps_message_value_set_entry_flag_binary      = 0x8,
    amps_message_value_set_entry_flag_array       = 0x10,
    amps_message_value_set_entry_flag_empty_array = 0x20,
    amps_message_value_set_entry_flag_string      = 0x40,
  };

  struct amps_message_value_set_entry
  {
    ///
    /// the hash of the xpath
    amps_message_xpath_hash hash;
    ///
    /// a value mask that indicates what data members are valid
    mutable uint32_t   value_mask;
    ///
    /// a set of flags to indicate various properties of the data
    uint16_t           value_flags;
    ///
    /// a hint to the type of the value
    uint16_t           type_hint;
    union
    {
      ///
      /// holds the value as an unsigned long integer
      mutable unsigned long u;
      ///
      /// holds the value as a signed long integer
      mutable long          l;
      ///
      /// holds the value as a double
      mutable double        d;
    };
    ///
    /// the pointer to bytes of the value for the xpath
    const char*        raw_bytes;
    ///
    /// the pointer to bytes of the tag for the xpath (message type dependent)
    const char*        tag_bytes;
    ///
    /// the  pointer to bytes of the string rep for the value (set within AMPS after insert into xpath value set)
    const char*        string_rep;
    ///
    /// the length of the value data
    uint32_t           raw_bytes_length;
    ///
    /// the length of the tag  (message type dependent)
    uint32_t           tag_length;
    ///
    /// the length of the string rep (set within AMPS after insert into xpath value set)
    uint32_t           string_rep_length;
    ///
    /// index+1 of next el in array
    uint32_t           array;
    ///
    /// the last index in the array
    uint32_t           last;
    ///
    /// the number of items in the array for this hash
    uint32_t           array_count;
    ///
    /// the first element of a map will contain the map count
    uint32_t           map_count;
    ///
    /// index of parent value
    uint32_t           parent;
    ///
    /// index of first child
    uint32_t           first_child;
    ///
    /// index of next sibling
    uint32_t           next_sibling;
  };

  inline bool amps_message_value_set_entry_is_enrichment(const amps_message_value_set_entry* pValueSetEntry_)
  {
    return (pValueSetEntry_->value_mask & amps_message_xpath_value_mask_is_enrichment);
  }

  /// AMPS convenience function to check if a value set entry value is
  /// null
  ///
  /// \param pValueSetEntry_ a pointer to the value set entry to be checked
  /// \returns true if null otherwise false
  inline bool amps_message_value_set_entry_is_null(const amps_message_value_set_entry* pValueSetEntry_)
  {
    return (!pValueSetEntry_)
      || (!pValueSetEntry_->hash
          && !pValueSetEntry_->raw_bytes
          && !pValueSetEntry_->raw_bytes_length);
  }

  /// AMPS convenience function to check if two value set entry values are equal
  ///
  /// \param pValueData1_     a pointer to the input data for the entry set value 1
  /// \param pValueSetEntry1_ a pointer to a value set entry
  /// \param pValueData2_     a pointer to the input data for the entry set value 2
  /// \param pValueSetEntry2_ a pointer to a value set entry
  /// \returns true if the value set entries are equal otherwise false
  inline bool amps_message_value_set_entry_equal(const amps_message_value_set_entry* pValueSetEntry1_,
                                                 const amps_message_value_set_entry* pValueSetEntry2_)
  {
    return ((!pValueSetEntry1_->hash
             && !pValueSetEntry1_->raw_bytes
             && !pValueSetEntry1_->raw_bytes_length)
            || (!pValueSetEntry2_->hash
                && !pValueSetEntry2_->raw_bytes
                && !pValueSetEntry2_->raw_bytes_length)
            || (pValueSetEntry1_->raw_bytes_length != pValueSetEntry2_->raw_bytes_length)
            || (__builtin_memcmp(pValueSetEntry1_->raw_bytes, pValueSetEntry2_->raw_bytes, pValueSetEntry1_->raw_bytes_length))) ?
      false :
      true;
  }

  /// AMPS convenience function to set a value set entry to a long.
  ///
  /// \param pEntry_         the entry to set to a long
  /// \param value_          the long value to set
  ///
  inline void amps_message_value_set_entry_set_long(amps_message_value_set_entry* pEntry_, long value_)
  {
    pEntry_->l = value_;
    pEntry_->value_mask = amps_message_xpath_value_mask_long | amps_message_xpath_value_mask_long_conversion;
    pEntry_->type_hint  = amps_message_xpath_value_type_int;
  }

  /// AMPS convenience function to set a value set entry to a long.
  ///
  /// \param pEntry_         the entry to set to a ulong
  /// \param value_          the long value to set
  ///
  inline void amps_message_value_set_entry_set_ulong(amps_message_value_set_entry* pEntry_, unsigned long value_)
  {
    pEntry_->u = value_;
    pEntry_->value_mask = amps_message_xpath_value_mask_ulong | amps_message_xpath_value_mask_ulong_conversion;
    pEntry_->type_hint  = amps_message_xpath_value_type_int;
  }

  /// AMPS convenience function to set a value set entry to a double.
  ///
  /// \param pEntry_         the entry to set to a double
  /// \param value_          the long value to set
  ///
  inline void amps_message_value_set_entry_set_double(amps_message_value_set_entry* pEntry_, double value_)
  {
    pEntry_->d = value_;
    pEntry_->value_mask = amps_message_xpath_value_mask_double | amps_message_xpath_value_mask_double_conversion;
    pEntry_->type_hint  = amps_message_xpath_value_type_double;
  }

  /// opaque handle to an xpath list
  typedef void* amps_message_xpath_list;
  /// opaque handle to an xpath find set
  typedef void* amps_message_xpath_find_set;
  /// opaque handle to an xpath value set
  typedef void* amps_message_xpath_value_set;

  /// a data structure representing a non-null terminated string
  struct amps_message_string
  {
    /// pointer to the start of the string data
    const char* data;
    /// length of the string data
    size_t      length;
  };

  /// contains a message to be serialized
  struct amps_output_message_data
  {
    /// the input message, if any
    amps_message_segment            message;
    /// the serialized output message
    amps_message_segment            data;
    /// the list of values to serialize to the message
    const amps_message_value_list*  value_list;
    /// the type of the message, one of the AMPS_MESSAGE_BODY_* values
    uint32_t                        body_type;
    /// the serialized size of the message. This is computed by
    /// compute_serialize_size and reset if the segment buffer is too
    /// small.
    uint32_t                        serialize_size;
    /// the message type context for this message
    amps_message_type_context       message_type_context;
    /// the module context for this message
    amps_context                    context;
    /// for delta messages, the source (original) message values, NULL otherwise
    amps_message_xpath_value_set    source_value_set;
    /// for delta messages, the update message values, NULL otherwise
    amps_message_xpath_value_set    update_value_set;
  };

  struct amps_delta_message_data
  {
    /// the resulting delta message will be written into output
    amps_message_segment                   output;
    /// the number of items in the resulting output
    size_t                                 output_count;
    /// the update message (typically the input publish message)
    const amps_message_string*             update_data;
    /// the source message data (typically the sow data)
    const amps_message_string*             source_data;
    /// the value set assocated with the update message
    amps_message_xpath_value_set           update_value_set;
    /// the value set assocated with the source message
    amps_message_xpath_value_set           source_value_set;
    /// the message type context
    amps_message_type_context              message_type_context;
    /// AMPS context - only set and used by AMPS
    amps_context                           context;
    /// reserve function that can be used to 'reallocate' the output segment
    amps_message_segment_reserve_function  reserve;
  };

  int amps_select_list_check(amps_select_list_handle,
                             const char*,
                             size_t);

  /// Reserves space for a delta message. After calling this function, the
  /// message will contain at least the number of segments specified.
  ///
  /// \param pMessage_ the message to reserve space in
  /// \param len_ the number of segments to reserve
  ///
  inline void amps_delta_message_data_reserve(amps_delta_message_data* pMessage_, size_t byteCount_)
  {
    amps_message_segment* pSegment = &pMessage_->output;
    if (pSegment->byte_count + byteCount_ < pSegment->protocol_buffer.allocated_byte_count)
    {
      return;
    }

    uint32_t newByteCount = 2 * pSegment->protocol_buffer.allocated_byte_count;
    if (pSegment->byte_count + byteCount_ >= newByteCount)
    {
      newByteCount += byteCount_;
    }
    pMessage_->reserve(pMessage_->context, pSegment, newByteCount);
  }


  /// typedef for the create_context function
  typedef amps_message_type_context (*amps_message_type_create_context_function)(amps_module_options);

  /// typedef for the destroy_context function
  typedef int (*amps_message_type_destroy_context_function)(amps_message_type_context);

  /// typedef for the get_supported_options function
  typedef int (*amps_message_type_get_supported_options_function)
  (amps_message_type_context, const char**, size_t*);

  /// tyepdef for the serialize_message function
  typedef int (*amps_message_type_serialize_message_function)(amps_output_message_data*);

  /// typedef for the compute_serialize_size function
  typedef int (*amps_message_type_compute_serialize_size_function)(amps_output_message_data*);


  /// typedef for the parse_message function
  typedef int (*amps_message_type_parse_message_function)(amps_input_message*,
                                                          amps_message_xpath_value_set);


  /// typedef for the partial_parse_message function
  typedef int (*amps_message_type_partial_parse_message_function)(amps_input_message*,
                                                                  const amps_message_xpath_find_set,
                                                                  amps_message_xpath_value_set);

  // defined in amps_common.h
  //typedef int (*amps_message_type_select_function)(amps_message_select_data*);

  /// typedef for the delta_publish_merge function
  typedef int (*amps_message_type_delta_publish_merge_function)(amps_delta_message_data*);

  /// typedef for the free_user_data function
  typedef void(*amps_message_type_free_user_data_function)(void*);

  /// handle for a message type module
  typedef uint64_t amps_message_type_impl_handle;

  /// Holds pointers to the functions in a message type module.
  struct amps_message_type_impl
  {
    /// the function called to create a message type context
    amps_message_type_create_context_function         create_context;

    /// the function called to destroy a message type context
    amps_message_type_destroy_context_function        destroy_context;

    /// the function called to compute the size of a message when serialized
    amps_message_type_compute_serialize_size_function compute_serialize_size;

    /// the function called to serialize a message
    amps_message_type_serialize_message_function      serialize_message;

    /// the function called to fully parse a message
    amps_message_type_parse_message_function          parse_message;

    /// the function called to partially parse a message
    amps_message_type_partial_parse_message_function  partial_parse_message;

    /// the function called when AMPS needs to merge messages to process a delta publish
    amps_message_type_delta_publish_merge_function    delta_publish_merge;

    /// the function called when AMPS needs to free user data
    amps_message_type_free_user_data_function         free_user_data;

    /// called when amps needs supported options
    amps_message_type_get_supported_options_function  get_supported_options;

    /// called to trace a raw message
    amps_message_type_trace_function                  trace_message;

    /// called to select fields from a message
    amps_message_type_select_function                 select_message;
  };


// The remainder of this file contains the functions that must be
// defined when implementing a custom message_type.

#if 0

extern "C"
{
  ///
  /// Create a unique message_type instance
  ///
  /// \param options_ from config file are passed in as options
  /// \returns a valid context or NULL
  ///
    amps_message_type_context amps_message_type_create_context_function(const amps_module_options options_);

  ///
  /// Free all resources associated with the message_type context *context_
  ///
  /// \param context_ the context to destroy
  /// \returns AMPS_SUCCESS or AMPS_FAILURE
  ///
    int amps_message_type_destroy_context_function(amps_message_type_context context_);


  ///
  /// Called when AMPS wants to find the supported options for a message type.
  /// Supported options are returned by the message type as a comma-delimited
  /// list of option names. Any option not present in this list is assumed by
  /// AMPS to be unsupported by the message type.
  ///
  /// \param context_ the message type context
  /// \param optionsOut_ a pointer to a pointer to a string containing the
  ///        comma-delimited supported options
  ///
  ///  supported options include:
  ///
  ///   sow_stats:      message type supports serializing the SOWStats message
  ///   delta:          message type supports serializing delta messages
  ///   view:           message type supports serializing view messages
  ///   client_status:  message type supports serializing ClientStatus messages
  ///   stop_timer:     message type supports serializing StopTimer messages
  ///
  /// \param optionsLengthOut_ a pointer to a size_t that specifies the length
  ///        of the returned options string.
  ///
  /// Example:
  /// \code{.c}
  ///   static const char* g_supported_options =
  ///     "sow_stats,delta,client_status,stop_timer"; // all except view
  ///
  ///   int amps_message_type_get_supported_options(
  ///     amps_message_type_context context_,
  ///     const char**              optionsOut_,
  ///     size_t*                   optionsLengthOut_)
  ///   {
  ///     *optionsOut_       = g_supported_options;
  ///     *optionsLengthOut_ = ::strlen(g_supported_options);
  ///   }
  /// \endcode

  ///
  /// Called when AMPS needs to produce a serialized output message. the
  /// output message structure will the values and data to be
  /// serialized.
  ///
  /// \param pOutputMessage_ a pointer to the elements required to serialize an output message
  /// \returns AMPS_SUCCESS or AMPS_FAILURE.
  ///
    int amps_message_type_serialize_message_function(amps_output_message_data* pOutputMessage_);

  ///
  /// Called when AMPS needs to fully parse a message for content filter
  /// matching, sow key generation or delta processing. A full parse of
  /// the message requires that all xpaths present in the input message
  /// have their corresponding xpath values inserted into the value set.
  ///
  /// \param pInputMessage_ a pointer to the input message to parse
  /// \param pValueSet_      the value set to insert the xpath values found during parsing
  /// \returns AMPS_SUCCESS or AMPS_FAILURE.
  ///
    int amps_message_type_parse_message_function(amps_input_message* pInputMessage_,
                                                 amps_message_xpath_value_set pValueSet_);

  ///
  /// Called when AMPS needs to partially parse a message for content
  /// filter matching, sow key generation or delta processing. The
  /// partial parsing function will be given a 'find set' that will
  /// enumerate all of the xpath values that should be inserted into the
  /// value set. It is possible for the parsing to use early termination
  /// after all xpaths in the find set are found expect in situations
  /// where arrays of xpaths make early termination impossible
  /// (dependent on message type specification).
  ///
  /// \param pInputMessage_ a pointer to the input message to parse
  /// \param pFindSet_       a set of xpaths to be searched in the input message
  /// \param pValueSet_      the value set to insert the xpath values found during parsing
  /// \returns AMPS_SUCCESS or AMPS_FAILURE.
  ///
    int amps_message_type_partial_parse_message_function(amps_input_message* pInputMessage_,
                                                         const amps_message_xpath_find_set pFindSet_,
                                                         amps_message_xpath_value_set pValueSet_);

  ///
  /// Merges the fields in the update message with the fields in the
  /// source message and writes the resulting message to the specified
  /// segment. AMPS ensures both message have been fully parsed by the
  /// message type library's parse_message function. AMPS uses the
  /// merged message as the new payload of a SOW record.
  ///
  /// \param pDeltaMessage_ a pointer to the elements required to serialize a delta merge message
  /// \returns the number of bytes written to the segment
  ///
    int amps_message_type_delta_publish_merge_function(amps_delta_message_data* pDeltaMessage_);

  } // extern "C"

#endif // Functional spec only section

  ///
  /// Computes a hash value for the given string.
  ///
  /// \param  pString_ a pointer to the input string. Does not need to be null-terminated.
  /// \param  len_     the length of the input string.
  /// \param baseHash_ the hash for the base of the input string.
  /// \returns the computed hash value for this string.
  ///
  amps_message_xpath_hash amps_message_xpath_hash_string(const char* pString_,
                                                         size_t byteCount_,
                                                         amps_message_xpath_hash baseHash_);

  /// returns an initial base hash for a value set
  ///
  /// \param pValueSet_ the value set where this hash value will be added.
  ///
  amps_message_xpath_hash amps_message_xpath_hash_get_base(amps_message_xpath_value_set pValueSet_);

  ///  adds a separator to the hash value
  ///
  /// \param baseHash_ the hash to add a separator to
  ///
  amps_message_xpath_hash amps_message_xpath_hash_add_separator(amps_message_xpath_hash baseHash_);

  ///
  /// Tests whether the given hash is present in the find set.
  ///
  /// \param pFindSet_ the find set to examine.
  /// \param hash_    the hash to look for in pFindSet_.
  /// \returns 0 if the hash is not present in pFindSet_, non-0 if it is present.
  ///
  int amps_message_xpath_find_set_find(const amps_message_xpath_find_set pFindSet_,
                                       amps_message_xpath_hash hash_);


  ///
  /// Adds the specified path to the find set
  ///
  /// \param pFindSet_ the find set to add to
  /// \param xpath_ the xpath to add
  /// \returns hash value for the supplied XPath.
  ///
  amps_message_xpath_hash
  amps_message_xpath_find_set_add(amps_message_xpath_find_set pFindSet_,
                                  const char* xpath_);

  ///
  /// Adds the specified path to the xpath list
  ///
  /// \param pXPathList_ the xpath list that is appended
  /// \param xpath_ the xpath to add
  /// \returns hash value for the supplied XPath.
  ///
  amps_message_xpath_hash
  amps_message_xpath_list_push_back(amps_message_xpath_list pXPathList_,
                                    const char* xpath_);
  ///
  /// Returns the data pointer for this
  /// amps_message_xpath_value_set. Offsets in this value sets are
  /// offsets from this pointer. Typically, this will be a pointer into
  /// an amps_input_message, but may be changed by message type
  /// libraries to provide extra space for deserialized message values.
  ///
  /// \param pValueSet_ the amps_message_xpath_value_set to retrieve the data pointer from.
  /// \returns the data pointer for this value set.
  ///
  const char* amps_message_xpath_get_value_data(amps_message_xpath_value_set pValueSet_);

  ///
  /// Sets the data pointer for this
  /// amps_message_xpath_value_set. Before doing this, ensure that the
  /// offsets in this value set are still valid offsets into the new
  /// data range you are specifying via this pointer. Used most
  /// typically when implementing merge functions, but may be
  /// called any time extra space is needed, outside of the message, for
  /// storage of keys and values found in a message. Caller is
  /// responsible for ensuring data_ remains valid for the lifetime of
  /// the original data.
  ///
  /// \param pValueSet_ the amps_message_xpath_value_set to set the data pointer on.
  /// \param data_ the new data pointer to use.
  ///
  void amps_message_xpath_set_value_data(amps_message_xpath_value_set pValueSet_, const char* data_);

  ///
  /// Returns a pointer to the beginning the value set array in a
  /// amps_message_xpath_value_set.
  ///
  /// \param pValueSet_ the amps_message_xpath_value_set to iterate.
  /// \returns the first amps_message_value_set_entry in
  ///          pValueSet_. Subsequent entries are adjacent in memory;
  ///          simply increment the pointer to retrieve the next value.
  ///          The point after the end of the array is given by
  ///          amps_message_xpath_end().
  ///
  amps_message_value_set_entry* amps_message_xpath_begin(amps_message_xpath_value_set pValueSet_);

  ///
  /// Returns a pointer to the end of the value set array in a
  /// amps_message_xpath_value_set
  ///
  /// \param pValueSet_ the amps_message_xpath_value_set
  /// \returns a pointer just after the last valid value in pValueSet_.
  ///
  amps_message_value_set_entry* amps_message_xpath_end(amps_message_xpath_value_set pValueSet_);

  ///
  /// Mark the valueSet as supporting hierarchy
  void amps_message_xpath_set_set_is_hierarchical(amps_message_xpath_value_set pValueSet_);
  int amps_message_xpath_set_is_hierarchical(amps_message_xpath_value_set pValueSet_);

  ///
  /// Finds an entry in pValueSet_ by its hash. Typically required when
  /// merging two messages; since each message has been fully parsed,
  /// value set contents may optionally be used to determine the
  /// differences between two messages.
  ///
  /// \param pValueSet_ the value set to search.
  /// \param hash_     the hash value to look for.
  /// \returns index of the found entry or amps_message_value_invalid_index
  ///  if the provided hash_ is not in the pValueSet_
  ///
  uint32_t amps_message_xpath_find_by_hash(amps_message_xpath_value_set pValueSet_,
                                           amps_message_xpath_hash hash_);

  ///
  /// Stores or replaces an entry in an
  /// amps_message_xpath_value_set. Use this function to record body
  /// values found in a parsed message body.
  ///
  /// \param pValueSet_ the value set to update.
  /// \param pFindSet_  the values to be found in the message
  /// \param pEntry_   the entry to add or update in pValueSet_.
  ///                  (note: this function will copy the amps_message_value_set_entry fields.)
  /// \returns index of the added entry
  ///
  uint32_t amps_message_xpath_update(amps_message_xpath_value_set pValueSet_,
                                     const amps_message_xpath_find_set pFindSet_,
                                     amps_message_value_set_entry* pEntry_);

  ///
  /// Retrieves the count of values in pValueSet_.
  ///
  /// \param pValueSet_ the value set
  ///
  /// \returns the count of values in self.
  ///
  uint32_t amps_message_xpath_count(amps_message_xpath_value_set pValueSet_);

  ///
  /// Retrieves an entry in an amps_message_xpath_set. Use this function
  /// to retrieve values found with ::amps_message_xpath_find_by_hash().
  ///
  /// \param pValueSet_ the value set that contains the value
  /// \param index_ the index of the value to get
  /// \returns the entry requested, or NULL if no such entry exists
  ///

  amps_message_value_set_entry* amps_message_xpath_get(amps_message_xpath_value_set pValueSet_, uint32_t index_);

  ///
  /// Clears the entries in an amps_message_xpath_value_set.
  ///
  /// \param pValueSet_ the value set to clear.
  ///
  void amps_message_xpath_clear(amps_message_xpath_value_set pValueSet_);

  ///
  /// Convienience implementation of get_supported options that returns
  ///  all options as supported. No need to call this, but you may specify
  ///  it as your "get_supported_options" entry point if your message type
  ///  supports all options.
  ///
  int amps_message_type_supports_all_options(amps_message_type_context,
                                             const char**,
                                             size_t*);
  ///
  /// Convienience implementation of get_supported options that returns
  ///  "delta,view" as your message type's supported options.
  ///
  int amps_message_type_supports_delta_and_view(amps_message_type_context,
                                                const char**,
                                                size_t*);

  int amps_message_type_default_trace_message(amps_message_type_context,
                                              const amps_trace_buffer*,
                                              amps_trace_output_buffer*);
} // extern "C"

#endif // _AMPS_SDK_MESSAGE_TYPE_AMPS_MESSAGE_TYPE_H_
