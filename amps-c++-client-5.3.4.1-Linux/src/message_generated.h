static int amps_xml_decode(const amps_char* buffer, size_t length)
{
  switch (buffer[0])
  {
  case 'C':
    switch (buffer[1])
    {
    case 'm':
      switch (buffer[2])
      {
      case 'd':
        switch (buffer[3])
        {
        case '>':
          return AMPS_Command;
          break;

        case 'I':
          return AMPS_CommandId;
          break;

        default:
          return -1;
        }
        break;

      default:
        return -1;
      }
      break;

    case 'l':
      return AMPS_ClientName;
      break;

    case 'r':
      return AMPS_CorrelationId;
      break;

    default:
      return -1;
    }
    break;

  case 'T':
    switch (buffer[1])
    {
    case 'p':
      return AMPS_Topic;
      break;

    case 'x':
      return AMPS_Timestamp;
      break;

    case 'm':
      return AMPS_TimeoutInterval;
      break;

    case 'o':
      switch (buffer[2])
      {
      case 'p':
        switch (buffer[3])
        {
        case 'N':
          return AMPS_TopNRecordsReturned;
          break;

        case 'i':
          return AMPS_TopicMatches;
          break;

        default:
          return -1;
        }
        break;

      default:
        return -1;
      }
      break;

    default:
      return -1;
    }
    break;

  case 'U':
    return AMPS_UserId;
    break;

  case 'F':
    return AMPS_Filter;
    break;

  case 'M':
    switch (buffer[1])
    {
    case 's':
      switch (buffer[2])
      {
      case 'g':
        switch (buffer[3])
        {
        case 'T':
          return AMPS_MessageType;
          break;

        case 'L':
          return AMPS_MessageLength;
          break;

        default:
          return -1;
        }
        break;

      default:
        return -1;
      }
      break;

    case 'a':
      return AMPS_Matches;
      break;

    default:
      return -1;
    }
    break;

  case 'A':
    return AMPS_AckType;
    break;

  case 'S':
    switch (buffer[1])
    {
    case 'u':
      switch (buffer[2])
      {
      case 'b':
        switch (buffer[3])
        {
        case 'I':
          switch (buffer[4])
          {
          case 'd':
            switch (buffer[5])
            {
            case '>':
              return AMPS_SubscriptionId;
              break;

            case 's':
              return AMPS_SubscriptionIds;
              break;

            default:
              return -1;
            }
            break;

          default:
            return -1;
          }
          break;

        default:
          return -1;
        }
        break;

      default:
        return -1;
      }
      break;

    case 't':
      return AMPS_Status;
      break;

    case 'o':
      switch (buffer[2])
      {
      case 'w':
        switch (buffer[3])
        {
        case 'K':
          switch (buffer[4])
          {
          case 'e':
            switch (buffer[5])
            {
            case 'y':
              switch (buffer[6])
              {
              case 's':
                return AMPS_SowKeys;
                break;

              case '>':
                return AMPS_SowKey;
                break;

              default:
                return -1;
              }
              break;

            default:
              return -1;
            }
            break;

          default:
            return -1;
          }
          break;

        default:
          return -1;
        }
        break;

      default:
        return -1;
      }
      break;

    case 'e':
      return AMPS_Sequence;
      break;

    default:
      return -1;
    }
    break;

  case 'V':
    return AMPS_Version;
    break;

  case 'E':
    return AMPS_Expiration;
    break;

  case 'H':
    return AMPS_Heartbeat;
    break;

  case 'L':
    return AMPS_LeasePeriod;
    break;

  case 'Q':
    return AMPS_QueryID;
    break;

  case 'B':
    switch (buffer[1])
    {
    case 't':
      return AMPS_BatchSize;
      break;

    case 'k':
      return AMPS_Bookmark;
      break;

    default:
      return -1;
    }
    break;

  case 'O':
    switch (buffer[1])
    {
    case 'r':
      return AMPS_OrderBy;
      break;

    case 'p':
      return AMPS_Options;
      break;

    default:
      return -1;
    }
    break;

  case 'P':
    return AMPS_Password;
    break;

  case 'R':
    switch (buffer[1])
    {
    case 'e':
      switch (buffer[2])
      {
      case 'c':
        switch (buffer[3])
        {
        case 'o':
          switch (buffer[4])
          {
          case 'r':
            switch (buffer[5])
            {
            case 'd':
              switch (buffer[6])
              {
              case 's':
                switch (buffer[7])
                {
                case 'I':
                  return AMPS_RecordsInserted;
                  break;

                case 'U':
                  return AMPS_RecordsUpdated;
                  break;

                case 'D':
                  return AMPS_SowDelete;
                  break;

                case 'R':
                  return AMPS_RecordsReturned;
                  break;

                default:
                  return -1;
                }
                break;

              default:
                return -1;
              }
              break;

            default:
              return -1;
            }
            break;

          default:
            return -1;
          }
          break;

        default:
          return -1;
        }
        break;

      case 'a':
        return AMPS_Reason;
        break;

      default:
        return -1;
      }
      break;

    default:
      return -1;
    }
    break;

  case 'G':
    return AMPS_GroupSequenceNumber;
    break;

  default:
    return -1;
  }
  return -1;
}


static char* g_FieldIdNames[] =
{
  "20000="
  , "20005="
  , "20001="
  , "20002="
  , "20003="
  , "20004="
  , "20006="
  , "20007="
  , "20008="
  , "20009="
  , "20011="
  , "20012="
  , "20015="
  , "20016="
  , "20017="
  , "20018="
  , "20019="
  , "20023="
  , "20025="
  , "20026="
  , "20032="
  , "20035="
  , "20036="
  , "20037="
  , "20038="
  , "20039="
  , "20052="
  , "20053="
  , "20054="
  , "20055="
  , "20056="
  , "20057="
  , "20058="
  , "20059="
  , "20060="
  , "20061="
  , "20062="
};
/* Based on FIX tag order */
static int g_decoder[] =
{
  AMPS_Command
  , AMPS_CommandId
  , AMPS_ClientName
  , AMPS_UserId
  , AMPS_Timestamp
  , AMPS_Topic
  , AMPS_Filter
  , AMPS_MessageType
  , AMPS_AckType
  , AMPS_SubscriptionId
  , -1
  , AMPS_Version
  , AMPS_Expiration
  , -1
  , -1
  , AMPS_Heartbeat
  , AMPS_TimeoutInterval
  , AMPS_LeasePeriod
  , AMPS_Status
  , AMPS_QueryID
  , -1
  , -1
  , -1
  , AMPS_BatchSize
  , -1
  , AMPS_TopNRecordsReturned
  , AMPS_OrderBy
  , -1
  , -1
  , -1
  , -1
  , -1
  , AMPS_SowKeys
  , -1
  , -1
  , AMPS_CorrelationId
  , AMPS_Sequence
  , AMPS_Bookmark
  , AMPS_Password
  , AMPS_Options
  , -1
  , -1
  , -1
  , -1
  , -1
  , -1
  , -1
  , -1
  , -1
  , -1
  , -1
  , -1
  , AMPS_RecordsInserted
  , AMPS_RecordsUpdated
  , AMPS_SowDelete
  , AMPS_RecordsReturned
  , AMPS_TopicMatches
  , AMPS_Matches
  , AMPS_MessageLength
  , AMPS_SowKey
  , AMPS_GroupSequenceNumber
  , AMPS_SubscriptionIds
  , AMPS_Reason
};

static char* g_xmlNames[] =
{
  "<Cmd>", "</Cmd>"
  , "<Tpc><![CDATA[", "]]></Tpc>"
  , "<CmdId><![CDATA[", "]]></CmdId>"
  , "<ClntName><![CDATA[", "]]></ClntName>"
  , "<UsrId><![CDATA[", "]]></UsrId>"
  , "<TxmTm>", "</TxmTm>"
  , "<Fltr><![CDATA[", "]]></Fltr>"
  , "<MsgTyp>", "</MsgTyp>"
  , "<AckTyp>", "</AckTyp>"
  , "<SubId><![CDATA[", "]]></SubId>"
  , "<Version>", "</Version>"
  , "<Expn>", "</Expn>"
  , "<Hrtbt>", "</Hrtbt>"
  , "<TmIntvl>", "</TmIntvl>"
  , "<LeasePrd>", "</LeasePrd>"
  , "<Status>", "</Status>"
  , "<QId><![CDATA[", "]]></QId>"
  , "<BtchSz>", "</BtchSz>"
  , "<TopN>", "</TopN>"
  , "<OrderBy>", "</OrderBy>"
  , "<SowKeys>", "</SowKeys>"
  , "<CrlId>", "</CrlId>"
  , "<Seq>", "</Seq>"
  , "<BkMrk>", "</BkMrk>"
  , "<PW><![CDATA[", "]]></PW>"
  , "<Opts>", "</Opts>"
  , "<RecordsInserted>", "</RecordsInserted>"
  , "<RecordsUpdated>", "</RecordsUpdated>"
  , "<RecordsDeleted>", "</RecordsDeleted>"
  , "<RecordsReturned>", "</RecordsReturned>"
  , "<TopicMatches>", "</TopicMatches>"
  , "<Matches>", "</Matches>"
  , "<MsgLn>", "</MsgLn>"
  , "<SowKey>", "</SowKey>"
  , "<GrpSqNum>", "</GrpSqNum>"
  , "<SubIds><![CDATA[", "]]></SubIds>"
  , "<Reason>", "</Reason>"
};
static size_t g_xmlNameLengthsBegin[] =
{
  3
  , 12
  , 14
  , 17
  , 14
  , 5
  , 13
  , 6
  , 6
  , 14
  , 7
  , 4
  , 5
  , 7
  , 8
  , 6
  , 12
  , 6
  , 4
  , 7
  , 7
  , 5
  , 3
  , 5
  , 11
  , 4
  , 15
  , 14
  , 14
  , 15
  , 12
  , 7
  , 5
  , 6
  , 8
  , 15
  , 6
};
static size_t g_xmlNameLengthsEnd[] =
{
  3
  , 6
  , 8
  , 11
  , 8
  , 5
  , 7
  , 6
  , 6
  , 8
  , 7
  , 4
  , 5
  , 7
  , 8
  , 6
  , 6
  , 6
  , 4
  , 7
  , 7
  , 5
  , 3
  , 5
  , 5
  , 4
  , 15
  , 14
  , 14
  , 15
  , 12
  , 7
  , 5
  , 6
  , 8
  , 9
  , 6
};
