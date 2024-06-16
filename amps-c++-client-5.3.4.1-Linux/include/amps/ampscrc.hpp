////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2010-2023 60East Technologies Inc., All Rights Reserved.
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
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////
#ifndef _AMPS_CRC_H_
#define _AMPS_CRC_H_ 1

#include <amps/ampsplusplus.hpp>
#ifdef _WIN32
  #include <intrin.h>
  #include <nmmintrin.h>
#elif defined(AMPS_SSE_42)
  #include <smmintrin.h>
#endif
namespace AMPS
{

  template<int x>
  class CRC
  {
    static const amps_uint64_t client_crc_table[256];
  public:
    static bool isSSE42Enabled()
    {
#ifdef AMPS_SSE_42
#ifdef _WIN32
      int cpuinfo[4];
      __cpuid(cpuinfo, 1);
      return (cpuinfo[2] & (1 << 20)) != 0;
#else
      unsigned int eax, ebx, ecx = 0, edx;
      __get_cpuid(1, &eax, &ebx, &ecx, &edx);
      return ecx & (1 << 20);
#endif
#else
      return false;
#endif
    }
    static amps_uint64_t crcNoSSE(const char* data_, size_t len_, amps_uint64_t crc_ = 0)
    {
      if (len_ == 0)
      {
        return crc_;
      }
      amps_uint64_t crc = crc_;
      size_t len = len_;
      const unsigned char* p = (const unsigned char*)data_;
      while (len--)
      {
        crc = (amps_uint64_t) (client_crc_table[((unsigned char)crc ^ *p++) & 0xff] ^ (crc >> 8));
      }
      return crc;
    }
#ifdef AMPS_SSE_42
    static amps_uint64_t crc(const char* data_, size_t len_, amps_uint64_t crc_ = 0)
    {
      if (len_ == 0)
      {
        return crc_;
      }
      amps_uint32_t crc = (amps_uint32_t)crc_;
      // align on a 8 byte boundary
      unsigned int beginLen = (unsigned int)((size_t)(data_) & 7);
      if (beginLen > len_)
      {
        beginLen = (unsigned int) len_;
      }

      // process 64 bytes at a time
      unsigned int middleLen = (len_ - beginLen) & ~0x3Fl;

      unsigned int i = 0;

      for (; i < beginLen; i++)
      {
        crc = _mm_crc32_u8(crc, *(data_ + i));
      }

      for (; i < beginLen + middleLen; i += 64)
      {
#ifdef AMPS_X64
        crc = (unsigned int) _mm_crc32_u64(crc, *(long long*)(data_ + i + 0) );
        crc = (unsigned int) _mm_crc32_u64(crc, *(long long*)(data_ + i + 8 ));
        crc = (unsigned int) _mm_crc32_u64(crc, *(long long*)(data_ + i + 16));
        crc = (unsigned int) _mm_crc32_u64(crc, *(long long*)(data_ + i + 24));
        crc = (unsigned int) _mm_crc32_u64(crc, *(long long*)(data_ + i + 32));
        crc = (unsigned int) _mm_crc32_u64(crc, *(long long*)(data_ + i + 40));
        crc = (unsigned int) _mm_crc32_u64(crc, *(long long*)(data_ + i + 48));
        crc = (unsigned int) _mm_crc32_u64(crc, *(long long*)(data_ + i + 56));
#else
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 0 ));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 4 ));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 8 ));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 12));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 16));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 20));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 24));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 28));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 32));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 36));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 40));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 44));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 48));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 52));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 56));
        crc = _mm_crc32_u32(crc, *(long*)(data_ + i + 60));

#endif
      }

      for (; i < len_; i++)
      {
        crc = _mm_crc32_u8(crc, *(data_ + i));
      }
      return crc;
    }
#endif
  };

  template<int x> const amps_uint64_t CRC<x>::client_crc_table[256] =
  {
    0x0000000000000000ULL, 0x7AD870C830358979ULL, 0xF5B0E190606B12F2ULL, 0x8F689158505E9B8BULL,
    0xC038E5739841B68FULL, 0xBAE095BBA8743FF6ULL, 0x358804E3F82AA47DULL, 0x4F50742BC81F2D04ULL,
    0xAB28ECB46814FE75ULL, 0xD1F09C7C5821770CULL, 0x5E980D24087FEC87ULL, 0x24407DEC384A65FEULL,
    0x6B1009C7F05548FAULL, 0x11C8790FC060C183ULL, 0x9EA0E857903E5A08ULL, 0xE478989FA00BD371ULL,
    0x7D08FF3B88BE6F81ULL, 0x07D08FF3B88BE6F8ULL, 0x88B81EABE8D57D73ULL, 0xF2606E63D8E0F40AULL,
    0xBD301A4810FFD90EULL, 0xC7E86A8020CA5077ULL, 0x4880FBD87094CBFCULL, 0x32588B1040A14285ULL,
    0xD620138FE0AA91F4ULL, 0xACF86347D09F188DULL, 0x2390F21F80C18306ULL, 0x594882D7B0F40A7FULL,
    0x1618F6FC78EB277BULL, 0x6CC0863448DEAE02ULL, 0xE3A8176C18803589ULL, 0x997067A428B5BCF0ULL,
    0xFA11FE77117CDF02ULL, 0x80C98EBF2149567BULL, 0x0FA11FE77117CDF0ULL, 0x75796F2F41224489ULL,
    0x3A291B04893D698DULL, 0x40F16BCCB908E0F4ULL, 0xCF99FA94E9567B7FULL, 0xB5418A5CD963F206ULL,
    0x513912C379682177ULL, 0x2BE1620B495DA80EULL, 0xA489F35319033385ULL, 0xDE51839B2936BAFCULL,
    0x9101F7B0E12997F8ULL, 0xEBD98778D11C1E81ULL, 0x64B116208142850AULL, 0x1E6966E8B1770C73ULL,
    0x8719014C99C2B083ULL, 0xFDC17184A9F739FAULL, 0x72A9E0DCF9A9A271ULL, 0x08719014C99C2B08ULL,
    0x4721E43F0183060CULL, 0x3DF994F731B68F75ULL, 0xB29105AF61E814FEULL, 0xC849756751DD9D87ULL,
    0x2C31EDF8F1D64EF6ULL, 0x56E99D30C1E3C78FULL, 0xD9810C6891BD5C04ULL, 0xA3597CA0A188D57DULL,
    0xEC09088B6997F879ULL, 0x96D1784359A27100ULL, 0x19B9E91B09FCEA8BULL, 0x636199D339C963F2ULL,
    0xDF7ADABD7A6E2D6FULL, 0xA5A2AA754A5BA416ULL, 0x2ACA3B2D1A053F9DULL, 0x50124BE52A30B6E4ULL,
    0x1F423FCEE22F9BE0ULL, 0x659A4F06D21A1299ULL, 0xEAF2DE5E82448912ULL, 0x902AAE96B271006BULL,
    0x74523609127AD31AULL, 0x0E8A46C1224F5A63ULL, 0x81E2D7997211C1E8ULL, 0xFB3AA75142244891ULL,
    0xB46AD37A8A3B6595ULL, 0xCEB2A3B2BA0EECECULL, 0x41DA32EAEA507767ULL, 0x3B024222DA65FE1EULL,
    0xA2722586F2D042EEULL, 0xD8AA554EC2E5CB97ULL, 0x57C2C41692BB501CULL, 0x2D1AB4DEA28ED965ULL,
    0x624AC0F56A91F461ULL, 0x1892B03D5AA47D18ULL, 0x97FA21650AFAE693ULL, 0xED2251AD3ACF6FEAULL,
    0x095AC9329AC4BC9BULL, 0x7382B9FAAAF135E2ULL, 0xFCEA28A2FAAFAE69ULL, 0x8632586ACA9A2710ULL,
    0xC9622C4102850A14ULL, 0xB3BA5C8932B0836DULL, 0x3CD2CDD162EE18E6ULL, 0x460ABD1952DB919FULL,
    0x256B24CA6B12F26DULL, 0x5FB354025B277B14ULL, 0xD0DBC55A0B79E09FULL, 0xAA03B5923B4C69E6ULL,
    0xE553C1B9F35344E2ULL, 0x9F8BB171C366CD9BULL, 0x10E3202993385610ULL, 0x6A3B50E1A30DDF69ULL,
    0x8E43C87E03060C18ULL, 0xF49BB8B633338561ULL, 0x7BF329EE636D1EEAULL, 0x012B592653589793ULL,
    0x4E7B2D0D9B47BA97ULL, 0x34A35DC5AB7233EEULL, 0xBBCBCC9DFB2CA865ULL, 0xC113BC55CB19211CULL,
    0x5863DBF1E3AC9DECULL, 0x22BBAB39D3991495ULL, 0xADD33A6183C78F1EULL, 0xD70B4AA9B3F20667ULL,
    0x985B3E827BED2B63ULL, 0xE2834E4A4BD8A21AULL, 0x6DEBDF121B863991ULL, 0x1733AFDA2BB3B0E8ULL,
    0xF34B37458BB86399ULL, 0x8993478DBB8DEAE0ULL, 0x06FBD6D5EBD3716BULL, 0x7C23A61DDBE6F812ULL,
    0x3373D23613F9D516ULL, 0x49ABA2FE23CC5C6FULL, 0xC6C333A67392C7E4ULL, 0xBC1B436E43A74E9DULL,
    0x95AC9329AC4BC9B5ULL, 0xEF74E3E19C7E40CCULL, 0x601C72B9CC20DB47ULL, 0x1AC40271FC15523EULL,
    0x5594765A340A7F3AULL, 0x2F4C0692043FF643ULL, 0xA02497CA54616DC8ULL, 0xDAFCE7026454E4B1ULL,
    0x3E847F9DC45F37C0ULL, 0x445C0F55F46ABEB9ULL, 0xCB349E0DA4342532ULL, 0xB1ECEEC59401AC4BULL,
    0xFEBC9AEE5C1E814FULL, 0x8464EA266C2B0836ULL, 0x0B0C7B7E3C7593BDULL, 0x71D40BB60C401AC4ULL,
    0xE8A46C1224F5A634ULL, 0x927C1CDA14C02F4DULL, 0x1D148D82449EB4C6ULL, 0x67CCFD4A74AB3DBFULL,
    0x289C8961BCB410BBULL, 0x5244F9A98C8199C2ULL, 0xDD2C68F1DCDF0249ULL, 0xA7F41839ECEA8B30ULL,
    0x438C80A64CE15841ULL, 0x3954F06E7CD4D138ULL, 0xB63C61362C8A4AB3ULL, 0xCCE411FE1CBFC3CAULL,
    0x83B465D5D4A0EECEULL, 0xF96C151DE49567B7ULL, 0x76048445B4CBFC3CULL, 0x0CDCF48D84FE7545ULL,
    0x6FBD6D5EBD3716B7ULL, 0x15651D968D029FCEULL, 0x9A0D8CCEDD5C0445ULL, 0xE0D5FC06ED698D3CULL,
    0xAF85882D2576A038ULL, 0xD55DF8E515432941ULL, 0x5A3569BD451DB2CAULL, 0x20ED197575283BB3ULL,
    0xC49581EAD523E8C2ULL, 0xBE4DF122E51661BBULL, 0x3125607AB548FA30ULL, 0x4BFD10B2857D7349ULL,
    0x04AD64994D625E4DULL, 0x7E7514517D57D734ULL, 0xF11D85092D094CBFULL, 0x8BC5F5C11D3CC5C6ULL,
    0x12B5926535897936ULL, 0x686DE2AD05BCF04FULL, 0xE70573F555E26BC4ULL, 0x9DDD033D65D7E2BDULL,
    0xD28D7716ADC8CFB9ULL, 0xA85507DE9DFD46C0ULL, 0x273D9686CDA3DD4BULL, 0x5DE5E64EFD965432ULL,
    0xB99D7ED15D9D8743ULL, 0xC3450E196DA80E3AULL, 0x4C2D9F413DF695B1ULL, 0x36F5EF890DC31CC8ULL,
    0x79A59BA2C5DC31CCULL, 0x037DEB6AF5E9B8B5ULL, 0x8C157A32A5B7233EULL, 0xF6CD0AFA9582AA47ULL,
    0x4AD64994D625E4DAULL, 0x300E395CE6106DA3ULL, 0xBF66A804B64EF628ULL, 0xC5BED8CC867B7F51ULL,
    0x8AEEACE74E645255ULL, 0xF036DC2F7E51DB2CULL, 0x7F5E4D772E0F40A7ULL, 0x05863DBF1E3AC9DEULL,
    0xE1FEA520BE311AAFULL, 0x9B26D5E88E0493D6ULL, 0x144E44B0DE5A085DULL, 0x6E963478EE6F8124ULL,
    0x21C640532670AC20ULL, 0x5B1E309B16452559ULL, 0xD476A1C3461BBED2ULL, 0xAEAED10B762E37ABULL,
    0x37DEB6AF5E9B8B5BULL, 0x4D06C6676EAE0222ULL, 0xC26E573F3EF099A9ULL, 0xB8B627F70EC510D0ULL,
    0xF7E653DCC6DA3DD4ULL, 0x8D3E2314F6EFB4ADULL, 0x0256B24CA6B12F26ULL, 0x788EC2849684A65FULL,
    0x9CF65A1B368F752EULL, 0xE62E2AD306BAFC57ULL, 0x6946BB8B56E467DCULL, 0x139ECB4366D1EEA5ULL,
    0x5CCEBF68AECEC3A1ULL, 0x2616CFA09EFB4AD8ULL, 0xA97E5EF8CEA5D153ULL, 0xD3A62E30FE90582AULL,
    0xB0C7B7E3C7593BD8ULL, 0xCA1FC72BF76CB2A1ULL, 0x45775673A732292AULL, 0x3FAF26BB9707A053ULL,
    0x70FF52905F188D57ULL, 0x0A2722586F2D042EULL, 0x854FB3003F739FA5ULL, 0xFF97C3C80F4616DCULL,
    0x1BEF5B57AF4DC5ADULL, 0x61372B9F9F784CD4ULL, 0xEE5FBAC7CF26D75FULL, 0x9487CA0FFF135E26ULL,
    0xDBD7BE24370C7322ULL, 0xA10FCEEC0739FA5BULL, 0x2E675FB4576761D0ULL, 0x54BF2F7C6752E8A9ULL,
    0xCDCF48D84FE75459ULL, 0xB71738107FD2DD20ULL, 0x387FA9482F8C46ABULL, 0x42A7D9801FB9CFD2ULL,
    0x0DF7ADABD7A6E2D6ULL, 0x772FDD63E7936BAFULL, 0xF8474C3BB7CDF024ULL, 0x829F3CF387F8795DULL,
    0x66E7A46C27F3AA2CULL, 0x1C3FD4A417C62355ULL, 0x935745FC4798B8DEULL, 0xE98F353477AD31A7ULL,
    0xA6DF411FBFB21CA3ULL, 0xDC0731D78F8795DAULL, 0x536FA08FDFD90E51ULL, 0x29B7D047EFEC8728ULL
  };

}
#endif

