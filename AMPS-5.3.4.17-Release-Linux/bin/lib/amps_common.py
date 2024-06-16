###############################################################################
##
## Copyright (c) 2012-2024 60East Technologies Inc., All Rights Reserved.
##
## This computer software is owned by 60East Technologies Inc. and is
## protected by U.S. copyright laws and other laws and by international
## treaties.  This computer software is furnished by 60East Technologies
## Inc. pursuant to a written license agreement and may be used, copied,
## transmitted, and stored only in accordance with the terms of such
## license agreement and with the inclusion of the above copyright notice.
## This computer software or any other copies thereof may not be provided
## or otherwise made available to any other person.
##
## U.S. Government Restricted Rights.  This computer software: (a) was
## developed at private expense and is in all respects the proprietary
## information of 60East Technologies Inc.; (b) was not developed with
## government funds; (c) is a trade secret of 60East Technologies Inc.
## for all purposes of the Freedom of Information Act; and (d) is a
## commercial item and thus, pursuant to Section 12.212 of the Federal
## Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
## Government's use, duplication or disclosure of the computer software
## is subject to the restrictions set forth by 60East Technologies Inc..
##
###############################################################################
###############################################################################
##
## amps_sow is a module for accessing AMPS SOW data.
##
###############################################################################


import array
import os
import struct
import sys
import ctypes
import subprocess


def version():
   return 2


def internal_debug(str):
   pass
   #sys.stderr.write("amps_common: %s\n" % str)
   #sys.stderr.flush()


class FileLabel:
    def __init__(self, magic, size, offset, crc):
        self.magic = magic
        self.size = size
        self.offset = offset
        self.crc = crc


def read_exact(f, size):
    data = f.read(size)
    data_len = len(data)
    if data_len <= 0:
        return data
    while(data_len < size):
        original_data_len = data_len
        data = data + f.read(size-data_len)
        data_len = len(data)
        if data_len == original_data_len:
           raise Exception("looking for exactly %s bytes but only found %s while reading from %s" % (size, data_len, f))
    return data



def _load_native_crc():
   _bin_dir  = os.path.dirname(os.path.abspath(__file__))
   _crclib_file = os.path.normpath(os.path.join(_bin_dir, "../../lib", "libamps_utilities_crc32.so"))
   ldd_out = ''
   try:
      ldd_out = subprocess.check_output(['ldd', _crclib_file]).decode("utf-8")
   except:
      return None
   if 'libasan' in ldd_out:
      return None
   _crclib = ctypes.cdll.LoadLibrary(_crclib_file)
   return getattr(_crclib, 'crc32')

_is_native_crc32_loaded = False
_native_crc32 = None

def load_native_crc32():
   global _is_native_crc32_loaded
   global _native_crc32
   try:
      _is_native_crc32_loaded = True
      _native_crc32 = _load_native_crc()
   except Exception as e:
      # failed to load the library, fall back to python implementation
      sys.stderr.write("warning: failed to load native crc library from %s\n" % e)
      sys.stderr.write(" ... falling back to pure python crc implementation\n")

_crc_table = array.array('I', [
            0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8,
            0xD4CA64EB, 0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3,
            0xAC78BF27, 0x5E133C24, 0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070,
            0x25AFD373, 0x36FF2087, 0xC494A384, 0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54,
            0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B, 0x20BD8EDE, 0xD2D60DDD, 0xC186FE29,
            0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35, 0xAA64D611, 0x580F5512,
            0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA, 0x30E349B1,
            0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
            0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696,
            0x6EF07595, 0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0,
            0x67DAFA54, 0x95B17957, 0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C,
            0xFE53516F, 0xED03A29B, 0x1F682198, 0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927,
            0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38, 0xDBFC821C, 0x2997011F, 0x3AC7F2EB,
            0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7, 0x61C69362, 0x93AD1061,
            0x80FDE395, 0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789, 0xEB1FCBAD,
            0x197448AE, 0x0A24BB5A, 0xF84F3859, 0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
            0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312, 0x44694011, 0x5739B3E5,
            0xA55230E6, 0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE,
            0xDDE0EB2A, 0x2F8B6829, 0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67,
            0xB7072F64, 0xA457DC90, 0x563C5F93, 0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043,
            0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C, 0x92A8FC17, 0x60C37F14, 0x73938CE0,
            0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC, 0x1871A4D8, 0xEA1A27DB,
            0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033, 0xA24BB5A6,
            0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
            0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81,
            0xFC588982, 0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5,
            0x94B49521, 0x66DF1622, 0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19,
            0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED, 0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530,
            0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F, 0x49547E0B, 0xBB3FFD08, 0xA86F0EFC,
            0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0, 0xD3D3E1AB, 0x21B862A8,
            0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540, 0x590AB964,
            0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
            0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2,
            0x37FACCF1, 0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9,
            0x4F48173D, 0xBD23943E, 0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A,
            0xC69F7B69, 0xD5CF889D, 0x27A40B9E, 0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E,
            0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351, 0x00000000, 0x13A29877, 0x274530EE,
            0x34E7A899, 0x4E8A61DC, 0x5D28F9AB, 0x69CF5132, 0x7A6DC945, 0x9D14C3B8, 0x8EB65BCF,
            0xBA51F356, 0xA9F36B21, 0xD39EA264, 0xC03C3A13, 0xF4DB928A, 0xE7790AFD, 0x3FC5F181,
            0x2C6769F6, 0x1880C16F, 0x0B225918, 0x714F905D, 0x62ED082A, 0x560AA0B3, 0x45A838C4,
            0xA2D13239, 0xB173AA4E, 0x859402D7, 0x96369AA0, 0xEC5B53E5, 0xFFF9CB92, 0xCB1E630B,
            0xD8BCFB7C, 0x7F8BE302, 0x6C297B75, 0x58CED3EC, 0x4B6C4B9B, 0x310182DE, 0x22A31AA9,
            0x1644B230, 0x05E62A47, 0xE29F20BA, 0xF13DB8CD, 0xC5DA1054, 0xD6788823, 0xAC154166,
            0xBFB7D911, 0x8B507188, 0x98F2E9FF, 0x404E1283, 0x53EC8AF4, 0x670B226D, 0x74A9BA1A,
            0x0EC4735F, 0x1D66EB28, 0x298143B1, 0x3A23DBC6, 0xDD5AD13B, 0xCEF8494C, 0xFA1FE1D5,
            0xE9BD79A2, 0x93D0B0E7, 0x80722890, 0xB4958009, 0xA737187E, 0xFF17C604, 0xECB55E73,
            0xD852F6EA, 0xCBF06E9D, 0xB19DA7D8, 0xA23F3FAF, 0x96D89736, 0x857A0F41, 0x620305BC,
            0x71A19DCB, 0x45463552, 0x56E4AD25, 0x2C896460, 0x3F2BFC17, 0x0BCC548E, 0x186ECCF9,
            0xC0D23785, 0xD370AFF2, 0xE797076B, 0xF4359F1C, 0x8E585659, 0x9DFACE2E, 0xA91D66B7,
            0xBABFFEC0, 0x5DC6F43D, 0x4E646C4A, 0x7A83C4D3, 0x69215CA4, 0x134C95E1, 0x00EE0D96,
            0x3409A50F, 0x27AB3D78, 0x809C2506, 0x933EBD71, 0xA7D915E8, 0xB47B8D9F, 0xCE1644DA,
            0xDDB4DCAD, 0xE9537434, 0xFAF1EC43, 0x1D88E6BE, 0x0E2A7EC9, 0x3ACDD650, 0x296F4E27,
            0x53028762, 0x40A01F15, 0x7447B78C, 0x67E52FFB, 0xBF59D487, 0xACFB4CF0, 0x981CE469,
            0x8BBE7C1E, 0xF1D3B55B, 0xE2712D2C, 0xD69685B5, 0xC5341DC2, 0x224D173F, 0x31EF8F48,
            0x050827D1, 0x16AABFA6, 0x6CC776E3, 0x7F65EE94, 0x4B82460D, 0x5820DE7A, 0xFBC3FAF9,
            0xE861628E, 0xDC86CA17, 0xCF245260, 0xB5499B25, 0xA6EB0352, 0x920CABCB, 0x81AE33BC,
            0x66D73941, 0x7575A136, 0x419209AF, 0x523091D8, 0x285D589D, 0x3BFFC0EA, 0x0F186873,
            0x1CBAF004, 0xC4060B78, 0xD7A4930F, 0xE3433B96, 0xF0E1A3E1, 0x8A8C6AA4, 0x992EF2D3,
            0xADC95A4A, 0xBE6BC23D, 0x5912C8C0, 0x4AB050B7, 0x7E57F82E, 0x6DF56059, 0x1798A91C,
            0x043A316B, 0x30DD99F2, 0x237F0185, 0x844819FB, 0x97EA818C, 0xA30D2915, 0xB0AFB162,
            0xCAC27827, 0xD960E050, 0xED8748C9, 0xFE25D0BE, 0x195CDA43, 0x0AFE4234, 0x3E19EAAD,
            0x2DBB72DA, 0x57D6BB9F, 0x447423E8, 0x70938B71, 0x63311306, 0xBB8DE87A, 0xA82F700D,
            0x9CC8D894, 0x8F6A40E3, 0xF50789A6, 0xE6A511D1, 0xD242B948, 0xC1E0213F, 0x26992BC2,
            0x353BB3B5, 0x01DC1B2C, 0x127E835B, 0x68134A1E, 0x7BB1D269, 0x4F567AF0, 0x5CF4E287,
            0x04D43CFD, 0x1776A48A, 0x23910C13, 0x30339464, 0x4A5E5D21, 0x59FCC556, 0x6D1B6DCF,
            0x7EB9F5B8, 0x99C0FF45, 0x8A626732, 0xBE85CFAB, 0xAD2757DC, 0xD74A9E99, 0xC4E806EE,
            0xF00FAE77, 0xE3AD3600, 0x3B11CD7C, 0x28B3550B, 0x1C54FD92, 0x0FF665E5, 0x759BACA0,
            0x663934D7, 0x52DE9C4E, 0x417C0439, 0xA6050EC4, 0xB5A796B3, 0x81403E2A, 0x92E2A65D,
            0xE88F6F18, 0xFB2DF76F, 0xCFCA5FF6, 0xDC68C781, 0x7B5FDFFF, 0x68FD4788, 0x5C1AEF11,
            0x4FB87766, 0x35D5BE23, 0x26772654, 0x12908ECD, 0x013216BA, 0xE64B1C47, 0xF5E98430,
            0xC10E2CA9, 0xD2ACB4DE, 0xA8C17D9B, 0xBB63E5EC, 0x8F844D75, 0x9C26D502, 0x449A2E7E,
            0x5738B609, 0x63DF1E90, 0x707D86E7, 0x0A104FA2, 0x19B2D7D5, 0x2D557F4C, 0x3EF7E73B,
            0xD98EEDC6, 0xCA2C75B1, 0xFECBDD28, 0xED69455F, 0x97048C1A, 0x84A6146D, 0xB041BCF4,
            0xA3E32483, 0x00000000, 0xA541927E, 0x4F6F520D, 0xEA2EC073, 0x9EDEA41A, 0x3B9F3664,
            0xD1B1F617, 0x74F06469, 0x38513EC5, 0x9D10ACBB, 0x773E6CC8, 0xD27FFEB6, 0xA68F9ADF,
            0x03CE08A1, 0xE9E0C8D2, 0x4CA15AAC, 0x70A27D8A, 0xD5E3EFF4, 0x3FCD2F87, 0x9A8CBDF9,
            0xEE7CD990, 0x4B3D4BEE, 0xA1138B9D, 0x045219E3, 0x48F3434F, 0xEDB2D131, 0x079C1142,
            0xA2DD833C, 0xD62DE755, 0x736C752B, 0x9942B558, 0x3C032726, 0xE144FB14, 0x4405696A,
            0xAE2BA919, 0x0B6A3B67, 0x7F9A5F0E, 0xDADBCD70, 0x30F50D03, 0x95B49F7D, 0xD915C5D1,
            0x7C5457AF, 0x967A97DC, 0x333B05A2, 0x47CB61CB, 0xE28AF3B5, 0x08A433C6, 0xADE5A1B8,
            0x91E6869E, 0x34A714E0, 0xDE89D493, 0x7BC846ED, 0x0F382284, 0xAA79B0FA, 0x40577089,
            0xE516E2F7, 0xA9B7B85B, 0x0CF62A25, 0xE6D8EA56, 0x43997828, 0x37691C41, 0x92288E3F,
            0x78064E4C, 0xDD47DC32, 0xC76580D9, 0x622412A7, 0x880AD2D4, 0x2D4B40AA, 0x59BB24C3,
            0xFCFAB6BD, 0x16D476CE, 0xB395E4B0, 0xFF34BE1C, 0x5A752C62, 0xB05BEC11, 0x151A7E6F,
            0x61EA1A06, 0xC4AB8878, 0x2E85480B, 0x8BC4DA75, 0xB7C7FD53, 0x12866F2D, 0xF8A8AF5E,
            0x5DE93D20, 0x29195949, 0x8C58CB37, 0x66760B44, 0xC337993A, 0x8F96C396, 0x2AD751E8,
            0xC0F9919B, 0x65B803E5, 0x1148678C, 0xB409F5F2, 0x5E273581, 0xFB66A7FF, 0x26217BCD,
            0x8360E9B3, 0x694E29C0, 0xCC0FBBBE, 0xB8FFDFD7, 0x1DBE4DA9, 0xF7908DDA, 0x52D11FA4,
            0x1E704508, 0xBB31D776, 0x511F1705, 0xF45E857B, 0x80AEE112, 0x25EF736C, 0xCFC1B31F,
            0x6A802161, 0x56830647, 0xF3C29439, 0x19EC544A, 0xBCADC634, 0xC85DA25D, 0x6D1C3023,
            0x8732F050, 0x2273622E, 0x6ED23882, 0xCB93AAFC, 0x21BD6A8F, 0x84FCF8F1, 0xF00C9C98,
            0x554D0EE6, 0xBF63CE95, 0x1A225CEB, 0x8B277743, 0x2E66E53D, 0xC448254E, 0x6109B730,
            0x15F9D359, 0xB0B84127, 0x5A968154, 0xFFD7132A, 0xB3764986, 0x1637DBF8, 0xFC191B8B,
            0x595889F5, 0x2DA8ED9C, 0x88E97FE2, 0x62C7BF91, 0xC7862DEF, 0xFB850AC9, 0x5EC498B7,
            0xB4EA58C4, 0x11ABCABA, 0x655BAED3, 0xC01A3CAD, 0x2A34FCDE, 0x8F756EA0, 0xC3D4340C,
            0x6695A672, 0x8CBB6601, 0x29FAF47F, 0x5D0A9016, 0xF84B0268, 0x1265C21B, 0xB7245065,
            0x6A638C57, 0xCF221E29, 0x250CDE5A, 0x804D4C24, 0xF4BD284D, 0x51FCBA33, 0xBBD27A40,
            0x1E93E83E, 0x5232B292, 0xF77320EC, 0x1D5DE09F, 0xB81C72E1, 0xCCEC1688, 0x69AD84F6,
            0x83834485, 0x26C2D6FB, 0x1AC1F1DD, 0xBF8063A3, 0x55AEA3D0, 0xF0EF31AE, 0x841F55C7,
            0x215EC7B9, 0xCB7007CA, 0x6E3195B4, 0x2290CF18, 0x87D15D66, 0x6DFF9D15, 0xC8BE0F6B,
            0xBC4E6B02, 0x190FF97C, 0xF321390F, 0x5660AB71, 0x4C42F79A, 0xE90365E4, 0x032DA597,
            0xA66C37E9, 0xD29C5380, 0x77DDC1FE, 0x9DF3018D, 0x38B293F3, 0x7413C95F, 0xD1525B21,
            0x3B7C9B52, 0x9E3D092C, 0xEACD6D45, 0x4F8CFF3B, 0xA5A23F48, 0x00E3AD36, 0x3CE08A10,
            0x99A1186E, 0x738FD81D, 0xD6CE4A63, 0xA23E2E0A, 0x077FBC74, 0xED517C07, 0x4810EE79,
            0x04B1B4D5, 0xA1F026AB, 0x4BDEE6D8, 0xEE9F74A6, 0x9A6F10CF, 0x3F2E82B1, 0xD50042C2,
            0x7041D0BC, 0xAD060C8E, 0x08479EF0, 0xE2695E83, 0x4728CCFD, 0x33D8A894, 0x96993AEA,
            0x7CB7FA99, 0xD9F668E7, 0x9557324B, 0x3016A035, 0xDA386046, 0x7F79F238, 0x0B899651,
            0xAEC8042F, 0x44E6C45C, 0xE1A75622, 0xDDA47104, 0x78E5E37A, 0x92CB2309, 0x378AB177,
            0x437AD51E, 0xE63B4760, 0x0C158713, 0xA954156D, 0xE5F54FC1, 0x40B4DDBF, 0xAA9A1DCC,
            0x0FDB8FB2, 0x7B2BEBDB, 0xDE6A79A5, 0x3444B9D6, 0x91052BA8, 0x00000000, 0xDD45AAB8,
            0xBF672381, 0x62228939, 0x7B2231F3, 0xA6679B4B, 0xC4451272, 0x1900B8CA, 0xF64463E6,
            0x2B01C95E, 0x49234067, 0x9466EADF, 0x8D665215, 0x5023F8AD, 0x32017194, 0xEF44DB2C,
            0xE964B13D, 0x34211B85, 0x560392BC, 0x8B463804, 0x924680CE, 0x4F032A76, 0x2D21A34F,
            0xF06409F7, 0x1F20D2DB, 0xC2657863, 0xA047F15A, 0x7D025BE2, 0x6402E328, 0xB9474990,
            0xDB65C0A9, 0x06206A11, 0xD725148B, 0x0A60BE33, 0x6842370A, 0xB5079DB2, 0xAC072578,
            0x71428FC0, 0x136006F9, 0xCE25AC41, 0x2161776D, 0xFC24DDD5, 0x9E0654EC, 0x4343FE54,
            0x5A43469E, 0x8706EC26, 0xE524651F, 0x3861CFA7, 0x3E41A5B6, 0xE3040F0E, 0x81268637,
            0x5C632C8F, 0x45639445, 0x98263EFD, 0xFA04B7C4, 0x27411D7C, 0xC805C650, 0x15406CE8,
            0x7762E5D1, 0xAA274F69, 0xB327F7A3, 0x6E625D1B, 0x0C40D422, 0xD1057E9A, 0xABA65FE7,
            0x76E3F55F, 0x14C17C66, 0xC984D6DE, 0xD0846E14, 0x0DC1C4AC, 0x6FE34D95, 0xB2A6E72D,
            0x5DE23C01, 0x80A796B9, 0xE2851F80, 0x3FC0B538, 0x26C00DF2, 0xFB85A74A, 0x99A72E73,
            0x44E284CB, 0x42C2EEDA, 0x9F874462, 0xFDA5CD5B, 0x20E067E3, 0x39E0DF29, 0xE4A57591,
            0x8687FCA8, 0x5BC25610, 0xB4868D3C, 0x69C32784, 0x0BE1AEBD, 0xD6A40405, 0xCFA4BCCF,
            0x12E11677, 0x70C39F4E, 0xAD8635F6, 0x7C834B6C, 0xA1C6E1D4, 0xC3E468ED, 0x1EA1C255,
            0x07A17A9F, 0xDAE4D027, 0xB8C6591E, 0x6583F3A6, 0x8AC7288A, 0x57828232, 0x35A00B0B,
            0xE8E5A1B3, 0xF1E51979, 0x2CA0B3C1, 0x4E823AF8, 0x93C79040, 0x95E7FA51, 0x48A250E9,
            0x2A80D9D0, 0xF7C57368, 0xEEC5CBA2, 0x3380611A, 0x51A2E823, 0x8CE7429B, 0x63A399B7,
            0xBEE6330F, 0xDCC4BA36, 0x0181108E, 0x1881A844, 0xC5C402FC, 0xA7E68BC5, 0x7AA3217D,
            0x52A0C93F, 0x8FE56387, 0xEDC7EABE, 0x30824006, 0x2982F8CC, 0xF4C75274, 0x96E5DB4D,
            0x4BA071F5, 0xA4E4AAD9, 0x79A10061, 0x1B838958, 0xC6C623E0, 0xDFC69B2A, 0x02833192,
            0x60A1B8AB, 0xBDE41213, 0xBBC47802, 0x6681D2BA, 0x04A35B83, 0xD9E6F13B, 0xC0E649F1,
            0x1DA3E349, 0x7F816A70, 0xA2C4C0C8, 0x4D801BE4, 0x90C5B15C, 0xF2E73865, 0x2FA292DD,
            0x36A22A17, 0xEBE780AF, 0x89C50996, 0x5480A32E, 0x8585DDB4, 0x58C0770C, 0x3AE2FE35,
            0xE7A7548D, 0xFEA7EC47, 0x23E246FF, 0x41C0CFC6, 0x9C85657E, 0x73C1BE52, 0xAE8414EA,
            0xCCA69DD3, 0x11E3376B, 0x08E38FA1, 0xD5A62519, 0xB784AC20, 0x6AC10698, 0x6CE16C89,
            0xB1A4C631, 0xD3864F08, 0x0EC3E5B0, 0x17C35D7A, 0xCA86F7C2, 0xA8A47EFB, 0x75E1D443,
            0x9AA50F6F, 0x47E0A5D7, 0x25C22CEE, 0xF8878656, 0xE1873E9C, 0x3CC29424, 0x5EE01D1D,
            0x83A5B7A5, 0xF90696D8, 0x24433C60, 0x4661B559, 0x9B241FE1, 0x8224A72B, 0x5F610D93,
            0x3D4384AA, 0xE0062E12, 0x0F42F53E, 0xD2075F86, 0xB025D6BF, 0x6D607C07, 0x7460C4CD,
            0xA9256E75, 0xCB07E74C, 0x16424DF4, 0x106227E5, 0xCD278D5D, 0xAF050464, 0x7240AEDC,
            0x6B401616, 0xB605BCAE, 0xD4273597, 0x09629F2F, 0xE6264403, 0x3B63EEBB, 0x59416782,
            0x8404CD3A, 0x9D0475F0, 0x4041DF48, 0x22635671, 0xFF26FCC9, 0x2E238253, 0xF36628EB,
            0x9144A1D2, 0x4C010B6A, 0x5501B3A0, 0x88441918, 0xEA669021, 0x37233A99, 0xD867E1B5,
            0x05224B0D, 0x6700C234, 0xBA45688C, 0xA345D046, 0x7E007AFE, 0x1C22F3C7, 0xC167597F,
            0xC747336E, 0x1A0299D6, 0x782010EF, 0xA565BA57, 0xBC65029D, 0x6120A825, 0x0302211C,
            0xDE478BA4, 0x31035088, 0xEC46FA30, 0x8E647309, 0x5321D9B1, 0x4A21617B, 0x9764CBC3,
            0xF54642FA, 0x2803E842
        ])



def _pycrc32(data, starter, offset=0, end=None):
   crc = int(starter)
   cursor = offset
   crc_len = len(data)
   if end is not None:
      crc_len = end
   num_uints = int((crc_len - offset)/4)
   for val in struct.Struct('I' * num_uints).unpack_from(data, offset=offset):
      crc = (crc ^ val) & 0xffffffff
      cursor += 4
      crc = _crc_table[768 + (crc & 0xFF)] ^ \
          _crc_table[512 + ((crc >> 8) & 0xFF)] ^ \
          _crc_table[256 + ((crc >> 16) & 0xFF)] ^ \
          _crc_table[(crc >> 24)] & 0xffffffff
   while cursor < crc_len:
      crc = _crc_table[(ord(data[cursor:cursor+1]) ^ crc) & 0xff] ^ (crc >> 8)
      cursor += 1
   return crc



class Crc32:
    def __init__(self):
       self.call_count = 0
       pass

    # data is a string, each character in the string is unpacked into four bytes
    def crc32(self, data, starter, offset=0, end=None):
        global _is_native_crc32_loaded
        global _native_crc32
        if end is None:
           crc_len = len(data)
        else:
           crc_len = end
        if not _is_native_crc32_loaded:
           load_native_crc32()
        if _native_crc32 is not None:
           return _native_crc32(data, offset, crc_len-offset, starter) & 0xffffffff
        else:
           return _pycrc32(data, starter, offset, end)

_crc64_table = [0x0000000000000000, 0x7AD870C830358979, 0xF5B0E190606B12F2, 0x8F689158505E9B8B,
      0xC038E5739841B68F, 0xBAE095BBA8743FF6, 0x358804E3F82AA47D, 0x4F50742BC81F2D04,
      0xAB28ECB46814FE75, 0xD1F09C7C5821770C, 0x5E980D24087FEC87, 0x24407DEC384A65FE,
      0x6B1009C7F05548FA, 0x11C8790FC060C183, 0x9EA0E857903E5A08, 0xE478989FA00BD371,
      0x7D08FF3B88BE6F81, 0x07D08FF3B88BE6F8, 0x88B81EABE8D57D73, 0xF2606E63D8E0F40A,
      0xBD301A4810FFD90E, 0xC7E86A8020CA5077, 0x4880FBD87094CBFC, 0x32588B1040A14285,
      0xD620138FE0AA91F4, 0xACF86347D09F188D, 0x2390F21F80C18306, 0x594882D7B0F40A7F,
      0x1618F6FC78EB277B, 0x6CC0863448DEAE02, 0xE3A8176C18803589, 0x997067A428B5BCF0,
      0xFA11FE77117CDF02, 0x80C98EBF2149567B, 0x0FA11FE77117CDF0, 0x75796F2F41224489,
      0x3A291B04893D698D, 0x40F16BCCB908E0F4, 0xCF99FA94E9567B7F, 0xB5418A5CD963F206,
      0x513912C379682177, 0x2BE1620B495DA80E, 0xA489F35319033385, 0xDE51839B2936BAFC,
      0x9101F7B0E12997F8, 0xEBD98778D11C1E81, 0x64B116208142850A, 0x1E6966E8B1770C73,
      0x8719014C99C2B083, 0xFDC17184A9F739FA, 0x72A9E0DCF9A9A271, 0x08719014C99C2B08,
      0x4721E43F0183060C, 0x3DF994F731B68F75, 0xB29105AF61E814FE, 0xC849756751DD9D87,
      0x2C31EDF8F1D64EF6, 0x56E99D30C1E3C78F, 0xD9810C6891BD5C04, 0xA3597CA0A188D57D,
      0xEC09088B6997F879, 0x96D1784359A27100, 0x19B9E91B09FCEA8B, 0x636199D339C963F2,
      0xDF7ADABD7A6E2D6F, 0xA5A2AA754A5BA416, 0x2ACA3B2D1A053F9D, 0x50124BE52A30B6E4,
      0x1F423FCEE22F9BE0, 0x659A4F06D21A1299, 0xEAF2DE5E82448912, 0x902AAE96B271006B,
      0x74523609127AD31A, 0x0E8A46C1224F5A63, 0x81E2D7997211C1E8, 0xFB3AA75142244891,
      0xB46AD37A8A3B6595, 0xCEB2A3B2BA0EECEC, 0x41DA32EAEA507767, 0x3B024222DA65FE1E,
      0xA2722586F2D042EE, 0xD8AA554EC2E5CB97, 0x57C2C41692BB501C, 0x2D1AB4DEA28ED965,
      0x624AC0F56A91F461, 0x1892B03D5AA47D18, 0x97FA21650AFAE693, 0xED2251AD3ACF6FEA,
      0x095AC9329AC4BC9B, 0x7382B9FAAAF135E2, 0xFCEA28A2FAAFAE69, 0x8632586ACA9A2710,
      0xC9622C4102850A14, 0xB3BA5C8932B0836D, 0x3CD2CDD162EE18E6, 0x460ABD1952DB919F,
      0x256B24CA6B12F26D, 0x5FB354025B277B14, 0xD0DBC55A0B79E09F, 0xAA03B5923B4C69E6,
      0xE553C1B9F35344E2, 0x9F8BB171C366CD9B, 0x10E3202993385610, 0x6A3B50E1A30DDF69,
      0x8E43C87E03060C18, 0xF49BB8B633338561, 0x7BF329EE636D1EEA, 0x012B592653589793,
      0x4E7B2D0D9B47BA97, 0x34A35DC5AB7233EE, 0xBBCBCC9DFB2CA865, 0xC113BC55CB19211C,
      0x5863DBF1E3AC9DEC, 0x22BBAB39D3991495, 0xADD33A6183C78F1E, 0xD70B4AA9B3F20667,
      0x985B3E827BED2B63, 0xE2834E4A4BD8A21A, 0x6DEBDF121B863991, 0x1733AFDA2BB3B0E8,
      0xF34B37458BB86399, 0x8993478DBB8DEAE0, 0x06FBD6D5EBD3716B, 0x7C23A61DDBE6F812,
      0x3373D23613F9D516, 0x49ABA2FE23CC5C6F, 0xC6C333A67392C7E4, 0xBC1B436E43A74E9D,
      0x95AC9329AC4BC9B5, 0xEF74E3E19C7E40CC, 0x601C72B9CC20DB47, 0x1AC40271FC15523E,
      0x5594765A340A7F3A, 0x2F4C0692043FF643, 0xA02497CA54616DC8, 0xDAFCE7026454E4B1,
      0x3E847F9DC45F37C0, 0x445C0F55F46ABEB9, 0xCB349E0DA4342532, 0xB1ECEEC59401AC4B,
      0xFEBC9AEE5C1E814F, 0x8464EA266C2B0836, 0x0B0C7B7E3C7593BD, 0x71D40BB60C401AC4,
      0xE8A46C1224F5A634, 0x927C1CDA14C02F4D, 0x1D148D82449EB4C6, 0x67CCFD4A74AB3DBF,
      0x289C8961BCB410BB, 0x5244F9A98C8199C2, 0xDD2C68F1DCDF0249, 0xA7F41839ECEA8B30,
      0x438C80A64CE15841, 0x3954F06E7CD4D138, 0xB63C61362C8A4AB3, 0xCCE411FE1CBFC3CA,
      0x83B465D5D4A0EECE, 0xF96C151DE49567B7, 0x76048445B4CBFC3C, 0x0CDCF48D84FE7545,
      0x6FBD6D5EBD3716B7, 0x15651D968D029FCE, 0x9A0D8CCEDD5C0445, 0xE0D5FC06ED698D3C,
      0xAF85882D2576A038, 0xD55DF8E515432941, 0x5A3569BD451DB2CA, 0x20ED197575283BB3,
      0xC49581EAD523E8C2, 0xBE4DF122E51661BB, 0x3125607AB548FA30, 0x4BFD10B2857D7349,
      0x04AD64994D625E4D, 0x7E7514517D57D734, 0xF11D85092D094CBF, 0x8BC5F5C11D3CC5C6,
      0x12B5926535897936, 0x686DE2AD05BCF04F, 0xE70573F555E26BC4, 0x9DDD033D65D7E2BD,
      0xD28D7716ADC8CFB9, 0xA85507DE9DFD46C0, 0x273D9686CDA3DD4B, 0x5DE5E64EFD965432,
      0xB99D7ED15D9D8743, 0xC3450E196DA80E3A, 0x4C2D9F413DF695B1, 0x36F5EF890DC31CC8,
      0x79A59BA2C5DC31CC, 0x037DEB6AF5E9B8B5, 0x8C157A32A5B7233E, 0xF6CD0AFA9582AA47,
      0x4AD64994D625E4DA, 0x300E395CE6106DA3, 0xBF66A804B64EF628, 0xC5BED8CC867B7F51,
      0x8AEEACE74E645255, 0xF036DC2F7E51DB2C, 0x7F5E4D772E0F40A7, 0x05863DBF1E3AC9DE,
      0xE1FEA520BE311AAF, 0x9B26D5E88E0493D6, 0x144E44B0DE5A085D, 0x6E963478EE6F8124,
      0x21C640532670AC20, 0x5B1E309B16452559, 0xD476A1C3461BBED2, 0xAEAED10B762E37AB,
      0x37DEB6AF5E9B8B5B, 0x4D06C6676EAE0222, 0xC26E573F3EF099A9, 0xB8B627F70EC510D0,
      0xF7E653DCC6DA3DD4, 0x8D3E2314F6EFB4AD, 0x0256B24CA6B12F26, 0x788EC2849684A65F,
      0x9CF65A1B368F752E, 0xE62E2AD306BAFC57, 0x6946BB8B56E467DC, 0x139ECB4366D1EEA5,
      0x5CCEBF68AECEC3A1, 0x2616CFA09EFB4AD8, 0xA97E5EF8CEA5D153, 0xD3A62E30FE90582A,
      0xB0C7B7E3C7593BD8, 0xCA1FC72BF76CB2A1, 0x45775673A732292A, 0x3FAF26BB9707A053,
      0x70FF52905F188D57, 0x0A2722586F2D042E, 0x854FB3003F739FA5, 0xFF97C3C80F4616DC,
      0x1BEF5B57AF4DC5AD, 0x61372B9F9F784CD4, 0xEE5FBAC7CF26D75F, 0x9487CA0FFF135E26,
      0xDBD7BE24370C7322, 0xA10FCEEC0739FA5B, 0x2E675FB4576761D0, 0x54BF2F7C6752E8A9,
      0xCDCF48D84FE75459, 0xB71738107FD2DD20, 0x387FA9482F8C46AB, 0x42A7D9801FB9CFD2,
      0x0DF7ADABD7A6E2D6, 0x772FDD63E7936BAF, 0xF8474C3BB7CDF024, 0x829F3CF387F8795D,
      0x66E7A46C27F3AA2C, 0x1C3FD4A417C62355, 0x935745FC4798B8DE, 0xE98F353477AD31A7,
      0xA6DF411FBFB21CA3, 0xDC0731D78F8795DA, 0x536FA08FDFD90E51, 0x29B7D047EFEC8728]

def _pycrc64(text, initial_hash = 18446744073709551615):
    hval = initial_hash
    for ch in text:
        hval = (hval >> 8) ^ _crc64_table[(hval & 255) ^ ord(ch)]
    return hval

def topic_hash(message_type,topic):
    """
    Returns the AMPS topic hash value corresponding to a given topic and message type name.
    """
    return _pycrc64(message_type + '\0' + topic)

def client_name_hash(client_name, instance_name=None):
    """
    Returns the AMPS client name hash value.
    """
    if instance_name is None:
       return _pycrc64(client_name)
    else:
       return _pycrc64(client_name + '\1' + instance_name)

