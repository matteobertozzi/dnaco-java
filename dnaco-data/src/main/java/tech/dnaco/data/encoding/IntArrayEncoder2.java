package tech.dnaco.data.encoding;

import java.io.IOException;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.strings.HumansUtil;

public class IntArrayEncoder2 {
  public enum SliceType {
    RLE, LIN, MIN
  }

  public record Slice(SliceType type, int start, int end, long value, long delta) {
    public int length() {
      return end - start;
    }
  }

  public static void get_slices(final SliceEncoder encoder, final long[] seq) throws IOException {
    final int MAX_LENGTH = (1 << 16) - 1;

    if (ArrayUtil.isEmpty(seq)) {
      return;
    }

    final int seq_length = seq.length;
    SliceType slice_type = SliceType.MIN;
    int slice_start = 0;
    int slice_end = 1;
    int slice_length = 1;
    long slice_value = seq[0];
    long slice_delta = 0;

    int rle_count = 1;
    int lin_count = 2;
    int min_savepoint_idx = slice_start;
    long min_savepoint_value = slice_value;
    long min_savepoint_delta = slice_delta;
    long min_lookahead_value = slice_value;
    long min_lookahead_delta = slice_delta;

    // The general idea is the following:
    // - start reading values one by one and add them to the current slice
    // - use 'rle_count' and 'lin_count' to keep track of how many constant
    // or linearly spaced consecutive values we find and as soon as it is
    // advantageous emit a slice of type RLE or MIN
    // - otherwise we are left with the problem to detect when it is convenient
    // to stop the current MIN slice to open another MIN slice: to detect such
    // cases we use the variable 'min_savepoint_idx' to mark an item when we
    // are sure than all values between 'slice_start' and 'min_savepoint_idx'
    // (inclusive) must belong a single slice of type MIN (i.e. when we are
    // sure that it is not conveninent to further divide that slice).
    //
    // |---- slice_start
    // | |---- min_savepoint_idx
    // | ||---- min_savepoint_idx + 1
    // | || |---- slice_end (= i at the end of each loop)
    // v VV V
    // [ *** past values ***, . ...... .. ..... ., *** future values *** ]
    //
    // 'min_savepoint_value' and 'min_savepoint_delta' keep track of the
    // base value and delta of the interval [slice_start, min_savepoint_idx]
    // 'min_lookahead_value' and 'min_lookahead_delta' keep track of the
    // base value and delta of the interval [min_savepoint_idx+1, slice_end]

    int i = 1;
    while (i < seq_length) {
      final long val = seq[i];

      // if __debug__: print(f"\n{'-' * 150}\nStart - ", _get_state_info())

      if (val < 0) {
        throw new IllegalArgumentException("Value at index " + i + " is " + val + " < 0");
      }

      // If current slice length exceeds MAX_LENGTH (specified by the protocol),
      // then forcibly close the current slice and start a new one
      if (slice_length == MAX_LENGTH) {
        if (slice_type == SliceType.MIN && min_savepoint_idx > slice_start) {
          encoder.add(new Slice(SliceType.MIN, slice_start, min_savepoint_idx + 1, min_savepoint_value, min_savepoint_delta));

          // slice_type = SliceType.MIN; // useless
          slice_start = min_savepoint_idx + 1;
          slice_length = slice_end - slice_start;
          slice_value = min_lookahead_value;
          slice_delta = min_lookahead_delta;
        } else {
          encoder.add(new Slice(slice_type, slice_start, slice_end, slice_value, slice_delta));

          slice_type = SliceType.MIN;
          slice_start = slice_end;
          slice_end += 1;
          slice_length = 1;
          slice_value = val;
          slice_delta = 0;
          rle_count = 1; // reset
          lin_count = 2; // reset
          i = slice_end;
        }

        // Reset
        min_savepoint_idx = slice_start;
        min_savepoint_value = slice_value;
        min_savepoint_delta = slice_delta;
        min_lookahead_value = slice_value;
        min_lookahead_delta = slice_delta;
        continue;
      }

      // [For testing purpose only] no slice should ever be longer than MAX_LENGTH
      if (slice_length > MAX_LENGTH) {
        throw new IllegalArgumentException(
            "slice length should never be greater than MAX_LENGTH, got " + slice_length + " > " + MAX_LENGTH);
      }

      // Continue or close current slice according to its type and the incoming 'val'
      if (slice_type == SliceType.MIN) {
        // While encoding a slice of type MIN we must detect incoming
        // subsequences that can be encoded using RLE or LIN and
        // switch to those encodings as soon as it is advantageous

        // Track of how many *consecutive* constant values
        // are there in the tail of current slice
        if (val == seq[i - 1]) {
          rle_count += 1;
          lin_count = 2; // reset
        } else {
          rle_count = 1; // reset
          // Track of how many *consecutive* (non-constant) linearly
          // spaced values are there in the tail of current slice
          if (slice_length > 1 && val - seq[i - 1] == seq[i - 1] - seq[i - 2]) {
            lin_count += 1;
          } else {
            lin_count = 2;
          }
        }

        slice_end += 1;
        slice_length += 1;

        final long prev_slice_delta = slice_delta;
        if (val > slice_value + slice_delta) {
          slice_delta = val - slice_value;
        } else if (val < slice_value) {
          slice_delta = slice_value + slice_delta - val;
          slice_value = val;
        }

        if (i - min_savepoint_idx > 1) {
          if (val > min_lookahead_value + min_lookahead_delta) {
            min_lookahead_delta = val - min_lookahead_value;
          } else if (val < min_lookahead_value) {
            min_lookahead_delta = min_lookahead_value + min_lookahead_delta - val;
            min_lookahead_value = val;
          }
        } else {
          min_lookahead_value = val;
          min_lookahead_delta = 0;
        }

        if (rle_count > 1) {
          final int _slice_length = slice_end - slice_start - rle_count;
          final long diff = (encoder.min_bit_size(slice_length, slice_value, slice_delta) -
                  encoder.slice_bit_size(_slice_length, slice_value, slice_delta) -
                  encoder.rle_bit_size(rle_count, val));
          if (diff > 0) {
            if (_slice_length > 0) {
              if (_slice_length == 1) {
                encoder.add(new Slice(SliceType.RLE, slice_start, slice_start + 1, seq[slice_start], 0));
              } else if (_slice_length == 2) {
                encoder.add(new Slice(SliceType.LIN, slice_start, slice_start + 2, seq[slice_start], seq[slice_start+1] - seq[slice_start]));
              } else {
                final int _slice_end = slice_start + _slice_length;
                // TODO: performance
                final long[] minmax = minMax(seq, slice_start, _slice_end);
                final long _slice_value = minmax[0];
                final long _slice_delta = minmax[1] - _slice_value;
                encoder.add(new Slice(slice_type, slice_start, _slice_end, _slice_value, _slice_delta));
              }
              slice_start += _slice_length;
            }
            slice_type = SliceType.RLE;
            slice_value = val;
            slice_delta = 0;
            i = slice_end;
            continue;
          }

        } else if (lin_count > 2) {
          final long lin_delta = val - seq[i - 1];
          final long lin_value = val - lin_delta * (lin_count - 1);
          final int _slice_length = slice_end - slice_start - lin_count;
          final long diff = (encoder.min_bit_size(slice_length, slice_value, slice_delta) -
                  encoder.slice_bit_size(_slice_length, slice_value, slice_delta) -
                  encoder.lin_bit_size(lin_count, lin_value, lin_delta));
          if (diff > 0) {
            if (_slice_length > 0) {
              if (_slice_length == 1) {
                encoder.add(new Slice(SliceType.RLE, slice_start, slice_start + 1, seq[slice_start], 0));
              } else if (_slice_length == 2) {
                encoder.add(new Slice(SliceType.LIN, slice_start, slice_start + 2, seq[slice_start], seq[slice_start+1] - seq[slice_start]));
              } else {
                final int _slice_end = slice_start + _slice_length;
                final long[] minmax = minMax(seq, slice_start, _slice_end);
                final long _slice_value = minmax[0]; // TODO: performance
                final long _slice_delta = minmax[1] - _slice_value; // TODO: performance
                encoder.add(new Slice(slice_type, slice_start, _slice_end, _slice_value, _slice_delta));
              }
              slice_start += _slice_length;
            }
            slice_type = SliceType.LIN;
            slice_value = lin_value;
            slice_delta = lin_delta;
            i = slice_end;
            continue;
          }
        }

        if (i - min_savepoint_idx > 2) {
          final int min_slice_length = min_savepoint_idx + 1 - slice_start;
          final long diff = (encoder.min_bit_size(slice_length, slice_value, slice_delta) -
                  encoder.slice_bit_size(min_slice_length, min_savepoint_value, min_savepoint_delta) -
                  encoder.min_bit_size(i - min_savepoint_idx, min_lookahead_value, min_lookahead_delta));

          if (diff > 0) {
            if (min_slice_length > 0) {
              encoder.add(new Slice(SliceType.MIN, slice_start, slice_start + min_slice_length, min_savepoint_value, min_savepoint_delta));
              slice_start += min_slice_length;
              // Reset
              min_savepoint_idx = slice_start;
              min_savepoint_value = min_lookahead_delta;
              min_savepoint_delta = min_lookahead_delta;
            } else {
              throw new IllegalStateException("should not be here");
            }

            slice_type = SliceType.MIN;
            slice_length = slice_end - slice_start;
            slice_value = min_lookahead_value;
            slice_delta = min_lookahead_delta;
            i = slice_end;
            continue;
          }
        }

        if (encoder.bit_size(prev_slice_delta) <= encoder.bit_size(val - slice_value)) {
          min_savepoint_idx = i;
          min_savepoint_value = slice_value;
          min_savepoint_delta = slice_delta;
          min_lookahead_value = val;
          min_lookahead_delta = 0;
        }

        i = slice_end;

      } else if (slice_type == SliceType.RLE) {
        if (val == slice_value) {
          // extend the current slice
          slice_end += 1;
          slice_length += 1;
          i = slice_end;
        } else {
          // Close the current slice and start a new one
          encoder.add(new Slice(slice_type, slice_start, i, slice_value, slice_delta));
          slice_type = SliceType.MIN;
          slice_start = i;
          slice_end += 1; // = slice_start + 1
          slice_length = 1;
          slice_value = val;
          slice_delta = 0;

          // Reset
          rle_count = 1;
          min_savepoint_idx = slice_start;
          min_savepoint_value = slice_value;
          min_savepoint_delta = slice_delta;
          min_lookahead_value = slice_value;
          min_lookahead_delta = slice_delta;

          if (slice_end != (slice_start + 1)) throw new IllegalStateException("got " + slice_end + " != " + (slice_start + 1));
          if ((i + 1) != slice_end) throw new IllegalStateException("got " + (i + 1) + " != " + slice_end);
          i = slice_end;
        }

      } else if (slice_type == SliceType.LIN) {
        if (val - seq[i - 1] == slice_delta) {
          // extend the current slice
          slice_end += 1;
          slice_length += 1;
          i = slice_end;
        } else {
          // Close the current slide and start a new one
          encoder.add(new Slice(slice_type, slice_start, i, slice_value, slice_delta));
          slice_type = SliceType.MIN;
          slice_start = i;
          slice_end += 1; // = slice_start + 1
          slice_length = 1;
          slice_value = val;
          slice_delta = 0;

          // Reset
          lin_count = 2;
          min_savepoint_idx = slice_start;
          min_savepoint_value = slice_value;
          min_savepoint_delta = slice_delta;
          min_lookahead_value = slice_value;
          min_lookahead_delta = slice_delta;

          if (slice_end != (slice_start + 1)) throw new IllegalStateException("got " + slice_end + " != " + (slice_start + 1));
          if ((i + 1) != slice_end) throw new IllegalStateException("got " + (i + 1) + " != " + slice_end);
          i = slice_end;
        }
      } else {
        throw new IllegalStateException("unexpected slice type: " + slice_type);
      }
    }

    if (slice_end != seq_length) {
      throw new IllegalStateException("Got " + slice_length + " != " + seq_length);
    }

    if (slice_length == 1) {
      encoder.add(new Slice(SliceType.RLE, slice_start, slice_end, slice_value, slice_delta));
    } else if (slice_length == 2) {
      encoder.add(new Slice(SliceType.LIN, slice_start, slice_end, seq[seq_length-2], seq[seq_length-1] - seq[seq_length-2]));
    } else {
      encoder.add(new Slice(slice_type, slice_start, slice_end, slice_value, slice_delta));
    }
  }

  private static long[] minMax(final long[] items, final int startIndex, final int endIndex) {
    final int length = endIndex - startIndex;

    if (length == 1) {
      return new long[] { items[startIndex], items[startIndex] };
    }

    final long[] minMax = new long[2];
    if (items[startIndex] < items[startIndex + 1]) {
      minMax[0] = items[startIndex];
      minMax[1] = items[startIndex + 1];
    } else {
      minMax[1] = items[startIndex];
      minMax[0] = items[startIndex + 1];
    }

    for (int i = 2, n = length - 2; i <= n; i += 2) {
      final int index = startIndex + i;
      if (items[index] < items[index + 1]) {
        minMax[0] = Math.min(minMax[0], items[index]);
        minMax[1] = Math.max(minMax[1], items[index + 1]);
      } else {
        minMax[1] = Math.max(minMax[1], items[index]);
        minMax[0] = Math.min(minMax[0], items[index + 1]);
      }
    }

    if ((length & 1) == 1) {
      minMax[0] = Math.min(minMax[0], items[endIndex - 1]);
      minMax[1] = Math.max(minMax[1], items[endIndex - 1]);
    }
    return minMax;
  }

  public static void main(final String[] args) throws IOException {
    final long[] data = new long[] {
      2901, 3179, 2366, 1946, 1502, 1263, 1447, 4443, 24636, 77498, 86306, 88554,
      149587, 271646, 370618, 365967, 318277, 312603, 263178, 167610, 79513, 31869, 10784, 4173, 5436, 4347, 3555,
      3049, 2836, 2956, 3089, 5316, 22966, 76243, 90703, 82441, 132620, 255654, 390258, 411863, 377952, 369522,
      322223, 217569, 115296, 47000, 15507, 4710, 5200, 4749, 4213, 3532, 3204, 3144, 3179, 5595, 22356, 76408, 91627,
      84498, 137119, 266637, 396286, 401159, 365803, 357069, 307270, 200658, 104251, 40105, 13442, 3692, 5942, 3841,
      3213, 2546, 2256, 2136, 2249, 4678, 22170, 76760, 88785, 82696, 136647, 263518, 392066, 390100, 352973, 338458,
      294393, 187891, 94204, 36488, 11243, 3650, 4643, 4084, 3332, 2661, 2417, 2261, 2321, 3620, 19319, 72863, 85930,
      79475, 137474, 259119, 376761, 375140, 336661, 316905, 273445, 173392, 81740, 30024, 8348, 2641, 2517, 2018,
      1354, 1281, 887, 605, 597, 726, 2064, 6144, 10559, 13721, 21848, 36176, 42868, 39603, 33851, 22952, 13361, 5668,
      2179, 1258, 729, 586, 1242, 858, 775, 688, 505, 352, 277, 280, 320, 366, 454, 682, 920, 760, 755, 726, 563, 614,
      596, 744, 1231, 1295, 1550, 2866
    };

    final long[] data1 = new long[] {
      908, 885, 860, 843, 820, 809, 786, 759, 732, 708, 690, 668, 643, 617, 591, 566, 542,
      522, 499, 479, 458, 432, 397, 372, 345, 322, 297, 267, 240, 216, 197, 173, 145, 123, 95, 68, 46, 18,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1001, 978, 954, 927, 903, 880, 858, 832, 811, 784, 757, 732, 708,
      681, 662, 639, 613, 587, 562, 538, 515, 491, 468, 445, 421, 399, 379, 359, 334, 309, 290, 266, 247,
      228, 209, 185, 163, 140, 110, 86, 59, 38, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 947, 928, 904, 880, 868, 850,
      828, 809, 790, 766, 739, 716, 693, 669, 651, 626, 600, 579, 553, 526, 502, 477, 456, 432, 412, 384, 359,
      336, 311, 288, 262, 242, 221, 200, 181, 160, 138, 117, 94, 69, 44, 22, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      941, 926, 907, 886, 864, 843, 826, 806, 786, 768, 748, 729, 710, 687, 666, 643, 622, 599, 594, 594, 594,
      594, 594, 594, 594, 594, 594, 594, 594, 594, 593, 593, 582, 559, 536, 515, 490, 466, 446, 424, 403, 383,
      357, 335, 315, 288, 266, 244, 221, 200, 176, 152, 130, 108, 85, 63, 41, 20, 0, 0, 861, 844, 831, 808, 790,
      775, 755, 737, 715, 693, 670, 656, 638, 616, 597, 575, 562, 546, 532, 514, 499, 476, 455, 431, 407, 382,
      358, 331, 307, 286, 262, 243, 223, 197, 177, 158, 143, 120, 99, 75, 52, 32, 15,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 861, 848, 827, 809, 797, 779, 762, 744, 724, 702, 680, 663, 643, 625,
      610, 588, 724, 706, 685, 660, 641, 623, 606, 583, 558, 534, 510, 489, 463, 441, 415, 552, 536, 519, 503,
      483, 466, 441, 418, 394, 377, 356, 339, 315, 288, 267, 256, 405, 392, 371, 348, 324, 302, 282, 260, 236,
      216, 188, 165, 136, 113, 253, 231, 217, 197, 170, 143, 124, 100, 75, 48, 32, 10, 0, 0, 232, 208, 184, 162,
      141, 119, 100, 78, 56, 30, 8, 0, 0, 0, 0, 0, 203, 181, 157, 139, 116, 92, 70, 42, 17, 0, 0, 0, 0, 265,
      241, 215, 189, 166, 143, 122, 101, 78, 53, 31, 9, 0, 0, 0, 223, 204, 186, 162, 140, 115, 91, 74, 51, 27,
      8, 0, 0, 0, 0, 222, 197, 175, 156, 131, 106, 80, 55, 34, 12, 0, 0, 0, 0, 249, 230, 208, 183, 160, 137,
      114, 93, 71, 47, 24, 4, 0, 0, 0, 232, 208, 185, 162, 145, 120, 98, 79, 53, 29, 6, 0, 0, 0, 242, 225, 205,
      180, 157, 132, 112, 90, 74, 49, 30, 9, 0, 0, 0, 0, 205, 179, 159, 141, 119, 97, 72, 50, 29, 10,
      0, 0, 0, 0, 220, 200, 176, 152, 131, 116
    };

    final long[] data2 = new long[] {
      495, 1755, 41590, 22829, 14110, 0, 25838, 32407, 26307, 27537, 21751, 27934, 24221, 22195, 27653, 21384, 28267, 26309, 21781, 24310, 20934, 17948, 24339, 21750, 17214, 6561, 3147, 2350, 2386, 2590, 2323, 2618, 2403, 2367, 2523, 2689, 2103, 2173, 2683, 2174, 2383, 2346, 2397, 3354, 2478, 2775, 2219, 2482, 2194, 2619, 2465, 2400, 2108, 2052, 2370, 2077, 2247, 2248, 2596, 1954, 2141, 2032, 2150, 2226, 1874, 2374, 2226, 2171, 2335, 1888, 2355, 2091, 2159, 2131, 2228, 2193, 2074, 1895, 1749, 2064, 2307, 2122, 2063, 1783, 1906, 2133, 2011, 2173, 1885, 1797, 1878, 2026, 2089, 1798, 1687, 1718, 1825, 2126, 1963, 1994, 1717, 1762, 1794, 1697, 2282, 2381, 1507, 1632, 1817, 1874, 1734, 1789, 1981, 1647, 1548, 1730, 1905, 1706, 1597, 1808, 1740, 1746, 1632, 1625, 1423, 1565, 1516, 1557, 1668, 1475, 1567, 1514, 1636, 1532, 1863, 1384, 1378, 1801, 1446, 1383, 1487, 1309, 1126, 1479, 1217, 1358, 1341, 1577, 1413, 1420, 1180, 1402, 1420, 1168, 1457, 1262, 1152, 1244, 1228, 1298, 1283, 1529, 1513, 1337, 1370, 1173, 1456, 1115, 1068, 1316, 1677, 1369, 1215, 1316, 1400, 1133, 922, 1198, 1167, 1003, 1135, 1095, 1241, 970, 806, 1071, 918, 900, 1106, 763, 985, 826, 817, 691, 1013, 859, 800, 1101, 690, 995, 592, 982, 723, 732, 1085, 1035, 1138, 641, 861, 806, 812, 943, 792, 669, 541, 501, 787, 612, 837, 648, 735, 558, 586, 661, 473, 685, 597, 662, 676, 972, 742, 592, 1076, 722, 731, 796, 452, 537, 532, 443, 581, 534, 366, 610, 451, 626, 557, 387, 548, 579, 521, 379, 328, 419, 420, 672, 297, 452, 358, 718, 312, 322, 321, 289, 327, 401, 397, 715, 475, 348, 234, 207, 287, 226, 258, 186, 193, 231, 211, 236, 200, 276, 236, 242, 142, 304, 275, 395, 146, 140, 197, 284, 163, 226, 186, 100, 161, 216, 258, 73, 223, 134, 236, 127, 132, 262, 205, 170, 318, 93, 91, 59, 84, 62, 198, 100, 78, 484, 243, 142, 91, 89, 60, 51, 67, 98, 76, 66, 64, 80, 32, 142, 250, 50, 147, 56, 93, 14, 29, 24, 87, 14, 268, 59, 11, 75, 99, 37, 92, 87, 216, 4, 96, 9, 8, 11, 2, 208, 120, 1, 0, 2, 63, 0, 0, 6, 0, 0, 2, 0, 492, 8, 8, 30, 12, 0, 2, 3, 2, 172, 20, 3, 21, 53, 1, 0, 1, 7, 0, 49, 321, 3, 0, 3, 5, 64, 2, 53, 2, 0, 1, 11, 196, 4, 23, 12, 6, 5, 9, 9, 18, 4, 9, 7, 5, 12, 5, 158, 5, 6, 59, 7, 4, 6, 4, 6, 7, 7, 7, 7, 158, 7, 123, 147, 245, 104, 2, 5, 0, 29, 7, 3, 0, 3, 5, 222, 4, 3, 0, 2, 4, 4, 4, 4, 0, 2, 3, 6, 7, 5, 5, 172, 4, 3
    };

    final long[] data3 = new long[] { 3, 2, 1 };
    final SliceEncoder encoder = new SliceEncoder(data3);
    get_slices(encoder, encoder.seq);

    final long compressedSize = encoder.bitEncoder.buffer().size();
    final long fullSize = encoder.seq.length * 8;
    System.out.println(encoder.seq.length + " -> " + HumansUtil.humanSize(fullSize) + "/" + HumansUtil.humanSize(compressedSize) + " -> " + (100 * (1.0 - ((double)compressedSize/fullSize))));
  }

  /*
  * HEADER (2 byte)
  *   |-------- 2 bits: encoding type (RLE, LIN or MIN)
  *   |    |-------- 2 bits: size in bits of the length of the encoded sequence (0:4bit, 1:8bit, 2:12bit, 3:16bit)
  *   |    |    |-------- 6 bits: size in bits of the "base" value
  *   |    |    |    |-------- 6 bits: size in bits for a single delta value
  *   v    v    v    v
  * +----+----+--------+--------+ +--------+ +------+-------+-----
  * | TT | .. | ...... | ...... | | length | | base | delta | ...
  * +----+----+--------+--------+ +--------+ +------+-------+-----
  */
  public static void decode(final byte[] data) {
  }

  public static class SliceEncoder {
    private final BitEncoder bitEncoder;
    private final long[] seq;

    public SliceEncoder(final long[] seq) {
      this.bitEncoder = new BitEncoder(1 << 10);
      this.seq = seq;
    }

    public void add(final Slice slice) throws IOException {
      System.out.println(slice);
      switch (slice.type()) {
        case RLE:
          writeRle(slice.length(), slice.value());
          break;
        case LIN:
          writeLin(slice.length(), slice.value(), slice.delta());
          break;
        case MIN:
          writeMin(slice.length(), slice.value(), slice.delta(), slice.start(), slice.end());
          break;
      }
    }

    private void writeRle(final int length, final long value) throws IOException {
      /*   |-------- 2 bits: encoding type (RLE, LIN or MIN)
       *   |    |-------- 2 bits: size in bits of the length of the encoded sequence (0:4bit, 1:8bit, 2:12bit, 3:16bit)
       *   |    |    |-------- 6 bits: size in bits of the "base" value
       *   |    |    |       |-------- N bits: length of the encoded sequence
       *   |    |    |       |        |-------- N bits: value
       *   v    v    v       v        v
       * +----+----+--------+--------+--------+
       * | 00 | .. | ...... | ...... | ...... |
       * +----+----+--------+--------+--------+
       */
      final int valueBits = bit_size(value);
      bitEncoder.add(SliceType.RLE.ordinal(), 2);
      bitEncoder.add(length > 0xff ? 1 : 0, 2);
      bitEncoder.add(valueBits, 6);
      bitEncoder.add(length, length > 0xff ? 16 : 8);
      bitEncoder.add(value, valueBits);
    }

    private void writeLin(final int length, final long value, final long delta) throws IOException {
      /*   |-------- 2 bits: encoding type (RLE, LIN or MIN)
       *   |    |-------- 2 bits: size in bits of the length of the encoded sequence (0:4bit, 1:8bit, 2:12bit, 3:16bit)
       *   |    |    |-------- 6 bits: size in bits of the "base" value
       *   |    |    |       |-------- N bits: length of the encoded sequence
       *   |    |    |       |        |-------- N bits: value
       *   |    |    |       |        |        |-------- N bits: delta
       *   v    v    v       v        v        v
       * +----+----+--------+--------+--------+--------+
       * | 01 | .. | ...... | ...... | ...... | ...... |
       * +----+----+--------+--------+--------+--------+
       */
      final int valueBits = bit_size(value);
      final int deltaBits = signed_bit_size(delta);
      bitEncoder.add(SliceType.LIN.ordinal(), 2);
      bitEncoder.add(length > 0xff ? 1 : 0, 2);
      bitEncoder.add(valueBits, 6);
      bitEncoder.add(deltaBits, 6);
      bitEncoder.add(length, length > 0xff ? 16 : 8);
      bitEncoder.add(value, valueBits);
      bitEncoder.add(delta, deltaBits);
    }

    private void writeMin(final int length, final long value, final long delta, final int start, final int end) throws IOException {
      final int valueBits = bit_size(value);
      final int deltaBits = signed_bit_size(delta);

      bitEncoder.add(value, valueBits);
      for (int i = start; i < end; ++i) {
        if (deltaBits < signed_bit_size(seq[i] - value)) {
          throw new IllegalArgumentException("BANG " + deltaBits + " " + signed_bit_size(seq[i] - value) + " " + (seq[i] - value) + " " + delta);
        }
        bitEncoder.add(seq[i] - value, deltaBits);
      }
    }

    private int bit_size(final long value) {
      if (value < 0) {
        throw new IllegalArgumentException("expected value >= 0, got " + value);
      }
      return value == 0 ? 1 : (64 - Long.numberOfLeadingZeros(value));
    }

    private int signed_bit_size(final long value) {
      return bit_size(Math.abs(value)) + 1;
    }

    private int rle_bit_size(final int length, final long value) {
      if (length == 0) return 0;
      return 16 + bit_size(length - 1) + bit_size(value);
    }

    private int lin_bit_size(final int length, final long value, final long delta) {
      if (length == 0) return 0;
      return 16 + bit_size(length - 1) + bit_size(value) + signed_bit_size(delta);
    }

    private int min_bit_size(final int length, final long value, final long delta) {
      if (length == 0) return 0;
      final int value_bs = bit_size(value);
      final int delta_bs = bit_size(delta);
      return 16 + bit_size(length - 1) + value_bs + length * delta_bs;
    }

    private int slice_bit_size(final int length, final long value, final long delta) {
      if (length == 0) {
        return 0;
      } else if (length == 1) {
        return rle_bit_size(length, value);
      } else if (length == 2) {
        return lin_bit_size(length, value, delta);
      } else /* if (length > 2) */ {
        return min_bit_size(length, value, delta);
      }
    }
  }
}
