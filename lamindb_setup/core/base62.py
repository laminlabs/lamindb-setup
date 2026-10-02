# Original licence below

# Copyright (c) 2014, Sumin Byeon
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# The views and conclusions contained in the software and documentation are those
# of the authors and should not be interpreted as representing official policies,
# either expressed or implied, of the FreeBSD Project.

BASE = 62
CHARSET_DEFAULT = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
CHARSET_INVERTED = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def increment_base62(s: str) -> str:
    # we don't need to throw an error for zzzz because uids are enforced to be unique
    # on the db level and have an enforced maximum length
    value = sum(CHARSET_DEFAULT.index(c) * (62**i) for i, c in enumerate(reversed(s)))
    value += 1
    result = ""
    while value:
        value, remainder = divmod(value, 62)
        result = CHARSET_DEFAULT[remainder] + result
    return result.zfill(len(s))


def encode(n, charset=CHARSET_DEFAULT):
    """Encodes a given integer ``n``."""
    chs = []
    while n > 0:
        n, r = divmod(n, BASE)
        chs.insert(0, charset[r])

    if not chs:
        return "0"

    return "".join(chs)


def encodebytes(barray, charset=CHARSET_DEFAULT):
    """Encodes a bytestring into a base62 string.

    :param barray: A byte array
    :type barray: bytes
    :rtype: str
    """
    _check_type(barray, bytes)

    # Count the number of leading zeros.
    leading_zeros_count = 0
    for i in range(len(barray)):
        if barray[i] != 0:
            break
        leading_zeros_count += 1

    # Encode the leading zeros as "0" followed by a character indicating the count.
    # This pattern may occur several times if there are many leading zeros.
    n, r = divmod(leading_zeros_count, len(charset) - 1)
    zero_padding = f"0{charset[-1]}" * n
    if r:
        zero_padding += f"0{charset[r]}"

    # Special case: the input is empty, or is entirely null bytes.
    if leading_zeros_count == len(barray):
        return zero_padding

    value = encode(int.from_bytes(barray, "big"), charset=charset)
    return zero_padding + value


def decode(encoded, charset=CHARSET_DEFAULT):
    """Decodes a base62 encoded value ``encoded``.

    :type encoded: str
    :rtype: int
    """
    _check_type(encoded, str)

    l, i, v = len(encoded), 0, 0
    for x in encoded:
        v += _value(x, charset=charset) * (BASE ** (l - (i + 1)))
        i += 1

    return v


def decodebytes(encoded, charset=CHARSET_DEFAULT):
    """Decodes a string of base62 data into a bytes object.

    :param encoded: A string to be decoded in base62
    :type encoded: str
    :rtype: bytes
    """
    leading_null_bytes = b""
    while encoded.startswith("0") and len(encoded) >= 2:
        leading_null_bytes += b"\x00" * _value(encoded[1], charset)
        encoded = encoded[2:]

    decoded = decode(encoded, charset=charset)
    buf = bytearray()
    while decoded > 0:
        buf.append(decoded & 0xFF)
        decoded //= 256
    buf.reverse()

    return leading_null_bytes + bytes(buf)


def _value(ch, charset):
    """Decodes an individual digit of a base62 encoded string."""
    try:
        return charset.index(ch)
    except ValueError:
        raise ValueError(f"base62: Invalid character ({ch})") from None


def _check_type(value, expected_type):
    """Checks if the input is in an appropriate type."""
    if not isinstance(value, expected_type):
        msg = f"Expected {expected_type} object, not {value.__class__.__name__}"
        raise TypeError(msg)
