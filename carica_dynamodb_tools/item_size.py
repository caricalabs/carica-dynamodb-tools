"""
Calculates DynamoDB stored item sizes.

Calculations are implemented according to these rules:

  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html

Those rules are not very detailed, so information from the following blog
post and project were incorporated into this implementation:

    https://zaccharles.medium.com/calculating-a-dynamodb-items-size-and-consumed-capacity-d1728942eb7c

    https://github.com/zaccharles/dynamodb-calculator

Since the code in this file is adapted from the `dynamodb-calculator` project,
it is distributed under the MIT license:

MIT License

Copyright (c) 2018 Zac Charles
Copyright (c) 2022 Arpio, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import base64
import math
from decimal import Decimal, localcontext

DYNAMODB_NUMBER_DIGITS = 38
EMPTY_DOC_BASE_SIZE = 3
NESTED_TYPE_BASE_SIZE = 1


def format_decimal(n_str: str) -> str:
    """
    Formats a string that represents a decimal number as a full-precision
    decimal string.  Leading and trailing zeroes are omitted from the output.
    """
    sign, digits, exp = Decimal(n_str).as_tuple()

    # Stringify all the digits and put them in a list we can modify
    digits_list = [str(d) for d in digits]
    if exp < 0:
        # Prepend zeros until we've constructed the full fractional
        # part, then prepend the decimal point
        while abs(exp) > len(digits_list):
            digits_list.insert(0, '0')
        digits_list.insert(exp, '.')
    elif exp > 0:
        # Append one zero for each order of magnitude
        digits_list.extend(('0',) * exp)

    # Trim leading zeros always.
    while digits_list and digits_list[0] == '0':
        digits_list.pop(0)

    # Trim trailing zeros in the fractional part
    if '.' in digits_list:
        while digits_list and digits_list[-1] == '0':
            digits_list.pop(-1)

    sign = '-' if sign else ''
    value = ''.join(digits_list)

    if not value or value == '.':
        value = '0'

    return sign + value


def measure_number(d_str: str) -> int:
    # Return the count of bytes DynamoDB will charge us for to store the specified decimal string.
    if '.' in d_str:
        p0, p1 = d_str.split('.')
        if p0 == '0':
            p0 = ''
            p1 = p1.lstrip('0')
        if len(p0) % 2 != 0:
            p0 = 'Z' + p0
        if len(p1) % 2 != 0:
            p1 = p1 + 'Z'
        return measure_number(p0 + p1)
    d_str = d_str.strip('0')
    return math.ceil(len(d_str) / 2)


def binary_size(b: str) -> int:
    return len(base64.b64decode(b))


def string_size(s: str) -> int:
    return len(bytes(s, 'utf-8'))


def number_size(n: str) -> int:
    # Measure the size of a decimal number string as DynamoDB does.
    # This is the trickiest part of sizing DynamoDB items, and the logic
    # was ported from code in:
    #
    #   https://github.com/zaccharles/dynamodb-calculator/blob/48e0dd984febc93b56d9d65d79e0cadf505d2da5/index.html
    #
    n_str = format_decimal(n)
    size = measure_number(n_str.replace('-', '')) + 1
    if n_str.startswith('-'):
        size += 1
    if size > 21:
        size = 21
    return size


def attr_size(attr: dict) -> int:
    if 'S' in attr:
        return string_size(attr['S'])
    if 'N' in attr:
        return number_size(attr['N'])
    if 'B' in attr:
        return binary_size(attr['B'])
    if 'BOOL' in attr:
        return 1
    if 'NULL' in attr:
        return 1
    if 'M' in attr:
        size = EMPTY_DOC_BASE_SIZE
        for m_name, m_value in attr['M'].items():
            size += string_size(m_name) + attr_size(m_value) + NESTED_TYPE_BASE_SIZE
        return size
    if 'L' in attr:
        size = EMPTY_DOC_BASE_SIZE
        for e in attr['L']:
            size += attr_size(e) + NESTED_TYPE_BASE_SIZE
        return size
    if 'SS' in attr:
        return sum(string_size(e) for e in attr['SS'])
    if 'NS' in attr:
        return sum(number_size(e) for e in attr['NS'])
    if 'BS' in attr:
        return sum(binary_size(e) for e in attr['BS'])
    raise ValueError('Unknown attribute type: ' + repr(attr))


def item_size(item: dict) -> int:
    with localcontext() as ctx:
        ctx.prec = DYNAMODB_NUMBER_DIGITS
        size = 0
        for attr_name, attr in item.items():
            size += string_size(attr_name)
            size += attr_size(attr)
        return size
