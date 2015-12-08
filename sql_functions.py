import re

def regexp_flags(regexp_modifier=None):
    flags = 0
    set_unicode = True
    for c in regexp_modifier:
        if c == 'b':
            # Treat strings as binary octets rather than UTF-8 characters.
            set_unicode = False
        elif c == 'c':
            # Forces the match to be case sensitive.
            # This is the default, no flag setting needed
            pass
        elif c == 'i':
            # Forces the match to be case insensitive.
            flags |= re.IGNORECASE
        elif c == 'm':
            # Treats the string being matched as multiple lines. With this
            # modifier, the start of line (^) and end of line ($) regular
            # expression operators match line breaks (\n) within the string.
            # Ordinarily, these operators only match the start and end of the
            # string.
            flags |= re.MULTILINE
        elif c == 'n':
            # Allows the single character regular expression operator (.) to
            # match a newline (\n). Normally, the . operator will match any
            # character except a newline.
            flags |= re.DOTALL
        elif c == 'x':
            # Allows you to document your regular expressions. It causes all
            # unescaped space characters and comments in the regular expression
            # to be ignored. Comments start with a hash character (#) and end
            # with a newline. All spaces in the regular expression that you
            # want to be matched in strings must be escaped with a backslash
            # (\) character.
            flags |= re.VERBOSE
    if set_unicode:
        flags |= re.UNICODE
    return flags

def sql_regexp_substr(txt, pattern, position=1, occurrence=1, regexp_modifier='c', captured_subexp=0):
    occurrence = 1 if occurrence < 1 else occurrence
    flags = regexp_flags(regexp_modifier)
    rx = re.compile(pattern, flags)
    matches = rx.finditer(txt, position-1)
    cnt = 0
    for match in matches:
        cnt += 1
        if occurrence == cnt:
            try:
                return match.group(captured_subexp)
            except IndexError:
                return ''
    return ''

def sql_regexp_count(txt, pattern, position=1, regexp_modifier='c'):
    flags = regexp_flags(regexp_modifier)
    rx = re.compile(pattern, flags)
    matches = rx.findall(txt, position-1)
    return len(matches)

def sql_regexp_like(txt, pattern, regexp_modifier='c'):
    flags = regexp_flags(regexp_modifier)
    rx = re.compile(pattern, flags)
    matches = rx.finditer(txt)
    for match in matches:
        return True
    return False

def sql_regexp_instr( txt, pattern, position=1, occurrence=1, return_position=0, regexp_modifier='c', captured_subexp=0):
    occurrence = 1 if occurrence < 1 else occurrence
    flags = regexp_flags(regexp_modifier)
    rx = re.compile(pattern, flags)
    matches = rx.finditer(txt, position-1)
    cnt = 0
    for match in matches:
        cnt += 1
        if occurrence == cnt:
            try:
                if return_position > 0:
                    return match.end(captured_subexp) + return_position
                else:
                    return match.start(captured_subexp) + 1
            except IndexError:
                return 0
    return 0

def sql_regexp_replace( txt, target, replacement=None, position=1, occurrence=1, regexp_modifier='c'):
    occurrence = 1 if occurrence < 1 else occurrence
    flags = regexp_flags(regexp_modifier)
    rx = re.compile(pattern, flags)
    retval = rx.sub(replacement, txt, position-1)
    cnt = 0
    for match in matches:
        cnt += 1
        if occurrence == cnt:
            try:
                return match.group(captured_subexp)
            except IndexError:
                return ''
    return ''

def sql_regexp_replace( txt, pattern, replacement='', position=1, occurrence=0, regexp_modifier='c'):
    class ReplWrapper(object):
        def __init__(self, replacement, occurrence):
            self.count = 0
            self.replacement = replacement
            self.occurrence = occurrence
        def repl(self, match):
            self.count += 1
            if self.occurrence == 0 or self.occurrence == self.count:
                return match.expand(self.replacement)
            else: 
                try:
                    return match.group(0)
                except IndexError:
                    return match.group(0)
    occurrence = 0 if occurrence < 0 else occurrence
    flags = regexp_flags(regexp_modifier)
    rx = re.compile(pattern, flags)
    replw = ReplWrapper(replacement, occurrence)
    return txt[0:position-1] + rx.sub(replw.repl, txt[position-1:])

def sql_left(txt, i):
    return txt[:i]

def sql_right(txt, i):
    return txt[i-1:]

def sql_split_part(txt, delim, field):
    try:
        return txt.split(delim)[field-1]
    except IndexError:
        return ''

def sql_substr(txt, pos, extent=None):
    if extent is not None:
        return txt[pos-1:pos-1+extent]
    elif extent == 0:
        return ''
    else:
        return txt[pos-1:]

def sql_instr(txt, subtxt, pos=1, occurrence=1):
    cnt = 0
    while cnt < occurrence:
        idx = txt.find(subtxt, pos - 1)
        if idx == -1:
            return 0
        cnt += 1
        pos = idx + 2 # pos is used starting at 1, plus need the next char
    return idx + 1


def sql_concat(txt1, txt2):
    return '' if txt1 is None else None + '' if txt2 is None else None

def sql_nvl(expr1, expr2):
    if expr1 is None or expr1 == '':
        return expr2
    return expr1

def sql_nvl2(expr1, expr2, expr3):
    if expr1 is None or expr1 == '':
        return expr3
    return expr2

def sql_null_if_zero(expr):
    if expr is None or expr == 0:
        return ''

def sql_zero_if_null(expr):
    if expr is None or expr == '':
        return 0

def sql_coalesce(*argv):
    for arg in argv:
        if arg is not None and arg != '':
            return arg
    return ''

def sql_decode(expr, *argv):
    for test_expr, retval in zip(argv[::2], argv[1::2]):
        if expr == test_expr:
            return retval
    if len(argv) % 2 == 1:
        return argv[-1]
    return ''
