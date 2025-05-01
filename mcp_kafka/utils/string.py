def parse_value(key: str, text: str, quotes=('"', "'")) -> str:
    key_pos = text.find(key)
    if key_pos == -1:
        return ''

    # Find the '=' after the key
    eq_pos = text.find('=', key_pos + len(key))
    if eq_pos == -1:
        return ''

    # Skip whitespace after '='
    i = eq_pos + 1
    while i < len(text) and text[i].isspace():
        i += 1

    # Check for quote character
    if i >= len(text) or text[i] not in quotes:
        return ''

    quote_char = text[i]
    i += 1
    value = []

    while i < len(text):
        ch = text[i]
        if ch == quote_char:
            if value and value[-1] == '\\':
                value[-1] = quote_char
            else:
                break
        else:
            value.append(ch)
        i += 1

    return ''.join(value)
