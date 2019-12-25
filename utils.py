

def to_camel_case(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


def get_cols(cols, from_list: list):
    if cols is None or from_list is None:
        return None
    if type(cols) == list:
        out = []
        for col in cols:
            if type(col) == int and len(from_list) >= col > 0:
                out.append(from_list[col - 1])
            elif type(col) == str and col in from_list:
                out.append(col)
        return out
    else:
        if type(cols) == int and len(from_list) >= cols > 0:
            return from_list[cols - 1]
        elif type(cols) == str and cols in from_list:
            return cols
        else:
            return None
